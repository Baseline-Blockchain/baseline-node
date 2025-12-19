"""
Blockchain management for Baseline.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass

import os

from ..config import ConfigError, NodeConfig
from ..storage import BlockStore, HeaderData, StateDB, UTXORecord
from ..time_sync import synchronized_time_int
from . import crypto, difficulty, script
from .block import MAX_BLOCK_WEIGHT, Block, BlockHeader, merkle_root_hash
from .fork import ForkManager
from .tx import COIN, Transaction, TxInput, TxOutput
from .upgrade import UpgradeManager


class ChainError(Exception):
    pass


GENESIS_PRIVKEY = 1
GENESIS_PUBKEY = crypto.generate_pubkey(GENESIS_PRIVKEY)
GENESIS_MESSAGE = b"Baseline genesis"
MAX_FUTURE_BLOCK_TIME = 2 * 60 * 60

CONSENSUS_DEFAULTS = {
    "coinbase_maturity": 5,
    "block_interval_target": 20,
    "retarget_interval": 20,
    "initial_bits": 0x207FFFFF,
    "subsidy_halving_interval": 150_000,
}


@dataclass
class ValidationResult:
    spent: list[UTXORecord]
    created: list[UTXORecord]
    fees: int


class UTXOView:
    def __init__(self, state_db: StateDB):
        self.state_db = state_db
        self.overlay: dict[tuple[str, int], UTXORecord | None] = {}

    def get(self, txid: str, vout: int) -> UTXORecord | None:
        key = (txid, vout)
        if key in self.overlay:
            return self.overlay[key]
        return self.state_db.get_utxo(txid, vout)

    def spend(self, txid: str, vout: int) -> UTXORecord:
        utxo = self.get(txid, vout)
        if utxo is None:
            raise ChainError(f"Missing UTXO {txid}:{vout}")
        self.overlay[(txid, vout)] = None
        return utxo

    def add(self, record: UTXORecord) -> None:
        self.overlay[(record.txid, record.vout)] = record

    def set(self, txid: str, vout: int, record: UTXORecord | None) -> None:
        self.overlay[(txid, vout)] = record

    def diff(self) -> tuple[list[tuple[str, int]], list[UTXORecord]]:
        spent: list[tuple[str, int]] = []
        created: list[UTXORecord] = []
        for (txid, vout), value in self.overlay.items():
            if value is None:
                spent.append((txid, vout))
            else:
                created.append(value)
        return spent, created

    def clear(self) -> None:
        self.overlay.clear()


class Chain:
    def __init__(self, config: NodeConfig, state_db: StateDB, block_store: BlockStore):
        self.config = config
        self.state_db = state_db
        self.block_store = block_store
        self.log = logging.getLogger("baseline.chain")
        self.max_target = difficulty.compact_to_target(self.config.mining.initial_bits)
        self.genesis_block = self._build_genesis_block()
        self.genesis_hash = self.genesis_block.block_hash()

        # Initialize fork and upgrade managers
        self.fork_manager = ForkManager(self)
        self.upgrade_manager = UpgradeManager(state_db)

        self._enforce_consensus_parameters()
        self._ensure_genesis()
        self.log.info("Current block height %s", self.state_db.get_best_tip()[1] if self.state_db.get_best_tip() else 0)

    def _build_genesis_block(self) -> Block:
        coinbase_script = self._encode_coinbase_script(height=0, extra=GENESIS_MESSAGE)
        coinbase = Transaction(
            version=1,
            inputs=[
                TxInput(
                    prev_txid="00" * 32,
                    prev_vout=0xFFFFFFFF,
                    script_sig=coinbase_script,
                    sequence=0xFFFFFFFF,
                )
            ],
            outputs=[
                TxOutput(
                    value=50 * COIN,
                    script_pubkey=b"\x76\xa9\x14" + crypto.hash160(GENESIS_PUBKEY) + b"\x88\xac",
                )
            ],
            lock_time=0,
        )
        merkle = merkle_root_hash([coinbase])
        header = BlockHeader(
            version=1,
            prev_hash="00" * 32,
            merkle_root=merkle,
            timestamp=1733966400,
            bits=self.config.mining.initial_bits,
            nonce=0,
        )
        block = Block(header=header, transactions=[coinbase])
        nonce = 0
        while not difficulty.check_proof_of_work(block.block_hash(), block.header.bits):
            nonce += 1
            block.header.nonce = nonce
            if nonce > 0xFFFFFFFF:
                raise ChainError("Failed to solve genesis block")
        self.log.info("Genesis block solved nonce=%s hash=%s", block.header.nonce, block.block_hash())
        return block

    def _encode_coinbase_script(self, height: int, extra: bytes) -> bytes:
        height_bytes = height.to_bytes((height.bit_length() + 7) // 8 or 1, "little")
        script = len(height_bytes).to_bytes(1, "little") + height_bytes
        if extra:
            limited = extra[:80]
            script += len(limited).to_bytes(1, "little") + limited
        return script

    def _ensure_genesis(self) -> None:
        best = self.state_db.get_best_tip()
        genesis_hash = self.genesis_hash
        if best is not None:
            return
        self.log.info("Initializing chain with genesis block %s", genesis_hash)
        raw = self.genesis_block.serialize()
        if not self.block_store.has_block(genesis_hash):
            self.block_store.append_block(bytes.fromhex(genesis_hash), raw)
        created = []
        for idx, txout in enumerate(self.genesis_block.transactions[0].outputs):
            created.append(
                UTXORecord(
                    txid=self.genesis_block.transactions[0].txid(),
                    vout=idx,
                    amount=txout.value,
                    script_pubkey=txout.script_pubkey,
                    height=0,
                    coinbase=True,
                )
            )
        self.state_db.apply_utxo_changes([], created)
        header = HeaderData(
            hash=genesis_hash,
            prev_hash=None,
            height=0,
            bits=self.genesis_block.header.bits,
            nonce=self.genesis_block.header.nonce,
            timestamp=self.genesis_block.header.timestamp,
            merkle_root=self.genesis_block.header.merkle_root,
            chainwork=str(difficulty.block_work(self.genesis_block.header.bits)),
            status=0,
        )
        self.state_db.store_header(header)
        self.state_db.store_undo_data(genesis_hash, [])
        self.state_db.set_best_tip(genesis_hash, 0)
        self.state_db.set_meta("best_work", header.chainwork)
        self.state_db.upsert_chain_tip(genesis_hash, 0, header.chainwork)

    def _enforce_consensus_parameters(self) -> None:
        mining = self.config.mining
        if getattr(mining, "allow_consensus_overrides", False):
            self.log.warning("Consensus overrides enabled; this node may diverge from main network parameters")
            return
        mismatches: list[str] = []
        for key, expected in CONSENSUS_DEFAULTS.items():
            actual = getattr(mining, key, None)
            if actual != expected:
                mismatches.append(f"{key}={actual} (expected {expected})")
        if mismatches:
            raise ConfigError(
                "Consensus parameter mismatch detected. "
                "Set mining.allow_consensus_overrides=true ONLY for isolated test nets. "
                f"Mismatches: {', '.join(mismatches)}"
            )

    def process_block_bytes(self, raw_block: bytes) -> dict[str, str | int | bool]:
        block = Block.parse(raw_block)
        return self.add_block(block, raw_block)

    def add_block(self, block: Block, raw_block: bytes | None = None) -> dict[str, str | int | bool]:
        block_hash = block.block_hash()
        existing_header = self.state_db.get_header(block_hash)
        if existing_header is not None and self.block_store.has_block(block_hash):
            return {"status": "duplicate", "hash": block_hash}
        if raw_block is None:
            raw_block = block.serialize()
        prev_hash = block.header.prev_hash
        if prev_hash == "00" * 32:
            if block_hash != self.genesis_hash:
                raise ChainError("Unexpected alternate genesis block")
            return {"status": "genesis"}
        parent_header = self.state_db.get_header(prev_hash)
        if parent_header is None:
            raise ChainError("Unknown parent block")
        height = parent_header.height + 1
        view = self._build_view_for_parent(prev_hash)
        validation = self._validate_block(block, height, parent_header, view)

        # Process upgrade signaling
        self.upgrade_manager.process_new_block(block.header, height)

        chainwork_int = int(parent_header.chainwork) + difficulty.block_work(block.header.bits)
        header = HeaderData(
            hash=block_hash,
            prev_hash=prev_hash,
            height=height,
            bits=block.header.bits,
            nonce=block.header.nonce,
            timestamp=block.header.timestamp,
            merkle_root=block.header.merkle_root,
            chainwork=str(chainwork_int),
            status=1,
        )
        self.block_store.append_block(bytes.fromhex(block_hash), raw_block)
        self.state_db.store_header(header)

        # Handle fork detection and reorganization
        block_accepted, reorganization_occurred = self.fork_manager.handle_new_block(block)

        if not block_accepted:
            # Block was rejected by fork manager
            return {"status": "rejected", "hash": block_hash, "height": height}

        if reorganization_occurred:
            # Fork manager handled the reorganization
            return {"status": "reorganized", "hash": block_hash, "height": height}

        # Continue with normal processing
        self.state_db.upsert_chain_tip(block_hash, height, header.chainwork)
        self.state_db.remove_chain_tip(prev_hash)
        best = self.state_db.get_best_tip()
        best_work = int(self.state_db.get_meta("best_work") or "0")
        extends_best = best and prev_hash == best[0]
        if extends_best:
            self._connect_main(block, block_hash, height, chainwork_int, validation)
            return {"status": "connected", "hash": block_hash, "height": height}
        if chainwork_int > best_work:
            self.log.info("New chainwork %s surpasses best %s; reorganizing", chainwork_int, best_work)
            self._reorganize_to(block_hash)
            return {"status": "reorganized", "hash": block_hash, "height": height}
        return {"status": "side", "hash": block_hash, "height": height}

    def _connect_main(self, block: Block, block_hash: str, height: int, chainwork: int, validation: ValidationResult) -> None:
        spent, created = self._collect_utxo_changes(validation)
        self.state_db.apply_utxo_changes(spent, created)
        self.state_db.store_undo_data(block_hash, validation.spent)
        self.state_db.set_header_status(block_hash, 0)
        self.state_db.set_best_tip(block_hash, height)
        self.state_db.set_meta("best_work", str(chainwork))
        self.state_db.upsert_chain_tip(block_hash, height, str(chainwork))
        if block.header.prev_hash:
            self.state_db.remove_chain_tip(block.header.prev_hash)
        self.log.info("Connected block %s height=%s txs=%s", block_hash, height, len(block.transactions))

    def _collect_utxo_changes(self, validation: ValidationResult) -> tuple[list[tuple[str, int]], list[UTXORecord]]:
        spent_keys = [(rec.txid, rec.vout) for rec in validation.spent]
        created = validation.created
        return spent_keys, created

    def _validate_block(self, block: Block, height: int, parent_header: HeaderData, view: UTXOView) -> ValidationResult:
        if block.weight() > MAX_BLOCK_WEIGHT:
            raise ChainError("Block exceeds weight limit")
        expected_bits = self._expected_bits(height, parent_header)
        if block.header.bits != expected_bits:
            raise ChainError("Unexpected difficulty bits")
        if not difficulty.check_proof_of_work(block.block_hash(), block.header.bits):
            raise ChainError("Invalid proof of work")
        median_time = self._median_time_past(parent_header.hash)
        if block.header.timestamp <= median_time:
            raise ChainError("Block timestamp too early")
        if block.header.timestamp > synchronized_time_int() + MAX_FUTURE_BLOCK_TIME:
            raise ChainError("Block timestamp too far in future")
        return self._apply_block_transactions(block, height, view, enforce_scripts=True)

    def _apply_block_transactions(self, block: Block, height: int, view: UTXOView, enforce_scripts: bool) -> ValidationResult:
        if not block.transactions:
            raise ChainError("Block has no transactions")
        spent_records: list[UTXORecord] = []
        created_records: list[UTXORecord] = []
        total_fees = 0
        seen_inputs: set[tuple[str, int]] = set()
        coinbase_tx = block.transactions[0]
        if not coinbase_tx.is_coinbase():
            raise ChainError("First transaction must be coinbase")
        self._validate_coinbase_script(coinbase_tx.inputs[0].script_sig, height)
        coinbase_value = sum(out.value for out in coinbase_tx.outputs)
        for out_index, txout in enumerate(coinbase_tx.outputs):
            record = UTXORecord(
                txid=coinbase_tx.txid(),
                vout=out_index,
                amount=txout.value,
                script_pubkey=txout.script_pubkey,
                height=height,
                coinbase=True,
            )
            view.add(record)
            created_records.append(record)
        subsidy = self._block_subsidy(height)
        for _tx_index, tx in enumerate(block.transactions[1:], start=1):
            if tx.is_coinbase():
                raise ChainError("Multiple coinbase transactions")
            input_value = 0
            for vin_index, txin in enumerate(tx.inputs):
                key = (txin.prev_txid, txin.prev_vout)
                if key in seen_inputs:
                    raise ChainError("Double spend inside block")
                seen_inputs.add(key)
                utxo = view.get(txin.prev_txid, txin.prev_vout)
                if utxo is None:
                    raise ChainError("Missing referenced output")
                if utxo.coinbase and height - utxo.height < self.config.mining.coinbase_maturity:
                    raise ChainError("Coinbase maturity violation")
                if enforce_scripts:
                    ctx = script.ExecutionContext(transaction=tx, input_index=vin_index)
                    if not script.run_script(txin.script_sig, utxo.script_pubkey, ctx):
                        raise ChainError("Script validation failed")
                view.spend(txin.prev_txid, txin.prev_vout)
                spent_records.append(utxo)
                input_value += utxo.amount
            output_value = sum(out.value for out in tx.outputs)
            if output_value > input_value:
                raise ChainError("Transaction creates money")
            total_fees += input_value - output_value
            for out_index, txout in enumerate(tx.outputs):
                record = UTXORecord(
                    txid=tx.txid(),
                    vout=out_index,
                    amount=txout.value,
                    script_pubkey=txout.script_pubkey,
                    height=height,
                    coinbase=False,
                )
                view.add(record)
                created_records.append(record)
        if coinbase_value > subsidy + total_fees:
            raise ChainError("Coinbase pays too much")
        return ValidationResult(spent=spent_records, created=created_records, fees=total_fees)

    def _validate_coinbase_script(self, script_sig: bytes, height: int) -> None:
        if not (2 <= len(script_sig) <= 100):
            raise ChainError("Coinbase script size invalid")
        idx = 0
        length = script_sig[idx]
        idx += 1
        if idx + length > len(script_sig):
            raise ChainError("Malformed coinbase height push")
        height_value = int.from_bytes(script_sig[idx : idx + length], "little")
        if height_value != height:
            raise ChainError("Coinbase height mismatch")

    def _block_subsidy(self, height: int) -> int:
        halvings = height // self.config.mining.subsidy_halving_interval
        subsidy = 50 * COIN
        subsidy >>= min(halvings, 32)
        return subsidy

    def _expected_bits(self, height: int, parent_header: HeaderData) -> int:
        interval = self.config.mining.retarget_interval
        if height % interval != 0 or height < interval:
            return parent_header.bits
        ancestor_hash = self._ancestor_hash(parent_header.hash, interval - 1)
        if ancestor_hash is None:
            return parent_header.bits
        ancestor = self.state_db.get_header(ancestor_hash)
        if ancestor is None:
            raise ChainError("Missing ancestor for retarget")
        actual_span = parent_header.timestamp - ancestor.timestamp
        target_span = self.config.mining.block_interval_target * interval
        return difficulty.calculate_new_bits(parent_header.bits, actual_span, target_span, self.max_target)

    def _ancestor_hash(self, block_hash: str, steps: int) -> str | None:
        current = block_hash
        for _ in range(steps):
            header = self.state_db.get_header(current)
            if header is None or header.prev_hash is None:
                return None
            current = header.prev_hash
        return current

    def _median_time_past(self, block_hash: str, window: int = 11) -> int:
        times = []
        current = block_hash
        for _ in range(window):
            header = self.state_db.get_header(current)
            if header is None:
                break
            times.append(header.timestamp)
            if header.prev_hash is None:
                break
            current = header.prev_hash
        times.sort()
        if not times:
            return 0
        return times[len(times) // 2]

    def _build_view_for_parent(self, parent_hash: str) -> UTXOView:
        view = UTXOView(self.state_db)
        branch_path, fork_hash = self._collect_branch_path(parent_hash)
        if fork_hash:
            self._rewind_view_to_main(view, fork_hash)
        for block_hash in branch_path:
            block = self._load_block(block_hash)
            header = self.state_db.get_header(block_hash)
            if header is None:
                raise ChainError("Missing header for branch block")
            self._apply_block_transactions(block, header.height, view, enforce_scripts=False)
        return view

    def _collect_branch_path(self, block_hash: str) -> tuple[list[str], str | None]:
        path: list[str] = []
        current = block_hash
        while True:
            header = self.state_db.get_header(current)
            if header is None:
                raise ChainError("Unknown header during branch traversal")
            status = header.status
            if status == 0:
                return list(reversed(path)), current
            path.append(current)
            if header.prev_hash is None:
                return list(reversed(path)), None
            current = header.prev_hash

    def _rewind_view_to_main(self, view: UTXOView, target_hash: str) -> None:
        best = self.state_db.get_best_tip()
        if not best:
            return
        current = best[0]
        if current == target_hash:
            return
        while current != target_hash:
            header = self.state_db.get_header(current)
            if header is None:
                raise ChainError("Header missing while rewinding view")
            block = self._load_block(current)
            undo = self.state_db.load_undo_data(current)
            self._disconnect_block_from_view(view, block, undo)
            if header.prev_hash is None:
                break
            current = header.prev_hash

    def _disconnect_block_from_view(self, view: UTXOView, block: Block, undo: Sequence[UTXORecord]) -> None:
        # restore spent outputs
        for record in undo:
            view.set(record.txid, record.vout, record)
        # remove block-created outputs
        for tx in block.transactions:
            txid = tx.txid()
            for index in range(len(tx.outputs)):
                view.set(txid, index, None)

    def _load_block(self, block_hash: str) -> Block:
        raw = self.block_store.get_block(block_hash)
        return Block.parse(raw)

    def _reorganize_to(self, new_tip_hash: str) -> None:
        best = self.state_db.get_best_tip()
        if best is None:
            raise ChainError("Best tip unknown for reorg")
        fork_hash, old_branch, new_branch = self._find_fork(best[0], new_tip_hash)
        self.log.info(
            "Reorg: old_tip=%s new_tip=%s fork=%s detach=%s attach=%s",
            best[0],
            new_tip_hash,
            fork_hash,
            len(old_branch),
            len(new_branch),
        )
        for block_hash in old_branch:
            self._disconnect_main_block(block_hash)
        fork_header = self.state_db.get_header(fork_hash)
        if fork_header is None:
            raise ChainError("Fork header missing")
        self.state_db.set_best_tip(fork_hash, fork_header.height)
        self.state_db.set_meta("best_work", fork_header.chainwork)
        for block_hash in new_branch:
            block = self._load_block(block_hash)
            parent_header = self.state_db.get_header(block.header.prev_hash)
            if parent_header is None:
                raise ChainError("Missing parent during attach")
            height = parent_header.height + 1
            view = self._build_view_for_parent(block.header.prev_hash)
            validation = self._validate_block(block, height, parent_header, view)
            chainwork = int(parent_header.chainwork) + difficulty.block_work(block.header.bits)
            self._connect_main(block, block_hash, height, chainwork, validation)

    def _disconnect_main_block(self, block_hash: str) -> None:
        header = self.state_db.get_header(block_hash)
        if header is None:
            raise ChainError("Cannot disconnect unknown block")
        block = self._load_block(block_hash)
        undo = self.state_db.load_undo_data(block_hash)
        for tx in reversed(block.transactions):
            txid = tx.txid()
            for index in range(len(tx.outputs)):
                self.state_db.remove_utxo(txid, index)
        for record in undo:
            self.state_db.add_utxo(record)
        self.state_db.delete_undo_data(block_hash)
        self.state_db.set_header_status(block_hash, 1)
        self.state_db.upsert_chain_tip(block_hash, header.height, header.chainwork)
        self.log.info("Disconnected block %s height=%s", block_hash, header.height)

    def _find_fork(self, old_tip: str, new_tip: str) -> tuple[str, list[str], list[str]]:
        old_path = set()
        current = old_tip
        while current:
            old_path.add(current)
            header = self.state_db.get_header(current)
            if header is None or header.prev_hash is None:
                break
            current = header.prev_hash
        new_branch: list[str] = []
        current = new_tip
        fork = self.genesis_hash
        while True:
            if current in old_path:
                fork = current
                break
            new_branch.append(current)
            header = self.state_db.get_header(current)
            if header is None or header.prev_hash is None:
                break
            current = header.prev_hash
        old_branch: list[str] = []
        current = old_tip
        while current != fork:
            old_branch.append(current)
            header = self.state_db.get_header(current)
            if header is None or header.prev_hash is None:
                break
            current = header.prev_hash
        new_branch.reverse()
        return fork, old_branch, new_branch
