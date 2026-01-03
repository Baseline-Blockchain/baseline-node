"""
Blockchain management for Baseline.
"""

from __future__ import annotations

import contextlib
import logging
import math
from collections.abc import Sequence
from dataclasses import dataclass

from ..config import ConfigError, NodeConfig
from ..core.address import script_from_address
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
GENESIS_LEGACY_SUBSIDY = 50 * COIN
MAX_FUTURE_BLOCK_TIME = 3 * 60  # 3 minutes

# Mainnet genesis (2025-12-28T00:00:00Z). This is consensus-critical.
MAINNET_GENESIS_TIMESTAMP = 1_766_880_000
MAINNET_GENESIS_MERKLE_ROOT = "1c633c7361f56181e314720e41068f7ce4dab2ddd2393e33e6ecc22f8ff3458e"
MAINNET_GENESIS_NONCE = 2
MAINNET_GENESIS_HASH = "8bce8b135b38c83a93d0366dee26c45f051bf0bcebbc4f4ad4a4067070939907"

# Private devnets (allow_consensus_overrides=true) use a deterministic genesis timestamp
# that stays comfortably in the past for test suites.
DEVNET_GENESIS_TIMESTAMP = 1_733_966_400
DEVNET_POW_LIMIT_BITS = 0x207FFFFF

FOUNDATION_FEE_NUMERATOR = 1
FOUNDATION_FEE_DENOMINATOR = 100

# Mainnet difficulty uses a per-block LWMA retarget to handle sudden hashrate swings.
LWMA_WINDOW = 60
LWMA_SOLVETIME_CLAMP_FACTOR = 6

CONSENSUS_DEFAULTS = {
    "coinbase_maturity": 20,
    "block_interval_target": 20,
    # "PoW limit" (easiest difficulty) used as the maximum target in LWMA and in the genesis bits field.
    "pow_limit_bits": 0x207FFFFF,
    # Launch difficulty (height 1), before LWMA has a meaningful window.
    "initial_bits": 0x207FFFFF,
    "subsidy_halving_interval": 4_158_884,
    "foundation_address": "NMUrmCNAH5VUrjLSvM4ULu7eNtD1i8qcyK",
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
        if getattr(self.config.mining, "allow_consensus_overrides", False):
            # Keep dev/test nets fast to mine unless explicitly configured otherwise.
            if self.config.mining.pow_limit_bits == CONSENSUS_DEFAULTS["pow_limit_bits"]:
                self.config.mining.pow_limit_bits = DEVNET_POW_LIMIT_BITS
        self.max_target = difficulty.compact_to_target(self.config.mining.pow_limit_bits)
        try:
            self.foundation_script = script_from_address(self.config.mining.foundation_address)
        except ValueError as exc:  # noqa: BLE001
            raise ConfigError("Invalid foundation address in config") from exc
        self.genesis_block = self._build_genesis_block()
        self.genesis_hash = self.genesis_block.block_hash()

        # Initialize fork and upgrade managers
        self.fork_manager = ForkManager(self)
        self.upgrade_manager = UpgradeManager(state_db)

        self._enforce_consensus_parameters()
        if getattr(self.config.mining, "allow_consensus_overrides", False):
            # Dev/test nets default to the easiest target unless explicitly overridden.
            if self.config.mining.initial_bits == CONSENSUS_DEFAULTS["initial_bits"]:
                self.config.mining.initial_bits = self.config.mining.pow_limit_bits
        self._ensure_genesis()

        # If metadata says we're at genesis but stray headers remain, prune them.
        best = self.state_db.get_best_tip()
        if best and best[1] == 0 and self.state_db.has_headers_beyond_genesis():
            self.log.warning("Found stray headers beyond genesis with no tip; pruning to genesis")
            with contextlib.suppress(Exception):
                self.state_db.reset_headers_to_genesis(self.genesis_hash)
            best = self.state_db.get_best_tip()

        # Heal missing main-chain headers from the block store if gaps are detected.
        if self.state_db.has_main_chain_gap():
            repaired = self.state_db.rebuild_main_headers_from_blocks(self.block_store)
            if repaired:
                self.log.warning("Rebuilt %s missing main-chain headers from block store", repaired)
            # Refresh best tip metadata if the rebuilt headers extend it.
            highest = self.state_db.get_highest_main_header()
            if highest and (best is None or highest.height > best[1]):
                self.state_db.set_best_tip(highest.hash, highest.height)
                self.state_db.set_meta("best_work", highest.chainwork)
                self.state_db.upsert_chain_tip(highest.hash, highest.height, highest.chainwork)
                best = self.state_db.get_best_tip()

        self.log.info("Current block height %s", self.state_db.get_best_tip()[1] if self.state_db.get_best_tip() else 0)

    def _build_genesis_block(self) -> Block:
        coinbase_script = self._encode_coinbase_script(height=0, extra=GENESIS_MESSAGE)
        genesis_subsidy = GENESIS_LEGACY_SUBSIDY if self.config.mining.allow_consensus_overrides else 0
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
                    value=genesis_subsidy,
                    script_pubkey=self.foundation_script,
                )
            ],
            lock_time=0,
        )
        merkle = merkle_root_hash([coinbase])
        timestamp = MAINNET_GENESIS_TIMESTAMP if not self.config.mining.allow_consensus_overrides else DEVNET_GENESIS_TIMESTAMP
        nonce = 0

        if not self.config.mining.allow_consensus_overrides:
            if merkle != MAINNET_GENESIS_MERKLE_ROOT:
                raise ChainError("Mainnet genesis merkle root mismatch; check foundation_address/coinbase encoding")
            nonce = MAINNET_GENESIS_NONCE

        header = BlockHeader(
            version=1,
            prev_hash="00" * 32,
            merkle_root=merkle,
            timestamp=timestamp,
            bits=self.config.mining.pow_limit_bits,
            nonce=nonce,
        )
        block = Block(header=header, transactions=[coinbase])

        if self.config.mining.allow_consensus_overrides:
            while not difficulty.check_proof_of_work(block.block_hash(), block.header.bits):
                nonce += 1
                block.header.nonce = nonce
                if nonce > 0xFFFFFFFF:
                    raise ChainError("Failed to solve genesis block")
            self.log.info("Genesis block solved nonce=%s hash=%s", block.header.nonce, block.block_hash())
            return block

        genesis_hash = block.block_hash()
        if not difficulty.check_proof_of_work(genesis_hash, block.header.bits):
            raise ChainError("Hardcoded mainnet genesis does not satisfy proof-of-work")
        if genesis_hash != MAINNET_GENESIS_HASH:
            raise ChainError("Hardcoded mainnet genesis hash mismatch; check build constants")
        self.log.info("Mainnet genesis block nonce=%s hash=%s", block.header.nonce, genesis_hash)
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
            # Refuse to run with an on-disk chainstate that doesn't match our computed genesis.
            header = self.state_db.get_header(genesis_hash)
            if header is None or header.height != 0:
                raise ChainError(
                    "Chainstate genesis mismatch. "
                    "If you changed mainnet/devnet consensus parameters, resync with --reset-chainstate."
                )
            if not self.block_store.has_block(genesis_hash):
                raise ChainError(
                    "Chainstate references genesis but block store is missing it. "
                    "Resync with --reset-chainstate."
                )
            return
        self.log.info("Initializing chain with genesis block %s", genesis_hash)
        raw = self.genesis_block.serialize()
        if not self.block_store.has_block(genesis_hash):
            self.block_store.append_block(bytes.fromhex(genesis_hash), raw)
        created = []
        for idx, txout in enumerate(self.genesis_block.transactions[0].outputs):
            if txout.value <= 0:
                continue
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
        self.state_db.index_block_addresses(self.genesis_block, 0)
        self.state_db.index_block_transactions(
            genesis_hash,
            0,
            [tx.txid() for tx in self.genesis_block.transactions],
        )
        self.state_db.record_block_metrics(
            genesis_hash,
            0,
            self.genesis_block.header.timestamp,
            len(self.genesis_block.transactions),
            total_fee=0,
            total_weight=self.genesis_block.weight(),
            total_size=len(self.genesis_block.serialize()),
        )
        header = HeaderData(
            hash=genesis_hash,
            prev_hash=None,
            height=0,
            bits=self.genesis_block.header.bits,
            nonce=self.genesis_block.header.nonce,
            timestamp=self.genesis_block.header.timestamp,
            merkle_root=self.genesis_block.header.merkle_root,
            chainwork=str(difficulty.block_work(self.genesis_block.header.bits)),
            version=self.genesis_block.header.version,
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
        if not self.block_store.has_block(prev_hash):
            raise ChainError("Parent block data missing")
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
            version=block.header.version,
            status=1,
        )
        self.block_store.append_block(bytes.fromhex(block_hash), raw_block)
        self.state_db.store_header(header)

        # Handle fork detection and reorganization
        block_accepted, reorganization_occurred, reorg_deferred = self.fork_manager.handle_new_block(block)

        if not block_accepted:
            # Block was rejected by fork manager
            return {"status": "rejected", "hash": block_hash, "height": height}

        if reorganization_occurred:
            # Fork manager handled the reorganization
            return {"status": "reorganized", "hash": block_hash, "height": height}

        # Continue with normal processing
        self.state_db.upsert_chain_tip(block_hash, height, header.chainwork)
        self.state_db.remove_chain_tip(prev_hash)
        if reorg_deferred:
            return {"status": "deferred", "hash": block_hash, "height": height}
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
        self.state_db.index_block_addresses(block, height)
        self.state_db.index_block_transactions(block_hash, height, [tx.txid() for tx in block.transactions])
        self.state_db.store_undo_data(block_hash, validation.spent)
        self.state_db.record_block_metrics(
            block_hash,
            height,
            block.header.timestamp,
            len(block.transactions),
            total_fee=validation.fees,
            total_weight=block.weight(),
            total_size=len(block.serialize()),
        )
        self.state_db.set_header_status(block_hash, 0)
        self.state_db.set_best_tip(block_hash, height)
        self.state_db.set_meta("best_work", str(chainwork))
        self.state_db.upsert_chain_tip(block_hash, height, str(chainwork))
        if block.header.prev_hash:
            self.state_db.remove_chain_tip(block.header.prev_hash)
        self.log.info("Connected block %s height=%s txs=%s", block_hash, height, len(block.transactions))
        mempool = getattr(self, "mempool", None)
        if mempool is not None:
            try:
                mempool.handle_block_connected(block)
            except Exception:
                self.log.debug("Mempool notification failed for block %s", block_hash, exc_info=True)

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
        merkle = merkle_root_hash(block.transactions)
        if block.header.merkle_root != merkle:
            raise ChainError("Merkle root mismatch")
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
        if not coinbase_tx.is_final(height, block.header.timestamp):
            raise ChainError("Coinbase lock_time not satisfied")
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
        foundation_required = self._foundation_reward(subsidy)
        if foundation_required > 0:
            paid = sum(out.value for out in coinbase_tx.outputs if out.script_pubkey == self.foundation_script)
            if paid < foundation_required:
                raise ChainError("Foundation fee missing")
        for _tx_index, tx in enumerate(block.transactions[1:], start=1):
            if tx.is_coinbase():
                raise ChainError("Multiple coinbase transactions")
            if not tx.is_final(height, block.header.timestamp):
                raise ChainError("Transaction lock_time not satisfied")
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
        interval = max(1, self.config.mining.subsidy_halving_interval)
        decay = math.pow(0.5, height / interval)
        subsidy = int((50 * COIN) * decay)
        return max(subsidy, 0)

    def _foundation_reward(self, subsidy: int) -> int:
        if subsidy <= 0:
            return 0
        return (subsidy * FOUNDATION_FEE_NUMERATOR + FOUNDATION_FEE_DENOMINATOR - 1) // FOUNDATION_FEE_DENOMINATOR

    def _expected_bits(self, height: int, parent_header: HeaderData) -> int:
        return self._expected_bits_lwma(height, parent_header)

    def _expected_bits_lwma(self, height: int, parent_header: HeaderData) -> int:
        # Zawy's LWMA: compute a linearly weighted solvetime average over a rolling window,
        # combined with an average target, to retarget every block.
        if height <= 1:
            return self.config.mining.initial_bits
        if height < 3:
            return parent_header.bits

        target_spacing = max(1, self.config.mining.block_interval_target)
        window = min(LWMA_WINDOW, height - 1)
        if window < 2:
            return parent_header.bits

        # Walk back along the ancestor path (not by height index) to gather the last window+1 headers.
        path: list[HeaderData] = []
        current = parent_header
        for _ in range(window + 1):
            if current is None:
                break
            path.append(current)
            if current.prev_hash is None:
                break
            current = self.state_db.get_header(current.prev_hash)

        if len(path) < window + 1:
            return parent_header.bits

        # Oldest -> newest
        path.reverse()
        actual_window = len(path) - 1
        sum_targets = 0
        sum_weighted_solvetime = 0
        max_solvetime = LWMA_SOLVETIME_CLAMP_FACTOR * target_spacing
        weight_sum = actual_window * (actual_window + 1) // 2

        for i in range(1, actual_window + 1):
            prev = path[i - 1]
            curr = path[i]
            solvetime = curr.timestamp - prev.timestamp
            solvetime = max(1, min(max_solvetime, solvetime))
            sum_weighted_solvetime += solvetime * i
            sum_targets += difficulty.compact_to_target(curr.bits)

        denom = actual_window * weight_sum * target_spacing
        next_target = (sum_targets * sum_weighted_solvetime) // max(1, denom)
        next_target = max(1, min(self.max_target, next_target))
        return difficulty.target_to_compact(next_target)

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

        # After successfully rewinding and reconnecting the chain, notify the
        # mempool (if any) of the reorganization.  This allows the mempool
        # to re-add transactions from the disconnected branch and to
        # revalidate existing transactions against the new main chain.  The
        # mempool may be attached to the chain dynamically via the Mempool
        # constructor.  We wrap the call in a try/except to avoid
        # propagating any mempool errors back to the chain logic.
        try:
            mempool = getattr(self, "mempool", None)
            if mempool is not None:
                # Provide the lists of detached and attached block hashes to
                # the mempool.  We use the local variables old_branch and
                # new_branch captured from the outer scope (defined at the
                # beginning of this method).
                mempool.handle_reorg(old_branch, new_branch)
        except Exception:
            # Log the error at debug level; mempool exceptions should not
            # interfere with chain operation.  Use broad exception catching
            # here to ensure robustness.
            import logging
            logging.getLogger("baseline.chain").debug("Mempool reorg handling failed", exc_info=True)

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
        self.state_db.remove_block_address_index(block)
        self.state_db.remove_block_transactions(block_hash)
        self.state_db.remove_block_metrics(block_hash)
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
