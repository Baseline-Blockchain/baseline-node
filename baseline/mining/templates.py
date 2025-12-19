"""
Block template construction and helper utilities for Stratum mining.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..core import crypto, difficulty
from ..core.block import Block, BlockHeader
from ..core.chain import Chain, UTXOView
from ..core.tx import Transaction, encode_varint
from ..mempool import Mempool
from ..storage import UTXORecord
from ..time_sync import synchronized_time_int

COINBASE_FLAGS = b"/Baseline/"
EXTRANONCE2_SIZE = 4


@dataclass(slots=True)
class Template:
    version: int
    prev_hash: str
    bits: int
    height: int
    timestamp: int
    coinb1: bytes
    coinb2: bytes
    transactions: list[Transaction]
    merkle_branches: list[bytes]
    fees: int
    target: int
    coinbase_value: int


class TemplateBuilder:
    def __init__(self, chain: Chain, mempool: Mempool, payout_script: bytes):
        self.chain = chain
        self.mempool = mempool
        self.payout_script = payout_script
        self.extranonce1_size = 4
        self.extranonce2_size = EXTRANONCE2_SIZE

    def build_template(self) -> Template:
        tip = self.chain.state_db.get_best_tip()
        if tip is None:
            raise RuntimeError("Chain tip unavailable")
        height = tip[1] + 1
        prev_hash = tip[0]
        parent_header = self.chain.state_db.get_header(prev_hash)
        if parent_header is None:
            raise RuntimeError(f"Parent header not found for hash {prev_hash}")
        view = self.chain._build_view_for_parent(prev_hash)
        selected, total_fees = self._select_transactions(height, view)
        subsidy = self.chain._block_subsidy(height)
        coinbase_value = subsidy + total_fees
        coinb1, coinb2 = self._build_coinbase_parts(height, coinbase_value)
        merkle_branches = self._build_merkle_branches(selected)
        timestamp = max(synchronized_time_int(), parent_header.timestamp + 1)
        bits = self.chain._expected_bits(height, parent_header)
        target = difficulty.compact_to_target(bits)
        return Template(
            version=1,
            prev_hash=prev_hash,
            bits=bits,
            height=height,
            timestamp=timestamp,
            coinb1=coinb1,
            coinb2=coinb2,
            transactions=selected,
            merkle_branches=merkle_branches,
            fees=total_fees,
            target=target,
            coinbase_value=coinbase_value,
        )

    def _select_transactions(self, height: int, view: UTXOView) -> tuple[list[Transaction], int]:
        with self.mempool.lock:
            entries = list(self.mempool.entries.values())
        entries.sort(key=lambda entry: entry.fee_rate, reverse=True)
        selected: list[Transaction] = []
        total_fees = 0
        for entry in entries:
            if self._try_apply(entry.tx, height, view):
                selected.append(entry.tx)
                total_fees += entry.fee
        return selected, total_fees

    def _try_apply(self, tx: Transaction, height: int, view: UTXOView) -> bool:
        snapshot = dict(view.overlay)
        try:
            seen_inputs = set()
            total_input = 0
            for txin in tx.inputs:
                key = (txin.prev_txid, txin.prev_vout)
                if key in seen_inputs:
                    raise ValueError("double spend")
                seen_inputs.add(key)
                utxo = view.get(txin.prev_txid, txin.prev_vout)
                if utxo is None:
                    raise ValueError("missing input")
                if utxo.coinbase and height - utxo.height < self.chain.config.mining.coinbase_maturity:
                    raise ValueError("coinbase maturity")
                total_input += utxo.amount
                view.spend(txin.prev_txid, txin.prev_vout)
            total_output = sum(out.value for out in tx.outputs)
            if total_output > total_input:
                raise ValueError("creates money")
            for idx, txout in enumerate(tx.outputs):
                record = UTXORecord(
                    txid=tx.txid(),
                    vout=idx,
                    amount=txout.value,
                    script_pubkey=txout.script_pubkey,
                    height=height,
                    coinbase=False,
                )
                view.add(record)
            return True
        except Exception:
            view.overlay = snapshot
            return False

    def _build_coinbase_parts(self, height: int, reward: int) -> tuple[bytes, bytes]:
        version = (1).to_bytes(4, "little")
        input_count = encode_varint(1)
        prev = bytes.fromhex("00" * 32)
        prev_vout = (0xFFFFFFFF).to_bytes(4, "little")
        height_push = self._encode_push(height.to_bytes((height.bit_length() + 7) // 8 or 1, "little"))
        flags_push = self._encode_push(COINBASE_FLAGS)
        extranonce_total = self.extranonce1_size + self.extranonce2_size
        extranonce_push = bytes([extranonce_total])
        script_len = len(height_push) + len(flags_push) + len(extranonce_push) + extranonce_total
        script_len_varint = encode_varint(script_len)
        sequence = (0xFFFFFFFF).to_bytes(4, "little")
        output_count = encode_varint(1)
        output_value = reward.to_bytes(8, "little")
        script_pubkey = self.payout_script
        script_pubkey_len = encode_varint(len(script_pubkey))
        lock_time = (0).to_bytes(4, "little")

        coinb1 = b"".join(
            [
                version,
                input_count,
                prev,
                prev_vout,
                script_len_varint,
                height_push,
                flags_push,
                extranonce_push,
            ]
        )
        coinb2 = b"".join(
            [
                sequence,
                output_count,
                output_value,
                script_pubkey_len,
                script_pubkey,
                lock_time,
            ]
        )
        return coinb1, coinb2

    def _encode_push(self, data: bytes) -> bytes:
        if len(data) < 0x4c:
            return bytes([len(data)]) + data
        raise ValueError("script push too long")

    def _build_merkle_branches(self, transactions: list[Transaction]) -> list[bytes]:
        if not transactions:
            return []
        hashes = [bytes.fromhex(tx.txid())[::-1] for tx in transactions]
        branch: list[bytes] = []
        layer = [None] + hashes
        index = 0
        while len(layer) > 1:
            if len(layer) % 2 == 1:
                layer.append(layer[-1])
            sibling_index = index ^ 1
            sibling = layer[sibling_index]
            if sibling is None:
                sibling = b"\x00" * 32
            branch.append(sibling)
            next_layer: list[bytes | None] = []
            for i in range(0, len(layer), 2):
                left = layer[i]
                right = layer[i + 1]
                if left is None:
                    next_layer.append(None)
                else:
                    if right is None:
                        right = left
                    next_layer.append(crypto.sha256d(left + right))
            layer = next_layer
            index //= 2
        return branch

    def assemble_block(
        self,
        template: Template,
        extranonce1: bytes,
        extranonce2: bytes,
        nonce: int,
        ntime: int,
    ) -> Block:
        coinbase_serialized = template.coinb1 + extranonce1 + extranonce2 + template.coinb2
        coinbase_tx = Transaction.parse(coinbase_serialized)
        coinbase_hash = crypto.sha256d(coinbase_serialized)
        merkle = coinbase_hash
        for sibling in template.merkle_branches:
            merkle = crypto.sha256d(merkle + sibling)
        merkle_root = merkle[::-1].hex()
        header = BlockHeader(
            version=template.version,
            prev_hash=template.prev_hash,
            merkle_root=merkle_root,
            timestamp=ntime,
            bits=template.bits,
            nonce=nonce,
        )
        block_txs = [coinbase_tx] + template.transactions
        return Block(header=header, transactions=block_txs)
