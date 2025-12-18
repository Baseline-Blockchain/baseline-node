"""
Block primitives.
"""

from __future__ import annotations

from dataclasses import dataclass

from . import crypto
from .tx import Transaction, decode_varint, encode_varint

MAX_BLOCK_WEIGHT = 4_000_000  # compatibility placeholder


class BlockSerializationError(Exception):
    pass


@dataclass(slots=True)
class BlockHeader:
    version: int
    prev_hash: str
    merkle_root: str
    timestamp: int
    bits: int
    nonce: int

    def serialize(self) -> bytes:
        encoded = self.version.to_bytes(4, "little")
        encoded += bytes.fromhex(self.prev_hash)[::-1]
        encoded += bytes.fromhex(self.merkle_root)[::-1]
        encoded += self.timestamp.to_bytes(4, "little")
        encoded += self.bits.to_bytes(4, "little")
        encoded += self.nonce.to_bytes(4, "little")
        return encoded

    def hash(self) -> str:
        return crypto.sha256d(self.serialize())[::-1].hex()


@dataclass
class Block:
    header: BlockHeader
    transactions: list[Transaction]

    def serialize(self) -> bytes:
        data = self.header.serialize()
        data += encode_varint(len(self.transactions))
        for tx in self.transactions:
            data += tx.serialize()
        return data

    def block_hash(self) -> str:
        return self.header.hash()

    def weight(self) -> int:
        # No witness, so weight == size * 4
        return len(self.serialize()) * 4

    @classmethod
    def parse(cls, raw: bytes) -> Block:
        if len(raw) < 80:
            raise BlockSerializationError("Block too small")
        offset = 0
        version = int.from_bytes(raw[offset : offset + 4], "little")
        offset += 4
        prev_hash = raw[offset : offset + 32][::-1].hex()
        offset += 32
        merkle_root = raw[offset : offset + 32][::-1].hex()
        offset += 32
        timestamp = int.from_bytes(raw[offset : offset + 4], "little")
        offset += 4
        bits = int.from_bytes(raw[offset : offset + 4], "little")
        offset += 4
        nonce = int.from_bytes(raw[offset : offset + 4], "little")
        offset += 4
        tx_count, offset = decode_varint(raw, offset)
        transactions: list[Transaction] = []
        for _ in range(tx_count):
            if offset >= len(raw):
                raise BlockSerializationError("Missing transaction bytes")
            tx, offset = Transaction.parse_from(raw, offset)
            transactions.append(tx)
        if offset != len(raw):
            raise BlockSerializationError("Trailing data found in block")
        header = BlockHeader(
            version=version,
            prev_hash=prev_hash,
            merkle_root=merkle_root,
            timestamp=timestamp,
            bits=bits,
            nonce=nonce,
        )
        block = cls(header=header, transactions=transactions)
        if not block.transactions:
            raise BlockSerializationError("Block must contain transactions")
        merkle = merkle_root_hash(block.transactions)
        if merkle != header.merkle_root:
            raise BlockSerializationError("Merkle root mismatch")
        return block


def merkle_root_hash(transactions: list[Transaction]) -> str:
    hashes = [bytes.fromhex(tx.txid())[::-1] for tx in transactions]
    if not hashes:
        return "00" * 32
    while len(hashes) > 1:
        if len(hashes) % 2 == 1:
            hashes.append(hashes[-1])
        new_hashes = []
        for i in range(0, len(hashes), 2):
            new_hashes.append(crypto.sha256d(hashes[i] + hashes[i + 1]))
        hashes = new_hashes
    return hashes[0][::-1].hex()
