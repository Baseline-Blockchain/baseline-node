"""
Transaction primitives.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from . import crypto

COIN = 100_000_000
MAX_MONEY = 21_000_000 * COIN


class TxSerializationError(Exception):
    pass


def encode_varint(value: int) -> bytes:
    if value < 0xfd:
        return value.to_bytes(1, "little")
    if value <= 0xFFFF:
        return b"\xfd" + value.to_bytes(2, "little")
    if value <= 0xFFFFFFFF:
        return b"\xfe" + value.to_bytes(4, "little")
    return b"\xff" + value.to_bytes(8, "little")


def decode_varint(data: bytes, offset: int = 0) -> tuple[int, int]:
    if offset >= len(data):
        raise TxSerializationError("Varint decoded past end")
    prefix = data[offset]
    offset += 1
    if prefix < 0xfd:
        return prefix, offset
    if prefix == 0xfd:
        if offset + 2 > len(data):
            raise TxSerializationError("Varint truncated")
        value = int.from_bytes(data[offset : offset + 2], "little")
        return value, offset + 2
    if prefix == 0xfe:
        if offset + 4 > len(data):
            raise TxSerializationError("Varint truncated")
        value = int.from_bytes(data[offset : offset + 4], "little")
        return value, offset + 4
    if offset + 8 > len(data):
        raise TxSerializationError("Varint truncated")
    value = int.from_bytes(data[offset : offset + 8], "little")
    return value, offset + 8


@dataclass(slots=True)
class TxInput:
    prev_txid: str
    prev_vout: int
    script_sig: bytes
    sequence: int = 0xFFFFFFFF

    def serialize(self) -> bytes:
        prev = bytes.fromhex(self.prev_txid)
        if len(prev) != 32:
            raise TxSerializationError("prev_txid must be 32 bytes")
        encoded = prev[::-1]  # little-endian as per Bitcoin
        encoded += self.prev_vout.to_bytes(4, "little")
        encoded += encode_varint(len(self.script_sig))
        encoded += self.script_sig
        encoded += self.sequence.to_bytes(4, "little")
        return encoded


@dataclass(slots=True)
class TxOutput:
    value: int
    script_pubkey: bytes

    def serialize(self) -> bytes:
        if not (0 <= self.value <= MAX_MONEY):
            raise TxSerializationError("Invalid output value")
        encoded = self.value.to_bytes(8, "little")
        encoded += encode_varint(len(self.script_pubkey))
        encoded += self.script_pubkey
        return encoded


@dataclass(slots=True)
class Transaction:
    version: int
    inputs: list[TxInput] = field(default_factory=list)
    outputs: list[TxOutput] = field(default_factory=list)
    lock_time: int = 0

    def serialize(self, *, script_override: tuple[int, bytes] | None = None) -> bytes:
        encoded = self.version.to_bytes(4, "little")
        encoded += encode_varint(len(self.inputs))
        for idx, txin in enumerate(self.inputs):
            script_sig = txin.script_sig
            if script_override and script_override[0] == idx:
                script_sig = script_override[1]
            encoded += bytes.fromhex(txin.prev_txid)[::-1]
            encoded += txin.prev_vout.to_bytes(4, "little")
            encoded += encode_varint(len(script_sig))
            encoded += script_sig
            encoded += txin.sequence.to_bytes(4, "little")
        encoded += encode_varint(len(self.outputs))
        for txout in self.outputs:
            encoded += txout.value.to_bytes(8, "little")
            encoded += encode_varint(len(txout.script_pubkey))
            encoded += txout.script_pubkey
        encoded += self.lock_time.to_bytes(4, "little")
        return encoded

    def txid(self) -> str:
        return crypto.sha256d(self.serialize())[::-1].hex()

    def is_coinbase(self) -> bool:
        if len(self.inputs) != 1:
            return False
        txin = self.inputs[0]
        return txin.prev_txid == "00" * 32 and txin.prev_vout == 0xFFFFFFFF

    def signature_hash(self, input_index: int, script_pubkey: bytes, sighash_type: int) -> bytes:
        if input_index >= len(self.inputs):
            raise TxSerializationError("Input index out of range")
        if sighash_type != 0x01:
            raise TxSerializationError("Only SIGHASH_ALL supported")
        encoded = self.version.to_bytes(4, "little")
        encoded += encode_varint(len(self.inputs))
        for idx, txin in enumerate(self.inputs):
            script = script_pubkey if idx == input_index else b""
            encoded += bytes.fromhex(txin.prev_txid)[::-1]
            encoded += txin.prev_vout.to_bytes(4, "little")
            encoded += encode_varint(len(script))
            encoded += script
            encoded += txin.sequence.to_bytes(4, "little")
        encoded += encode_varint(len(self.outputs))
        for txout in self.outputs:
            encoded += txout.value.to_bytes(8, "little")
            encoded += encode_varint(len(txout.script_pubkey))
            encoded += txout.script_pubkey
        encoded += self.lock_time.to_bytes(4, "little")
        encoded += sighash_type.to_bytes(4, "little")
        return crypto.sha256d(encoded)

    @classmethod
    def parse_from(cls, raw: bytes, offset: int = 0) -> tuple[Transaction, int]:
        if offset + 10 > len(raw):
            raise TxSerializationError("Transaction too small")
        version = int.from_bytes(raw[offset : offset + 4], "little")
        offset += 4
        in_count, offset = decode_varint(raw, offset)
        inputs: list[TxInput] = []
        for _ in range(in_count):
            if offset + 36 > len(raw):
                raise TxSerializationError("Truncated input")
            prev_txid = raw[offset : offset + 32][::-1].hex()
            offset += 32
            prev_vout = int.from_bytes(raw[offset : offset + 4], "little")
            offset += 4
            script_len, offset = decode_varint(raw, offset)
            script_sig = raw[offset : offset + script_len]
            offset += script_len
            sequence = int.from_bytes(raw[offset : offset + 4], "little")
            offset += 4
            inputs.append(TxInput(prev_txid=prev_txid, prev_vout=prev_vout, script_sig=script_sig, sequence=sequence))
        out_count, offset = decode_varint(raw, offset)
        outputs: list[TxOutput] = []
        for _ in range(out_count):
            if offset + 8 > len(raw):
                raise TxSerializationError("Truncated output value")
            value = int.from_bytes(raw[offset : offset + 8], "little")
            offset += 8
            script_len, offset = decode_varint(raw, offset)
            script_pubkey = raw[offset : offset + script_len]
            offset += script_len
            outputs.append(TxOutput(value=value, script_pubkey=script_pubkey))
        if offset + 4 > len(raw):
            raise TxSerializationError("Missing locktime")
        lock_time = int.from_bytes(raw[offset : offset + 4], "little")
        offset += 4
        tx = cls(version=version, inputs=inputs, outputs=outputs, lock_time=lock_time)
        tx.validate_basic()
        return tx, offset

    @classmethod
    def parse(cls, raw: bytes) -> Transaction:
        tx, offset = cls.parse_from(raw, 0)
        if offset != len(raw):
            raise TxSerializationError("Trailing data in transaction")
        return tx

    def validate_basic(self) -> None:
        if not self.inputs:
            raise TxSerializationError("Transaction must have inputs")
        if not self.outputs:
            raise TxSerializationError("Transaction must have outputs")
        if len(self.serialize()) > 1_000_000:
            raise TxSerializationError("Transaction exceeds max size")
        if any(out.value < 0 or out.value > MAX_MONEY for out in self.outputs):
            raise TxSerializationError("Output value out of range")
        total = sum(out.value for out in self.outputs)
        if total > MAX_MONEY:
            raise TxSerializationError("Total output exceeds money supply")
