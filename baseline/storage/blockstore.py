"""
Append-only block store for deterministic persistence.
"""

from __future__ import annotations

import os
import struct
import threading
from pathlib import Path

__all__ = ["BlockStore", "BlockStoreError"]

LEN_STRUCT = struct.Struct(">I")
INDEX_STRUCT = struct.Struct(">32sQI")


class BlockStoreError(Exception):
    """Raised when the block store cannot service a request."""


class BlockStore:
    """
    Stores serialized blocks sequentially inside a single data file.

    Each block is prefixed with a 4-byte length and accompanied by an index entry
    mapping its hash to (offset, length). The store fsyncs metadata after every
    append to protect against crashes mid-write.
    """

    def __init__(self, directory: Path):
        self.directory = directory
        self.directory.mkdir(parents=True, exist_ok=True)
        self.data_path = self.directory / "blocks.dat"
        self.index_path = self.directory / "blocks.idx"
        self._lock = threading.RLock()
        self._index: dict[str, tuple[int, int]] = {}
        self._init_files()
        self._load_index()

    def _init_files(self) -> None:
        for path in (self.data_path, self.index_path):
            if not path.exists():
                path.touch()

    def _load_index(self) -> None:
        size = self.data_path.stat().st_size
        with self.index_path.open("rb") as idx:
            pos = 0
            while True:
                chunk = idx.read(INDEX_STRUCT.size)
                if not chunk:
                    break
                if len(chunk) != INDEX_STRUCT.size:
                    raise BlockStoreError(f"Corrupt block index at offset {pos}")
                block_hash, offset, length = INDEX_STRUCT.unpack(chunk)
                if offset + 4 + length > size:
                    raise BlockStoreError(f"Index entry exceeds data file for hash {block_hash.hex()}")
                hex_hash = block_hash.hex()
                self._index[hex_hash] = (offset, length)
                pos += INDEX_STRUCT.size

    def _fsync(self, fh) -> None:
        fh.flush()
        os.fsync(fh.fileno())

    def _normalize_hash(self, block_hash: bytes | str) -> bytes:
        if isinstance(block_hash, str):
            if len(block_hash) != 64:
                raise BlockStoreError("Block hash must be 32 bytes (64 hex chars)")
            try:
                return bytes.fromhex(block_hash)
            except ValueError as exc:
                raise BlockStoreError("Block hash must be hex encoded") from exc
        if len(block_hash) != 32:
            raise BlockStoreError("Block hash must be 32 bytes")
        return block_hash

    def has_block(self, block_hash: bytes | str) -> bool:
        hex_hash = self._normalize_hash(block_hash).hex()
        with self._lock:
            return hex_hash in self._index

    def append_block(self, block_hash: bytes | str, raw_block: bytes) -> None:
        if not isinstance(raw_block, (bytes, bytearray)):
            raise BlockStoreError("raw_block must be bytes")
        block_hash_bytes = self._normalize_hash(block_hash)
        hex_hash = block_hash_bytes.hex()
        payload = bytes(raw_block)
        payload_len = len(payload)
        if payload_len == 0:
            raise BlockStoreError("Cannot store empty block")
        with self._lock:
            if hex_hash in self._index:
                raise BlockStoreError(f"Block {hex_hash} already stored")
            with self.data_path.open("r+b") as data_fh:
                data_fh.seek(0, os.SEEK_END)
                offset = data_fh.tell()
                data_fh.write(LEN_STRUCT.pack(payload_len))
                data_fh.write(payload)
                self._fsync(data_fh)
            with self.index_path.open("ab") as index_fh:
                index_fh.write(INDEX_STRUCT.pack(block_hash_bytes, offset, payload_len))
                self._fsync(index_fh)
            self._index[hex_hash] = (offset, payload_len)

    def get_block(self, block_hash: bytes | str) -> bytes:
        hex_hash = self._normalize_hash(block_hash).hex()
        with self._lock:
            try:
                offset, length = self._index[hex_hash]
            except KeyError as exc:
                raise BlockStoreError(f"Unknown block {hex_hash}") from exc
        with self.data_path.open("rb") as data_fh:
            data_fh.seek(offset)
            length_prefix = data_fh.read(LEN_STRUCT.size)
            if len(length_prefix) != LEN_STRUCT.size:
                raise BlockStoreError(f"Block length missing for {hex_hash}")
            (stored_length,) = LEN_STRUCT.unpack(length_prefix)
            if stored_length != length:
                raise BlockStoreError(f"Block length mismatch for {hex_hash}")
            block = data_fh.read(length)
            if len(block) != length:
                raise BlockStoreError(f"Block payload truncated for {hex_hash}")
            return block

    def block_count(self) -> int:
        with self._lock:
            return len(self._index)

    def tip(self) -> str | None:
        with self._lock:
            if not self._index:
                return None
            # Latest appended block corresponds to highest offset
            return max(self._index.items(), key=lambda item: item[1][0])[0]

    def check(self) -> None:
        """Runs lightweight consistency checks on data + index files."""
        with self._lock:
            data_size = self.data_path.stat().st_size
            offset = 0
            with self.data_path.open("rb") as data_fh:
                while offset < data_size:
                    length_prefix = data_fh.read(LEN_STRUCT.size)
                    if not length_prefix:
                        break
                    if len(length_prefix) != LEN_STRUCT.size:
                        raise BlockStoreError(f"Truncated length prefix at offset {offset}")
                    (length,) = LEN_STRUCT.unpack(length_prefix)
                    if length <= 0:
                        raise BlockStoreError(f"Invalid length {length} at offset {offset}")
                    payload = data_fh.read(length)
                    if len(payload) != length:
                        raise BlockStoreError(f"Truncated block payload at offset {offset}")
                    offset += LEN_STRUCT.size + length
            for hex_hash, (entry_offset, entry_len) in self._index.items():
                if entry_offset + LEN_STRUCT.size + entry_len > data_size:
                    raise BlockStoreError(f"Index entry for {hex_hash} exceeds data file size")
                with self.data_path.open("rb") as data_fh:
                    data_fh.seek(entry_offset)
                    prefix = data_fh.read(LEN_STRUCT.size)
                    if len(prefix) != LEN_STRUCT.size:
                        raise BlockStoreError(f"Missing block length for {hex_hash}")
                    (length,) = LEN_STRUCT.unpack(prefix)
                    if length != entry_len:
                        raise BlockStoreError(f"Length mismatch for {hex_hash}")

    def iter_hashes(self):
        with self._lock:
            yield from sorted(self._index.keys(), key=lambda h: self._index[h][0])
