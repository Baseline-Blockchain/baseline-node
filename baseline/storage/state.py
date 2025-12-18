"""
SQLite-backed blockchain state database.
"""

from __future__ import annotations

import contextlib
import json
import sqlite3
import threading
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path

__all__ = [
    "StateDB",
    "StateDBError",
    "UTXORecord",
    "HeaderData",
]


class StateDBError(Exception):
    """Raised when the state database encounters an unrecoverable issue."""


@dataclass(frozen=True, slots=True)
class UTXORecord:
    txid: str
    vout: int
    amount: int
    script_pubkey: bytes
    height: int
    coinbase: bool


@dataclass(frozen=True, slots=True)
class HeaderData:
    hash: str
    prev_hash: str | None
    height: int
    bits: int
    nonce: int
    timestamp: int
    merkle_root: str
    chainwork: str
    status: int = 0  # 0=main chain, 1=side chain


class StateDB:
    """Wraps a SQLite DB that tracks headers, chain tips, and the UTXO set."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, timeout=30, isolation_level=None, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.row_factory = sqlite3.Row
        self._closed = False
        self._init_schema()
        self.run_startup_checks()

    def close(self) -> None:
        if not self._closed:
            self._conn.close()
            self._closed = True

    def __del__(self):
        with contextlib.suppress(Exception):
            self.close()

    @property
    def closed(self) -> bool:
        return self._closed

    def _init_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS headers (
                    hash TEXT PRIMARY KEY,
                    prev_hash TEXT,
                    height INTEGER NOT NULL,
                    bits INTEGER NOT NULL,
                    nonce INTEGER NOT NULL,
                    timestamp INTEGER NOT NULL,
                    merkle_root TEXT NOT NULL,
                    chainwork TEXT NOT NULL,
                    status INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS headers_height_idx ON headers(height);

                CREATE TABLE IF NOT EXISTS chain_tips (
                    hash TEXT PRIMARY KEY,
                    height INTEGER NOT NULL,
                    work TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS utxos (
                    txid TEXT NOT NULL,
                    vout INTEGER NOT NULL,
                    amount INTEGER NOT NULL,
                    script_pubkey BLOB NOT NULL,
                    height INTEGER NOT NULL,
                    coinbase INTEGER NOT NULL,
                    PRIMARY KEY (txid, vout)
                );

                CREATE TABLE IF NOT EXISTS block_undo (
                    hash TEXT PRIMARY KEY,
                    data BLOB NOT NULL
                );
                """
            )

    def _ensure_open(self) -> None:
        if self._closed:
            raise StateDBError("StateDB connection is closed")

    @contextlib.contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        self._ensure_open()
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except Exception:
                self._conn.rollback()
                raise
            else:
                self._conn.commit()
            finally:
                cursor.close()

    def set_meta(self, key: str, value: str) -> None:
        self._ensure_open()
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO meta(key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    def get_meta(self, key: str, default: str | None = None) -> str | None:
        self._ensure_open()
        with self._lock:
            row = self._conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            if row is None:
                return default
            return row["value"]

    def store_header(self, header: HeaderData) -> None:
        self._ensure_open()
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO headers(hash, prev_hash, height, bits, nonce, timestamp, merkle_root, chainwork, status)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(hash) DO UPDATE SET
                    prev_hash=excluded.prev_hash,
                    height=excluded.height,
                    bits=excluded.bits,
                    nonce=excluded.nonce,
                    timestamp=excluded.timestamp,
                    merkle_root=excluded.merkle_root,
                    chainwork=excluded.chainwork,
                    status=excluded.status
                """,
                (
                    header.hash,
                    header.prev_hash,
                    header.height,
                    header.bits,
                    header.nonce,
                    header.timestamp,
                    header.merkle_root,
                    header.chainwork,
                    header.status,
                ),
            )

    def get_header(self, block_hash: str) -> HeaderData | None:
        self._ensure_open()
        with self._lock:
            row = self._conn.execute(
                "SELECT hash, prev_hash, height, bits, nonce, timestamp, merkle_root, chainwork, status "
                "FROM headers WHERE hash=?",
                (block_hash,),
            ).fetchone()
        if row is None:
            return None
        return HeaderData(
            hash=row["hash"],
            prev_hash=row["prev_hash"],
            height=row["height"],
            bits=row["bits"],
            nonce=row["nonce"],
            timestamp=row["timestamp"],
            merkle_root=row["merkle_root"],
            chainwork=row["chainwork"],
            status=row["status"],
        )

    def set_best_tip(self, block_hash: str, height: int) -> None:
        self._ensure_open()
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO meta(key, value) VALUES('best_hash', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (block_hash,),
            )
            conn.execute(
                "INSERT INTO meta(key, value) VALUES('best_height', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(height),),
            )

    def get_best_tip(self) -> tuple[str, int] | None:
        self._ensure_open()
        with self._lock:
            best_hash = self.get_meta("best_hash")
            best_height = self.get_meta("best_height")
        if best_hash is None or best_height is None:
            return None
        return best_hash, int(best_height)

    def upsert_chain_tip(self, block_hash: str, height: int, work: str) -> None:
        self._ensure_open()
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO chain_tips(hash, height, work) VALUES (?, ?, ?) "
                "ON CONFLICT(hash) DO UPDATE SET height=excluded.height, work=excluded.work",
                (block_hash, height, work),
            )

    def remove_chain_tip(self, block_hash: str) -> None:
        self._ensure_open()
        with self.transaction() as conn:
            conn.execute("DELETE FROM chain_tips WHERE hash=?", (block_hash,))

    def list_chain_tips(self) -> list[tuple[str, int, str]]:
        self._ensure_open()
        with self._lock:
            rows = self._conn.execute(
                "SELECT hash, height, work FROM chain_tips ORDER BY height DESC"
            ).fetchall()
            return [(row["hash"], row["height"], row["work"]) for row in rows]

    def set_header_status(self, block_hash: str, status: int) -> None:
        self._ensure_open()
        with self.transaction() as conn:
            conn.execute("UPDATE headers SET status=? WHERE hash=?", (status, block_hash))

    def get_header_status(self, block_hash: str) -> int | None:
        self._ensure_open()
        with self._lock:
            row = self._conn.execute("SELECT status FROM headers WHERE hash=?", (block_hash,)).fetchone()
        if row is None:
            return None
        return row["status"]

    def get_main_header_at_height(self, height: int) -> HeaderData | None:
        self._ensure_open()
        with self._lock:
            row = self._conn.execute(
                """
                SELECT hash, prev_hash, height, bits, nonce, timestamp, merkle_root, chainwork, status
                FROM headers WHERE height=? AND status=0
                """,
                (height,),
            ).fetchone()
        if row is None:
            return None
        return HeaderData(
            hash=row["hash"],
            prev_hash=row["prev_hash"],
            height=row["height"],
            bits=row["bits"],
            nonce=row["nonce"],
            timestamp=row["timestamp"],
            merkle_root=row["merkle_root"],
            chainwork=row["chainwork"],
            status=row["status"],
        )

    def store_undo_data(self, block_hash: str, records: Sequence[UTXORecord]) -> None:
        self._ensure_open()
        payload = json.dumps(
            [
                {
                    "txid": rec.txid,
                    "vout": rec.vout,
                    "amount": rec.amount,
                    "script_pubkey": rec.script_pubkey.hex(),
                    "height": rec.height,
                    "coinbase": rec.coinbase,
                }
                for rec in records
            ]
        ).encode("utf-8")
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO block_undo(hash, data) VALUES(?, ?) "
                "ON CONFLICT(hash) DO UPDATE SET data=excluded.data",
                (block_hash, payload),
            )

    def load_undo_data(self, block_hash: str) -> list[UTXORecord]:
        self._ensure_open()
        with self._lock:
            row = self._conn.execute("SELECT data FROM block_undo WHERE hash=?", (block_hash,)).fetchone()
        if row is None:
            return []
        blob = row["data"]
        if isinstance(blob, bytes):
            blob = blob.decode("utf-8")
        payload = json.loads(blob)
        return [
            UTXORecord(
                txid=item["txid"],
                vout=item["vout"],
                amount=item["amount"],
                script_pubkey=bytes.fromhex(item["script_pubkey"]),
                height=item["height"],
                coinbase=bool(item["coinbase"]),
            )
            for item in payload
        ]

    def delete_undo_data(self, block_hash: str) -> None:
        self._ensure_open()
        with self.transaction() as conn:
            conn.execute("DELETE FROM block_undo WHERE hash=?", (block_hash,))

    def add_utxo(self, record: UTXORecord) -> None:
        self._ensure_open()
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO utxos(txid, vout, amount, script_pubkey, height, coinbase)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(txid, vout) DO UPDATE SET
                    amount=excluded.amount,
                    script_pubkey=excluded.script_pubkey,
                    height=excluded.height,
                    coinbase=excluded.coinbase
                """,
                (
                    record.txid,
                    record.vout,
                    record.amount,
                    record.script_pubkey,
                    record.height,
                    1 if record.coinbase else 0,
                ),
            )

    def remove_utxo(self, txid: str, vout: int) -> bool:
        self._ensure_open()
        with self.transaction() as conn:
            cur = conn.execute("DELETE FROM utxos WHERE txid=? AND vout=?", (txid, vout))
            return cur.rowcount > 0

    def get_utxo(self, txid: str, vout: int) -> UTXORecord | None:
        self._ensure_open()
        with self._lock:
            row = self._conn.execute(
                """
                SELECT txid, vout, amount, script_pubkey, height, coinbase
                FROM utxos WHERE txid=? AND vout=?
                """,
                (txid, vout),
            ).fetchone()
        if row is None:
            return None
        return UTXORecord(
            txid=row["txid"],
            vout=row["vout"],
            amount=row["amount"],
            script_pubkey=row["script_pubkey"],
            height=row["height"],
            coinbase=bool(row["coinbase"]),
        )

    def get_utxos_by_scripts(self, scripts: Sequence[bytes]) -> list[UTXORecord]:
        self._ensure_open()
        if not scripts:
            return []
        placeholders = ",".join("?" for _ in scripts)
        query = (
            "SELECT txid, vout, amount, script_pubkey, height, coinbase "
            f"FROM utxos WHERE script_pubkey IN ({placeholders})"
        )
        with self._lock:
            rows = self._conn.execute(query, scripts).fetchall()
        return [
            UTXORecord(
                txid=row["txid"],
                vout=row["vout"],
                amount=row["amount"],
                script_pubkey=row["script_pubkey"],
                height=row["height"],
                coinbase=bool(row["coinbase"]),
            )
            for row in rows
        ]

    def apply_utxo_changes(
        self,
        spent: Sequence[tuple[str, int]],
        created: Sequence[UTXORecord],
    ) -> None:
        self._ensure_open()
        with self.transaction() as conn:
            for txid, vout in spent:
                conn.execute("DELETE FROM utxos WHERE txid=? AND vout=?", (txid, vout))
            for utxo in created:
                conn.execute(
                    """
                    INSERT INTO utxos(txid, vout, amount, script_pubkey, height, coinbase)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(txid, vout) DO UPDATE SET
                        amount=excluded.amount,
                        script_pubkey=excluded.script_pubkey,
                        height=excluded.height,
                        coinbase=excluded.coinbase
                    """,
                    (
                        utxo.txid,
                        utxo.vout,
                        utxo.amount,
                        utxo.script_pubkey,
                        utxo.height,
                        1 if utxo.coinbase else 0,
                    ),
                )

    def run_startup_checks(self) -> None:
        self._ensure_open()
        with self._lock:
            result = self._conn.execute("PRAGMA quick_check").fetchone()
            if not result or result[0] != "ok":
                raise StateDBError(f"quick_check failed: {result[0] if result else 'unknown'}")
            tip = self.get_best_tip()
            if tip:
                best_hash, _ = tip
                header = self.get_header(best_hash)
                if header is None:
                    raise StateDBError("Best tip header missing from headers table")
            self._conn.execute("PRAGMA wal_checkpoint(PASSIVE)")

    def vacuum(self) -> None:
        self._ensure_open()
        with self._lock:
            self._conn.execute("VACUUM")
