"""
SQLite-backed blockchain state database.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import queue
import sqlite3
import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ..core.address import address_from_script
from ..core.tx import COIN

if TYPE_CHECKING:
    from ..core.block import Block

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
    version: int = 1
    status: int = 0  # 0=main chain, 1=side chain


class StateDB:
    """Wraps a SQLite DB that tracks headers, chain tips, and the UTXO set."""

    def __init__(self, db_path: Path, *, synchronous: str = "FULL"):
        self.db_path = db_path
        self._synchronous = str(synchronous).upper()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, timeout=30, isolation_level=None, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(f"PRAGMA synchronous={self._synchronous}")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.row_factory = sqlite3.Row
        self._reader_local = threading.local()
        self._reader_lock = threading.Lock()
        self._reader_conns: set[sqlite3.Connection] = set()
        self._write_queue: queue.Queue[
            tuple[Callable[[sqlite3.Connection], Any] | None, threading.Event, list[Any]]
        ] = queue.Queue()
        self._writer_stop = threading.Event()
        self._writer_thread = threading.Thread(target=self._writer_loop, name="state-writer", daemon=True)
        self._writer_thread.start()
        self._closed = False
        self._init_schema()
        self.run_startup_checks()

    @property
    def synchronous(self) -> str:
        return self._synchronous

    def set_synchronous(self, mode: str) -> None:
        self._ensure_open()
        normalized = str(mode).upper()
        with self._lock:
            self._synchronous = normalized
            self._conn.execute(f"PRAGMA synchronous={normalized}")

    def checkpoint_wal(self, mode: str = "PASSIVE") -> None:
        self._ensure_open()
        normalized = str(mode).upper()
        with self._lock:
            self._conn.execute(f"PRAGMA wal_checkpoint({normalized})")

    def close(self) -> None:
        if not self._closed:
            # Serialize shutdown with any in-flight readers/writers so we don't
            # close the SQLite handle while another thread is mid-transaction.
            with self._lock:
                self._writer_stop.set()
                sentinel_done = threading.Event()
                self._write_queue.put((None, sentinel_done, []))
                sentinel_done.wait()
                if self._writer_thread.is_alive():
                    self._writer_thread.join(timeout=1.0)
                self._conn.close()
                self._closed = True
        self._close_readers()

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
                    version INTEGER NOT NULL DEFAULT 1,
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

                CREATE TABLE IF NOT EXISTS upgrades (
                    name TEXT PRIMARY KEY,
                    activation_height INTEGER,
                    activation_time INTEGER
                );

                CREATE TABLE IF NOT EXISTS address_history (
                    address TEXT NOT NULL,
                    txid TEXT NOT NULL,
                    vout INTEGER NOT NULL,
                    amount INTEGER NOT NULL,
                    height INTEGER NOT NULL,
                    PRIMARY KEY(address, txid, vout)
                );
                CREATE INDEX IF NOT EXISTS address_history_addr_height_idx
                    ON address_history(address, height);

                CREATE TABLE IF NOT EXISTS address_utxos (
                    address TEXT NOT NULL,
                    txid TEXT NOT NULL,
                    vout INTEGER NOT NULL,
                    amount INTEGER NOT NULL,
                    height INTEGER NOT NULL,
                    script_pubkey BLOB NOT NULL,
                    PRIMARY KEY(txid, vout)
                );
                CREATE INDEX IF NOT EXISTS address_utxos_address_idx
                    ON address_utxos(address);
                CREATE TABLE IF NOT EXISTS tx_index (
                    txid TEXT PRIMARY KEY,
                    block_hash TEXT NOT NULL,
                    height INTEGER NOT NULL,
                    position INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS tx_index_height_idx ON tx_index(height);
                CREATE TABLE IF NOT EXISTS block_metrics (
                    hash TEXT PRIMARY KEY,
                    height INTEGER NOT NULL,
                    tx_count INTEGER NOT NULL,
                    timestamp INTEGER NOT NULL,
                    total_fee INTEGER NOT NULL,
                    total_weight INTEGER NOT NULL,
                    total_size INTEGER NOT NULL,
                    cumulative_tx INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS block_metrics_height_idx ON block_metrics(height);
                """
            )

    def _ensure_open(self) -> None:
        if self._closed:
            raise StateDBError("StateDB connection is closed")

    def _reader_conn(self) -> sqlite3.Connection:
        self._ensure_open()
        conn = getattr(self._reader_local, "conn", None)
        if conn is not None:
            return conn
        conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA synchronous={self._synchronous}")
        conn.execute("PRAGMA foreign_keys=ON")
        with self._reader_lock:
            self._reader_conns.add(conn)
        self._reader_local.conn = conn
        return conn

    def _close_readers(self) -> None:
        with self._reader_lock:
            conns = list(self._reader_conns)
            self._reader_conns.clear()
        for conn in conns:
            with contextlib.suppress(Exception):
                conn.close()

    def transaction(self) -> contextlib.AbstractContextManager[sqlite3.Connection]:
        class _WriteContext:
            def __init__(self, outer: StateDB):
                self.outer = outer
                self.conn: sqlite3.Connection | None = None

            def __enter__(self) -> sqlite3.Connection:
                self.outer._ensure_open()
                self.outer._lock.acquire()
                self.conn = self.outer._conn
                return self.conn

            def __exit__(self, exc_type, exc, tb) -> bool:
                conn = self.conn
                self.conn = None
                try:
                    if conn is not None:
                        if exc_type:
                            conn.rollback()
                        else:
                            conn.commit()
                finally:
                    self.outer._lock.release()
                return False

        return _WriteContext(self)

    def set_meta(self, key: str, value: str) -> None:
        self._ensure_open()
        self._enqueue_write(
            lambda conn: conn.execute(
                "INSERT INTO meta(key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
        )

    def get_meta(self, key: str, default: str | None = None) -> str | None:
        self._ensure_open()
        conn = self._reader_conn()
        row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        if row is None:
            return default
        return row["value"]

    def store_header(self, header: HeaderData) -> None:
        self._ensure_open()
        self._enqueue_write(
            lambda conn: conn.execute(
                """
                INSERT INTO headers(hash, prev_hash, height, bits, nonce, timestamp, merkle_root, chainwork, version, status)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(hash) DO UPDATE SET
                    prev_hash=excluded.prev_hash,
                    height=excluded.height,
                    bits=excluded.bits,
                    nonce=excluded.nonce,
                    timestamp=excluded.timestamp,
                    merkle_root=excluded.merkle_root,
                    chainwork=excluded.chainwork,
                    version=excluded.version,
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
                    header.version,
                    header.status,
                ),
            )
        )
        # Only main-chain headers should ever move the node's active tip.
        # Side-chain (header-sync) data must not overwrite best_hash/best_height.
        if int(getattr(header, "status", 0) or 0) == 0:
            try:
                self._maybe_promote_best_tip(header.hash, int(header.height), str(header.chainwork))
            except Exception:  # noqa: S110
                pass

    def _maybe_promote_best_tip(self, new_hash: str, new_height: int, new_chainwork: str) -> None:
        """Promote meta.best_* if the new MAIN-chain header is heavier.

        Important: this must only be called for main-chain headers (status=0).
        """
        self._ensure_open()
        new_work = int(new_chainwork or 0)

        def _write(conn: sqlite3.Connection) -> None:
            # Prefer meta.best_work if present (keeps the trio consistent).
            row = conn.execute("SELECT value FROM meta WHERE key='best_work'").fetchone()
            cur_work = -1
            if row and row[0] is not None:
                try:
                    cur_work = int(row[0])
                except Exception:
                    cur_work = -1
            else:
                # Fallback: derive from current best_hash header.
                row2 = conn.execute("SELECT value FROM meta WHERE key='best_hash'").fetchone()
                cur_best_hash = row2[0] if row2 else None
                if cur_best_hash:
                    r3 = conn.execute(
                        "SELECT chainwork FROM headers WHERE hash=? AND status=0",
                        (cur_best_hash,),
                    ).fetchone()
                    if r3 and r3[0] is not None:
                        try:
                            cur_work = int(r3[0])
                        except Exception:
                            cur_work = -1

            if new_work <= cur_work:
                return

            conn.execute(
                "INSERT INTO meta(key, value) VALUES('best_hash', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (new_hash,),
            )
            conn.execute(
                "INSERT INTO meta(key, value) VALUES('best_height', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(int(new_height)),),
            )
            conn.execute(
                "INSERT INTO meta(key, value) VALUES('best_work', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(int(new_work)),),
            )

        self._enqueue_write(_write)

    def get_header(self, block_hash: str) -> HeaderData | None:
        self._ensure_open()
        conn = self._reader_conn()
        row = conn.execute(
            "SELECT hash, prev_hash, height, bits, nonce, timestamp, merkle_root, chainwork, version, status "
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
            version=row["version"],
            status=row["status"],
        )

    def store_headers_bulk(self, headers: Sequence[HeaderData]) -> None:
        """Insert/update many headers in ONE write job (single SQLite transaction)."""
        self._ensure_open()
        if not headers:
            return

        sql = """
        INSERT INTO headers(hash, prev_hash, height, bits, nonce, timestamp, merkle_root, chainwork, version, status)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(hash) DO UPDATE SET
            prev_hash=excluded.prev_hash,
            height=excluded.height,
            bits=excluded.bits,
            nonce=excluded.nonce,
            timestamp=excluded.timestamp,
            merkle_root=excluded.merkle_root,
            chainwork=excluded.chainwork,
            version=excluded.version,
            status=excluded.status
        """

        rows = [
            (
                h.hash,
                h.prev_hash,
                int(h.height),
                int(h.bits),
                int(h.nonce),
                int(h.timestamp),
                h.merkle_root,
                str(h.chainwork),
                int(h.version),
                int(h.status),
            )
            for h in headers
        ]

        # Only main-chain headers may move the active best tip.
        main_candidates = [h for h in headers if int(getattr(h, "status", 0) or 0) == 0]
        best_h = max(main_candidates, key=lambda h: int(getattr(h, "chainwork", 0) or 0)) if main_candidates else None
        best_hash = best_h.hash if best_h else None
        best_height = int(best_h.height) if best_h else None
        best_work = int(getattr(best_h, "chainwork", 0) or 0) if best_h else None

        def _write(conn: sqlite3.Connection) -> None:
            conn.executemany(sql, rows)

            if best_hash is None:
                return
            # Compare against meta.best_work (if present) and promote if heavier.
            row = conn.execute("SELECT value FROM meta WHERE key='best_work'").fetchone()
            cur_work = -1
            if row and row[0] is not None:
                try:
                    cur_work = int(row[0])
                except Exception:
                    cur_work = -1

            if int(best_work or 0) > cur_work:
                conn.execute(
                    "INSERT INTO meta(key, value) VALUES('best_hash', ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (best_hash,),
                )
                conn.execute(
                    "INSERT INTO meta(key, value) VALUES('best_height', ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (str(int(best_height or 0)),),
                )
                conn.execute(
                    "INSERT INTO meta(key, value) VALUES('best_work', ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (str(int(best_work or 0)),),
                )

        self._enqueue_write(_write)


    def set_best_tip(self, block_hash: str, height: int) -> None:
        self._ensure_open()
        def _update(conn: sqlite3.Connection) -> None:
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
        self._enqueue_write(_update)

    def reset_headers_to_genesis(self, genesis_hash: str) -> None:
        """Drop cached headers beyond genesis and reset tip metadata."""
        self._ensure_open()
        with self.transaction() as conn:
            # Keep the genesis header, drop everything else.
            conn.execute("DELETE FROM headers WHERE hash != ?", (genesis_hash,))
            conn.execute("DELETE FROM chain_tips WHERE hash != ?", (genesis_hash,))

            # Re-anchor metadata to genesis.
            row = conn.execute(
                "SELECT height, chainwork FROM headers WHERE hash=?",
                (genesis_hash,),
            ).fetchone()
            height = int(row["height"]) if row else 0
            chainwork = str(row["chainwork"]) if row else "0"
            conn.execute(
                "INSERT INTO meta(key, value) VALUES('best_hash', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (genesis_hash,),
            )
            conn.execute(
                "INSERT INTO meta(key, value) VALUES('best_height', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(height),),
            )
            conn.execute(
                "INSERT INTO meta(key, value) VALUES('best_work', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (chainwork,),
            )

    def get_best_tip(self) -> tuple[str, int] | None:
        self._ensure_open()
        best_hash = self.get_meta("best_hash")
        best_height = self.get_meta("best_height")
        if best_hash is None or best_height is None:
            return None
        return best_hash, int(best_height)

    def upsert_chain_tip(self, block_hash: str, height: int, work: str) -> None:
        self._ensure_open()
        self._enqueue_write(
            lambda conn: conn.execute(
                "INSERT INTO chain_tips(hash, height, work) VALUES (?, ?, ?) "
                "ON CONFLICT(hash) DO UPDATE SET height=excluded.height, work=excluded.work",
                (block_hash, height, work),
            )
        )

    def remove_chain_tip(self, block_hash: str) -> None:
        self._ensure_open()
        self._enqueue_write(lambda conn: conn.execute("DELETE FROM chain_tips WHERE hash=?", (block_hash,)))

    def list_chain_tips(self) -> list[tuple[str, int, str]]:
        self._ensure_open()
        conn = self._reader_conn()
        rows = conn.execute(
            "SELECT hash, height, work FROM chain_tips ORDER BY height DESC"
        ).fetchall()
        return [(row["hash"], row["height"], row["work"]) for row in rows]

    def set_header_status(self, block_hash: str, status: int) -> None:
        self._ensure_open()
        self._enqueue_write(lambda conn: conn.execute("UPDATE headers SET status=? WHERE hash=?", (status, block_hash)))

    def get_header_status(self, block_hash: str) -> int | None:
        self._ensure_open()
        conn = self._reader_conn()
        row = conn.execute("SELECT status FROM headers WHERE hash=?", (block_hash,)).fetchone()
        if row is None:
            return None
        return row["status"]

    def get_main_header_at_height(self, height: int) -> HeaderData | None:
        self._ensure_open()
        conn = self._reader_conn()
        row = conn.execute(
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
        self._enqueue_write(
            lambda conn: conn.execute(
                "INSERT INTO block_undo(hash, data) VALUES(?, ?) "
                "ON CONFLICT(hash) DO UPDATE SET data=excluded.data",
                (block_hash, payload),
            )
        )

    def load_undo_data(self, block_hash: str) -> list[UTXORecord]:
        self._ensure_open()
        conn = self._reader_conn()
        row = conn.execute("SELECT data FROM block_undo WHERE hash=?", (block_hash,)).fetchone()
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
            self._upsert_address_utxo(conn, record)

    def remove_utxo(self, txid: str, vout: int) -> bool:
        self._ensure_open()
        with self.transaction() as conn:
            cur = conn.execute("DELETE FROM utxos WHERE txid=? AND vout=?", (txid, vout))
            conn.execute("DELETE FROM address_utxos WHERE txid=? AND vout=?", (txid, vout))
            return cur.rowcount > 0

    def get_utxo(self, txid: str, vout: int) -> UTXORecord | None:
        self._ensure_open()
        conn = self._reader_conn()
        row = conn.execute(
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
        conn = self._reader_conn()
        rows = conn.execute(query, scripts).fetchall()
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

    def get_utxo_set_stats(self) -> dict[str, Any]:
        """Aggregate statistics for gettxoutsetinfo."""
        self._ensure_open()
        total_amount = 0
        txouts = 0
        hasher = hashlib.sha256()
        conn = self._reader_conn()
        cursor = conn.execute("SELECT txid, vout, amount, script_pubkey FROM utxos ORDER BY txid, vout")
        for row in cursor:
            txouts += 1
            amount = row["amount"]
            total_amount += amount
            txid = row["txid"]
            vout = row["vout"]
            script_pubkey = row["script_pubkey"]
            hasher.update(txid.encode("utf-8"))
            hasher.update(vout.to_bytes(4, "little"))
            if isinstance(script_pubkey, bytes):
                hasher.update(script_pubkey)
            else:
                hasher.update(bytes(script_pubkey))
        transactions_row = conn.execute("SELECT COUNT(DISTINCT txid) AS cnt FROM utxos").fetchone()
        best = self.get_best_tip()
        best_hash = best[0] if best else "00" * 32
        height = best[1] if best else 0
        total_coins = total_amount / COIN
        stats = {
            "height": height,
            "bestblock": best_hash,
            "transactions": transactions_row["cnt"] if transactions_row else 0,
            "txouts": txouts,
            "bogosize": txouts * 50,
            "hash_serialized_2": hasher.hexdigest(),
            "muhash": hasher.hexdigest(),
            "disk_size": self.db_path.stat().st_size if self.db_path.exists() else 0,
            "total_amount": total_coins,
            "total_unspendable_amount": 0,
        }
        return stats

    def apply_utxo_changes(
        self,
        spent: Sequence[tuple[str, int]],
        created: Sequence[UTXORecord],
    ) -> None:
        self._ensure_open()
        with self.transaction() as conn:
            for txid, vout in spent:
                conn.execute("DELETE FROM utxos WHERE txid=? AND vout=?", (txid, vout))
                conn.execute("DELETE FROM address_utxos WHERE txid=? AND vout=?", (txid, vout))
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
                self._upsert_address_utxo(conn, utxo)

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

    # Address index helpers -----------------------------------------------------

    def index_block_addresses(self, block: Block, height: int) -> None:
        self._ensure_open()
        entries: list[tuple[str, str, int, int, int]] = []
        for tx in block.transactions:
            txid = tx.txid()
            for vout, txout in enumerate(tx.outputs):
                address = self._address_from_script(txout.script_pubkey)
                if not address:
                    continue
                entries.append((address, txid, vout, txout.value, height))
        if not entries:
            return
        def _write(conn: sqlite3.Connection) -> None:
            for address, txid, vout, amount, h in entries:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO address_history(address, txid, vout, amount, height)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (address, txid, vout, amount, h),
                )
        self._enqueue_write(_write)

    def remove_block_address_index(self, block: Block) -> None:
        self._ensure_open()
        txids = [tx.txid() for tx in block.transactions]
        if not txids:
            return
        def _write(conn: sqlite3.Connection) -> None:
            for txid in txids:
                conn.execute("DELETE FROM address_history WHERE txid=?", (txid,))
        self._enqueue_write(_write)

    def get_address_utxos(self, addresses: Sequence[str], *, limit: int | None = None, offset: int = 0) -> list[dict[str, Any]]:
        self._ensure_open()
        if not addresses:
            return []
        placeholders = ",".join("?" for _ in addresses)
        query = (
            "SELECT address, txid, vout, amount, height, script_pubkey "
            f"FROM address_utxos WHERE address IN ({placeholders}) ORDER BY height DESC, txid DESC, vout DESC"
        )
        params: list[Any] = list(addresses)
        if limit is not None:
            query += " LIMIT ? OFFSET ?"
            params.extend([int(limit), int(offset)])
        conn = self._reader_conn()
        rows = conn.execute(query, tuple(params)).fetchall()
        return [
            {
                "address": row["address"],
                "txid": row["txid"],
                "vout": row["vout"],
                "amount": row["amount"],
                "height": row["height"],
                "script_pubkey": row["script_pubkey"],
            }
            for row in rows
        ]

    def get_address_balance(
        self,
        addresses: Sequence[str],
        *,
        tip_height: int | None = None,
        maturity: int | None = None,
    ) -> tuple[int, int, int, int]:
        self._ensure_open()
        if not addresses:
            return 0, 0, 0, 0
        placeholders = ",".join("?" for _ in addresses)
        params = tuple(addresses)
        conn = self._reader_conn()
        rows = conn.execute(
            f"""
            SELECT au.amount AS amount, au.height AS height, COALESCE(u.coinbase, 0) AS coinbase
            FROM address_utxos au
            LEFT JOIN utxos u ON au.txid = u.txid AND au.vout = u.vout
            WHERE au.address IN ({placeholders})
            """,
            params,
        ).fetchall()
        received = conn.execute(
            f"SELECT COALESCE(SUM(amount), 0) as total FROM address_history WHERE address IN ({placeholders})",
            params,
        ).fetchone()["total"]
        balance = sum(int(row["amount"]) for row in rows)
        matured = balance
        immature = 0
        if tip_height is not None and maturity is not None:
            matured = 0
            immature = 0
            for row in rows:
                amount = int(row["amount"])
                is_coinbase = bool(row["coinbase"])
                height = int(row["height"])
                if is_coinbase and tip_height - height < maturity:
                    immature += amount
                else:
                    matured += amount
        return int(balance or 0), int(received or 0), int(matured or 0), int(immature or 0)

    def get_address_txids(
        self,
        addresses: Sequence[str],
        start: int | None = None,
        end: int | None = None,
        *,
        include_height: bool = False,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[Any]:
        self._ensure_open()
        if not addresses:
            return []
        placeholders = ",".join("?" for _ in addresses)
        params: list[Any] = list(addresses)
        query = (
            "SELECT txid, MIN(height) as height FROM address_history "
            f"WHERE address IN ({placeholders})"
        )
        if start is not None:
            query += " AND height >= ?"
            params.append(int(start))
        if end is not None:
            query += " AND height <= ?"
            params.append(int(end))
        query += " GROUP BY txid ORDER BY height DESC"
        if limit is not None:
            query += " LIMIT ? OFFSET ?"
            params.extend([int(limit), int(offset)])
        conn = self._reader_conn()
        rows = conn.execute(query, tuple(params)).fetchall()
        if not include_height:
            return [row["txid"] for row in rows]
        results: list[dict[str, Any]] = []
        for row in rows:
            height = row["height"]
            header = self.get_main_header_at_height(height)
            block_hash = header.hash if header else None
            results.append({"txid": row["txid"], "height": height, "blockhash": block_hash})
        return results

    def get_rich_list(self, limit: int = 25, offset: int = 0) -> list[dict[str, Any]]:
        """Return the richest addresses by current UTXO balance.

        This derives balances from the address UTXO index (address_utxos).
        """
        self._ensure_open()
        limit = max(1, int(limit))
        offset = max(0, int(offset))
        conn = self._reader_conn()
        rows = conn.execute(
            """
            SELECT address, COALESCE(SUM(amount), 0) AS balance_liners
            FROM address_utxos
            GROUP BY address
            ORDER BY balance_liners DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        ).fetchall()
        return [{"address": row["address"], "balance_liners": int(row["balance_liners"])} for row in rows]

    # Transaction + block metrics helpers --------------------------------------

    def index_block_transactions(self, block_hash: str, height: int, txids: Sequence[str]) -> None:
        self._ensure_open()
        if not txids:
            return
        def _write(conn: sqlite3.Connection) -> None:
            for position, txid in enumerate(txids):
                conn.execute(
                    """
                    INSERT INTO tx_index(txid, block_hash, height, position)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(txid) DO UPDATE SET
                        block_hash=excluded.block_hash,
                        height=excluded.height,
                        position=excluded.position
                    """,
                    (txid, block_hash, height, position),
                )
        self._enqueue_write(_write)

    def remove_block_transactions(self, block_hash: str) -> None:
        self._ensure_open()
        self._enqueue_write(lambda conn: conn.execute("DELETE FROM tx_index WHERE block_hash=?", (block_hash,)))

    def get_transaction_location(self, txid: str) -> tuple[str, int, int] | None:
        self._ensure_open()
        conn = self._reader_conn()
        row = conn.execute(
            "SELECT block_hash, height, position FROM tx_index WHERE txid=?",
            (txid,),
        ).fetchone()
        if row is None:
            return None
        return row["block_hash"], row["height"], row["position"]

    def txindex_rebuild_start(self, *, sample_size: int = 20) -> int | None:
        self._ensure_open()
        highest = self.get_highest_main_header()
        if not highest:
            return None
        best_height = int(highest.height)
        conn = self._reader_conn()
        row = conn.execute(
            "SELECT value FROM meta WHERE key='txindex_height'",
        ).fetchone()
        stored_height = None
        if row and row["value"] is not None:
            try:
                stored_height = int(row["value"])
            except Exception:
                stored_height = None
        mismatch = self._txindex_tail_mismatch(conn, best_height, sample_size)
        if stored_height is None:
            if mismatch is None:
                self.set_meta("txindex_height", str(best_height))
                return None
            return 0
        if stored_height >= best_height and mismatch is None:
            return None
        if stored_height < best_height and mismatch is None:
            self.set_meta("txindex_height", str(best_height))
            return None
        if stored_height < best_height:
            return max(0, stored_height + 1)
        return 0

    def _txindex_tail_mismatch(
        self,
        conn: sqlite3.Connection,
        best_height: int,
        sample_size: int,
    ) -> int | None:
        sample = max(1, int(sample_size))
        start = max(0, int(best_height) - sample + 1)
        for height in range(int(best_height), start - 1, -1):
            row = conn.execute(
                "SELECT hash FROM headers WHERE height=? AND status=0 LIMIT 1",
                (height,),
            ).fetchone()
            if not row:
                return height
            block_hash = row["hash"]
            metrics = conn.execute(
                "SELECT tx_count FROM block_metrics WHERE hash=? LIMIT 1",
                (block_hash,),
            ).fetchone()
            if metrics:
                expected = int(metrics["tx_count"])
                existing = conn.execute(
                    "SELECT COUNT(1) AS cnt FROM tx_index WHERE block_hash=?",
                    (block_hash,),
                ).fetchone()
                if not existing or int(existing["cnt"]) != expected:
                    return height
            else:
                existing = conn.execute(
                    "SELECT 1 FROM tx_index WHERE block_hash=? LIMIT 1",
                    (block_hash,),
                ).fetchone()
                if not existing:
                    return height
        return None

    def rebuild_tx_index_from_blocks(
        self,
        block_store,
        *,
        start_height: int = 0,
        end_height: int | None = None,
    ) -> tuple[int, int]:
        """Backfill tx_index by walking main-chain blocks."""
        from ..core.block import Block

        self._ensure_open()
        if end_height is None:
            highest = self.get_highest_main_header()
            if not highest:
                return 0, 0
            end_height = highest.height
        if end_height < start_height:
            return 0, 0
        blocks_indexed = 0
        txs_indexed = 0
        last_height = None
        with self.transaction() as conn:
            for height in range(int(start_height), int(end_height) + 1):
                row = conn.execute(
                    "SELECT hash FROM headers WHERE height=? AND status=0 LIMIT 1",
                    (height,),
                ).fetchone()
                if not row:
                    break
                block_hash = row["hash"]
                metrics = conn.execute(
                    "SELECT tx_count FROM block_metrics WHERE hash=? LIMIT 1",
                    (block_hash,),
                ).fetchone()
                if metrics:
                    expected = int(metrics["tx_count"])
                    existing = conn.execute(
                        "SELECT COUNT(1) AS cnt FROM tx_index WHERE block_hash=?",
                        (block_hash,),
                    ).fetchone()
                    if existing and int(existing["cnt"]) == expected:
                        last_height = height
                        continue
                    conn.execute("DELETE FROM tx_index WHERE block_hash=?", (block_hash,))
                else:
                    existing = conn.execute(
                        "SELECT 1 FROM tx_index WHERE block_hash=? LIMIT 1",
                        (block_hash,),
                    ).fetchone()
                    if existing:
                        last_height = height
                        continue
                raw = block_store.get_block(block_hash)
                block = Block.parse(raw)
                txids = [tx.txid() for tx in block.transactions]
                if not txids:
                    continue
                rows = [
                    (txid, block_hash, height, position)
                    for position, txid in enumerate(txids)
                ]
                conn.executemany(
                    """
                    INSERT INTO tx_index(txid, block_hash, height, position)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(txid) DO UPDATE SET
                        block_hash=excluded.block_hash,
                        height=excluded.height,
                        position=excluded.position
                    """,
                    rows,
                )
                blocks_indexed += 1
                txs_indexed += len(txids)
                last_height = height
            if last_height is not None:
                conn.execute(
                    "INSERT INTO meta(key, value) VALUES('txindex_height', ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (str(int(last_height)),),
                )
        return blocks_indexed, txs_indexed

    def record_block_metrics(
        self,
        block_hash: str,
        height: int,
        timestamp: int,
        tx_count: int,
        *,
        total_fee: int,
        total_weight: int,
        total_size: int,
    ) -> None:
        self._ensure_open()
        prev_cumulative = 0
        if height > 0:
            conn = self._reader_conn()
            row = conn.execute(
                "SELECT cumulative_tx FROM block_metrics WHERE height=?",
                (height - 1,),
            ).fetchone()
            if row:
                prev_cumulative = int(row["cumulative_tx"])
        cumulative_tx = prev_cumulative + tx_count
        self._enqueue_write(
            lambda conn: conn.execute(
                """
                INSERT INTO block_metrics(hash, height, tx_count, timestamp, total_fee, total_weight, total_size, cumulative_tx)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(hash) DO UPDATE SET
                    height=excluded.height,
                    tx_count=excluded.tx_count,
                    timestamp=excluded.timestamp,
                    total_fee=excluded.total_fee,
                    total_weight=excluded.total_weight,
                    total_size=excluded.total_size,
                    cumulative_tx=excluded.cumulative_tx
                """,
                (block_hash, height, tx_count, timestamp, total_fee, total_weight, total_size, cumulative_tx),
            )
        )

    def remove_block_metrics(self, block_hash: str) -> None:
        self._ensure_open()
        self._enqueue_write(lambda conn: conn.execute("DELETE FROM block_metrics WHERE hash=?", (block_hash,)))

    def get_block_metrics_by_height(self, height: int) -> dict[str, Any] | None:
        self._ensure_open()
        conn = self._reader_conn()
        row = conn.execute(
            """
            SELECT hash, height, tx_count, timestamp, total_fee, total_weight, total_size, cumulative_tx
            FROM block_metrics WHERE height=?
            """,
            (height,),
        ).fetchone()
        if row is None:
            return None
        return dict(row)

    def get_block_metrics_by_hash(self, block_hash: str) -> dict[str, Any] | None:
        self._ensure_open()
        conn = self._reader_conn()
        row = conn.execute(
            """
            SELECT hash, height, tx_count, timestamp, total_fee, total_weight, total_size, cumulative_tx
            FROM block_metrics WHERE hash=?
            """,
            (block_hash,),
        ).fetchone()
        if row is None:
            return None
        return dict(row)

    def get_cumulative_tx(self, height: int) -> int:
        metrics = self.get_block_metrics_by_height(height)
        if not metrics:
            return 0
        return int(metrics["cumulative_tx"])

    def _upsert_address_utxo(self, conn: sqlite3.Connection, record: UTXORecord) -> None:
        address = self._address_from_script(record.script_pubkey)
        if not address:
            return
        conn.execute(
            """
            INSERT INTO address_utxos(address, txid, vout, amount, height, script_pubkey)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(txid, vout) DO UPDATE SET
                address=excluded.address,
                amount=excluded.amount,
                height=excluded.height,
                script_pubkey=excluded.script_pubkey
            """,
            (
                address,
                record.txid,
                record.vout,
                record.amount,
                record.height,
                record.script_pubkey,
            ),
        )

    def _address_from_script(self, script: bytes) -> str | None:
        try:
            return address_from_script(script)
        except Exception:
            return None

    # Upgrade tracking methods
    def set_upgrade_activation_height(self, upgrade_name: str, height: int) -> None:
        """Record the activation height of an upgrade."""
        self._ensure_open()
        import time
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO upgrades(name, activation_height, activation_time) VALUES (?, ?, ?) "
                "ON CONFLICT(name) DO UPDATE SET "
                "activation_height=excluded.activation_height, "
                "activation_time=excluded.activation_time",
                (upgrade_name, height, int(time.time())),
            )

    def get_upgrade_activation_height(self, upgrade_name: str) -> int | None:
        """Get the activation height of an upgrade."""
        self._ensure_open()
        conn = self._reader_conn()
        row = conn.execute(
            "SELECT activation_height FROM upgrades WHERE name=?",
            (upgrade_name,)
        ).fetchone()
        return row["activation_height"] if row else None

    def get_header_by_height(self, height: int) -> HeaderData | None:
        """Get header by height (assumes main chain)."""
        self._ensure_open()
        conn = self._reader_conn()
        row = conn.execute(
            "SELECT hash, prev_hash, height, bits, nonce, timestamp, merkle_root, chainwork, version, status "
            "FROM headers WHERE height=? AND status=0 LIMIT 1",
            (height,)
        ).fetchone()
        if not row:
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
            version=row["version"],
            status=row["status"],
        )

    def get_headers_range(self, start_height: int, end_height: int) -> list[HeaderData]:
        """Get headers for main chain heights [start_height, end_height] inclusive."""
        self._ensure_open()
        if end_height < start_height:
            return []
        conn = self._reader_conn()
        rows = conn.execute(
            "SELECT hash, prev_hash, height, bits, nonce, timestamp, merkle_root, chainwork, version, status "
            "FROM headers WHERE status=0 AND height BETWEEN ? AND ? ORDER BY height ASC",
            (start_height, end_height),
        ).fetchall()
        return [
            HeaderData(
                hash=row["hash"],
                prev_hash=row["prev_hash"],
                height=row["height"],
                bits=row["bits"],
                nonce=row["nonce"],
                timestamp=row["timestamp"],
                merkle_root=row["merkle_root"],
                chainwork=row["chainwork"],
                version=row["version"],
                status=row["status"],
            )
            for row in rows
        ]

    def get_max_header_height(self) -> int:
        self._ensure_open()
        conn = self._reader_conn()
        row = conn.execute("SELECT MAX(height) AS max_height FROM headers").fetchone()
        if row is None or row["max_height"] is None:
            return 0
        return int(row["max_height"])

    def has_headers_beyond_genesis(self) -> bool:
        """Return True if any header with height > 0 exists."""
        self._ensure_open()
        conn = self._reader_conn()
        row = conn.execute("SELECT 1 FROM headers WHERE height > 0 LIMIT 1").fetchone()
        return row is not None

    def has_main_chain_gap(self) -> bool:
        """Detect if main-chain headers are missing for any height up to the max."""
        self._ensure_open()
        conn = self._reader_conn()
        row = conn.execute(
            "SELECT MAX(height) AS max_height, COUNT(*) AS cnt FROM headers WHERE status=0"
        ).fetchone()
        if row is None or row["max_height"] is None:
            return False
        max_height = int(row["max_height"])
        count = int(row["cnt"])
        return count != max_height + 1

    def rebuild_main_headers_from_blocks(self, block_store) -> int:
        """
        Rebuild missing main-chain headers using block_metrics + block_store.

        Returns the number of headers inserted.
        """
        from ..core import difficulty  # imported here to avoid cycles
        from ..core.block import Block

        self._ensure_open()
        repaired = 0
        with self.transaction() as conn:
            row = conn.execute("SELECT MAX(height) AS max_height FROM block_metrics").fetchone()
            max_height = int(row["max_height"] or 0)
            for height in range(0, max_height + 1):
                existing = conn.execute(
                    "SELECT 1 FROM headers WHERE height=? AND status=0 LIMIT 1", (height,)
                ).fetchone()
                if existing:
                    continue
                metrics = conn.execute(
                    "SELECT hash FROM block_metrics WHERE height=? LIMIT 1", (height,)
                ).fetchone()
                if not metrics:
                    break
                block_hash = metrics["hash"]
                raw = block_store.get_block(block_hash)
                block = Block.parse(raw)
                if block.block_hash() != block_hash:
                    continue
                if height == 0:
                    prev_hash = None
                    chainwork_int = difficulty.block_work(block.header.bits)
                else:
                    parent = conn.execute(
                        "SELECT chainwork, height FROM headers WHERE hash=? AND status=0",
                        (block.header.prev_hash,),
                    ).fetchone()
                    if not parent or int(parent["height"]) != height - 1:
                        break
                    chainwork_int = int(parent["chainwork"]) + difficulty.block_work(block.header.bits)
                    prev_hash = block.header.prev_hash
                conn.execute(
                    """
                    INSERT INTO headers(hash, prev_hash, height, bits, nonce, timestamp, merkle_root, chainwork, version, status)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                    ON CONFLICT(hash) DO UPDATE SET
                        prev_hash=excluded.prev_hash,
                        height=excluded.height,
                        bits=excluded.bits,
                        nonce=excluded.nonce,
                        timestamp=excluded.timestamp,
                        merkle_root=excluded.merkle_root,
                        chainwork=excluded.chainwork,
                        version=excluded.version,
                        status=excluded.status
                    """,
                    (
                        block_hash,
                        prev_hash,
                        height,
                        block.header.bits,
                        block.header.nonce,
                        block.header.timestamp,
                        block.header.merkle_root,
                        str(chainwork_int),
                        block.header.version,
                    ),
                )
                repaired += 1
        return repaired

    def get_highest_main_header(self) -> HeaderData | None:
        """Return the highest main-chain header (status=0)."""
        self._ensure_open()
        conn = self._reader_conn()
        row = conn.execute(
            "SELECT hash, prev_hash, height, bits, nonce, timestamp, merkle_root, chainwork, version, status "
            "FROM headers WHERE status=0 ORDER BY height DESC LIMIT 1"
        ).fetchone()
        if not row:
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
            version=row["version"],
            status=row["status"],
        )

    def get_highest_chainwork_header(self) -> HeaderData | None:
        """Return the header with the highest chainwork (any status)."""
        self._ensure_open()
        conn = self._reader_conn()
        row = conn.execute(
            "SELECT hash, prev_hash, height, bits, nonce, timestamp, merkle_root, chainwork, version, status "
            "FROM headers ORDER BY CAST(chainwork AS INTEGER) DESC, height DESC LIMIT 1"
        ).fetchone()
        if not row:
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
            version=row["version"],
            status=row["status"],
        )

    def reanchor_main_chain(self, tip_hash: str | None = None) -> tuple[int, int]:
        """
        Rebuild the main chain (status=0) by walking parents from the given tip.

        Returns (path_length, final_height).
        """
        self._ensure_open()
        tip = self.get_header(tip_hash) if tip_hash else self.get_highest_chainwork_header()
        if tip is None:
            return 0, 0
        path: list[HeaderData] = []
        current = tip
        while current:
            path.append(current)
            if current.prev_hash is None:
                break
            parent = self.get_header(current.prev_hash)
            if parent is None:
                break
            current = parent
        # Ensure we reached genesis
        if not path or path[-1].prev_hash is not None:
            return 0, 0
        path.reverse()  # Genesis first
        with self.transaction() as conn:
            conn.execute("UPDATE headers SET status=1")
            for idx, hdr in enumerate(path):
                conn.execute(
                    """
                    UPDATE headers
                    SET status=0, height=?
                    WHERE hash=?
                    """,
                    (idx, hdr.hash),
                )
            # Reset chain tips to this path tip
            tip_hdr = path[-1]
            conn.execute("DELETE FROM chain_tips")
            conn.execute(
                "INSERT INTO chain_tips(hash, height, work) VALUES(?, ?, ?)",
                (tip_hdr.hash, tip_hdr.height, tip_hdr.chainwork),
            )
            # Update metadata
            conn.execute(
                "INSERT INTO meta(key, value) VALUES('best_hash', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (tip_hdr.hash,),
            )
            conn.execute(
                "INSERT INTO meta(key, value) VALUES('best_height', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(tip_hdr.height),),
            )
            conn.execute(
                "INSERT INTO meta(key, value) VALUES('best_work', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (tip_hdr.chainwork,),
            )
        return len(path), path[-1].height
    def _enqueue_write(self, fn: Callable[[sqlite3.Connection], Any]) -> None:
        if self._closed:
            raise StateDBError("StateDB connection is closed")
        done = threading.Event()
        result: list[Any] = []
        self._write_queue.put((fn, done, result))
        done.wait()
        if result and isinstance(result[0], Exception):
            raise result[0]

    def _writer_loop(self) -> None:
        while not self._writer_stop.is_set():
            try:
                fn, done, result_holder = self._write_queue.get()
            except Exception:
                continue
            if fn is None:
                done.set()
                if self._writer_stop.is_set():
                    break
                continue
            try:
                with self.transaction() as conn:
                    fn(conn)
                result_holder.append(None)
            except Exception as exc:
                result_holder.append(exc)
            finally:
                done.set()
