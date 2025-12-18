"""
Thread-safe mempool implementation with policy enforcement.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from .core import script
from .core.chain import Chain
from .core.tx import Transaction, TxInput
from .policy import MIN_RELAY_FEE_RATE, PolicyError, check_standard_tx
from .storage import UTXORecord

OutPoint = tuple[str, int]


class MempoolError(Exception):
    pass


@dataclass
class MempoolEntry:
    tx: Transaction
    fee: int
    size: int
    time: float
    depends: set[str] = field(default_factory=set)
    peer_id: str | None = None

    @property
    def weight(self) -> int:
        return self.size * 4

    @property
    def fee_rate(self) -> float:
        return self.fee / max(1, self.weight)


@dataclass
class OrphanEntry:
    tx: Transaction
    missing: set[OutPoint]
    indexed_keys: set[OutPoint]
    peer_id: str | None
    time: float


class Mempool:
    def __init__(
        self,
        chain: Chain,
        *,
        max_weight: int = 5_000_000,
        max_orphans: int = 100,
        min_fee_rate: int = MIN_RELAY_FEE_RATE,
        listeners: Sequence[Callable[[Transaction], None]] | None = None,
    ):
        self.chain = chain
        self.log = logging.getLogger("baseline.mempool")
        self.max_weight = max_weight
        self.max_orphans = max_orphans
        self.min_fee_rate = min_fee_rate
        self.entries: dict[str, MempoolEntry] = {}
        self.dep_index: dict[str, set[str]] = {}
        self.orphans: dict[str, OrphanEntry] = {}
        self.orphan_index: dict[OutPoint, set[str]] = {}
        self.spent_outpoints: set[OutPoint] = set()
        self.total_weight = 0
        self.lock = threading.RLock()
        self.peer_stats: dict[str, tuple[float, float]] = {}
        self.peer_quota_bytes = 500_000.0
        self.peer_refill_per_sec = self.peer_quota_bytes / 60.0
        self._listeners: list[Callable[[Transaction], None]] = []
        if listeners:
            for listener in listeners:
                self.register_listener(listener)

    def register_listener(self, callback: Callable[[Transaction], None]) -> None:
        self._listeners.append(callback)

    def _notify_new_tx(self, tx: Transaction) -> None:
        for cb in list(self._listeners):
            try:
                cb(tx)
            except Exception:
                self.log.exception("Mempool listener failed")

    def transaction_ids(self) -> list[str]:
        with self.lock:
            return list(self.entries.keys())

    def get(self, txid: str) -> Transaction | None:
        with self.lock:
            entry = self.entries.get(txid)
            return entry.tx if entry else None

    def accept_transaction(self, tx: Transaction, *, peer_id: str | None = None, propagate: bool = True) -> dict[str, int | str]:
        txid = tx.txid()
        size = len(tx.serialize())
        now = time.time()
        with self.lock:
            if txid in self.entries:
                raise MempoolError("Transaction already in mempool")
            if tx.is_coinbase():
                raise MempoolError("Coinbase transactions are invalid in mempool")
            self._rate_limit_peer(peer_id, size)
            result = self._validate_and_insert(tx, txid, size, now, peer_id)
        if result.get("status") == "accepted":
            self._notify_new_tx(tx)
            if propagate:
                self._process_new_outputs(txid, len(tx.outputs))
        return result

    def _validate_and_insert(
        self,
        tx: Transaction,
        txid: str,
        size: int,
        now: float,
        peer_id: str | None,
    ) -> dict[str, int | str | list[OutPoint]]:
        missing: set[OutPoint] = set()
        depends: set[str] = set()
        total_input = 0
        best = self.chain.state_db.get_best_tip()
        next_height = (best[1] + 1) if best else 0
        for vin in tx.inputs:
            outpoint = (vin.prev_txid, vin.prev_vout)
            if outpoint in self.spent_outpoints:
                raise MempoolError("Input already spent by mempool entry")
            fetched = self._fetch_utxo(vin)
            if fetched is None:
                missing.add(outpoint)
                continue
            value, script_pubkey, height, coinbase, from_mempool = fetched
            if coinbase:
                maturity = self.chain.config.mining.coinbase_maturity
                if next_height - height < maturity:
                    raise MempoolError("Coinbase input not matured")
            total_input += value
            if from_mempool:
                depends.add(vin.prev_txid)
        if missing:
            self._add_orphan(tx, missing, peer_id, now)
            return {"status": "orphan", "missing": list(missing)}
        total_output = sum(out.value for out in tx.outputs)
        fee = total_input - total_output
        if fee < 0:
            raise MempoolError("Transaction spends more than inputs")
        try:
            check_standard_tx(tx, fee, size, self.min_fee_rate)
        except PolicyError as exc:
            raise MempoolError(str(exc)) from exc
        self._verify_scripts(tx)
        entry = MempoolEntry(tx=tx, fee=fee, size=size, time=now, depends=depends, peer_id=peer_id)
        self.entries[txid] = entry
        for dep in depends:
            self.dep_index.setdefault(dep, set()).add(txid)
        for vin in tx.inputs:
            self.spent_outpoints.add((vin.prev_txid, vin.prev_vout))
        self.total_weight += entry.weight
        self._evict_if_needed()
        return {"status": "accepted", "txid": txid, "fee": fee, "size": size}

    def contains(self, txid: str) -> bool:
        with self.lock:
            return txid in self.entries

    def entry(self, txid: str) -> MempoolEntry | None:
        with self.lock:
            return self.entries.get(txid)

    def _verify_scripts(self, tx: Transaction) -> None:
        for idx, txin in enumerate(tx.inputs):
            utxo = self._lookup_output(txin.prev_txid, txin.prev_vout)
            if utxo is None:
                raise MempoolError("Missing UTXO during script verification")
            ctx = script.ExecutionContext(transaction=tx, input_index=idx)
            if not script.run_script(txin.script_sig, utxo.script_pubkey, ctx):
                raise MempoolError("Script validation failed")

    def _fetch_utxo(self, txin: TxInput) -> tuple[int, bytes, int, bool, bool] | None:
        entry = self.entries.get(txin.prev_txid)
        if entry:
            if txin.prev_vout >= len(entry.tx.outputs):
                raise MempoolError("Referenced output index out of range")
            out = entry.tx.outputs[txin.prev_vout]
            return out.value, out.script_pubkey, 0, False, True
        utxo = self.chain.state_db.get_utxo(txin.prev_txid, txin.prev_vout)
        if utxo is None:
            return None
        return utxo.amount, utxo.script_pubkey, utxo.height, utxo.coinbase, False

    def _lookup_output(self, txid: str, vout: int) -> UTXORecord | None:
        entry = self.entries.get(txid)
        if entry:
            if vout >= len(entry.tx.outputs):
                return None
            txout = entry.tx.outputs[vout]
            return UTXORecord(
                txid=txid,
                vout=vout,
                amount=txout.value,
                script_pubkey=txout.script_pubkey,
                height=0,
                coinbase=False,
            )
        return self.chain.state_db.get_utxo(txid, vout)

    def _process_new_outputs(self, txid: str, output_count: int) -> None:
        queue: list[OutPoint] = [(txid, i) for i in range(output_count)]
        while queue:
            key = queue.pop()
            ready = self._collect_ready_orphans(key)
            for orphan in ready:
                try:
                    result = self.accept_transaction(orphan.tx, peer_id=orphan.peer_id, propagate=False)
                except MempoolError as exc:
                    self.log.warning("Dropping orphan %s: %s", orphan.tx.txid(), exc)
                    continue
                if result.get("status") == "accepted":
                    queue.extend((orphan.tx.txid(), idx) for idx in range(len(orphan.tx.outputs)))

    def _collect_ready_orphans(self, key: OutPoint) -> list[OrphanEntry]:
        ready: list[OrphanEntry] = []
        with self.lock:
            bucket = self.orphan_index.get(key)
            if not bucket:
                return ready
            for txid in list(bucket):
                bucket.discard(txid)
                orphan = self.orphans.get(txid)
                if not orphan:
                    continue
                orphan.indexed_keys.discard(key)
                orphan.missing.discard(key)
                if not orphan.missing:
                    removed = self._remove_orphan(txid)
                    if removed:
                        ready.append(removed)
            if not bucket:
                self.orphan_index.pop(key, None)
        return ready

    def _remove_orphan(self, txid: str) -> OrphanEntry | None:
        orphan = self.orphans.pop(txid, None)
        if orphan is None:
            return None
        for key in list(orphan.indexed_keys):
            bucket = self.orphan_index.get(key)
            if bucket:
                bucket.discard(txid)
                if not bucket:
                    self.orphan_index.pop(key, None)
        return orphan

    def _add_orphan(self, tx: Transaction, missing: set[OutPoint], peer_id: str | None, now: float) -> None:
        txid = tx.txid()
        orphan = OrphanEntry(tx=tx, missing=set(missing), indexed_keys=set(missing), peer_id=peer_id, time=now)
        self.orphans[txid] = orphan
        for key in missing:
            self.orphan_index.setdefault(key, set()).add(txid)
        if len(self.orphans) > self.max_orphans:
            self._evict_orphan()

    def _evict_orphan(self) -> None:
        if not self.orphans:
            return
        oldest_txid = min(self.orphans.items(), key=lambda item: item[1].time)[0]
        self._remove_orphan(oldest_txid)

    def _evict_if_needed(self) -> None:
        while self.total_weight > self.max_weight and self.entries:
            txid, entry = min(self.entries.items(), key=lambda item: item[1].fee_rate)
            self.log.info("Evicting low feerate tx %s", txid)
            self._drop_entry(txid)

    def _drop_entry(self, txid: str) -> None:
        entry = self.entries.pop(txid, None)
        if entry is None:
            return
        self.total_weight -= entry.weight
        for txin in entry.tx.inputs:
            self.spent_outpoints.discard((txin.prev_txid, txin.prev_vout))
        parents = entry.depends
        for parent in parents:
            children = self.dep_index.get(parent)
            if children:
                children.discard(txid)
                if not children:
                    self.dep_index.pop(parent, None)
        children = self.dep_index.pop(txid, set())
        for child in list(children):
            self._drop_entry(child)

    def remove_confirmed(self, transactions: Sequence[Transaction]) -> None:
        with self.lock:
            for tx in transactions:
                self._drop_entry(tx.txid())

    def _rate_limit_peer(self, peer_id: str | None, cost: int) -> None:
        if not peer_id:
            return
        now = time.time()
        tokens, updated = self.peer_stats.get(peer_id, (self.peer_quota_bytes, now))
        tokens = min(self.peer_quota_bytes, tokens + (now - updated) * self.peer_refill_per_sec)
        if tokens < cost:
            raise MempoolError("Peer submission rate exceeded")
        tokens -= cost
        self.peer_stats[peer_id] = (tokens, now)
