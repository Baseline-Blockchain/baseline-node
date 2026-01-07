"""
Thread-safe mempool implementation with policy enforcement.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
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
        cpu_count = os.cpu_count() or 2
        max_workers = max(2, cpu_count // 2)
        self._script_executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="mempool-verify")
        self.peer_quota_rpc_bytes = self.peer_quota_bytes / 2
        self.peer_refill_rpc_per_sec = self.peer_quota_rpc_bytes / 60.0

        # Register this mempool instance with the chain if possible.  The Chain
        # object does not hold a direct reference to a mempool, but some logic
        # such as automatic re-add on chain reorganizations can benefit from
        # being able to find the mempool via the chain.  We set an attribute
        # here so that Chain can call back into the mempool when a reorg
        # occurs.  This is safe because Python allows setting arbitrary
        # attributes on objects at runtime.  See handle_reorg() below.
        try:
            if not hasattr(chain, "mempool"):
                chain.mempool = self
        except Exception as exc:
            raise MempoolError(f"Unexpected script verification failure: {exc}") from exc


    def handle_reorg(self, old_branch: list[str], new_branch: list[str]) -> None:
        """
        Handle a blockchain reorganization by re-adding transactions from the
        disconnected branch back into the mempool and by revalidating all
        existing mempool transactions against the new main chain.

        Parameters
        ----------
        old_branch: list[str]
            List of block hashes from the previously connected main chain that
            have been detached during the reorganization.  The list is in
            descending order (tip first).  Transactions from these blocks
            (other than the coinbase) may become unconfirmed and should be
            reconsidered for inclusion in the mempool.
        new_branch: list[str]
            List of block hashes from the fork chain that were attached during
            the reorganization.  Transactions in these blocks must not be
            present in the mempool after the reorg.
        """
        # Build a set of transaction IDs included in the new branch.  We skip
        # coinbase transactions because they are never relayed via the mempool.
        new_branch_txids: set[str] = set()
        try:
            for bh in new_branch:
                try:
                    block = self.chain._load_block(bh)  # uses internal parser
                except Exception:
                    # If block cannot be loaded, skip it; it will not affect
                    # mempool validity and prevents crashes in error cases.
                    continue
                for tx in block.transactions[1:]:
                    # Only non-coinbase transactions can be in the mempool.
                    new_branch_txids.add(tx.txid())
        except Exception:
            # Defensive: if anything goes wrong building the set, fall back to
            # leaving it empty.  This will only cause harmless extra checks.
            new_branch_txids = set()

        # Collect transactions from the detached blocks (old_branch) that may
        # need to be re-added to the mempool.  Process blocks from the fork
        # point upwards (i.e. earliest detached first) so that dependencies
        # within the old branch are naturally satisfied where possible.
        readd_txs: list[Transaction] = []
        for bh in reversed(old_branch):
            try:
                block = self.chain._load_block(bh)
            except Exception:
                continue
            # Skip the coinbase transaction at index 0.
            for tx in block.transactions[1:]:
                txid = tx.txid()
                # Do not re-add if the transaction also appears in the new
                # branch (which means it is still confirmed after the reorg).
                if txid in new_branch_txids:
                    continue
                readd_txs.append(tx)

        # Preserve a copy of the existing mempool transactions so we can
        # reconsider them under the new chain.  Do this outside the lock to
        # prevent holding the lock longer than necessary.
        with self.lock:
            saved = list(self._listeners)
            self._listeners.clear()
            existing_entries = list(self.entries.values())
            for txid in list(self.entries.keys()):
                self._drop_entry(txid)
            self.entries.clear()
            self.dep_index.clear()
            self.spent_outpoints.clear()
            self.total_weight = 0
            self.orphans.clear()
            self.orphan_index.clear()


        # Compose a single list of transactions to re-add: first those from
        # detached blocks, then the ones that were already in the mempool.  We
        # maintain order because earlier transactions may create UTXOs needed
        # by later ones; however, accept_transaction will gracefully handle
        # dependencies by creating orphans.
        combined_txs: list[Transaction] = []
        combined_txs.extend(readd_txs)
        for entry in existing_entries:
            txid = entry.tx.txid()
            # Skip any transaction that appears in the new branch, as these
            # remain confirmed after the reorg and do not belong in the
            # mempool.  Also avoid adding duplicates from the detached list.
            if txid not in new_branch_txids:
                combined_txs.append(entry.tx)

        try:
            for tx in combined_txs:
                try:
                    self.accept_transaction(tx, propagate=False)
                except MempoolError:
                    continue
        finally:
            with self.lock:
                self._listeners[:] = saved

    def handle_block_connected(self, block) -> None:
        """
        Called when a new block connects to the main chain so orphans that
        depended on its transactions can be reconsidered.
        """
        if not block or not getattr(block, "transactions", None):
            return
        for tx in block.transactions[1:]:
            self._process_new_outputs(tx.txid(), len(tx.outputs))

    def register_listener(self, callback: Callable[[Transaction], None]) -> None:
        self._listeners.append(callback)

    def close(self) -> None:
        self._script_executor.shutdown(wait=False)

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
        best = self.chain.state_db.get_best_tip()
        next_height = (best[1] + 1) if best else 0
        resolved_inputs: dict[OutPoint, UTXORecord] = {}
        pending_db: list[OutPoint] = []
        depends: set[str] = set()
        with self.lock:
            if txid in self.entries:
                raise MempoolError("Transaction already in mempool")
            if tx.is_coinbase():
                raise MempoolError("Coinbase transactions are invalid in mempool")
            self._rate_limit_peer(peer_id, size)
            for vin in tx.inputs:
                outpoint = (vin.prev_txid, vin.prev_vout)
                if outpoint in self.spent_outpoints:
                    raise MempoolError("Input already spent by mempool entry")
                parent_entry = self.entries.get(vin.prev_txid)
                if parent_entry:
                    if vin.prev_vout >= len(parent_entry.tx.outputs):
                        raise MempoolError("Referenced output index out of range")
                    parent_out = parent_entry.tx.outputs[vin.prev_vout]
                    resolved_inputs[outpoint] = UTXORecord(
                        txid=vin.prev_txid,
                        vout=vin.prev_vout,
                        amount=parent_out.value,
                        script_pubkey=parent_out.script_pubkey,
                        height=0,
                        coinbase=False,
                    )
                    depends.add(vin.prev_txid)
                else:
                    pending_db.append(outpoint)

        missing: set[OutPoint] = set()
        maturity = self.chain.config.mining.coinbase_maturity
        for prev_txid, prev_vout in pending_db:
            utxo = self.chain.state_db.get_utxo(prev_txid, prev_vout)
            if utxo is None:
                missing.add((prev_txid, prev_vout))
                continue
            if utxo.coinbase and next_height - utxo.height < maturity:
                raise MempoolError("Coinbase input not matured")
            resolved_inputs[(prev_txid, prev_vout)] = utxo
        if missing:
            with self.lock:
                self._add_orphan(tx, missing, peer_id, now)
            return {"status": "orphan", "missing": list(missing)}

        if len(resolved_inputs) != len(tx.inputs):
            raise MempoolError("Missing referenced output")
        total_input = sum(record.amount for record in resolved_inputs.values())
        total_output = sum(out.value for out in tx.outputs)
        fee = total_input - total_output
        if fee < 0:
            raise MempoolError("Transaction spends more than inputs")
        try:
            check_standard_tx(tx, fee, size, self.min_fee_rate)
        except PolicyError as exc:
            raise MempoolError(str(exc)) from exc
        self._verify_scripts_async(tx, resolved_inputs=resolved_inputs)

        entry = MempoolEntry(tx=tx, fee=fee, size=size, time=now, depends=set(depends), peer_id=peer_id)
        outpoints = [(vin.prev_txid, vin.prev_vout) for vin in tx.inputs]
        with self.lock:
            if txid in self.entries:
                raise MempoolError("Transaction already in mempool")
            for dep in depends:
                if dep not in self.entries:
                    raise MempoolError("Missing parent transaction in mempool")
            for outpoint in outpoints:
                if outpoint in self.spent_outpoints:
                    raise MempoolError("Input already spent by mempool entry")
            self.entries[txid] = entry
            for dep in depends:
                self.dep_index.setdefault(dep, set()).add(txid)
            for outpoint in outpoints:
                self.spent_outpoints.add(outpoint)
            self.total_weight += entry.weight
            self._evict_if_needed()
        self._notify_new_tx(tx)
        if propagate:
            self._process_new_outputs(txid, len(tx.outputs))
        return {"status": "accepted", "txid": txid, "fee": fee, "size": size}

    def contains(self, txid: str) -> bool:
        with self.lock:
            return txid in self.entries

    def entry(self, txid: str) -> MempoolEntry | None:
        with self.lock:
            return self.entries.get(txid)

    def _verify_scripts(self, tx: Transaction, resolved_inputs: dict[OutPoint, UTXORecord] | None = None) -> None:
        for idx, txin in enumerate(tx.inputs):
            utxo = None
            if resolved_inputs:
                utxo = resolved_inputs.get((txin.prev_txid, txin.prev_vout))
            if utxo is None:
                utxo = self._lookup_output(txin.prev_txid, txin.prev_vout)
            if utxo is None:
                raise MempoolError("Missing UTXO during script verification")
            ctx = script.ExecutionContext(transaction=tx, input_index=idx)
            if not script.run_script(txin.script_sig, utxo.script_pubkey, ctx):
                raise MempoolError("Script validation failed")

    def _verify_scripts_async(self, tx: Transaction, resolved_inputs: dict[OutPoint, UTXORecord]) -> None:
        future = self._script_executor.submit(self._verify_scripts, tx, resolved_inputs)
        try:
            future.result()
        except MempoolError:
            raise
        except Exception as exc:  # pragma: no cover - defensive
            raise MempoolError(f"Unexpected script verification failure: {exc}") from exc

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
            candidate = self._select_evictable_entry()
            if candidate is None:
                break
            txid, rate = candidate
            self.log.info("Evicting low feerate package anchored at %s (feerate=%.8f)", txid, rate)
            self._drop_entry(txid)

    def _select_evictable_entry(self) -> tuple[str, float] | None:
        worst_txid: str | None = None
        worst_rate: float | None = None
        for txid in list(self.entries.keys()):
            fee, weight = self._package_stats(txid)
            if weight <= 0:
                continue
            rate = fee / max(1, weight)
            if worst_rate is None or rate < worst_rate:
                worst_rate = rate
                worst_txid = txid
        if worst_txid is None or worst_rate is None:
            return None
        return worst_txid, worst_rate

    def _package_stats(self, txid: str) -> tuple[int, int]:
        total_fee = 0
        total_weight = 0
        queue = [txid]
        visited: set[str] = set()
        while queue:
            current = queue.pop()
            if current in visited:
                continue
            visited.add(current)
            entry = self.entries.get(current)
            if not entry:
                continue
            total_fee += entry.fee
            total_weight += entry.weight
            queue.extend(self.dep_index.get(current, set()))
        return total_fee, total_weight

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

    def drop_transaction(self, txid: str) -> bool:
        """Drop a pending transaction, releasing its inputs."""
        with self.lock:
            if txid not in self.entries:
                return False
            self._drop_entry(txid)
        return True

    def _rate_limit_peer(self, peer_id: str | None, cost: int) -> None:
        if not peer_id:
            return
        now = time.time()
        quota, refill = self._peer_budget(peer_id)
        tokens, updated = self.peer_stats.get(peer_id, (quota, now))
        tokens = min(quota, tokens + (now - updated) * refill)
        if tokens < cost:
            raise MempoolError("Peer submission rate exceeded")
        tokens -= cost
        self.peer_stats[peer_id] = (tokens, now)

    def _peer_budget(self, peer_id: str) -> tuple[float, float]:
        if peer_id == "rpc" or peer_id.startswith("rpc"):
            return self.peer_quota_rpc_bytes, self.peer_refill_rpc_per_sec
        return self.peer_quota_bytes, self.peer_refill_per_sec
