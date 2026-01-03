"""
Pool-mode payout accounting and transaction builder.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from ..core import crypto
from ..core.address import script_from_address
from ..core.tx import Transaction, TxInput, TxOutput
from ..policy import MIN_RELAY_FEE_RATE, required_fee
from ..storage import StateDB, UTXORecord


@dataclass
class WorkerState:
    address: str
    script: bytes
    balance: int = 0


class PayoutTracker:
    def __init__(
        self,
        data_path: Path,
        pool_privkey: int,
        pool_pubkey: bytes,
        pool_script: bytes,
        *,
        maturity: int,
        min_payout: int,
        pool_fee_percent: float,
    ):
        self.path = data_path
        self.pool_privkey = pool_privkey
        self.pool_pubkey = pool_pubkey
        self.pool_script = pool_script
        self.pool_fee_percent = pool_fee_percent
        self.maturity = maturity
        self.min_payout = min_payout
        self.round_shares: dict[str, float] = {}
        self.workers: dict[str, WorkerState] = {}
        self.pending_blocks: list[dict[str, object]] = []
        self.matured_utxos: list[dict[str, object]] = []
        self.pool_balance = 0
        self.min_fee_rate = MIN_RELAY_FEE_RATE
        # 1 byte len + sig (<=73) + 1 byte len + pubkey length
        self._script_sig_estimate = len(self.pool_pubkey) + 75
        self.lock = threading.RLock()
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            self.path.parent.mkdir(parents=True, exist_ok=True)
            return
        data = json.loads(self.path.read_text("utf-8"))
        for worker, info in data.get("workers", {}).items():
            self.workers[worker] = WorkerState(
                address=info["address"],
                script=bytes.fromhex(info["script"]),
                balance=info["balance"],
            )
        self.round_shares = {k: float(v) for k, v in data.get("round_shares", {}).items()}
        self.pending_blocks = data.get("pending_blocks", [])
        self.matured_utxos = data.get("matured_utxos", [])
        self.pool_balance = data.get("pool_balance", 0)

    def _save(self) -> None:
        data = {
            "workers": {
                worker: {"address": ws.address, "script": ws.script.hex(), "balance": ws.balance}
                for worker, ws in self.workers.items()
            },
            "round_shares": self.round_shares,
            "pending_blocks": self.pending_blocks,
            "matured_utxos": self.matured_utxos,
            "pool_balance": self.pool_balance,
        }
        self.path.write_text(json.dumps(data, indent=2), encoding="utf-8")

    def register_worker(self, worker_id: str, address: str) -> None:
        script = script_from_address(address)
        with self.lock:
            existing = self.workers.get(worker_id)
            if existing and existing.address == address:
                return
            balance = existing.balance if existing else 0
            self.workers[worker_id] = WorkerState(address=address, script=script, balance=balance)
            self._save()

    def record_share(self, worker_id: str, address: str, difficulty: float) -> None:
        with self.lock:
            self.register_worker(worker_id, address)
            self.round_shares[worker_id] = self.round_shares.get(worker_id, 0.0) + max(difficulty, 1.0)
            self._save()

    def record_block(self, height: int, coinbase_txid: str, reward: int, vout: int = 0) -> None:
        with self.lock:
            if not self.round_shares:
                return
            distributable = reward
            pool_fee = int(reward * (self.pool_fee_percent / 100.0))
            distributable -= pool_fee
            snapshot = dict(self.round_shares)
            self.pending_blocks.append(
                {
                    "height": height,
                    "txid": coinbase_txid,
                    "total_reward": reward,
                    "distributable": distributable,
                    "pool_fee": pool_fee,
                    "shares": snapshot,
                    "vout": vout,
                    "time": time.time(),
                }
            )
            self.round_shares.clear()
            self._save()

    def process_maturity(self, best_height: int) -> None:
        changed = False
        matured: list[dict[str, object]] = []
        with self.lock:
            remaining = []
            for entry in self.pending_blocks:
                if best_height - entry["height"] >= self.maturity:
                    matured.append(entry)
                    changed = True
                else:
                    remaining.append(entry)
            self.pending_blocks = remaining
            for entry in matured:
                shares: dict[str, float] = entry["shares"]  # type: ignore[assignment]
                total_shares = sum(shares.values())
                if total_shares <= 0:
                    total_shares = 1
                distributable = int(entry["distributable"])
                paid = 0
                for worker, share_value in shares.items():
                    portion = int(distributable * (share_value / total_shares))
                    if portion <= 0:
                        continue
                    ws = self.workers.setdefault(worker, WorkerState(address="", script=b""))
                    ws.balance += portion
                    paid += portion
                leftover = distributable - paid
                self.pool_balance += int(entry["pool_fee"]) + leftover
                self.matured_utxos.append(
                    {"txid": entry["txid"], "amount": entry["total_reward"], "vout": entry.get("vout", 0)}
                )
            if changed:
                self._save()

    def _gather_payees(self, max_outputs: int) -> list[tuple[str, WorkerState, int]]:
        payees: list[tuple[str, WorkerState, int]] = []
        for worker_id, state in self.workers.items():
            if state.balance >= self.min_payout:
                payees.append((worker_id, state, state.balance))
            if len(payees) >= max_outputs:
                break
        return payees

    def _scale_payees(self, payees: list[tuple[str, WorkerState, int]], available: int) -> list[tuple[str, WorkerState, int]]:
        total_out = sum(amount for _, _, amount in payees)
        if total_out <= 0 or available <= 0:
            return []
        ratio = min(1.0, available / total_out)
        scaled: list[tuple[str, WorkerState, int]] = []
        running = 0
        for idx, (worker_id, state, amount) in enumerate(payees):
            if idx == len(payees) - 1:
                new_amount = max(0, available - running)
            else:
                new_amount = int(amount * ratio)
            if new_amount <= 0:
                continue
            scaled.append((worker_id, state, new_amount))
            running += new_amount
            if running >= available:
                break
        return scaled

    def _build_payout(
        self, payees: list[tuple[str, WorkerState, int]], spendable: list[tuple[UTXORecord, dict[str, object]]]
    ) -> tuple[Transaction, list[tuple[str, WorkerState, int]], list[dict[str, object]], int, int, int] | None:
        if not spendable:
            return None
        inputs: list[UTXORecord] = [rec for rec, _ in spendable]
        consumed_infos = [info for _, info in spendable]
        input_sum = sum(rec.amount for rec in inputs)
        if input_sum <= 0:
            return None
        payees_local = list(payees)
        fee = 0
        estimated_size = 0
        change = 0
        for _ in range(8):
            total_out = sum(amount for _, _, amount in payees_local)
            if total_out <= 0:
                return None
            change = input_sum - total_out - fee
            if change < 0:
                available_for_outputs = input_sum - fee
                payees_local = self._scale_payees(payees_local, available_for_outputs)
                continue
            tx_inputs = [
                TxInput(prev_txid=rec.txid, prev_vout=rec.vout, script_sig=b"", sequence=0xFFFFFFFF) for rec in inputs
            ]
            tx_outputs = [TxOutput(value=amount, script_pubkey=state.script) for _, state, amount in payees_local]
            if change > 0:
                tx_outputs.append(TxOutput(value=change, script_pubkey=self.pool_script))
            tx_candidate = Transaction(version=1, inputs=tx_inputs, outputs=tx_outputs, lock_time=0)
            estimated_size = len(tx_candidate.serialize()) + len(tx_inputs) * self._script_sig_estimate
            new_fee = required_fee(estimated_size, self.min_fee_rate)
            if new_fee == fee and change >= 0:
                return tx_candidate, payees_local, consumed_infos, new_fee, change, estimated_size
            fee = new_fee
        return None

    def create_payout_transaction(self, state_db: StateDB, max_outputs: int = 16) -> Transaction | None:
        with self.lock:
            self.prune_stale_entries(state_db)
            payees = self._gather_payees(max_outputs)
            if not payees or not self.matured_utxos:
                return None
            spendable: list[tuple[UTXORecord, dict[str, object]]] = []
            for utxo_info in self.matured_utxos:
                vout = int(utxo_info.get("vout", 0))
                record = state_db.get_utxo(utxo_info["txid"], vout)
                if record is None:
                    continue
                spendable.append((record, utxo_info))

            result = self._build_payout(payees, spendable)
            if result is None:
                return None
            tx, final_payees, consumed_infos, _fee, _change, _size = result
            self._sign_transaction(tx)
            for _worker_id, state, amount in final_payees:
                state.balance -= amount
            for info in consumed_infos:
                self.matured_utxos.remove(info)
            self._save()
            return tx

    def preview_payout(self, state_db: StateDB, max_outputs: int = 16) -> dict[str, object] | None:
        """Dry-run payout assembly without mutating balances or UTXOs."""
        with self.lock:
            self.prune_stale_entries(state_db)
            payees = self._gather_payees(max_outputs)
            if not payees or not self.matured_utxos:
                return None
            spendable: list[tuple[UTXORecord, dict[str, object]]] = []
            for utxo_info in self.matured_utxos:
                vout = int(utxo_info.get("vout", 0))
                record = state_db.get_utxo(utxo_info["txid"], vout)
                if record is None:
                    continue
                spendable.append((record, utxo_info))
            result = self._build_payout(payees, spendable)
            if result is None:
                return None
            tx, final_payees, consumed_infos, fee, change, estimated_size = result
            def _lookup_amount(txid: str, vout: int) -> int:
                rec = state_db.get_utxo(txid, vout)
                return rec.amount if rec else 0
            input_sum = sum(_lookup_amount(txin.prev_txid, txin.prev_vout) for txin in tx.inputs)
            return {
                "payees": [
                    {"worker_id": worker_id, "address": state.address, "amount": amount}
                    for worker_id, state, amount in final_payees
                ],
                "matured_utxos": [
                    {
                        "txid": info.get("txid"),
                        "vout": info.get("vout", 0),
                        "amount": _lookup_amount(info.get("txid", ""), int(info.get("vout", 0))),
                    }
                    for info in consumed_infos
                ],
                "inputs_used": [
                    {
                        "txid": txin.prev_txid,
                        "vout": txin.prev_vout,
                        "amount": _lookup_amount(txin.prev_txid, txin.prev_vout),
                    }
                    for txin in tx.inputs
                ],
                "fee": fee,
                "change": max(change, 0),
                "total_output": sum(amount for _, _, amount in final_payees),
                "estimated_size": estimated_size,
                "input_sum": input_sum,
            }

    def _sign_transaction(self, tx: Transaction) -> None:
        for idx, _ in enumerate(tx.inputs):
            sighash = tx.signature_hash(idx, self.pool_script, 0x01)
            signature = crypto.sign(sighash, self.pool_privkey) + b"\x01"
            script_sig = bytes([len(signature)]) + signature + bytes([len(self.pool_pubkey)]) + self.pool_pubkey
            tx.inputs[idx].script_sig = script_sig

    def prune_stale_entries(self, state_db: StateDB, *, drop_matured: bool = False) -> dict[str, int]:
        """Remove pending (and optionally matured) entries that no longer exist in chainstate."""
        stale_pending = 0
        stale_matured = 0
        with self.lock:
            new_pending = []
            for entry in self.pending_blocks:
                vout = int(entry.get("vout", 0))
                if state_db.get_utxo(entry.get("txid", ""), vout) is None:
                    stale_pending += 1
                    continue
                new_pending.append(entry)
            self.pending_blocks = new_pending
            if drop_matured:
                new_matured = []
                for utxo in self.matured_utxos:
                    vout = int(utxo.get("vout", 0))
                    if state_db.get_utxo(utxo.get("txid", ""), vout) is None:
                        stale_matured += 1
                        continue
                    new_matured.append(utxo)
                self.matured_utxos = new_matured
            if stale_pending or (drop_matured and stale_matured):
                self._save()
        return {"stale_pending": stale_pending, "stale_matured": stale_matured}

    def reconcile_balances(self, state_db: StateDB, *, apply: bool = False) -> dict[str, object]:
        """Scale worker balances down to spendable matured UTXOs when ledger is overcommitted."""
        with self.lock:
            self.prune_stale_entries(state_db, drop_matured=apply)
            spendable_total = 0
            for utxo in self.matured_utxos:
                vout = int(utxo.get("vout", 0))
                rec = state_db.get_utxo(utxo.get("txid", ""), vout)
                if rec:
                    spendable_total += rec.amount
            owed_total = sum(ws.balance for ws in self.workers.values())
            shortfall = max(0, owed_total - spendable_total)
            ratio = 1.0
            adjustments: list[dict[str, object]] = []
            if owed_total > 0 and shortfall > 0:
                ratio = spendable_total / owed_total if spendable_total > 0 else 0.0
                running_total = 0
                for idx, (worker_id, ws) in enumerate(self.workers.items()):
                    new_balance = int(ws.balance * ratio)
                    if idx == len(self.workers) - 1:
                        # ensure rounding does not exceed spendable_total
                        remaining = max(0, spendable_total - running_total)
                        new_balance = min(new_balance, remaining)
                    delta = ws.balance - new_balance
                    adjustments.append(
                        {"worker_id": worker_id, "before": ws.balance, "after": new_balance, "reduced": delta}
                    )
                    if apply:
                        ws.balance = new_balance
                    running_total += new_balance
                if apply:
                    self._save()
            return {
                "spendable_total": spendable_total,
                "owed_total": owed_total,
                "shortfall": shortfall,
                "ratio": ratio,
                "applied": bool(apply),
                "adjustments": adjustments[:50],  # limit response size
            }
