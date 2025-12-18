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
        self.tx_fee = 1_000
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

    def record_block(self, height: int, coinbase_txid: str, reward: int) -> None:
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
                self.matured_utxos.append({"txid": entry["txid"], "amount": entry["total_reward"]})
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

    def create_payout_transaction(self, state_db: StateDB, max_outputs: int = 16) -> Transaction | None:
        with self.lock:
            payees = self._gather_payees(max_outputs)
            if not payees or not self.matured_utxos:
                return None
            total_out = sum(amount for _, _, amount in payees)
            inputs: list[UTXORecord] = []
            consumed: list[dict[str, object]] = []
            input_sum = 0
            for utxo_info in list(self.matured_utxos):
                record = state_db.get_utxo(utxo_info["txid"], 0)
                if record is None:
                    self.matured_utxos.remove(utxo_info)
                    continue
                inputs.append(record)
                consumed.append(utxo_info)
                input_sum += record.amount
                if input_sum >= total_out + self.tx_fee:
                    break
            if input_sum < total_out + self.tx_fee:
                return None
            tx_inputs = [
                TxInput(prev_txid=rec.txid, prev_vout=rec.vout, script_sig=b"", sequence=0xFFFFFFFF)
                for rec in inputs
            ]
            tx_outputs = [TxOutput(value=amount, script_pubkey=state.script) for _, state, amount in payees]
            change = input_sum - total_out - self.tx_fee
            if change > 0:
                tx_outputs.append(TxOutput(value=change, script_pubkey=self.pool_script))
            tx = Transaction(version=1, inputs=tx_inputs, outputs=tx_outputs, lock_time=0)
            for idx, _ in enumerate(tx.inputs):
                sighash = tx.signature_hash(idx, self.pool_script, 0x01)
                signature = crypto.sign(sighash, self.pool_privkey) + b"\x01"
                script_sig = bytes([len(signature)]) + signature + bytes([len(self.pool_pubkey)]) + self.pool_pubkey
                tx.inputs[idx].script_sig = script_sig
            for _worker_id, state, amount in payees:
                state.balance -= amount
            for info in consumed:
                self.matured_utxos.remove(info)
            self._save()
            return tx
