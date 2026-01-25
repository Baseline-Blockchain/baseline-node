"""
Simple wallet manager with address derivation, balance tracking, and RPC helpers.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import secrets
import subprocess
import threading
import time
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal
from pathlib import Path

from .core import crypto
from .core.address import script_from_address
from .core.block import Block
from .core.tx import COIN, Transaction, TxInput, TxOutput
from .mempool import Mempool, MempoolError
from .net.server import P2PServer
from .policy import MIN_RELAY_FEE_RATE, required_fee
from .storage import BlockStore, StateDB


class WalletError(Exception):
    """Base wallet error."""


class WalletLockedError(WalletError):
    """Raised when a wallet operation requires an unlocked wallet."""


def coins_to_liners(amount: Decimal | float | str | int) -> int:
    if isinstance(amount, int):
        return amount
    quantized = Decimal(str(amount)).quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
    liners = int((quantized * COIN).to_integral_value(rounding=ROUND_DOWN))
    if liners < 0:
        raise ValueError("Amount must be positive")
    return liners


def liners_to_coins(value: int) -> float:
    return float(Decimal(value) / COIN)


def _xor_bytes(left: bytes, right: bytes) -> bytes:
    return bytes(a ^ b for a, b in zip(left, right))


def _hmac_sha256(key: bytes, payload: bytes) -> bytes:
    return hmac.new(key, payload, hashlib.sha256).digest()


def _derive_keys(passphrase: str, salt: bytes) -> tuple[bytes, bytes]:
    """Derive independent encryption and authentication keys.

    The first 32 bytes are compatible with the previous dklen=32 derivation.
    """

    material = hashlib.pbkdf2_hmac("sha256", passphrase.encode("utf-8"), salt, 200_000, dklen=64)
    return material[:32], material[32:]


def _keystream(key: bytes, context: bytes) -> bytes:
    return _hmac_sha256(key, b"baseline-wallet-keystream:" + context)


@dataclass
class WalletTransaction:
    txid: str
    amount: int
    category: str
    addresses: list[str]
    time: int
    blockhash: str | None
    height: int | None
    fee: int | None


class WalletManager:
    def __init__(
        self,
        path: Path,
        state_db: StateDB,
        block_store: BlockStore,
        mempool: Mempool,
        network: P2PServer | None = None,
        wallet_notify: str | None = None,
    ):
        self.path = path
        self.state_db = state_db
        self.block_store = block_store
        self.mempool = mempool
        self.network = network
        self.log = logging.getLogger("baseline.wallet")
        self.lock = threading.RLock()
        self.wallet_notify = wallet_notify.strip() if wallet_notify else None
        self._wallet_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="WalletWriter")
        self._sync_lock = threading.Lock()
        self._unlocked_seed: bytes | None = None
        self._unlock_until: float | None = None
        self._active_key: bytes | None = None
        self._active_mac_key: bytes | None = None
        self.data: dict[str, object] = {}
        self._sync_event = threading.Event()
        self._sync_stop = threading.Event()
        self._sync_thread: threading.Thread | None = None
        self._sync_should_abort: Callable[[], bool] | None = None
        self._pending_sync_lock = threading.Lock()
        self._pending_sync_max_blocks: int | None = None
        self._sync_status_lock = threading.Lock()
        self._next_sync_after: float = 0.0
        self._sync_status: dict[str, object] = {
            "syncing": False,
            "last_error": "",
            "last_sync": None,
            "processed_height": -1,
        }
        self._missing_block_backoff: dict[str, float] = {}
        self._missing_block_retry_after: dict[str, float] = {}
        self._load()
        if not self.data["addresses"] and not self.is_encrypted():
            self.get_new_address("default")
        self._sync_status["processed_height"] = int(self.data.get("processed_height", -1))

    def stop(self) -> None:
        """Shut down the writer pool."""
        self._wallet_executor.shutdown(wait=True)

    def _load(self) -> None:
        if not self.path.exists():
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.data = self._default_state()
            self._save()
            return
        with self.path.open("r", encoding="utf-8") as fh:
            self.data = json.load(fh)
        if "version" not in self.data:
            self.data = self._default_state()
        self.data.setdefault("addresses", {})
        self.data.setdefault("next_index", 0)
        self.data.setdefault("transactions", {})
        self.data.setdefault("outputs", {})
        self.data.setdefault("processed_height", -1)
        self.data.setdefault("encrypted", False)
        self.data.setdefault("seed_encrypted", None)
        self.data.setdefault("seed_mac", None)
        self.data.setdefault("salt", None)
        self.data.setdefault("enc_version", 1)
        self.data.setdefault("schedules", {})
        addresses = self.data.setdefault("addresses", {})
        for entry in addresses.values():
            entry.setdefault("watch_only", False)
            entry.setdefault("label", "")

    def _default_state(self) -> dict[str, object]:
        return {
            "version": 1,
            "seed": secrets.token_hex(32),
            "seed_encrypted": None,
            "salt": None,
            "encrypted": False,
            "next_index": 0,
            "addresses": {},
            "transactions": {},
            "outputs": {},
            "schedules": {},
            "processed_height": -1,
        }

    def _save(self) -> None:
        """Schedule wallet state write to disk."""
        data_str = json.dumps(self.data, indent=2)
        self._wallet_executor.submit(self._do_write_disk, data_str)

    def _do_write_disk(self, data_str: str) -> None:
        """Perform proper disk I/O. Runs in background thread."""
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(data_str, encoding="utf-8")
        tmp.replace(self.path)

    def _seed_bytes(self) -> bytes:
        seed_hex = self.data.get("seed")
        if seed_hex:
            return bytes.fromhex(seed_hex)
        if not self.data.get("encrypted"):
            raise WalletError("Wallet seed unavailable")
        if self._unlocked_seed:
            return self._unlocked_seed
        raise WalletLockedError("Wallet locked")

    def _derive_privkey(self, index: int) -> int:
        seed = self._seed_bytes()
        index_bytes = index.to_bytes(4, "big")
        digest = hashlib.sha256(seed + index_bytes).digest()
        priv = int.from_bytes(digest, "big") % (crypto.SECP_N - 1)
        return priv + 1

    def _decode_wif(self, wif: str) -> tuple[int, bool]:
        try:
            data = crypto.base58check_decode(wif)
        except crypto.CryptoError as exc:
            raise ValueError("Invalid WIF key") from exc
        if not data:
            raise ValueError("Invalid WIF payload")
        version = data[0]
        if version not in (0x80, 0xef, 0x2f, 0x35):
            raise ValueError("Unsupported WIF version")
        if len(data) == 34 and data[-1] == 0x01:
            compressed = True
            priv_bytes = data[1:-1]
        elif len(data) == 33:
            compressed = False
            priv_bytes = data[1:]
        else:
            raise ValueError("Unexpected WIF payload length")
        priv_int = int.from_bytes(priv_bytes, "big")
        if not (1 <= priv_int < crypto.SECP_N):
            raise ValueError("Private key out of range")
        return priv_int, compressed

    def _store_external_privkey(self, entry: dict[str, object], priv_bytes: bytes) -> None:
        if self.is_encrypted():
            if not self._active_key:
                raise WalletLockedError("Unlock wallet before importing keys")
            enc_version = int(self.data.get("enc_version") or 1)
            if enc_version >= 2:
                address = entry.get("address")
                if not isinstance(address, str) or not address:
                    raise WalletError("Missing address for encrypted key")
                encrypted = _xor_bytes(priv_bytes, _keystream(self._active_key, b"privkey:" + address.encode("utf-8")))
            else:
                encrypted = _xor_bytes(priv_bytes, self._active_key)
            entry["privkey_encrypted"] = encrypted.hex()
            if self._active_mac_key:
                prefix = b"privkey-v2:" if enc_version >= 2 else b"privkey-v1:"
                entry["privkey_mac"] = _hmac_sha256(self._active_mac_key, prefix + bytes.fromhex(entry["privkey_encrypted"])).hex()
            entry.pop("privkey", None)
        else:
            entry["privkey"] = priv_bytes.hex()
            entry.pop("privkey_encrypted", None)
            entry.pop("privkey_mac", None)

    def _load_external_privkey(self, entry: dict[str, object]) -> int | None:
        priv_hex = entry.get("privkey")
        if priv_hex:
            return int(priv_hex, 16)
        priv_enc = entry.get("privkey_encrypted")
        if priv_enc:
            if not self._active_key:
                raise WalletLockedError("Wallet locked")
            enc_version = int(self.data.get("enc_version") or 1)
            mac_hex = entry.get("privkey_mac")
            if mac_hex and self._active_mac_key:
                prefix = b"privkey-v2:" if enc_version >= 2 else b"privkey-v1:"
                expected = _hmac_sha256(self._active_mac_key, prefix + bytes.fromhex(priv_enc)).hex()
                if not hmac.compare_digest(expected, str(mac_hex)):
                    raise WalletError("Corrupted encrypted private key")
            if enc_version >= 2:
                address = entry.get("address")
                if not isinstance(address, str) or not address:
                    raise WalletError("Missing address for encrypted key")
                priv_bytes = _xor_bytes(
                    bytes.fromhex(priv_enc),
                    _keystream(self._active_key, b"privkey:" + address.encode("utf-8")),
                )
            else:
                priv_bytes = _xor_bytes(bytes.fromhex(priv_enc), self._active_key)
            return int.from_bytes(priv_bytes, "big")
        return None

    def _derive_address(self, index: int) -> tuple[str, int]:
        priv = self._derive_privkey(index)
        pub = crypto.generate_pubkey(priv)
        address = crypto.address_from_pubkey(pub)
        return address, priv

    def get_new_address(self, label: str | None = None) -> str:
        with self.lock:
            index = int(self.data.get("next_index", 0))
            address, priv = self._derive_address(index)
            entry = {
                "index": index,
                "label": label or "",
                "created": time.time(),
                "watch_only": False,
            }
            self.data.setdefault("addresses", {})[address] = entry
            self.data["next_index"] = index + 1
            self._save()
        return address

    def _script_map(self, addresses: Sequence[str] | None = None) -> dict[str, bytes]:
        mapping: dict[str, bytes] = {}
        entries = self.data.get("addresses", {})
        for addr in entries:
            if addresses and addr not in addresses:
                continue
            mapping[addr] = script_from_address(addr)
        return mapping

    def list_unspent(self, min_conf: int = 1, max_conf: int = 9999999, addresses: Sequence[str] | None = None) -> list[dict[str, object]]:
        script_map = self._script_map(addresses)
        if not script_map:
            return []
        utxos = self.state_db.get_utxos_by_scripts(list(script_map.values()))
        best = self.state_db.get_best_tip()
        best_height = best[1] if best else 0
        spent_in_mempool = set(self.mempool.spent_outpoints)
        result = []
        script_lookup = {script.hex(): addr for addr, script in script_map.items()}
        for utxo in utxos:
            key = (utxo.txid, utxo.vout)
            if key in spent_in_mempool:
                continue
            confirmations = best_height - utxo.height + 1 if best else 0
            if confirmations < min_conf or confirmations > max_conf:
                continue
            addr = script_lookup.get(utxo.script_pubkey.hex())
            result.append(
                {
                    "txid": utxo.txid,
                    "vout": utxo.vout,
                    "address": addr,
                    "amount": liners_to_coins(utxo.amount),
                    "amount_liners": utxo.amount,
                    "confirmations": confirmations,
                    "scriptPubKey": utxo.script_pubkey.hex(),
                    "height": utxo.height,
                    "coinbase": utxo.coinbase,
                }
            )
        return result

    def get_balance(self, min_conf: int = 1) -> float:
        unspent = self.list_unspent(min_conf=min_conf)
        total = sum(int(entry.get("amount_liners") or coins_to_liners(entry["amount"])) for entry in unspent)
        return liners_to_coins(total)

    def _lookup_privkey(self, address: str) -> int:
        entries = self.data.get("addresses", {})
        meta = entries.get(address)
        if meta is None:
            raise ValueError("Address not in wallet")
        if meta.get("watch_only"):
            raise ValueError("Watch-only address has no private key")
        index = meta["index"]
        if index is not None:
            return self._derive_privkey(index)
        priv = self._load_external_privkey(meta)
        if priv is None:
            raise ValueError("Missing private key for address")
        return priv

    def _sign_transaction(self, tx: Transaction, selected: list[dict[str, object]]) -> None:
        for idx, entry in enumerate(selected):
            address = entry["address"]
            priv = self._lookup_privkey(address)
            script = script_from_address(address)
            sighash = tx.signature_hash(idx, script, 0x01)
            signature = crypto.sign(sighash, priv) + b"\x01"
            pubkey = crypto.generate_pubkey(priv)
            script_sig = bytes([len(signature)]) + signature + bytes([len(pubkey)]) + pubkey
            tx.inputs[idx].script_sig = script_sig

    def send_to_address(
        self,
        dest_address: str,
        amount: Decimal | float | str | int,
        *,
        fee: int | None = None,
        fee_rate: int = MIN_RELAY_FEE_RATE,
        from_addresses: Sequence[str] | None = None,
        change_address: str | None = None,
        comment: str | None = None,
        comment_to: str | None = None,
    ) -> str:
        amount_liners = coins_to_liners(amount)
        if amount_liners <= 0:
            raise ValueError("Amount must be positive")
        if fee is not None and fee < 0:
            raise ValueError("Fee must be >= 0")
        if fee_rate <= 0:
            raise ValueError("fee_rate must be > 0")
        unspent = self.list_unspent()
        entries = self.data.get("addresses", {})
        allowed_set: set[str] | None = None
        if from_addresses:
            allowed_set = set()
            for addr in from_addresses:
                meta = entries.get(addr)
                if meta is None:
                    raise ValueError(f"Address {addr} not found in wallet")
                if meta.get("watch_only"):
                    raise ValueError(f"Address {addr} is watch-only and cannot spend")
                allowed_set.add(addr)
        if change_address:
            change_meta = entries.get(change_address)
            if change_meta is None or change_meta.get("watch_only"):
                raise ValueError("Change address must belong to wallet and be spendable")
        spendable = []
        for entry in unspent:
            meta = entries.get(entry["address"])
            if not meta or meta.get("watch_only"):
                continue
            if allowed_set and entry["address"] not in allowed_set:
                continue
            spendable.append(entry)
        if not spendable:
            raise ValueError("No spendable inputs")
        selected: list[dict[str, object]] = []
        total = 0
        spend_index = 0
        chosen_change = change_address
        if not chosen_change and spendable:
            chosen_change = str(spendable[0]["address"])
        if not chosen_change:
            for addr, meta in entries.items():
                if not meta.get("watch_only"):
                    chosen_change = addr
                    break
        if not chosen_change:
            raise ValueError("No address available for change output")

        def _build_signed_tx(*, include_change: bool, absolute_fee: int) -> Transaction:
            inputs = [
                TxInput(
                    prev_txid=entry["txid"],
                    prev_vout=entry["vout"],
                    script_sig=b"",
                    sequence=0xFFFFFFFF,
                )
                for entry in selected
            ]
            outputs = [TxOutput(value=amount_liners, script_pubkey=script_from_address(dest_address))]
            if include_change:
                change_value = total - amount_liners - absolute_fee
                if change_value > 0:
                    outputs.append(TxOutput(value=change_value, script_pubkey=script_from_address(chosen_change)))
            tx = Transaction(version=1, inputs=inputs, outputs=outputs, lock_time=0)
            self._sign_transaction(tx, selected)
            return tx

        if fee is not None:
            fee_guess = fee
            while total < amount_liners + fee_guess:
                if spend_index >= len(spendable):
                    raise ValueError("Insufficient funds")
                entry = spendable[spend_index]
                spend_index += 1
                selected.append(entry)
                total += int(entry.get("amount_liners") or coins_to_liners(entry["amount"]))
            tx = _build_signed_tx(include_change=True, absolute_fee=fee_guess)
            final_fee = fee_guess
        else:
            max_fee_iters = 10
            while True:
                if spend_index >= len(spendable) and not selected:
                    raise ValueError("No spendable inputs")
                if not selected:
                    entry = spendable[spend_index]
                    spend_index += 1
                    selected.append(entry)
                    total += int(entry.get("amount_liners") or coins_to_liners(entry["amount"]))

                tx_no = _build_signed_tx(include_change=False, absolute_fee=0)
                fee_required_no = required_fee(len(tx_no.serialize()), fee_rate)
                if total < amount_liners + fee_required_no:
                    if spend_index >= len(spendable):
                        raise ValueError("Insufficient funds")
                    entry = spendable[spend_index]
                    spend_index += 1
                    selected.append(entry)
                    total += int(entry.get("amount_liners") or coins_to_liners(entry["amount"]))
                    continue

                fee_guess = fee_required_no
                tx_with_change: Transaction | None = None
                for _ in range(max_fee_iters):
                    change_value = total - amount_liners - fee_guess
                    if change_value <= 0:
                        tx_with_change = None
                        break
                    candidate = _build_signed_tx(include_change=True, absolute_fee=fee_guess)
                    fee_needed = required_fee(len(candidate.serialize()), fee_rate)
                    if fee_needed <= fee_guess:
                        tx_with_change = candidate
                        break
                    fee_guess = fee_needed

                if tx_with_change is not None:
                    tx = tx_with_change
                    final_fee = fee_guess
                    break

                fee_available = total - amount_liners
                if fee_available >= fee_required_no and spend_index >= len(spendable):
                    # Can't make a change output; fall back to burning the remainder as fee.
                    tx = tx_no
                    final_fee = fee_available
                    break

                if spend_index >= len(spendable):
                    raise ValueError("Insufficient funds")
                entry = spendable[spend_index]
                spend_index += 1
                selected.append(entry)
                total += int(entry.get("amount_liners") or coins_to_liners(entry["amount"]))
        try:
            self.mempool.accept_transaction(tx, peer_id="wallet")
        except MempoolError as exc:
            raise ValueError(f"Transaction rejected: {exc}") from exc
        self._record_transaction(
            tx.txid(),
            amount=-amount_liners,
            category="send",
            addresses=[dest_address],
            blockhash=None,
            height=None,
            fee=final_fee,
            comment=comment,
            comment_to=comment_to,
        )
        return tx.txid()

    def _serialize_schedule_entry(self, entry: dict[str, object]) -> dict[str, object]:
        result: dict[str, object] = dict(entry)
        result["inputs"] = [dict(inp) for inp in entry.get("inputs", [])]
        result["amount"] = liners_to_coins(entry["amount_liners"])
        result["fee"] = liners_to_coins(entry["fee_liners"])
        if entry.get("cancel_fee_liners") is not None:
            result["cancel_fee"] = liners_to_coins(entry["cancel_fee_liners"])
        return result

    def create_scheduled_transaction(
        self,
        dest_address: str,
        amount: Decimal | float | str | int,
        *,
        lock_time: int,
        cancelable: bool = False,
        fee: int | None = None,
        fee_rate: int = MIN_RELAY_FEE_RATE,
        from_addresses: Sequence[str] | None = None,
        change_address: str | None = None,
    ) -> dict[str, object]:
        amount_liners = coins_to_liners(amount)
        if amount_liners <= 0:
            raise ValueError("Amount must be positive")
        lock_time = int(lock_time)
        if lock_time < 0 or lock_time > 0xFFFFFFFF:
            raise ValueError("lock_time must be between 0 and 2^32-1")
        if fee is not None and fee < 0:
            raise ValueError("Fee must be >= 0")
        if fee_rate <= 0:
            raise ValueError("fee_rate must be > 0")
        unspent = self.list_unspent()
        entries = self.data.get("addresses", {})
        allowed_set: set[str] | None = None
        if from_addresses:
            allowed_set = set()
            for addr in from_addresses:
                meta = entries.get(addr)
                if meta is None:
                    raise ValueError(f"Address {addr} not found in wallet")
                if meta.get("watch_only"):
                    raise ValueError(f"Address {addr} is watch-only and cannot spend")
                allowed_set.add(addr)
        if change_address:
            change_meta = entries.get(change_address)
            if change_meta is None or change_meta.get("watch_only"):
                raise ValueError("Change address must belong to wallet and be spendable")
        spendable = []
        for entry in unspent:
            meta = entries.get(entry["address"])
            if not meta or meta.get("watch_only"):
                continue
            if allowed_set and entry["address"] not in allowed_set:
                continue
            spendable.append(entry)
        if not spendable:
            raise ValueError("No spendable inputs")
        selected: list[dict[str, object]] = []
        total = 0
        spend_index = 0
        chosen_change = change_address
        if not chosen_change and spendable:
            chosen_change = str(spendable[0]["address"])
        if not chosen_change:
            for addr, meta in entries.items():
                if not meta.get("watch_only"):
                    chosen_change = addr
                    break
        if not chosen_change:
            raise ValueError("No address available for change output")

        def _build_signed_schedule_tx(*, include_change: bool, absolute_fee: int) -> Transaction:
            inputs = [
                TxInput(
                    prev_txid=entry["txid"],
                    prev_vout=entry["vout"],
                    script_sig=b"",
                    sequence=0xFFFFFFFF,
                )
                for entry in selected
            ]
            outputs = [TxOutput(value=amount_liners, script_pubkey=script_from_address(dest_address))]
            if include_change:
                change_value = total - amount_liners - absolute_fee
                if change_value > 0:
                    outputs.append(TxOutput(value=change_value, script_pubkey=script_from_address(chosen_change)))
            tx = Transaction(version=1, inputs=inputs, outputs=outputs, lock_time=lock_time)
            self._sign_transaction(tx, selected)
            return tx

        if fee is not None:
            fee_guess = fee
            while total < amount_liners + fee_guess:
                if spend_index >= len(spendable):
                    raise ValueError("Insufficient funds")
                entry = spendable[spend_index]
                spend_index += 1
                selected.append(entry)
                total += int(entry.get("amount_liners") or coins_to_liners(entry["amount"]))
            tx = _build_signed_schedule_tx(include_change=True, absolute_fee=fee_guess)
            final_fee = fee_guess
        else:
            max_fee_iters = 10
            while True:
                if not selected:
                    if spend_index >= len(spendable):
                        raise ValueError("No spendable inputs")
                    entry = spendable[spend_index]
                    spend_index += 1
                    selected.append(entry)
                    total += int(entry.get("amount_liners") or coins_to_liners(entry["amount"]))

                tx_no = _build_signed_schedule_tx(include_change=False, absolute_fee=0)
                fee_required_no = required_fee(len(tx_no.serialize()), fee_rate)
                if total < amount_liners + fee_required_no:
                    if spend_index >= len(spendable):
                        raise ValueError("Insufficient funds")
                    entry = spendable[spend_index]
                    spend_index += 1
                    selected.append(entry)
                    total += int(entry.get("amount_liners") or coins_to_liners(entry["amount"]))
                    continue

                fee_guess = fee_required_no
                tx_with_change: Transaction | None = None
                for _ in range(max_fee_iters):
                    change_value = total - amount_liners - fee_guess
                    if change_value <= 0:
                        tx_with_change = None
                        break
                    candidate = _build_signed_schedule_tx(include_change=True, absolute_fee=fee_guess)
                    fee_needed = required_fee(len(candidate.serialize()), fee_rate)
                    if fee_needed <= fee_guess:
                        tx_with_change = candidate
                        break
                    fee_guess = fee_needed

                if tx_with_change is not None:
                    tx = tx_with_change
                    final_fee = fee_guess
                    break

                fee_available = total - amount_liners
                if fee_available >= fee_required_no and spend_index >= len(spendable):
                    tx = tx_no
                    final_fee = fee_available
                    break

                if spend_index >= len(spendable):
                    raise ValueError("Insufficient funds")
                entry = spendable[spend_index]
                spend_index += 1
                selected.append(entry)
                total += int(entry.get("amount_liners") or coins_to_liners(entry["amount"]))
        try:
            self.mempool.accept_transaction(tx, peer_id="scheduled")
        except MempoolError as exc:
            raise ValueError(f"Scheduled transaction rejected: {exc}") from exc
        inputs_meta = [
            {
                "txid": entry["txid"],
                "vout": entry["vout"],
                "address": entry["address"],
                "amount": int(entry.get("amount_liners") or coins_to_liners(entry["amount"])),
            }
            for entry in selected
        ]
        raw_tx = tx.serialize().hex()
        schedule_id = crypto.sha256d(tx.serialize() + lock_time.to_bytes(4, "little") + (b"\x01" if cancelable else b"\x00")).hex()
        schedules = self.data.setdefault("schedules", {})
        schedule_entry: dict[str, object] = {
            "schedule_id": schedule_id,
            "txid": tx.txid(),
            "raw_tx": raw_tx,
            "dest_address": dest_address,
            "amount_liners": amount_liners,
            "fee_liners": final_fee,
            "lock_time": lock_time,
            "cancelable": cancelable,
            "status": "pending",
            "created": int(time.time()),
            "inputs": inputs_meta,
            "change_address": chosen_change,
            "owner_address": chosen_change,
            "cancel_txid": None,
            "cancel_fee_liners": None,
            "confirmed_height": None,
            "confirmed_blockhash": None,
        }
        schedules[schedule_id] = schedule_entry
        self._record_transaction(
            tx.txid(),
            amount=-amount_liners,
            category="scheduled",
            addresses=[dest_address],
            blockhash=None,
            height=None,
            fee=final_fee,
        )
        return self._serialize_schedule_entry(schedule_entry)

    def list_scheduled_transactions(self) -> list[dict[str, object]]:
        schedules = self.data.setdefault("schedules", {})
        ordered = sorted(schedules.values(), key=lambda entry: entry.get("created", 0), reverse=True)
        return [self._serialize_schedule_entry(entry) for entry in ordered]

    def get_schedule(self, schedule_id: str) -> dict[str, object]:
        entry = self.data.setdefault("schedules", {}).get(schedule_id)
        if entry is None:
            raise WalletError("Scheduled transaction not found")
        return self._serialize_schedule_entry(entry)

    def cancel_scheduled_transaction(self, schedule_id: str) -> dict[str, object]:
        schedules = self.data.setdefault("schedules", {})
        entry = schedules.get(schedule_id)
        if not entry:
            raise WalletError("Scheduled transaction not found")
        if not entry.get("cancelable"):
            raise WalletError("Scheduled transaction is not cancelable")
        if entry.get("status") != "pending":
            raise WalletError("Scheduled transaction is no longer pending")
        txid = entry["txid"]
        self.mempool.drop_transaction(txid)
        owner = entry.get("owner_address") or entry["inputs"][0]["address"]
        refund_tx, refund_fee = self._build_refund_transaction(entry, owner)
        self.mempool.accept_transaction(refund_tx, peer_id="scheduled-refund")
        entry["status"] = "canceled"
        entry["cancel_txid"] = refund_tx.txid()
        entry["cancel_fee_liners"] = refund_fee
        entry["canceled_at"] = int(time.time())
        self._save()
        self._record_transaction(
            refund_tx.txid(),
            amount=refund_tx.outputs[0].value,
            category="schedule_refund",
            addresses=[owner],
            blockhash=None,
            height=None,
            fee=refund_fee,
        )
        return self._serialize_schedule_entry(entry)

    def _build_refund_transaction(self, entry: dict[str, object], owner_address: str) -> tuple[Transaction, int]:
        inputs = entry["inputs"]
        total_input = sum(inp["amount"] for inp in inputs)
        target_script = script_from_address(owner_address)
        output_value = total_input
        while True:
            tx_inputs = [
                TxInput(prev_txid=inp["txid"], prev_vout=inp["vout"], script_sig=b"", sequence=0xFFFFFFFF)
                for inp in inputs
            ]
            tx_outputs = [TxOutput(value=output_value, script_pubkey=target_script)]
            tx = Transaction(version=1, inputs=tx_inputs, outputs=tx_outputs, lock_time=0)
            self._sign_transaction(tx, inputs)
            size = len(tx.serialize())
            fee = required_fee(size)
            new_output = total_input - fee
            if new_output < 0:
                raise WalletError("Not enough funds to cover refund fee")
            if new_output == output_value:
                return tx, fee
            output_value = new_output

    def _run_walletnotify(self, txid: str) -> None:
        cmd = self.wallet_notify
        if not cmd:
            return
        if "%s" in cmd:
            cmd = cmd.replace("%s", txid)
        else:
            cmd = f"{cmd} {txid}"
        try:
            subprocess.Popen(cmd, shell=True)
        except Exception as exc:  # noqa: BLE001
            self.log.warning("walletnotify failed for %s: %s", txid, exc)

    def _record_transaction(
        self,
        txid: str,
        *,
        amount: int,
        category: str,
        addresses: list[str],
        blockhash: str | None,
        height: int | None,
        fee: int | None,
        timestamp: int | None = None,
        comment: str | None = None,
        comment_to: str | None = None,
    ) -> None:
        entry = {
            "amount": amount,
            "category": category,
            "addresses": addresses,
            "time": timestamp or int(time.time()),
            "blockhash": blockhash,
            "height": height,
            "fee": fee,
            "comment": comment or "",
            "comment_to": comment_to or "",
        }
        with self.lock:
            txs = self.data.setdefault("transactions", {})
            existed = txid in txs
            txs[txid] = entry
            self._save()
        if not existed:
            self._run_walletnotify(txid)

    def sync_chain(
        self,
        should_abort: Callable[[], bool] | None = None,
        max_blocks: int | None = None,
    ) -> bool:
        dirty = self._reconcile_schedules_with_chain()
        best = self.state_db.get_best_tip()
        if not best:
            if dirty:
                self._save()
            return True
        best_height = best[1]
        processed_height = int(self.data.get("processed_height", -1))
        start = processed_height + 1
        if start > best_height:
            if dirty:
                self._save()
            self._update_sync_status(processed_height=processed_height, syncing=False)
            return True
        self._update_sync_status(syncing=True, last_error="")

        def aborted() -> bool:
            return bool(should_abort and should_abort())

        script_map = self._script_map()
        scripts = {script.hex(): addr for addr, script in script_map.items()}
        outputs: dict[str, dict] = self.data.setdefault("outputs", {})
        txs: dict[str, dict] = self.data.setdefault("transactions", {})
        blocks_processed = 0
        dirty = bool(dirty)
        last_error: str | None = None
        for height in range(start, best_height + 1):
            if aborted():
                break
            header = self.state_db.get_main_header_at_height(height)
            if header is None:
                continue
            try:
                raw = self.block_store.get_block(header.hash)
            except Exception as exc:  # noqa: BLE001
                err = f"Block data missing for {header.hash}: {exc}"
                now = time.time()
                last_log = self._missing_block_backoff.get(header.hash, 0)
                next_retry = self._missing_block_retry_after.get(header.hash, 0.0)
                if now < next_retry:
                    last_error = err
                    self._next_sync_after = max(self._next_sync_after, next_retry)
                    break
                if now - last_log > 30:
                    self.log.error(err)
                    self._missing_block_backoff[header.hash] = now
                last_error = err
                # Try to request the missing block from peers before giving up.
                requested = False
                if self.network:
                    requested = self.network.request_block(header.hash)
                self._missing_block_retry_after[header.hash] = now + 10.0
                if requested:
                    time.sleep(1.0)
                    try:
                        raw = self.block_store.get_block(header.hash)
                        err = None
                    except Exception as exc:  # noqa: BLE001
                        self.log.debug("Block %s still missing after request: %s", header.hash, exc)
                if err:
                    self._update_sync_status(
                        syncing=False,
                        last_error=err,
                        processed_height=self.data.get("processed_height", -1),
                    )
                    self._next_sync_after = max(self._next_sync_after, now + 10.0)
                    break
            block = Block.parse(raw)
            for tx in block.transactions:
                txid = tx.txid()
                incoming = 0
                outgoing = 0
                addresses: list[str] = []
                # detect spent outputs
                for txin in tx.inputs:
                    key = f"{txin.prev_txid}:{txin.prev_vout}"
                    owned = outputs.get(key)
                    if owned:
                        outgoing += owned["value"]
                        outputs.pop(key, None)
                # detect new outputs
                for idx, txout in enumerate(tx.outputs):
                    script_hex = txout.script_pubkey.hex()
                    address = scripts.get(script_hex)
                    if address:
                        incoming += txout.value
                        addresses.append(address)
                        outputs[f"{txid}:{idx}"] = {"value": txout.value, "address": address, "height": height}
                delta = incoming - outgoing
                if delta == 0:
                    continue
                category = "receive" if delta > 0 else "send"
                existing = txs.get(txid)
                timestamp = header.timestamp
                fee = existing.get("fee") if existing else None
                comment = existing.get("comment", "") if existing else ""
                comment_to = existing.get("comment_to", "") if existing else ""
                txs[txid] = {
                    "amount": delta,
                    "category": category,
                    "addresses": addresses or (existing.get("addresses") if existing else []),
                    "time": timestamp,
                    "blockhash": header.hash,
                    "height": height,
                    "fee": fee,
                    "comment": comment,
                    "comment_to": comment_to,
                }
                if existing is None or existing.get("height") != height or existing.get("blockhash") != header.hash:
                    self._run_walletnotify(txid)
                if self._mark_schedule_confirmed(txid, height, header.hash):
                    dirty = True
            self.data["processed_height"] = height
            dirty = True
            blocks_processed += 1
            if max_blocks and blocks_processed >= max_blocks:
                break
        if dirty:
            self._save()
        processed_height = int(self.data.get("processed_height", -1))
        self._update_sync_status(
            syncing=False,
            last_sync=time.time() if last_error is None else self._sync_status.get("last_sync"),
            processed_height=processed_height,
            last_error=last_error or "",
        )
        return processed_height >= best_height

    def _reconcile_schedules_with_chain(self) -> bool:
        schedules = self.data.setdefault("schedules", {})
        dirty = False
        for entry in schedules.values():
            if entry.get("status") != "confirmed":
                continue
            blockhash = entry.get("confirmed_blockhash")
            height = entry.get("confirmed_height")
            if not blockhash or height is None:
                continue
            status = self.state_db.get_header_status(blockhash)
            if status != 0:
                self._reset_schedule_confirmation(entry)
                dirty = True
                continue
            main_header = self.state_db.get_main_header_at_height(int(height))
            if main_header is None or main_header.hash != blockhash:
                self._reset_schedule_confirmation(entry)
                dirty = True
        return dirty

    def _reset_schedule_confirmation(self, entry: dict[str, object]) -> None:
        entry["status"] = "pending"
        entry["confirmed_height"] = None
        entry["confirmed_blockhash"] = None
        entry["confirmed_at"] = None

    def _mark_schedule_confirmed(self, txid: str, height: int, blockhash: str) -> bool:
        schedules = self.data.setdefault("schedules", {})
        for entry in schedules.values():
            if entry.get("txid") == txid and entry.get("status") == "pending":
                entry["status"] = "confirmed"
                entry["confirmed_height"] = height
                entry["confirmed_blockhash"] = blockhash
                entry["confirmed_at"] = int(time.time())
                return True
        return False

    def rescan_wallet(self) -> None:
        """Reset processed height and clear cached outputs/transactions."""

        with self.lock:
            self.data["outputs"] = {}
            self.data["transactions"] = {}
            self.data["processed_height"] = -1
            self._save()
        self.request_sync()

    def get_transaction(self, txid: str) -> dict[str, object] | None:
        txs: dict[str, dict] = self.data.get("transactions", {})
        entry = txs.get(txid)
        if entry is None:
            return None
        best = self.state_db.get_best_tip()
        confirmations = 0
        if best and entry.get("height") is not None:
            confirmations = best[1] - entry["height"] + 1
        result = dict(entry)
        result["txid"] = txid
        result["amount"] = liners_to_coins(entry["amount"])
        result["confirmations"] = confirmations
        if entry.get("fee") is not None:
            result["fee"] = liners_to_coins(entry["fee"])
        result.setdefault("comment", "")
        result.setdefault("comment_to", "")
        return result

    def list_transactions(self, count: int = 10, skip: int = 0) -> list[dict[str, object]]:
        txs: dict[str, dict] = self.data.get("transactions", {})
        ordered = sorted(txs.items(), key=lambda item: item[1]["time"], reverse=True)
        sliced = ordered[skip : skip + count]
        result = []
        best = self.state_db.get_best_tip()
        for txid, entry in sliced:
            confirmations = 0
            if best and entry.get("height") is not None:
                confirmations = best[1] - entry["height"] + 1
            result.append(
                {
                    "txid": txid,
                    "category": entry["category"],
                    "amount": liners_to_coins(entry["amount"]),
                    "time": entry["time"],
                    "confirmations": confirmations,
                    "addresses": entry.get("addresses", []),
                    "comment": entry.get("comment", ""),
                    "comment_to": entry.get("comment_to", ""),
                }
            )
        return result

    def wallet_info(self) -> dict[str, object]:
        best = self.state_db.get_best_tip()
        status = self.sync_status()
        return {
            "encrypted": self.is_encrypted(),
            "locked": self.is_locked(),
            "address_count": len(self.data.get("addresses", {})),
            "next_index": int(self.data.get("next_index", 0)),
            "best_height": best[1] if best else -1,
            "syncing": bool(status["syncing"]),
            "last_sync": status["last_sync"],
            "processed_height": status["processed_height"],
        }

    # Background sync management ------------------------------------------------

    def start_background_sync(self, should_abort: Callable[[], bool] | None = None) -> None:
        with self._sync_lock:
            if self._sync_thread and self._sync_thread.is_alive():
                return
            if should_abort is not None:
                self._sync_should_abort = should_abort
            self._sync_stop.clear()
            thread = threading.Thread(target=self._sync_worker, name="wallet-sync", daemon=True)
            self._sync_thread = thread
            thread.start()

    def ensure_background_sync(self, should_abort: Callable[[], bool] | None = None) -> None:
        if self._sync_thread and self._sync_thread.is_alive():
            return
        self.start_background_sync(should_abort or self._sync_should_abort)

    def stop_background_sync(self) -> None:
        with self._sync_lock:
            if not self._sync_thread:
                return
            self._sync_stop.set()
            self._sync_event.set()
            thread = self._sync_thread
            self._sync_thread = None
        thread.join(timeout=1.0)

    def request_sync(self, max_blocks: int | None = None) -> None:
        if time.time() < self._next_sync_after:
            return
        with self._pending_sync_lock:
            if max_blocks is None:
                self._pending_sync_max_blocks = None
            elif self._pending_sync_max_blocks is None:
                self._pending_sync_max_blocks = max_blocks
            else:
                self._pending_sync_max_blocks = max(max_blocks, self._pending_sync_max_blocks)
        self._sync_event.set()

    def needs_sync(self) -> bool:
        best = self.state_db.get_best_tip()
        if not best:
            return False
        processed = int(self.data.get("processed_height", -1))
        return processed < best[1]

    def sync_status(self) -> dict[str, object]:
        with self._sync_status_lock:
            return dict(self._sync_status)

    def list_addresses(self) -> list[dict[str, object]]:
        result: list[dict[str, object]] = []
        for address, meta in self.data.get("addresses", {}).items():
            result.append(
                {
                    "address": address,
                    "label": meta.get("label", ""),
                    "watch_only": bool(meta.get("watch_only")),
                    "external": bool(meta.get("external")),
                    "created": meta.get("created"),
                    "index": meta.get("index"),
                }
            )
        return result

    def address_balances(self, min_conf: int = 1) -> list[dict[str, object]]:
        unspent = self.list_unspent(min_conf=min_conf)
        totals: dict[str, int] = {}
        for entry in unspent:
            addr = entry.get("address")
            if not addr:
                continue
            totals[addr] = totals.get(addr, 0) + int(entry.get("amount_liners") or coins_to_liners(entry["amount"]))
        result: list[dict[str, object]] = []
        for addr, meta in self.data.get("addresses", {}).items():
            liners = totals.get(addr, 0)
            result.append(
                {
                    "address": addr,
                    "balance": liners_to_coins(liners),
                    "label": meta.get("label", ""),
                    "watch_only": bool(meta.get("watch_only")),
                    "spendable": not bool(meta.get("watch_only")),
                }
            )
        return result

    def import_private_key(self, wif: str, label: str | None = None, rescan: bool = True) -> str:
        priv_int, compressed = self._decode_wif(wif)
        priv_bytes = priv_int.to_bytes(32, "big")
        pubkey = crypto.generate_pubkey(priv_int, compressed=compressed)
        address = crypto.address_from_pubkey(pubkey)
        entry = {
            "address": address,
            "index": None,
            "label": label or "",
            "created": time.time(),
            "watch_only": False,
            "external": True,
            "compressed": compressed,
        }
        with self.lock:
            self._store_external_privkey(entry, priv_bytes)
            self.data.setdefault("addresses", {})[address] = entry
            self._save()
        if rescan:
            # Full rescan so historical transactions for this key appear in history.
            self.rescan_wallet()
        return address

    # Wallet security helpers --------------------------------------------------

    def is_encrypted(self) -> bool:
        return bool(self.data.get("encrypted"))

    def is_locked(self) -> bool:
        if not self.is_encrypted():
            return False
        return self._unlocked_seed is None

    def _derive_address_for_seed(self, seed: bytes, index: int) -> str:
        index_bytes = index.to_bytes(4, "big")
        digest = hashlib.sha256(seed + index_bytes).digest()
        priv = int.from_bytes(digest, "big") % (crypto.SECP_N - 1)
        priv += 1
        pub = crypto.generate_pubkey(priv)
        return crypto.address_from_pubkey(pub)

    def _seed_matches_wallet(self, seed: bytes) -> bool:
        addresses = self.data.get("addresses", {})
        if not isinstance(addresses, dict) or not addresses:
            return True
        best_index: int | None = None
        best_address: str | None = None
        for address, meta in addresses.items():
            if not isinstance(address, str):
                continue
            if not isinstance(meta, dict):
                continue
            if bool(meta.get("watch_only")):
                continue
            index = meta.get("index")
            if not isinstance(index, int):
                continue
            if best_index is None or index < best_index:
                best_index = index
                best_address = address
        if best_index is None or best_address is None:
            return True
        derived = self._derive_address_for_seed(seed, best_index)
        return derived == best_address

    def encrypt_wallet(self, passphrase: str) -> None:
        if self.is_encrypted():
            raise WalletError("Wallet already encrypted")
        if not passphrase:
            raise WalletError("Passphrase must not be empty")
        seed_hex = self.data.get("seed")
        if not seed_hex:
            raise WalletError("Seed missing")
        seed = bytes.fromhex(seed_hex)
        salt = secrets.token_bytes(16)
        enc_key, mac_key = _derive_keys(passphrase, salt)
        encrypted = _xor_bytes(seed, _keystream(enc_key, b"seed"))
        seed_mac = _hmac_sha256(mac_key, b"seed-v2:" + encrypted).hex()
        self.data["seed"] = None
        self.data["seed_encrypted"] = encrypted.hex()
        self.data["seed_mac"] = seed_mac
        self.data["salt"] = salt.hex()
        self.data["encrypted"] = True
        self.data["enc_version"] = 2
        for address, entry in self.data.get("addresses", {}).items():
            priv_hex = entry.pop("privkey", None)
            if priv_hex:
                entry["address"] = address
                cipher = _xor_bytes(bytes.fromhex(priv_hex), _keystream(enc_key, b"privkey:" + address.encode("utf-8")))
                entry["privkey_encrypted"] = cipher.hex()
                entry["privkey_mac"] = _hmac_sha256(mac_key, b"privkey-v2:" + cipher).hex()
        self._unlocked_seed = None
        self._active_key = None
        self._active_mac_key = None
        self._unlock_until = None
        self._save()

    def unlock_wallet(self, passphrase: str, timeout: int) -> None:
        if not self.is_encrypted():
            raise WalletError("Wallet is not encrypted")
        if not passphrase:
            raise WalletError("Passphrase must not be empty")
        salt_hex = self.data.get("salt")
        encrypted_hex = self.data.get("seed_encrypted")
        if not salt_hex or not encrypted_hex:
            raise WalletError("Corrupted encrypted seed")
        salt = bytes.fromhex(salt_hex)
        encrypted = bytes.fromhex(encrypted_hex)
        enc_key, mac_key = _derive_keys(passphrase, salt)
        enc_version = int(self.data.get("enc_version") or 1)
        seed_mac_hex = self.data.get("seed_mac")
        if seed_mac_hex:
            prefix = b"seed-v2:" if enc_version >= 2 else b"seed-v1:"
            expected = _hmac_sha256(mac_key, prefix + encrypted).hex()
            if not hmac.compare_digest(expected, str(seed_mac_hex)):
                raise WalletError("Incorrect passphrase or corrupted wallet")
        if enc_version >= 2:
            seed = _xor_bytes(encrypted, _keystream(enc_key, b"seed"))
        else:
            seed = _xor_bytes(encrypted, enc_key)
            if not self._seed_matches_wallet(seed):
                raise WalletError("Incorrect passphrase or corrupted wallet")
            if not seed_mac_hex:
                self.data["seed_mac"] = _hmac_sha256(mac_key, b"seed-v1:" + encrypted).hex()
                for _address, entry in self.data.get("addresses", {}).items():
                    if not isinstance(entry, dict):
                        continue
                    enc = entry.get("privkey_encrypted")
                    if isinstance(enc, str) and enc and not entry.get("privkey_mac"):
                        entry["privkey_mac"] = _hmac_sha256(mac_key, b"privkey-v1:" + bytes.fromhex(enc)).hex()
                self._save()
        self._unlocked_seed = seed
        self._active_key = enc_key
        self._active_mac_key = mac_key
        timeout = max(1, int(timeout))
        self._unlock_until = time.time() + timeout

    def lock_wallet(self) -> None:
        self._unlocked_seed = None
        self._active_key = None
        self._active_mac_key = None
        self._unlock_until = None

    def tick(self) -> None:
        if self._unlock_until and time.time() > self._unlock_until:
            self.lock_wallet()

    # Backup and import --------------------------------------------------------

    def export_seed(self) -> str:
        seed = self._seed_bytes()
        return seed.hex()

    def import_seed(self, seed_hex: str, wipe_existing: bool = True) -> None:
        if not seed_hex:
            raise WalletError("Seed missing")
        try:
            seed_bytes = bytes.fromhex(seed_hex)
        except ValueError as exc:
            raise WalletError("Invalid seed encoding") from exc
        if len(seed_bytes) != 32:
            raise WalletError("Seed must be 32 bytes")
        with self.lock:
            if self.is_encrypted():
                if not self._active_key:
                    raise WalletLockedError("Unlock wallet before importing seed")
                enc_version = int(self.data.get("enc_version") or 1)
                if enc_version >= 2:
                    encrypted = _xor_bytes(seed_bytes, _keystream(self._active_key, b"seed"))
                    self.data["seed_encrypted"] = encrypted.hex()
                    if self._active_mac_key:
                        self.data["seed_mac"] = _hmac_sha256(self._active_mac_key, b"seed-v2:" + encrypted).hex()
                else:
                    encrypted = _xor_bytes(seed_bytes, self._active_key)
                    self.data["seed_encrypted"] = encrypted.hex()
                    if self._active_mac_key:
                        self.data["seed_mac"] = _hmac_sha256(self._active_mac_key, b"seed-v1:" + encrypted).hex()
            else:
                self.data["seed"] = seed_hex
            if wipe_existing:
                self.data["addresses"] = {}
                self.data["transactions"] = {}
                self.data["outputs"] = {}
                self.data["next_index"] = 0
                self.data["processed_height"] = -1
            if self._active_key:
                self._unlocked_seed = seed_bytes
            self._save()

    def dump_wallet(self, path: Path) -> str:
        payload = {
            "version": 1,
            "created": int(time.time()),
            "seed": self.export_seed(),
            "next_index": int(self.data.get("next_index", 0)),
            "addresses": [
                {
                    "address": addr,
                    "index": meta.get("index"),
                    "label": meta.get("label", ""),
                    "watch_only": meta.get("watch_only", False),
                    "created": meta.get("created"),
                }
                for addr, meta in self.data.get("addresses", {}).items()
            ],
        }
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return str(path)

    def import_wallet(self, path: Path, rescan: bool = True) -> None:
        path = Path(path)
        if not path.exists():
            raise WalletError("Backup file not found")
        with path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
        seed_hex = payload.get("seed")
        if not seed_hex:
            raise WalletError("Backup missing seed")
        self.import_seed(seed_hex, wipe_existing=True)
        addresses = payload.get("addresses", [])
        next_index = int(payload.get("next_index", 0))
        with self.lock:
            addr_map: dict[str, dict] = {}
            owned_max = -1
            for entry in addresses:
                addr = entry.get("address")
                if not addr:
                    continue
                watch_only = bool(entry.get("watch_only"))
                created_ts = entry.get("created", time.time())
                label = entry.get("label", "")
                if watch_only:
                    addr_map[addr] = {
                        "index": None,
                        "label": label,
                        "created": created_ts,
                        "watch_only": True,
                    }
                    continue
                idx = entry.get("index")
                if idx is None:
                    continue
                derived_addr, _ = self._derive_address(int(idx))
                addr_map[derived_addr] = {
                    "index": int(idx),
                    "label": label,
                    "created": created_ts,
                    "watch_only": False,
                }
                owned_max = max(owned_max, int(idx))
            self.data["addresses"] = addr_map
            if next_index:
                self.data["next_index"] = max(next_index, owned_max + 1)
            else:
                self.data["next_index"] = owned_max + 1 if owned_max >= 0 else 0
            self._save()
        if rescan:
            # Reset scan height to include historical activity for imported keys.
            self.rescan_wallet()

    def import_address(self, address: str, label: str | None = None, rescan: bool = True) -> None:
        script_from_address(address)  # raises if invalid
        with self.lock:
            entries = self.data.setdefault("addresses", {})
            entries[address] = {
                "index": None,
                "label": label or "",
                "created": time.time(),
                "watch_only": True,
            }
            self._save()
        if rescan:
            # Rescan to pick up historical UTXOs for the watch-only address.
            self.rescan_wallet()

    def _sync_worker(self) -> None:
        while not self._sync_stop.is_set():
            self._sync_event.wait()
            self._sync_event.clear()
            if self._sync_stop.is_set():
                break
            max_blocks = self._drain_pending_sync_request()
            try:
                now = time.time()
                if now < self._next_sync_after:
                    time.sleep(max(0, self._next_sync_after - now))
                    if self._background_should_abort():
                        continue
                self.sync_chain(self._background_should_abort, max_blocks)
            except Exception as exc:  # noqa: BLE001 - background logging
                self._update_sync_status(syncing=False, last_error=str(exc))

    def _drain_pending_sync_request(self) -> int | None:
        with self._pending_sync_lock:
            value = self._pending_sync_max_blocks
            self._pending_sync_max_blocks = None
            return value

    def _background_should_abort(self) -> bool:
        if self._sync_stop.is_set():
            return True
        if self._sync_should_abort and self._sync_should_abort():
            return True
        return False

    def _update_sync_status(self, **updates: object) -> None:
        with self._sync_status_lock:
            self._sync_status.update(updates)
