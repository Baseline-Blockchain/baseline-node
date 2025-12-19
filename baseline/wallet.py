"""
Simple wallet manager with address derivation, balance tracking, and RPC helpers.
"""

from __future__ import annotations

import hashlib
import json
import secrets
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal
from pathlib import Path

from .core import crypto
from .core.address import script_from_address
from .core.block import Block
from .core.tx import COIN, Transaction, TxInput, TxOutput
from .mempool import Mempool, MempoolError
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


def _derive_key(passphrase: str, salt: bytes) -> bytes:
    return hashlib.pbkdf2_hmac("sha256", passphrase.encode("utf-8"), salt, 200_000, dklen=32)


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
    def __init__(self, path: Path, state_db: StateDB, block_store: BlockStore, mempool: Mempool):
        self.path = path
        self.state_db = state_db
        self.block_store = block_store
        self.mempool = mempool
        self.lock = threading.RLock()
        self._unlocked_seed: bytes | None = None
        self._unlock_until: float | None = None
        self._active_key: bytes | None = None
        self.data: dict[str, object] = {}
        self._load()
        if not self.data["addresses"] and not self.is_encrypted():
            self.get_new_address("default")

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
        self.data.setdefault("salt", None)
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
            "processed_height": -1,
        }

    def _save(self) -> None:
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, indent=2), encoding="utf-8")
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
            encrypted = _xor_bytes(priv_bytes, self._active_key)
            entry["privkey_encrypted"] = encrypted.hex()
            entry.pop("privkey", None)
        else:
            entry["privkey"] = priv_bytes.hex()
            entry.pop("privkey_encrypted", None)

    def _load_external_privkey(self, entry: dict[str, object]) -> int | None:
        priv_hex = entry.get("privkey")
        if priv_hex:
            return int(priv_hex, 16)
        priv_enc = entry.get("privkey_encrypted")
        if priv_enc:
            if not self._active_key:
                raise WalletLockedError("Wallet locked")
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
                    "confirmations": confirmations,
                    "scriptPubKey": utxo.script_pubkey.hex(),
                    "height": utxo.height,
                    "coinbase": utxo.coinbase,
                }
            )
        return result

    def get_balance(self, min_conf: int = 1) -> float:
        unspent = self.list_unspent(min_conf=min_conf)
        total = sum(coins_to_liners(entry["amount"]) for entry in unspent)
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

    def send_to_address(
        self,
        dest_address: str,
        amount: Decimal | float | str | int,
        *,
        fee: int = 1_000,
        from_addresses: Sequence[str] | None = None,
        change_address: str | None = None,
        comment: str | None = None,
        comment_to: str | None = None,
    ) -> str:
        amount_liners = coins_to_liners(amount)
        if amount_liners <= 0:
            raise ValueError("Amount must be positive")
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
        selected = []
        total = 0
        for entry in spendable:
            selected.append(entry)
            total += coins_to_liners(entry["amount"])
            if total >= amount_liners + fee:
                break
        if total < amount_liners + fee:
            raise ValueError("Insufficient funds")
        inputs = [TxInput(prev_txid=entry["txid"], prev_vout=entry["vout"], script_sig=b"", sequence=0xFFFFFFFF) for entry in selected]
        outputs = [TxOutput(value=amount_liners, script_pubkey=script_from_address(dest_address))]
        change = total - amount_liners - fee
        chosen_change = change_address or selected[0]["address"]
        if not chosen_change:
            for addr, meta in entries.items():
                if not meta.get("watch_only"):
                    chosen_change = addr
                    break
        if not chosen_change:
            raise ValueError("No address available for change output")
        if change > 0:
            outputs.append(TxOutput(value=change, script_pubkey=script_from_address(chosen_change)))
        tx = Transaction(version=1, inputs=inputs, outputs=outputs, lock_time=0)
        for idx, entry in enumerate(selected):
            address = entry["address"]
            priv = self._lookup_privkey(address)
            script = script_from_address(address)
            sighash = tx.signature_hash(idx, script, 0x01)
            signature = crypto.sign(sighash, priv) + b"\x01"
            pubkey = crypto.generate_pubkey(priv)
            script_sig = bytes([len(signature)]) + signature + bytes([len(pubkey)]) + pubkey
            tx.inputs[idx].script_sig = script_sig
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
            fee=fee,
            comment=comment,
            comment_to=comment_to,
        )
        return tx.txid()

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
            self.data.setdefault("transactions", {})[txid] = entry
            self._save()

    def sync_chain(
        self,
        should_abort: Callable[[], bool] | None = None,
        max_blocks: int | None = None,
    ) -> bool:
        best = self.state_db.get_best_tip()
        if not best:
            return True
        best_height = best[1]
        start = int(self.data.get("processed_height", -1)) + 1
        if start > best_height:
            return True

        def aborted() -> bool:
            return bool(should_abort and should_abort())

        script_map = self._script_map()
        scripts = {script.hex(): addr for addr, script in script_map.items()}
        outputs: dict[str, dict] = self.data.setdefault("outputs", {})
        txs: dict[str, dict] = self.data.setdefault("transactions", {})
        blocks_processed = 0
        dirty = False
        for height in range(start, best_height + 1):
            if aborted():
                break
            header = self.state_db.get_main_header_at_height(height)
            if header is None:
                continue
            raw = self.block_store.get_block(header.hash)
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
                entry = txs.get(txid)
                timestamp = header.timestamp
                fee = entry.get("fee") if entry else None
                comment = entry.get("comment", "") if entry else ""
                comment_to = entry.get("comment_to", "") if entry else ""
                txs[txid] = {
                    "amount": delta,
                    "category": category,
                    "addresses": addresses or (entry.get("addresses") if entry else []),
                    "time": timestamp,
                    "blockhash": header.hash,
                    "height": height,
                    "fee": fee,
                    "comment": comment,
                    "comment_to": comment_to,
                }
            self.data["processed_height"] = height
            dirty = True
            blocks_processed += 1
            if max_blocks and blocks_processed >= max_blocks:
                break
        if dirty:
            self._save()
        return self.data.get("processed_height", -1) >= best_height

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
        return {
            "encrypted": self.is_encrypted(),
            "locked": self.is_locked(),
            "address_count": len(self.data.get("addresses", {})),
            "next_index": int(self.data.get("next_index", 0)),
            "best_height": best[1] if best else -1,
        }

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
            totals[addr] = totals.get(addr, 0) + coins_to_liners(entry["amount"])
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
            self.sync_chain()
        return address

    # Wallet security helpers --------------------------------------------------

    def is_encrypted(self) -> bool:
        return bool(self.data.get("encrypted"))

    def is_locked(self) -> bool:
        if not self.is_encrypted():
            return False
        return self._unlocked_seed is None

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
        key = _derive_key(passphrase, salt)
        encrypted = _xor_bytes(seed, key)
        self.data["seed"] = None
        self.data["seed_encrypted"] = encrypted.hex()
        self.data["salt"] = salt.hex()
        self.data["encrypted"] = True
        for entry in self.data.get("addresses", {}).values():
            priv_hex = entry.pop("privkey", None)
            if priv_hex:
                cipher = _xor_bytes(bytes.fromhex(priv_hex), key)
                entry["privkey_encrypted"] = cipher.hex()
        self._unlocked_seed = None
        self._active_key = None
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
        key = _derive_key(passphrase, salt)
        seed = _xor_bytes(encrypted, key)
        self._unlocked_seed = seed
        self._active_key = key
        timeout = max(1, int(timeout))
        self._unlock_until = time.time() + timeout

    def lock_wallet(self) -> None:
        self._unlocked_seed = None
        self._active_key = None
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
                encrypted = _xor_bytes(seed_bytes, self._active_key)
                self.data["seed_encrypted"] = encrypted.hex()
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
            self.sync_chain()

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
            self.sync_chain()
