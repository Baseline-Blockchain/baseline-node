"""
Wallet-specific RPC helpers extracted from the main handler.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from ..policy import MIN_RELAY_FEE_RATE
from ..wallet import WalletError, WalletLockedError, WalletManager, coins_to_liners
from .errors import RPCError


class WalletRPCMixin:
    """Mixin providing wallet JSON-RPC helpers."""

    wallet: WalletManager | None

    def _wallet_method_map(self) -> dict[str, Callable[..., Any]]:
        return {
            "getnewaddress": self.getnewaddress,
            "getbalance": self.getbalance,
            "listaddresses": self.listaddresses,
            "listaddressbalances": self.listaddressbalances,
            "listunspent": self.listunspent,
            "getreceivedbyaddress": self.getreceivedbyaddress,
            "sendtoaddress": self.sendtoaddress,
            "createscheduledtx": self.createscheduledtx,
            "listscheduledtx": self.listscheduledtx,
            "getschedule": self.getschedule,
            "cancelscheduledtx": self.cancelscheduledtx,
            "gettransaction": self.rpc_gettransaction,
            "listtransactions": self.listtransactions,
            "rescanwallet": self.rescanwallet,
            "encryptwallet": self.encryptwallet,
            "walletpassphrase": self.walletpassphrase,
            "walletlock": self.walletlock,
            "dumpwallet": self.dumpwallet,
            "importwallet": self.importwallet,
            "importaddress": self.importaddress,
            "walletinfo": self.walletinfo,
            "importprivkey": self.importprivkey,
            "exportseed": self.exportseed,
            "importseed": self.importseed,
        }

    def _require_wallet(self) -> WalletManager:
        if not self.wallet:
            raise RPCError(-32601, "Wallet disabled")
        return self.wallet

    def _wallet_call(self, func: Callable[[WalletManager, Any], Any], *args, sync: bool = True, **kwargs) -> Any:
        wallet = self._require_wallet()
        wallet.ensure_background_sync()
        if sync and wallet.needs_sync():
            wallet.request_sync()
        try:
            return func(wallet, *args, **kwargs)
        except WalletLockedError as exc:
            raise RPCError(-13, str(exc)) from exc
        except WalletError as exc:
            raise RPCError(-4, str(exc)) from exc
        except ValueError as exc:
            raise RPCError(-3, str(exc)) from exc

    def getnewaddress(self, label: str | None = None) -> str:
        return self._wallet_call(lambda w: w.get_new_address(label))

    def getbalance(self, account: str | None = None, min_conf: int = 1) -> float:
        return self._wallet_call(lambda w: w.get_balance(int(min_conf)))

    def listaddresses(self) -> list[dict[str, object]]:
        return self._wallet_call(lambda w: w.list_addresses())

    def listaddressbalances(self, min_conf: int = 1) -> list[dict[str, object]]:
        return self._wallet_call(lambda w: w.address_balances(int(min_conf)))

    def listunspent(
        self,
        min_conf: int = 1,
        max_conf: int = 9999999,
        addresses: list[str] | None = None,
    ) -> list[dict[str, object]]:
        return self._wallet_call(lambda w: w.list_unspent(int(min_conf), int(max_conf), addresses))

    def sendtoaddress(
        self,
        address: str,
        amount: float,
        comment: str | None = None,
        comment_to: str | None = None,
        options: dict[str, Any] | None = None,
    ) -> str:
        opts = options or {}
        from_addresses = opts.get("fromaddresses")
        change_address = opts.get("changeaddress")
        fee_override = opts.get("fee")
        feerate_override = opts.get("feerate")
        fee_liners = None
        if fee_override is not None:
            fee_liners = coins_to_liners(fee_override)
        fee_rate_liners = None
        if feerate_override is not None:
            fee_rate_liners = coins_to_liners(feerate_override)
        return self._wallet_call(
            lambda w: w.send_to_address(
                address,
                amount,
                fee=fee_liners,
                fee_rate=fee_rate_liners if fee_rate_liners is not None else MIN_RELAY_FEE_RATE,
                from_addresses=from_addresses,
                change_address=change_address,
                comment=comment,
                comment_to=comment_to,
            )
        )

    def createscheduledtx(
        self,
        address: str,
        amount: float,
        lock_time: int,
        cancelable: bool,
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        opts = options or {}
        from_addresses = opts.get("fromaddresses")
        change_address = opts.get("changeaddress")
        fee_override = opts.get("fee")
        feerate_override = opts.get("feerate")
        fee_liners = coins_to_liners(fee_override) if fee_override is not None else None
        fee_rate_liners = coins_to_liners(feerate_override) if feerate_override is not None else MIN_RELAY_FEE_RATE
        return self._wallet_call(
            lambda w: w.create_scheduled_transaction(
                address,
                amount,
                lock_time=int(lock_time),
                cancelable=bool(cancelable),
                fee=fee_liners,
                fee_rate=fee_rate_liners,
                from_addresses=from_addresses,
                change_address=change_address,
            )
        )

    def getreceivedbyaddress(self, address: str, min_conf: int = 1) -> float:
        def _received(wallet: WalletManager) -> float:
            balances = wallet.address_balances(int(min_conf))
            for entry in balances:
                if entry["address"] == address:
                    return float(entry["balance"])
            return 0.0

        return self._wallet_call(_received)

    def rpc_gettransaction(self, txid: str, include_watchonly: bool = False) -> dict[str, Any]:
        result = self._wallet_call(lambda w: w.get_transaction(txid))
        if result is None:
            raise RPCError(-5, "Transaction not found in wallet")
        return result

    def listtransactions(self, label: str = "*", count: int = 10, skip: int = 0, include_watchonly: bool = False) -> list[dict[str, object]]:
        return self._wallet_call(lambda w: w.list_transactions(count=int(count), skip=int(skip)))

    def listscheduledtx(self) -> list[dict[str, Any]]:
        return self._wallet_call(lambda w: w.list_scheduled_transactions())

    def getschedule(self, schedule_id: str) -> dict[str, Any]:
        return self._wallet_call(lambda w: w.get_schedule(schedule_id))

    def cancelscheduledtx(self, schedule_id: str) -> dict[str, Any]:
        return self._wallet_call(lambda w: w.cancel_scheduled_transaction(schedule_id))

    def rescanwallet(self) -> str:
        self._wallet_call(lambda w: w.rescan_wallet(), sync=False)
        return "wallet rescan started"

    def encryptwallet(self, passphrase: str) -> str:
        self._wallet_call(lambda w: w.encrypt_wallet(passphrase), sync=False)
        return "wallet encrypted"

    def walletpassphrase(self, passphrase: str, timeout: int) -> str:
        self._wallet_call(lambda w: w.unlock_wallet(passphrase, int(timeout)), sync=False)
        return "wallet unlocked"

    def walletlock(self) -> str:
        self._wallet_call(lambda w: w.lock_wallet(), sync=False)
        return "wallet locked"

    def dumpwallet(self, filename: str) -> str:
        path = Path(filename)
        return self._wallet_call(lambda w: w.dump_wallet(path), sync=False)

    def importwallet(self, filename: str, rescan: bool = True) -> str:
        path = Path(filename)
        self._wallet_call(lambda w: w.import_wallet(path, rescan=bool(rescan)), sync=False)
        return "wallet imported"

    def importaddress(self, address: str, label: str | None = None, rescan: bool = True) -> str:
        self._wallet_call(lambda w: w.import_address(address, label, bool(rescan)))
        return "address imported"

    def walletinfo(self) -> dict[str, Any]:
        return self._wallet_call(lambda w: w.wallet_info())

    def importprivkey(self, privkey: str, label: str | None = None, rescan: bool = True) -> str:
        self._wallet_call(lambda w: w.import_private_key(privkey, label, bool(rescan)))
        return "key imported"

    def exportseed(self) -> str:
        return self._wallet_call(lambda w: w.export_seed(), sync=False)

    def importseed(self, seed: str, wipe_existing: bool = True) -> str:
        self._wallet_call(lambda w: w.import_seed(seed, wipe_existing=bool(wipe_existing)), sync=False)
        return "seed imported"
