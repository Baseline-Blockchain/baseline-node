"""
JSON-RPC method handlers for Baseline.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

from ..core import difficulty
from ..core.block import Block
from ..core.chain import Chain, ChainError
from ..core.tx import COIN, Transaction
from ..mempool import Mempool, MempoolError
from ..mining.templates import TemplateBuilder
from ..policy import MIN_RELAY_FEE_RATE
from ..storage import BlockStore, StateDB
from ..time_sync import TimeManager
from ..wallet import WalletError, WalletLockedError, WalletManager, coins_to_liners


class RPCError(Exception):
    """Raised when an RPC request cannot be satisfied."""

    def __init__(self, code: int, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class RPCHandlers:
    def __init__(
        self,
        chain: Chain,
        mempool: Mempool,
        block_store: BlockStore,
        template_builder: TemplateBuilder | None,
        network: Any | None = None,
        wallet: WalletManager | None = None,
        time_manager: TimeManager | None = None,
    ):
        self.chain = chain
        self.mempool = mempool
        self.block_store = block_store
        self.state_db: StateDB = chain.state_db
        self.template_builder = template_builder
        self.network = network
        self.wallet = wallet
        self.time_manager = time_manager
        self._methods = {
            "getblockcount": self.getblockcount,
            "getbestblockhash": self.getbestblockhash,
            "getdifficulty": self.getdifficulty,
            "getrawmempool": self.getrawmempool,
            "getaddressutxos": self.getaddressutxos,
            "getaddressbalance": self.getaddressbalance,
            "getaddresstxids": self.getaddresstxids,
            "getblockhash": self.getblockhash,
            "getblock": self.getblock,
            "getrawtransaction": self.getrawtransaction,
            "gettxout": self.gettxout,
            "sendrawtransaction": self.sendrawtransaction,
            "getblocktemplate": self.getblocktemplate,
            "getblockchaininfo": self.getblockchaininfo,
            "getnetworkinfo": self.getnetworkinfo,
            "getmininginfo": self.getmininginfo,
            "submitblock": self.submitblock,
            "gettimesyncinfo": self.gettimesyncinfo,
            "getindexinfo": self.getindexinfo,
        }
        if self.wallet:
            self._methods.update(
                {
                    "getnewaddress": self.getnewaddress,
                    "getbalance": self.getbalance,
                    "listaddresses": self.listaddresses,
                    "listaddressbalances": self.listaddressbalances,
                    "listunspent": self.listunspent,
                    "getreceivedbyaddress": self.getreceivedbyaddress,
                    "sendtoaddress": self.sendtoaddress,
                    "gettransaction": self.rpc_gettransaction,
                    "listtransactions": self.listtransactions,
                    "encryptwallet": self.encryptwallet,
                    "walletpassphrase": self.walletpassphrase,
                    "walletlock": self.walletlock,
                    "dumpwallet": self.dumpwallet,
                    "importwallet": self.importwallet,
                    "importaddress": self.importaddress,
                    "walletinfo": self.walletinfo,
                    "importprivkey": self.importprivkey,
                }
            )

    def dispatch(self, method: str, params: list[Any]) -> Any:
        handler = self._methods.get(method)
        if handler is None:
            raise RPCError(-32601, "Method not found")
        try:
            return handler(*params)
        except TypeError as exc:
            raise RPCError(-32602, f"Invalid parameters for {method}: {exc}") from exc

    # RPC method implementations -------------------------------------------------

    def getblockcount(self) -> int:
        best = self.state_db.get_best_tip()
        return best[1] if best else 0

    def getbestblockhash(self) -> str:
        best = self.state_db.get_best_tip()
        if not best:
            return self.chain.genesis_hash
        return best[0]

    def getdifficulty(self) -> float:
        return self._current_difficulty()

    def getrawmempool(self, verbose: bool = False) -> Any:
        with self.mempool.lock:
            entries = dict(self.mempool.entries)
        if not verbose:
            return list(entries.keys())
        result = {}
        for txid, entry in entries.items():
            result[txid] = {
                "size": entry.size,
                "fee": entry.fee / COIN,
                "time": int(entry.time),
                "depends": list(entry.depends),
            }
        return result

    def getaddressutxos(self, options: Any) -> list[dict[str, Any]]:
        addresses = self._parse_address_list(options)
        rows = self.state_db.get_address_utxos(addresses)
        return [
            {
                "address": row["address"],
                "txid": row["txid"],
                "outputIndex": row["vout"],
                "script": row["script_pubkey"].hex(),
                "liners": row["amount"],
                "height": row["height"],
            }
            for row in rows
        ]

    def getaddressbalance(self, options: Any) -> dict[str, float]:
        addresses = self._parse_address_list(options)
        balance, received = self.state_db.get_address_balance(addresses)
        return {
            "balance_liners": balance,
            "received_liners": received,
            "balance": balance / COIN,
            "received": received / COIN,
        }

    def getaddresstxids(self, options: Any) -> list[str]:
        addresses, start, end = self._parse_address_list_with_range(options)
        return self.state_db.get_address_txids(addresses, start=start, end=end)

    def getblockhash(self, height: int) -> str:
        header = self.state_db.get_main_header_at_height(int(height))
        if header is None:
            raise RPCError(-8, "Block height out of range")
        return header.hash

    def getblock(self, block_hash: str, verbose: bool = True) -> Any:
        header = self.state_db.get_header(block_hash)
        if header is None:
            raise RPCError(-5, "Block not found")
        raw = self.block_store.get_block(block_hash)
        if not verbose:
            return raw.hex()
        block = Block.parse(raw)
        best = self.state_db.get_best_tip()
        confirmations = 0
        if best and header.status == 0:
            confirmations = best[1] - header.height + 1
        result = {
            "hash": header.hash,
            "confirmations": confirmations,
            "size": len(raw),
            "height": header.height,
            "version": block.header.version,
            "merkleroot": header.merkle_root,
            "time": header.timestamp,
            "nonce": header.nonce,
            "bits": f"{header.bits:08x}",
            "previousblockhash": header.prev_hash,
            "tx": [tx.serialize().hex() for tx in block.transactions],
        }
        return result

    def getrawtransaction(self, txid: str, verbose: bool = False) -> Any:
        tx, block_hash, height = self._find_transaction(txid)
        if tx is None:
            raise RPCError(-5, "No such transaction")
        if not verbose:
            return tx.serialize().hex()
        best = self.state_db.get_best_tip()
        confirmations = 0
        if best and height is not None:
            confirmations = max(0, best[1] - height + 1)
        return {
            "txid": tx.txid(),
            "hash": tx.txid(),
            "size": len(tx.serialize()),
            "hex": tx.serialize().hex(),
            "blockhash": block_hash,
            "confirmations": confirmations,
            "time": int(time.time()),
            "vin": [
                {"txid": vin.prev_txid, "vout": vin.prev_vout, "sequence": vin.sequence}
                for vin in tx.inputs
            ],
            "vout": [
                {"n": idx, "value": txout.value, "scriptPubKey": txout.script_pubkey.hex()}
                for idx, txout in enumerate(tx.outputs)
            ],
        }

    def gettxout(self, txid: str, vout: int, include_mempool: bool = True) -> dict[str, Any] | None:
        if include_mempool:
            tx = self.mempool.get(txid)
            if tx and 0 <= vout < len(tx.outputs):
                out = tx.outputs[vout]
                return {
                    "bestblock": None,
                    "confirmations": 0,
                    "value": out.value,
                    "scriptPubKey": out.script_pubkey.hex(),
                    "coinbase": False,
                }
        utxo = self.state_db.get_utxo(txid, int(vout))
        if utxo is None:
            return None
        best = self.state_db.get_best_tip()
        confirmations = 0
        if best:
            confirmations = max(0, best[1] - utxo.height + 1)
        return {
            "bestblock": best[0] if best else None,
            "confirmations": confirmations,
            "value": utxo.amount,
            "scriptPubKey": utxo.script_pubkey.hex(),
            "coinbase": utxo.coinbase,
        }

    def sendrawtransaction(self, raw_hex: str) -> str:
        try:
            tx = Transaction.parse(bytes.fromhex(raw_hex))
        except Exception as exc:
            raise RPCError(-22, f"TX decode failed: {exc}") from exc
        try:
            self.mempool.accept_transaction(tx, peer_id="rpc")
        except MempoolError as exc:
            raise RPCError(-26, str(exc)) from exc
        return tx.txid()

    def getblocktemplate(self, _template_request: dict[str, Any] | None = None) -> dict[str, Any]:
        if self.template_builder is None:
            raise RPCError(-38, "Mining not available")
        template = self.template_builder.build_template()
        tx_entries = [
            {
                "data": tx.serialize().hex(),
                "txid": tx.txid(),
                "hash": tx.txid(),
            }
            for tx in template.transactions
        ]
        return {
            "version": template.version,
            "previousblockhash": template.prev_hash,
            "transactions": tx_entries,
            "coinbasevalue": template.coinbase_value,
            "bits": f"{template.bits:08x}",
            "target": f"{template.target:064x}",
            "mintime": template.timestamp,
            "curtime": template.timestamp,
            "height": template.height,
            "coinb1": template.coinb1.hex(),
            "coinb2": template.coinb2.hex(),
            "extranonce": {
                "size": self.template_builder.extranonce1_size + self.template_builder.extranonce2_size
            },
        }

    def submitblock(self, block_hex: str) -> dict[str, Any]:
        try:
            raw = bytes.fromhex(block_hex)
        except ValueError as exc:
            raise RPCError(-22, f"Block decode failed: {exc}") from exc
        try:
            block = Block.parse(raw)
        except Exception as exc:
            raise RPCError(-22, f"Block parse failed: {exc}") from exc
        try:
            result = self.chain.add_block(block, raw)
        except ChainError as exc:
            raise RPCError(-26, str(exc)) from exc
        return {"status": result.get("status"), "hash": block.block_hash(), "height": result.get("height")}

    def getblockchaininfo(self) -> dict[str, Any]:
        best = self.state_db.get_best_tip()
        if not best:
            # No blocks yet, return genesis info
            best_hash = self.chain.genesis_hash
            height = 0
            best_header = None
        else:
            best_hash, height = best
            best_header = self.state_db.get_header(best_hash)

        # Calculate difficulty from current bits
        current_difficulty = self._current_difficulty()

        # Calculate chainwork (cumulative work)
        chainwork = 0
        if best_header:
            # Sum work from genesis to current tip
            current_hash = best_hash
            while current_hash and current_hash != "00" * 32:
                header = self.state_db.get_header(current_hash)
                if header is None:
                    break
                chainwork += difficulty.block_work(header.bits)
                current_hash = header.prev_hash

        # Get block time and median time
        block_time = int(time.time())
        median_time = int(time.time())
        if best_header:
            block_time = best_header.timestamp
            # Calculate median time past (simplified - just use current block time)
            median_time = best_header.timestamp

        size_on_disk = self._chain_storage_bytes()

        headers_height = self.state_db.get_max_header_height()
        verification_progress = 0.0
        if headers_height > 0:
            verification_progress = min(1.0, height / headers_height)
        now = int(time.time())
        is_ibd = True
        if best_header:
            synced_height = height >= max(1, self.chain.config.mining.retarget_interval)
            recent_enough = best_header.timestamp > now - 24 * 60 * 60
            is_ibd = not (synced_height and recent_enough)

        return {
            "chain": "main",  # Could be made configurable
            "blocks": height,
            "headers": headers_height,
            "bestblockhash": best_hash,
            "difficulty": current_difficulty,
            "time": block_time,
            "mediantime": median_time,
            "verificationprogress": verification_progress,
            "initialblockdownload": is_ibd,
            "chainwork": f"{chainwork:016x}",
            "size_on_disk": size_on_disk,
            "pruned": False,
            "warnings": []
        }

    def getnetworkinfo(self) -> dict[str, Any]:
        peers = getattr(self.network, "peers", {}) or {}
        connections = len(peers)
        inbound = sum(1 for peer in peers.values() if not getattr(peer, "outbound", False))
        outbound = connections - inbound
        timeoffset = 0
        if self.time_manager and hasattr(self.time_manager, "get_offset"):
            try:
                timeoffset = float(self.time_manager.get_offset())  # type: ignore[attr-defined]
            except Exception:
                timeoffset = 0
        networkactive = bool(self.network)
        stop_event = getattr(self.network, "_stop_event", None)
        if stop_event is not None and hasattr(stop_event, "is_set"):
            try:
                networkactive = not stop_event.is_set()
            except Exception as exc:
                logging.getLogger("baseline.rpc").debug("stop_event check failed: %s", exc)

        # Build networks array (simplified)
        networks = [
            {
                "name": "ipv4",
                "limited": False,
                "reachable": True,
                "proxy": "",
                "proxy_randomize_credentials": False
            },
            {
                "name": "ipv6",
                "limited": False,
                "reachable": True,
                "proxy": "",
                "proxy_randomize_credentials": False
            }
        ]
        localaddresses: list[dict[str, Any]] = []
        known = getattr(self.network, "known_addresses", {}) if self.network else {}
        for addr in list(known.values())[:5]:
            localaddresses.append({"address": addr.host, "port": addr.port, "score": 1})

        return {
            "version": 10000,  # Version number format similar to Bitcoin Core
            "subversion": "/Baseline:0.1.0/",
            "protocolversion": 1,
            "localservices": "0000000000000001",  # NODE_NETWORK
            "localservicesnames": ["NETWORK"],
            "localrelay": True,
            "timeoffset": timeoffset,
            "connections": connections,
            "connections_in": inbound,
            "connections_out": outbound,
            "networkactive": networkactive,
            "networks": networks,
            "relayfee": MIN_RELAY_FEE_RATE / COIN,
            "incrementalfee": MIN_RELAY_FEE_RATE / COIN,
            "localaddresses": localaddresses,
            "warnings": []
        }

    def getmininginfo(self) -> dict[str, Any]:
        height = self.getblockcount()
        difficulty_value = self._current_difficulty()
        target_spacing = max(1, self.chain.config.mining.block_interval_target)
        network_hash_ps = difficulty_value * (2 ** 32) / target_spacing
        with self.mempool.lock:
            pooled_tx = len(self.mempool.entries)
        return {
            "blocks": height,
            "currentblockweight": 0,
            "currentblocktx": 0,
            "difficulty": difficulty_value,
            "networkhashps": network_hash_ps,
            "pooledtx": pooled_tx,
            "chain": "main",
            "warnings": ""
        }

    def getindexinfo(self) -> dict[str, Any]:
        """Expose index status similar to Bitcoin Core."""
        best = self.state_db.get_best_tip()
        best_hash = best[0] if best else self.chain.genesis_hash
        best_height = best[1] if best else 0
        info = {
            "txindex": {
                "synced": True,
                "best_block_height": best_height,
                "best_block_hash": best_hash,
            },
            "coinstatsindex": {
                "synced": False,
                "best_block_height": best_height,
                "best_block_hash": best_hash,
            },
        }
        return info

    def _require_wallet(self) -> WalletManager:
        if not self.wallet:
            raise RPCError(-32601, "Wallet disabled")
        return self.wallet

    def _wallet_call(self, func, *args, sync: bool = True, **kwargs):
        wallet = self._require_wallet()
        if sync:
            wallet.sync_chain()
        try:
            return func(wallet, *args, **kwargs)
        except WalletLockedError as exc:
            raise RPCError(-13, str(exc)) from exc
        except WalletError as exc:
            raise RPCError(-4, str(exc)) from exc
        except ValueError as exc:
            raise RPCError(-3, str(exc)) from exc

    def _parse_address_list(self, options: Any) -> list[str]:
        if isinstance(options, dict):
            addresses = options.get("addresses")
        else:
            addresses = options
        if isinstance(addresses, str):
            addresses = [addresses]
        if not isinstance(addresses, list) or not addresses:
            raise RPCError(-8, "addresses must be a non-empty list")
        normalized = [str(addr) for addr in addresses]
        return normalized

    def _parse_address_list_with_range(self, options: Any) -> tuple[list[str], int | None, int | None]:
        start = None
        end = None
        if isinstance(options, dict):
            addresses = options.get("addresses")
            if "start" in options:
                start = int(options["start"])
            if "end" in options:
                end = int(options["end"])
        else:
            addresses = options
        return self._parse_address_list(addresses), start, end

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
        fee_liners = None
        if fee_override is not None:
            fee_liners = coins_to_liners(fee_override)
        return self._wallet_call(
            lambda w: w.send_to_address(
                address,
                amount,
                fee=fee_liners if fee_liners is not None else MIN_RELAY_FEE_RATE,
                from_addresses=from_addresses,
                change_address=change_address,
                comment=comment,
                comment_to=comment_to,
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

    # Helper utilities ----------------------------------------------------------

    def _find_transaction(self, txid: str) -> tuple[Transaction | None, str | None, int | None]:
        tx = self.mempool.get(txid)
        if tx:
            return tx, None, None
        best = self.state_db.get_best_tip()
        if not best:
            return None, None, None
        current_hash = best[0]
        while current_hash:
            header = self.state_db.get_header(current_hash)
            if header is None:
                break
            raw = self.block_store.get_block(current_hash)
            block = Block.parse(raw)
            for tx in block.transactions:
                if tx.txid() == txid:
                    return tx, current_hash, header.height
            if header.prev_hash is None:
                break
            current_hash = header.prev_hash
        return None, None, None

    def gettimesyncinfo(self) -> dict[str, Any]:
        """Get time synchronization status and information."""
        if not self.time_manager:
            return {
                "enabled": False,
                "synchronized": False,
                "offset": 0.0,
                "message": "NTP synchronization is disabled"
            }

        status = self.time_manager.get_sync_status()
        return {
            "enabled": True,
            "synchronized": status["synchronized"],
            "offset": status["offset"],
            "last_sync": status.get("last_sync"),
            "time_since_sync": status.get("time_since_sync"),
            "drift_rate": status.get("drift_rate"),
            "servers": status.get("servers", []),
            "system_time": time.time(),
            "synchronized_time": status.get("synchronized_time", time.time())
        }

    def _current_difficulty(self) -> float:
        best = self.state_db.get_best_tip()
        if not best:
            return 1.0
        header = self.state_db.get_header(best[0])
        if header is None:
            return 1.0
        target = difficulty.compact_to_target(header.bits)
        max_target = difficulty.compact_to_target(self.chain.config.mining.initial_bits)
        return max_target / target if target > 0 else 1.0

    def _chain_storage_bytes(self) -> int:
        total = 0
        paths: list[Path] = [
            getattr(self.block_store, "data_path", None),
            getattr(self.block_store, "index_path", None),
            getattr(self.state_db, "db_path", None),
        ]
        db_path = getattr(self.state_db, "db_path", None)
        if db_path:
            for suffix in ("-wal", "-shm"):
                paths.append(Path(f"{db_path}{suffix}"))
        for path in paths:
            if isinstance(path, Path) and path.exists():
                total += path.stat().st_size
        return total
