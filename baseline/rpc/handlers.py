"""
JSON-RPC method handlers for Baseline.
"""

from __future__ import annotations

import logging
import math
import threading
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any

from ..core import difficulty
from ..core.block import Block, BlockHeader
from ..core.chain import Chain, ChainError, UTXOView
from ..core.tx import COIN, Transaction
from ..mempool import Mempool, MempoolError
from ..mining import PayoutTracker, StratumServer
from ..mining.templates import TemplateBuilder
from ..policy import MIN_RELAY_FEE_RATE
from ..storage import BlockStore, HeaderData, StateDB, UTXORecord
from ..time_sync import TimeManager
from ..wallet import WalletManager
from .errors import RPCError
from .wallet_rpc import WalletRPCMixin


class RPCHandlers(WalletRPCMixin):
    def __init__(
        self,
        chain: Chain,
        mempool: Mempool,
        block_store: BlockStore,
        template_builder: TemplateBuilder | None,
        network: Any | None = None,
        wallet: WalletManager | None = None,
        time_manager: TimeManager | None = None,
        payout_tracker: PayoutTracker | None = None,
        stratum_server: StratumServer | None = None,
    ):
        self.chain = chain
        self.mempool = mempool
        self.block_store = block_store
        self.state_db: StateDB = chain.state_db
        self.template_builder = template_builder
        self.network = network
        self.wallet = wallet
        self.time_manager = time_manager
        self.payout_tracker = payout_tracker
        self.stratum = stratum_server
        self._start_time = time.time()
        self._cache_lock = threading.RLock()
        self._block_cache: OrderedDict[str, tuple[Block, HeaderData]] = OrderedDict()
        self._block_cache_limit = 64
        self._tx_cache: OrderedDict[str, tuple[Transaction, str | None, int | None]] = OrderedDict()
        self._tx_cache_limit = 512
        self._raw_tx_cache: OrderedDict[tuple[str, str], dict[str, Any]] = OrderedDict()
        self._raw_tx_cache_limit = self._tx_cache_limit
        self._block_stats_cache: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._block_stats_limit = 128
        self._methods = {
            "getblockcount": self.getblockcount,
            "getbestblockhash": self.getbestblockhash,
            "getdifficulty": self.getdifficulty,
            "getrawmempool": self.getrawmempool,
            "getrichlist": self.getrichlist,
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
            "estimatesmartfee": self.estimatesmartfee,
            "getblockheader": self.getblockheader,
            "getchaintxstats": self.getchaintxstats,
            "getblockstats": self.getblockstats,
            "getmempoolinfo": self.getmempoolinfo,
            "getnettotals": self.getnettotals,
            "getpeerinfo": self.getpeerinfo,
            "uptime": self.uptime,
            "gettxoutsetinfo": self.gettxoutsetinfo,
            "getpoolstats": self.getpoolstats,
            "getpoolworkers": self.getpoolworkers,
            "getpoolpendingblocks": self.getpoolpendingblocks,
            "getpoolmatured": self.getpoolmatured,
            "getpoolpayoutpreview": self.getpoolpayoutpreview,
            "getstratumsessions": self.getstratumsessions,
            "poolreconcile": self.poolreconcile,
            "getchaintips": self.getchaintips,
        }
        if self.wallet:
            self._methods.update(self._wallet_method_map())

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
            if not verbose:
                return list(self.mempool.entries.keys())
            result: dict[str, Any] = {}
            for txid, entry in self.mempool.entries.items():
                result[txid] = {
                    "size": entry.size,
                    "fee": entry.fee / COIN,
                    "time": int(entry.time),
                    "depends": list(entry.depends),
                }
        return result

    def getrichlist(self, count: int = 25, offset: int = 0) -> list[dict[str, Any]]:
        """Return richest addresses by current UTXO balance."""
        count = int(count)
        offset = int(offset)
        if count <= 0:
            raise RPCError(-8, "count must be > 0")
        if offset < 0:
            raise RPCError(-8, "offset must be >= 0")
        count = min(count, 5000)
        rows = self.state_db.get_rich_list(count, offset)
        for row in rows:
            row["balance"] = row["balance_liners"] / COIN
        return rows

    def getaddressutxos(self, options: Any) -> list[dict[str, Any]]:
        default_limit = 500
        addresses = self._parse_address_list(options)
        limit = None
        offset = 0
        if isinstance(options, dict):
            try:
                limit = int(options.get("limit")) if options.get("limit") is not None else None
                offset = int(options.get("offset") or 0)
            except Exception as exc:
                raise RPCError(-8, "limit/offset must be integers") from exc
            if offset < 0:
                raise RPCError(-8, "offset must be >= 0")
            if limit is not None and limit <= 0:
                raise RPCError(-8, "limit must be > 0")
        if limit is None:
            limit = default_limit
        rows = self.state_db.get_address_utxos(addresses, limit=limit, offset=offset)
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
        tip = self.state_db.get_best_tip()
        best_height = tip[1] if tip else 0
        maturity = self.chain.config.mining.coinbase_maturity
        balance, received, matured, immature = self.state_db.get_address_balance(
            addresses, tip_height=best_height, maturity=maturity
        )
        return {
            "balance_liners": balance,
            "received_liners": received,
            "matured_liners": matured,
            "immature_liners": immature,
            "balance": balance / COIN,
            "received": received / COIN,
            "matured": matured / COIN,
            "immature": immature / COIN,
        }

    def getaddresstxids(self, options: Any) -> list[Any]:
        default_limit = 500
        include_height = False
        limit = None
        offset = 0
        if isinstance(options, dict):
            include_height = bool(options.get("include_height"))
            try:
                limit = int(options.get("limit")) if options.get("limit") is not None else None
                offset = int(options.get("offset") or 0)
            except Exception as exc:
                raise RPCError(-8, "limit/offset must be integers") from exc
            if offset < 0:
                raise RPCError(-8, "offset must be >= 0")
            if limit is not None and limit <= 0:
                raise RPCError(-8, "limit must be > 0")
        if limit is None:
            limit = default_limit
        addresses, start, end = self._parse_address_list_with_range(options)
        return self.state_db.get_address_txids(
            addresses,
            start=start,
            end=end,
            include_height=include_height,
            limit=limit,
            offset=offset,
        )

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
        median_time = self.chain._median_time_past(header.hash)
        next_header = self.state_db.get_main_header_at_height(header.height + 1)
        difficulty_value = self._difficulty_from_bits(header.bits)
        result = {
            "hash": header.hash,
            "confirmations": confirmations,
            "size": len(raw),
             "strippedsize": len(raw),
             "weight": block.weight(),
            "height": header.height,
            "version": block.header.version,
            "versionHex": f"{block.header.version:08x}",
            "merkleroot": header.merkle_root,
            "time": header.timestamp,
            "mediantime": median_time,
            "nonce": header.nonce,
            "bits": f"{header.bits:08x}",
            "difficulty": difficulty_value,
            "chainwork": header.chainwork,
            "previousblockhash": header.prev_hash,
            "nextblockhash": next_header.hash if next_header else None,
            "tx": [tx.txid() for tx in block.transactions],
            "nTx": len(block.transactions),
            "hex": raw.hex(),
        }
        return result

    def getrawtransaction(self, txid: str, verbose: bool = False, block_hash: str | None = None) -> Any:
        tx, block_hash, height = self._find_transaction(txid, block_hash=block_hash)
        if tx is None:
            raise RPCError(-5, "No such transaction")
        if not verbose:
            return tx.serialize().hex()
        txid = tx.txid()
        prev_cache: dict[str, Transaction] = {}
        spent_map = self._build_spent_output_map(block_hash)
        best = self.state_db.get_best_tip()
        confirmations = 0
        if best and height is not None:
            confirmations = max(0, best[1] - height + 1)
        cached = self._get_cached_raw_tx(txid, block_hash)
        if cached:
            response = dict(cached)
            response["confirmations"] = confirmations
            return response
        fee_liners = self._transaction_fee(tx, cache=prev_cache, spent_map=spent_map)
        timestamp = int(time.time())
        if height is not None:
            header = None
            if block_hash:
                header = self.state_db.get_header(block_hash)
            if header is None:
                header = self.state_db.get_main_header_at_height(height)
            if header:
                timestamp = header.timestamp
        vin_entries: list[dict[str, Any]] = []
        for vin in tx.inputs:
            if vin.prev_txid == "00" * 32 and vin.prev_vout == 0xFFFFFFFF:
                vin_entries.append({"coinbase": vin.script_sig.hex(), "sequence": vin.sequence})
                continue
            value = self._resolve_prev_output_value(
                vin.prev_txid,
                vin.prev_vout,
                prev_cache,
                spent_map=spent_map,
            )
            entry: dict[str, Any] = {
                "txid": vin.prev_txid,
                "vout": vin.prev_vout,
                "sequence": vin.sequence,
            }
            if value is not None:
                entry["value_liners"] = value
                entry["value"] = value / COIN
            vin_entries.append(entry)
        raw_tx = tx.serialize()
        response = {
            "txid": txid,
            "hash": txid,
            "size": len(raw_tx),
            "hex": raw_tx.hex(),
            "blockhash": block_hash,
            "confirmations": confirmations,
            "fee": fee_liners / COIN,
            "fee_liners": fee_liners,
            "time": timestamp,
            "vin": vin_entries,
            "vout": [
                {"n": idx, "value": txout.value, "scriptPubKey": txout.script_pubkey.hex()}
                for idx, txout in enumerate(tx.outputs)
            ],
        }
        self._remember_raw_tx(txid, block_hash, response)
        return response

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
        status = result.get("status")
        block_hash = block.block_hash()
        if status in {"connected", "reorganized"}:
            if self.mempool:
                try:
                    self.mempool.remove_confirmed(block.transactions)
                except Exception as exc:  # pragma: no cover - defensive
                    logging.getLogger("baseline.rpc").debug("Failed to prune mempool after submitblock: %s", exc)
            # Announce the new block to peers for propagation
            if self.network and hasattr(self.network, "announce_block"):
                try:
                    self.network.announce_block(block_hash)
                except Exception as exc:  # pragma: no cover - defensive
                    logging.getLogger("baseline.rpc").debug("Failed to announce block %s: %s", block_hash, exc)
        return {"status": status, "hash": block_hash, "height": result.get("height")}

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

        current_difficulty = self._current_difficulty()
        block_time = int(time.time())
        median_time = block_time
        if best_header:
            block_time = best_header.timestamp
            median_time = best_header.timestamp

        size_on_disk = self._chain_storage_bytes()
        headers_height = self.state_db.get_max_header_height()
        verification_progress = min(1.0, height / headers_height) if headers_height else 0.0
        now = int(time.time())
        is_ibd = True
        if best_header:
            synced_height = height >= max(1, self.chain.config.mining.retarget_interval)
            recent_enough = best_header.timestamp > now - 24 * 60 * 60
            is_ibd = not (synced_height and recent_enough)
        best_work = int(self.state_db.get_meta("best_work") or "0")
        chainwork_hex = f"{best_work:016x}"
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
            "chainwork": chainwork_hex,
            "size_on_disk": size_on_disk,
            "pruned": False,
            "warnings": []
        }

    def getchaintips(self) -> list[dict[str, Any]]:
        tips = self.state_db.list_chain_tips()
        best = self.state_db.get_best_tip()
        best_hash = best[0] if best else None

        results = []
        for block_hash, height, work in tips:
            status = "active" if block_hash == best_hash else "valid-fork"
            results.append({
                "height": height,
                "hash": block_hash,
                "branchlen": 0,
                "status": status,
            })
        return results

    def getnetworkinfo(self) -> dict[str, Any]:
        snapshot = None
        if self.network and hasattr(self.network, "snapshot_network_state"):
            try:
                snapshot = self.network.snapshot_network_state()
            except Exception as exc:
                logging.getLogger("baseline.rpc").debug("network snapshot failed: %s", exc)

        if snapshot:
            connections = int(snapshot.get("connections", 0) or 0)
            inbound = int(snapshot.get("inbound", 0) or 0)
            outbound = int(snapshot.get("outbound", 0) or 0)
            localaddresses = list(snapshot.get("localaddresses", []) or [])
            networkactive = bool(snapshot.get("networkactive", False))
        else:
            peers = getattr(self.network, "peers", {}) or {}
            connections = len(peers)
            inbound = sum(1 for peer in peers.values() if not getattr(peer, "outbound", False))
            outbound = connections - inbound
            localaddresses = []
            known = getattr(self.network, "known_addresses", {}) if self.network else {}
            for addr in list(known.values())[:5]:
                localaddresses.append({"address": addr.host, "port": addr.port, "score": 1})
            networkactive = bool(self.network)
            stop_event = getattr(self.network, "_stop_event", None)
            if stop_event is not None and hasattr(stop_event, "is_set"):
                try:
                    networkactive = not stop_event.is_set()
                except Exception as exc:
                    logging.getLogger("baseline.rpc").debug("stop_event check failed: %s", exc)

        timeoffset = 0
        if self.time_manager and hasattr(self.time_manager, "get_offset"):
            try:
                timeoffset = float(self.time_manager.get_offset())  # type: ignore[attr-defined]
            except Exception:
                timeoffset = 0

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
        network_hash_ps = self._network_hashps(difficulty_value, target_spacing)
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
            "addressindex": {
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

    def estimatesmartfee(self, target_blocks: int = 1, estimate_mode: str | None = None) -> dict[str, Any]:
        """Estimate a feerate (BLINE/kB) using current mempool data."""
        feerate = self._estimate_fee_rate(int(target_blocks))
        return {
            "feerate": feerate,
            "blocks": int(target_blocks),
            "errors": [],
            "estimate_mode": estimate_mode or "CONSERVATIVE",
        }

    def getblockheader(self, block_hash: str, verbose: bool = True) -> Any:
        header = self.state_db.get_header(block_hash)
        if header is None:
            raise RPCError(-5, "Block not found")
        best = self.state_db.get_best_tip()
        confirmations = 0
        if best and header.status == 0:
            confirmations = max(0, best[1] - header.height + 1)
        tx_count = 0
        if verbose:
            try:
                block, _ = self._load_block_by_hash(header.hash)
            except RPCError:
                tx_count = 0
            else:
                tx_count = len(block.transactions)
        if not verbose:
            header_obj = BlockHeader(
                version=getattr(header, "version", 1),
                prev_hash=header.prev_hash or "00" * 32,
                merkle_root=header.merkle_root,
                timestamp=header.timestamp,
                bits=header.bits,
                nonce=header.nonce,
            )
            return header_obj.serialize().hex()
        next_header = self.state_db.get_main_header_at_height(header.height + 1)
        return {
            "hash": header.hash,
            "confirmations": confirmations,
            "height": header.height,
            "version": getattr(header, "version", 1),
            "versionHex": f"{getattr(header, 'version', 1):08x}",
            "merkleroot": header.merkle_root,
            "time": header.timestamp,
            "mediantime": header.timestamp,
            "nonce": header.nonce,
            "bits": f"{header.bits:08x}",
            "difficulty": self._difficulty_from_bits(header.bits),
            "chainwork": header.chainwork,
            "previousblockhash": header.prev_hash,
            "nextblockhash": next_header.hash if next_header else None,
            "nTx": tx_count,
        }

    def getchaintxstats(self, nblocks: int | None = None, blockhash: str | None = None) -> dict[str, Any]:
        best = self.state_db.get_best_tip()
        if not best:
            raise RPCError(-8, "Blockchain not initialized")
        if blockhash:
            end_metrics = self.state_db.get_block_metrics_by_hash(blockhash)
            if end_metrics is None:
                return self._compute_chaintxstats_slow(nblocks, blockhash)
        else:
            _, best_height = best
            end_metrics = self.state_db.get_block_metrics_by_height(best_height)
            if end_metrics is None:
                return self._compute_chaintxstats_slow(nblocks, blockhash)
        end_height = end_metrics["height"]
        if nblocks is None or nblocks <= 0:
            start_height = 0
        else:
            start_height = max(0, end_height - int(nblocks))
        start_metrics = self.state_db.get_block_metrics_by_height(start_height)
        if start_metrics is None:
            return self._compute_chaintxstats_slow(nblocks, blockhash)
        total_tx = int(end_metrics["cumulative_tx"])
        window_tx = total_tx - int(start_metrics["cumulative_tx"])
        window_interval = max(1, int(end_metrics["timestamp"]) - int(start_metrics["timestamp"]))
        window_block_count = end_height - start_height
        txrate = window_tx / window_interval if window_interval > 0 else 0.0
        return {
            "time": int(end_metrics["timestamp"]),
            "txcount": total_tx,
            "window_final_block_height": end_height,
            "window_final_block_hash": end_metrics["hash"],
            "window_block_count": window_block_count,
            "window_interval": window_interval,
            "window_tx_count": window_tx,
            "txrate": txrate,
        }

    def getmempoolinfo(self) -> dict[str, Any]:
        with self.mempool.lock:
            size = len(self.mempool.entries)
            total_bytes = sum(entry.size for entry in self.mempool.entries.values())
            total_weight = self.mempool.total_weight
        return {
            "loaded": True,
            "size": size,
            "bytes": total_bytes,
            "usage": total_weight,
            "maxmempool": self.mempool.max_weight // 4,
            "mempoolminfee": self.mempool.min_fee_rate / COIN,
            "minrelaytxfee": MIN_RELAY_FEE_RATE / COIN,
            "unbroadcastcount": 0,
            "fullrbf": False,
        }

    def getnettotals(self) -> dict[str, Any]:
        now = int(time.time() * 1000)
        bytes_recv = int(getattr(self.network, "bytes_received", 0) or 0) if self.network else 0
        bytes_sent = int(getattr(self.network, "bytes_sent", 0) or 0) if self.network else 0
        return {
            "totalbytesrecv": bytes_recv,
            "totalbytessent": bytes_sent,
            "timemillis": now,
            "uploadtarget": {
                "timeframe": 0,
                "target": 0,
                "target_reached": False,
                "serve_historical_blocks": True,
                "bytes_left_in_cycle": 0,
                "time_left_in_cycle": 0,
            },
        }

    def getpeerinfo(self) -> list[dict[str, Any]]:
        if not self.network:
            return []
        if hasattr(self.network, "snapshot_peers"):
            try:
                peers = self.network.snapshot_peers()
            except Exception as exc:
                logging.getLogger("baseline.rpc").debug("peer snapshot failed: %s", exc)
                peers = []
        else:
            peers = []
            peers_dict = getattr(self.network, "peers", {}) or {}
            for peer in peers_dict.values():
                peers.append(
                    {
                        "peer_id": peer.peer_id,
                        "address": peer.address,
                        "outbound": peer.outbound,
                        "last_send": peer.last_send,
                        "last_message": peer.last_message,
                        "bytes_sent": peer.bytes_sent,
                        "bytes_received": peer.bytes_received,
                        "latency": peer.latency,
                        "remote_version": peer.remote_version or {},
                    }
                )
        best = self.state_db.get_best_tip()
        tip_height = best[1] if best else 0
        local_addr = f"{self.network.host}:{self.network.listen_port}"
        results: list[dict[str, Any]] = []
        for peer in peers:
            address = peer.get("address") or ("", 0)
            peer_id = peer.get("peer_id", "")
            try:
                numeric_id = int(peer_id.lstrip("P"))
            except ValueError:
                numeric_id = 0
            remote = peer.get("remote_version") or {}
            info = {
                "id": numeric_id,
                "addr": f"{address[0]}:{address[1]}",
                "addrlocal": local_addr,
                "services": remote.get("services", 0),
                "relaytxes": True,
                "lastsend": int(peer.get("last_send", 0) or 0),
                "lastrecv": int(peer.get("last_message", 0) or 0),
                "bytessent": peer.get("bytes_sent", 0) or 0,
                "bytesrecv": peer.get("bytes_received", 0) or 0,
                "conntime": remote.get("timestamp", int(peer.get("last_message", 0) or 0)),
                "timeoffset": 0,
                "pingtime": peer.get("latency", 0) or 0,
                "minping": peer.get("latency", 0) or 0,
                "version": remote.get("version", 0),
                "subver": remote.get("agent", ""),
                "inbound": not peer.get("outbound", False),
                "startingheight": remote.get("height", 0),
                "synced_blocks": tip_height,
                "synced_headers": tip_height,
                "banscore": 0,
                "whitelisted": False,
                "permissions": [],
                "feefilter": self.mempool.min_fee_rate / COIN,
            }
            results.append(info)
        return results

    def uptime(self) -> int:
        return int(time.time() - self._start_time)

    def gettxoutsetinfo(self, options: Any | None = None) -> dict[str, Any]:
        stats = self.state_db.get_utxo_set_stats()
        # Bitcoin Core allows a dict of options; we simply ignore unsupported selectors
        if isinstance(options, dict):
            hash_type = options.get("hash_type")
            if hash_type == "none":
                stats.pop("muhash", None)
                stats.pop("hash_serialized_2", None)
        return stats

    # Pool / Stratum helpers ------------------------------------------------------

    def _require_pool_tracker(self, require_stratum: bool = False) -> PayoutTracker:
        if not self.payout_tracker:
            raise RPCError(-38, "Mining not available")
        if require_stratum and not self.stratum:
            raise RPCError(-38, "Mining not available")
        return self.payout_tracker

    def getpoolstats(self) -> dict[str, Any]:
        tracker = self._require_pool_tracker()
        with tracker.lock:
            pending = len(tracker.pending_blocks)
            matured = len(tracker.matured_utxos)
            round_shares = len(tracker.round_shares)
            workers = len(tracker.workers)
            pool_balance = tracker.pool_balance
            pool_fee_percent = tracker.pool_fee_percent
            min_payout = tracker.min_payout
            maturity = tracker.maturity
        stratum_info: dict[str, Any] = {"enabled": bool(self.stratum)}
        if self.stratum:
            try:
                stratum_info.update(self.stratum.snapshot_stats())
            except Exception as exc:
                logging.getLogger("baseline.rpc").debug("stratum snapshot failed: %s", exc)
        return {
            "enabled": True,
            "pool_fee_percent": pool_fee_percent,
            "min_payout": min_payout,
            "coinbase_maturity": maturity,
            "pool_balance_liners": pool_balance,
            "pool_balance": pool_balance / COIN,
            "pending_blocks": pending,
            "matured_utxos": matured,
            "round_shares": round_shares,
            "workers": workers,
            "stratum": stratum_info,
        }

    def getpoolworkers(self, offset: int = 0, limit: int = 50, include_zero: bool = False) -> dict[str, Any]:
        tracker = self._require_pool_tracker()
        offset = int(offset)
        limit = int(limit)
        if offset < 0:
            raise RPCError(-8, "offset must be >= 0")
        if limit <= 0:
            raise RPCError(-8, "limit must be > 0")
        include_zero = bool(include_zero)
        with tracker.lock:
            entries = []
            for worker_id, state in tracker.workers.items():
                balance = state.balance
                shares = float(tracker.round_shares.get(worker_id, 0.0))
                if not include_zero and balance <= 0 and shares <= 0:
                    continue
                entries.append(
                    {
                        "worker_id": worker_id,
                        "address": state.address,
                        "balance_liners": balance,
                        "balance": balance / COIN,
                        "round_shares": shares,
                    }
                )
            entries.sort(key=lambda item: (-item["balance_liners"], item["worker_id"]))
        total = len(entries)
        window = entries[offset : offset + limit]
        return {
            "total": total,
            "offset": offset,
            "limit": limit,
            "workers": window,
        }

    def getpoolpendingblocks(self) -> list[dict[str, Any]]:
        tracker = self._require_pool_tracker()
        with tracker.lock:
            pending = list(tracker.pending_blocks)
        pending.sort(key=lambda entry: entry.get("height", 0), reverse=True)
        return [
            {
                "height": int(entry.get("height", 0)),
                "txid": entry.get("txid"),
                "total_reward": int(entry.get("total_reward", 0)),
                "distributable": int(entry.get("distributable", 0)),
                "pool_fee": int(entry.get("pool_fee", 0)),
                "vout": int(entry.get("vout", 0)),
                "time": float(entry.get("time", 0.0)),
                "shares": entry.get("shares", {}),
            }
            for entry in pending
        ]

    def getpoolmatured(self) -> list[dict[str, Any]]:
        tracker = self._require_pool_tracker()
        with tracker.lock:
            matured = list(tracker.matured_utxos)
        matured.sort(key=lambda entry: entry.get("amount", 0), reverse=True)
        return [
            {
                "txid": entry.get("txid"),
                "vout": int(entry.get("vout", 0)),
                "amount": int(entry.get("amount", 0)),
            }
            for entry in matured
        ]

    def getpoolpayoutpreview(self, max_outputs: int = 0) -> Any:
        tracker = self._require_pool_tracker()
        max_outputs = int(max_outputs)
        if max_outputs < 0:
            raise RPCError(-8, "max_outputs must be >= 0")
        preview = tracker.preview_payout(self.state_db, max_outputs=max_outputs or None)
        if preview is None:
            return None
        return {
            "payees": [
                {
                    "worker_id": entry["worker_id"],
                    "address": entry["address"],
                    "amount_liners": entry["amount"],
                    "amount": entry["amount"] / COIN,
                }
                for entry in preview["payees"]
            ],
            "matured_utxos": [
                {
                    "txid": entry["txid"],
                    "vout": entry["vout"],
                    "amount_liners": entry["amount"],
                    "amount": entry["amount"] / COIN,
                }
                for entry in preview["matured_utxos"]
            ],
            "inputs_used": preview["inputs_used"],
            "fee_liners": preview["fee"],
            "fee": preview["fee"] / COIN,
            "change_liners": preview["change"],
            "change": preview["change"] / COIN,
            "total_output_liners": preview["total_output"],
            "total_output": preview["total_output"] / COIN,
            "estimated_size": preview["estimated_size"],
            "input_sum_liners": preview["input_sum"],
            "input_sum": preview["input_sum"] / COIN,
        }

    def getstratumsessions(self) -> dict[str, Any]:
        self._require_pool_tracker(require_stratum=True)
        assert self.stratum
        sessions = self.stratum.snapshot_sessions()
        return {"count": len(sessions), "sessions": sessions}

    def poolreconcile(self, apply: bool = False) -> dict[str, Any]:
        tracker = self._require_pool_tracker()
        return tracker.reconcile_balances(self.state_db, apply=bool(apply))

    def getblockstats(self, hash_or_height: Any, stats: list[str] | None = None) -> dict[str, Any]:
        if isinstance(hash_or_height, str):
            header = self.state_db.get_header(hash_or_height)
            if header is None:
                raise RPCError(-5, "Block not found")
            block_hash = header.hash
        else:
            height = int(hash_or_height)
            header = self.state_db.get_main_header_at_height(height)
            if header is None:
                raise RPCError(-8, "Block height out of range")
            block_hash = header.hash
        cached = self._get_block_stats_cached(block_hash)
        if cached is None:
            block, _ = self._load_block_by_hash(block_hash)
            stats_all = self._compute_block_stats(block, header)
            self._remember_block_stats(block_hash, stats_all)
        else:
            stats_all = cached
        if stats:
            filtered = {key: stats_all[key] for key in stats if key in stats_all}
            return filtered
        return stats_all

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

    def _estimate_fee_rate(self, target_blocks: int) -> float:
        samples = self._collect_fee_rate_samples()
        if not samples:
            return max(MIN_RELAY_FEE_RATE, 1) / COIN
        target = max(1, target_blocks)
        quantile = max(0.1, min(0.95, 1.0 - 0.1 * (target - 1)))
        if len(samples) == 1:
            selected = samples[0]
        else:
            index = int(round((len(samples) - 1) * quantile))
            index = max(0, min(len(samples) - 1, index))
            selected = samples[index]
        return max(selected, MIN_RELAY_FEE_RATE) / COIN

    def _collect_fee_rate_samples(self) -> list[int]:
        with self.mempool.lock:
            entries = list(self.mempool.entries.values())
        samples: list[int] = []
        for entry in entries:
            size = max(1, entry.size)
            liners_per_kb = int(entry.fee * 1000 / size)
            samples.append(liners_per_kb)
        samples.sort()
        return samples

    # Helper utilities ----------------------------------------------------------

    def _remember_block(self, block_hash: str, block: Block, header: HeaderData) -> None:
        with self._cache_lock:
            cache = self._block_cache
            cache[block_hash] = (block, header)
            cache.move_to_end(block_hash)
            if len(cache) > self._block_cache_limit:
                cache.popitem(last=False)

    def _get_cached_block(self, block_hash: str) -> tuple[Block, HeaderData] | None:
        with self._cache_lock:
            cached = self._block_cache.get(block_hash)
            if cached:
                self._block_cache.move_to_end(block_hash)
                return cached
            return None

    def _remember_tx(self, tx: Transaction, block_hash: str | None, height: int | None) -> None:
        with self._cache_lock:
            cache = self._tx_cache
            cache[tx.txid()] = (tx, block_hash, height)
            cache.move_to_end(tx.txid())
            if len(cache) > self._tx_cache_limit:
                cache.popitem(last=False)

    def _get_cached_tx(self, txid: str) -> tuple[Transaction, str | None, int | None] | None:
        with self._cache_lock:
            cached = self._tx_cache.get(txid)
            if cached:
                self._tx_cache.move_to_end(txid)
                return cached
            return None

    def _get_cached_raw_tx(self, txid: str, block_hash: str | None) -> dict[str, Any] | None:
        key = (txid, block_hash or "")
        with self._cache_lock:
            cached = self._raw_tx_cache.get(key)
            if cached:
                self._raw_tx_cache.move_to_end(key)
                return cached
            return None

    def _remember_raw_tx(self, txid: str, block_hash: str | None, payload: dict[str, Any]) -> None:
        key = (txid, block_hash or "")
        with self._cache_lock:
            cache = self._raw_tx_cache
            cache[key] = payload
            cache.move_to_end(key)
            if len(cache) > self._raw_tx_cache_limit:
                cache.popitem(last=False)

    def _remember_block_stats(self, block_hash: str, stats: dict[str, Any]) -> None:
        with self._cache_lock:
            cache = self._block_stats_cache
            cache[block_hash] = stats
            cache.move_to_end(block_hash)
            if len(cache) > self._block_stats_limit:
                cache.popitem(last=False)

    def _get_block_stats_cached(self, block_hash: str) -> dict[str, Any] | None:
        with self._cache_lock:
            stats = self._block_stats_cache.get(block_hash)
            if stats:
                self._block_stats_cache.move_to_end(block_hash)
                return stats
            return None

    def _load_block_by_hash(self, block_hash: str) -> tuple[Block, HeaderData]:
        cached = self._get_cached_block(block_hash)
        if cached:
            return cached
        header = self.state_db.get_header(block_hash)
        if header is None:
            raise RPCError(-5, "Block not found")
        raw = self.block_store.get_block(block_hash)
        block = Block.parse(raw)
        self._remember_block(block_hash, block, header)
        return block, header

    def _block_by_height(self, height: int) -> tuple[Block, HeaderData]:
        header = self.state_db.get_main_header_at_height(int(height))
        if header is None:
            raise RPCError(-8, "Block height out of range")
        return self._load_block_by_hash(header.hash)

    def _find_transaction(
        self,
        txid: str,
        *,
        block_hash: str | None = None,
    ) -> tuple[Transaction | None, str | None, int | None]:
        cached = self._get_cached_tx(txid)
        if cached:
            return cached
        if block_hash:
            try:
                block, header = self._load_block_by_hash(block_hash)
            except RPCError:
                pass
            else:
                for tx in block.transactions:
                    if tx.txid() == txid:
                        self._remember_tx(tx, block_hash, header.height)
                        return tx, block_hash, header.height
                return None, None, None
        tx = self.mempool.get(txid)
        if tx:
            self._remember_tx(tx, None, None)
            return tx, None, None
        best = self.state_db.get_best_tip()
        if not best:
            return None, None, None
        location = self.state_db.get_transaction_location(txid)
        if location:
            located_hash, _, _ = location
            try:
                block, header = self._load_block_by_hash(located_hash)
            except RPCError:
                pass
            else:
                position = location[2]
                if 0 <= position < len(block.transactions):
                    candidate = block.transactions[position]
                    if candidate.txid() == txid:
                        self._remember_tx(candidate, located_hash, header.height)
                        return candidate, located_hash, header.height
                for candidate in block.transactions:
                    if candidate.txid() == txid:
                        self._remember_tx(candidate, located_hash, header.height)
                        return candidate, located_hash, header.height
        current_hash = best[0]
        while current_hash:
            header = self.state_db.get_header(current_hash)
            if header is None:
                break
            raw = self.block_store.get_block(current_hash)
            block = Block.parse(raw)
            self._remember_block(current_hash, block, header)
            txids: list[str] = []
            matched: Transaction | None = None
            for tx in block.transactions:
                candidate_txid = tx.txid()
                txids.append(candidate_txid)
                if candidate_txid == txid:
                    matched = tx
            if matched:
                try:
                    self.state_db.index_block_transactions(current_hash, header.height, txids)
                except Exception:
                    logging.getLogger("baseline.rpc").debug(
                        "Failed to backfill tx index for %s",
                        current_hash,
                        exc_info=True,
                    )
                self._remember_tx(matched, current_hash, header.height)
                return matched, current_hash, header.height
            if header.prev_hash is None:
                break
            current_hash = header.prev_hash
        return None, None, None

    def _resolve_prev_output_value(
        self,
        prev_txid: str,
        prev_vout: int,
        cache: dict[str, Transaction],
        *,
        spent_map: dict[tuple[str, int], int] | None = None,
    ) -> int | None:
        if spent_map is not None:
            value = spent_map.get((prev_txid, prev_vout))
            if value is not None:
                return value
        utxo = self.state_db.get_utxo(prev_txid, prev_vout)
        if utxo:
            return utxo.amount
        prev_tx = cache.get(prev_txid)
        if prev_tx is None:
            prev_tx, _, _ = self._find_transaction(prev_txid)
            if prev_tx:
                cache[prev_txid] = prev_tx
        if prev_tx and 0 <= prev_vout < len(prev_tx.outputs):
            return prev_tx.outputs[prev_vout].value
        return None

    def _transaction_fee(
        self,
        tx: Transaction,
        *,
        cache: dict[str, Transaction] | None = None,
        spent_map: dict[tuple[str, int], int] | None = None,
    ) -> int:
        """Derive the fee for a transaction by summing referenced outputs."""
        if tx.is_coinbase():
            return 0
        output_sum = sum(out.value for out in tx.outputs)
        input_sum = 0
        prev_cache: dict[str, Transaction] = cache if cache is not None else {}
        for vin in tx.inputs:
            value = self._resolve_prev_output_value(
                vin.prev_txid,
                vin.prev_vout,
                prev_cache,
                spent_map=spent_map,
            )
            if value is None:
                logging.warning(
                    "Unable to resolve input %s:%s while computing fee for %s",
                    vin.prev_txid,
                    vin.prev_vout,
                    tx.txid(),
                )
                continue
            input_sum += value
        return max(0, input_sum - output_sum)

    def _build_spent_output_map(self, block_hash: str | None) -> dict[tuple[str, int], int] | None:
        if not block_hash:
            return None
        try:
            undo = self.state_db.load_undo_data(block_hash)
        except Exception:
            return None
        if not undo:
            return None
        return {(record.txid, record.vout): record.amount for record in undo}

    def gettimesyncinfo(self) -> dict[str, Any]:
        """Get time synchronization status and information."""
        if not self.time_manager:
            return {
                "enabled": False,
                "synchronized": False,
                "offset": 0.0,
                "last_sync": None,
                "time_since_sync": None,
                "drift_rate": None,
                "servers": [],
                "system_time": time.time(),
                "synchronized_time": time.time(),
                "message": "NTP synchronization is disabled",
            }

        status = self.time_manager.get_sync_status()
        servers = getattr(getattr(self.time_manager, "ntp_client", None), "servers", [])
        return {
            "enabled": status.get("enabled", True),
            "synchronized": status.get("synchronized", False),
            "offset": status.get("offset", 0.0),
            "last_sync": status.get("last_sync"),
            "time_since_sync": status.get("time_since_sync"),
            "drift_rate": status.get("drift_rate"),
            "servers": servers,
            "system_time": status.get("system_time", time.time()),
            "synchronized_time": status.get("sync_time", time.time()),
        }

    def _current_difficulty(self) -> float:
        best = self.state_db.get_best_tip()
        if not best:
            return 1.0
        header = self.state_db.get_header(best[0])
        if header is None:
            return 1.0
        return self._difficulty_from_bits(header.bits)

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

    def _difficulty_from_bits(self, bits: int) -> float:
        target = difficulty.compact_to_target(bits)
        if target <= 0:
            return 1.0
        max_target = max(1, difficulty.compact_to_target(self.chain.config.mining.pow_limit_bits))
        return max_target / target

    def _network_hashps(self, difficulty_value: float, target_spacing: float) -> float:
        """
        Estimate network hashrate using the chain's pow_limit as the diff1 target.

        Expected hashes per block = (2^256 / pow_limit) * difficulty.
        """
        if target_spacing <= 0:
            target_spacing = 1.0
        pow_limit = max(1, self.chain.max_target)
        hashes_per_block = (2**256 / pow_limit) * difficulty_value
        return hashes_per_block / target_spacing

    def _compute_block_stats(self, block: Block, header: HeaderData) -> dict[str, Any]:
        block_bytes = block.serialize()
        block_size = len(block_bytes)
        block_weight = block.weight()
        tx_sizes: list[int] = []
        tx_fees: list[int] = []
        fee_rates: list[int] = []
        total_outputs = 0
        total_fee = 0
        inputs_count = 0
        outputs_count = 0
        coinbase_outputs = 0
        if header.height > 0:
            view = self.chain._build_view_for_parent(header.prev_hash or self.chain.genesis_hash)
        else:
            view = UTXOView(self.state_db)
        for index, tx in enumerate(block.transactions):
            serialized = tx.serialize()
            tx_size = len(serialized)
            tx_sizes.append(tx_size)
            outputs_count += len(tx.outputs)
            output_sum = sum(out.value for out in tx.outputs)
            total_outputs += output_sum
            if index == 0:
                coinbase_outputs = output_sum
            else:
                inputs_count += len(tx.inputs)
                input_sum = 0
                for txin in tx.inputs:
                    utxo = view.get(txin.prev_txid, txin.prev_vout)
                    if utxo is None:
                        raise RPCError(-5, "Missing referenced output while computing block stats")
                    view.spend(txin.prev_txid, txin.prev_vout)
                    input_sum += utxo.amount
                fee = max(0, input_sum - output_sum)
                total_fee += fee
                tx_fees.append(fee)
                fee_rate = fee // max(1, tx_size)
                fee_rates.append(fee_rate)
            # add outputs for intra-block spends
            for out_index, txout in enumerate(tx.outputs):
                record = UTXORecord(
                    txid=tx.txid(),
                    vout=out_index,
                    amount=txout.value,
                    script_pubkey=txout.script_pubkey,
                    height=header.height,
                    coinbase=index == 0,
                )
                view.add(record)
        subsidy = self.chain._block_subsidy(header.height)
        if coinbase_outputs and total_fee == 0:
            implied_fee = max(0, coinbase_outputs - subsidy)
            total_fee = implied_fee
        avg_fee = int(round(total_fee / max(1, len(tx_fees)))) if tx_fees else 0
        avg_fee_rate = int(round(sum(fee_rates) / len(fee_rates))) if fee_rates else 0
        avg_tx_size = int(round(sum(tx_sizes) / len(tx_sizes))) if tx_sizes else 0
        median_fee = self._median(tx_fees)
        median_tx_size = self._median(tx_sizes)
        percentiles = self._percentiles(fee_rates, [10, 25, 50, 75, 90])
        utxo_increase = outputs_count - inputs_count
        return {
            "avgfee": avg_fee,
            "avgfeerate": avg_fee_rate,
            "avgtxsize": avg_tx_size,
            "blockhash": header.hash,
            "feerate_percentiles": percentiles,
            "height": header.height,
            "ins": inputs_count,
            "maxfee": max(tx_fees) if tx_fees else 0,
            "maxfeerate": max(fee_rates) if fee_rates else 0,
            "maxtxsize": max(tx_sizes) if tx_sizes else 0,
            "medianfee": median_fee,
            "mediantime": self.chain._median_time_past(header.hash),
            "mediantxsize": median_tx_size,
            "minfee": min(tx_fees) if tx_fees else 0,
            "minfeerate": min(fee_rates) if fee_rates else 0,
            "mintxsize": min(tx_sizes) if tx_sizes else 0,
            "outs": outputs_count,
            "subsidy": subsidy,
            "swtotal_size": 0,
            "swtotal_weight": 0,
            "swtxs": 0,
            "time": header.timestamp,
            "total_out": total_outputs,
            "total_size": block_size,
            "total_weight": block_weight,
            "totalfee": total_fee,
            "txs": len(block.transactions),
            "utxo_increase": utxo_increase,
            "utxo_size_inc": utxo_increase * 117,
        }

    def _median(self, values: list[int]) -> int:
        if not values:
            return 0
        sorted_vals = sorted(values)
        mid = len(sorted_vals) // 2
        if len(sorted_vals) % 2 == 1:
            return sorted_vals[mid]
        return int(round((sorted_vals[mid - 1] + sorted_vals[mid]) / 2))

    def _percentiles(self, values: list[int], percentiles: list[int]) -> list[int]:
        if not values:
            return [0 for _ in percentiles]
        sorted_vals = sorted(values)
        n = len(sorted_vals)
        results: list[int] = []
        for pct in percentiles:
            if n == 1:
                results.append(sorted_vals[0])
                continue
            rank = pct / 100 * (n - 1)
            low = math.floor(rank)
            high = math.ceil(rank)
            if low == high:
                results.append(sorted_vals[low])
            else:
                fraction = rank - low
                interpolated = sorted_vals[low] + (sorted_vals[high] - sorted_vals[low]) * fraction
                results.append(int(round(interpolated)))
        return results

    def _compute_chaintxstats_slow(self, nblocks: int | None, blockhash: str | None) -> dict[str, Any]:
        best = self.state_db.get_best_tip()
        if not best:
            raise RPCError(-8, "Blockchain not initialized")
        if blockhash:
            end_header = self.state_db.get_header(blockhash)
            if end_header is None:
                raise RPCError(-5, "Block not found")
        else:
            end_header = self.state_db.get_header(best[0])
        assert end_header is not None
        end_height = end_header.height
        if nblocks is None or nblocks <= 0:
            start_height = 0
        else:
            start_height = max(0, end_height - int(nblocks))
        start_header = self.state_db.get_main_header_at_height(start_height)
        if start_header is None:
            raise RPCError(-8, "Start block missing")
        total_tx = 0
        window_tx = 0
        for height in range(0, end_height + 1):
            block, _ = self._block_by_height(height)
            tx_count = len(block.transactions)
            total_tx += tx_count
            if height > start_height:
                window_tx += tx_count
        window_interval = max(1, end_header.timestamp - start_header.timestamp)
        window_block_count = end_height - start_height
        txrate = window_tx / window_interval if window_interval > 0 else 0
        return {
            "time": end_header.timestamp,
            "txcount": total_tx,
            "window_final_block_height": end_height,
            "window_final_block_hash": end_header.hash,
            "window_block_count": window_block_count,
            "window_interval": window_interval,
            "window_tx_count": window_tx,
            "txrate": txrate,
        }
