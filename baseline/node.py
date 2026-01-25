"""
Baseline node orchestration.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time

from .config import NodeConfig, parse_pool_private_key
from .core import crypto
from .core.chain import Chain, ChainError
from .mempool import Mempool, MempoolError
from .mining import PayoutTracker, StratumServer, TemplateBuilder
from .net import P2PServer
from .rpc import RPCHandlers, RPCServer
from .storage import BlockStore, BlockStoreError, StateDB, StateDBError
from .time_sync import NTPClient, TimeManager, set_time_manager
from .wallet import WalletManager

LOOP_MONITOR_INTERVAL = 5.0
LOOP_LAG_WARNING_SECONDS = 0.5


class BaselineNode:
    """Coordinates the different networking and chain services."""

    def __init__(self, config: NodeConfig) -> None:
        self.config = config
        self.log = logging.getLogger("baseline.node")
        self._tasks: dict[str, asyncio.Task] = {}
        self._stop_event = asyncio.Event()
        self._shutdown_requested = False
        self._started = False
        blocks_dir = self.config.data_dir / "blocks"
        chainstate_path = self.config.data_dir / "chainstate" / "state.sqlite3"
        self.block_store = BlockStore(blocks_dir, fsync_interval=self.config.storage.blockstore_fsync_interval)
        self.state_db = StateDB(chainstate_path, synchronous=self.config.storage.sqlite_synchronous)
        self.chain = None
        self.mempool = None
        self.network = None
        self.template_builder: TemplateBuilder | None = None
        self.payout_tracker: PayoutTracker | None = None
        self.stratum: StratumServer | None = None
        self.rpc_server: RPCServer | None = None
        self.rpc_handlers: RPCHandlers | None = None
        self.wallet: WalletManager | None = None
        self.time_manager: TimeManager | None = None

    async def start(self) -> None:
        if self._started:
            return
        self._shutdown_requested = False
        self.log.info("Starting node; data_dir=%s", self.config.data_dir)
        try:
            self.block_store.check()
        except BlockStoreError as exc:
            self.log.error("Block store check failed: %s", exc)
            raise
        try:
            self.state_db.run_startup_checks()
        except StateDBError as exc:
            self.log.error("State DB check failed: %s", exc)
            raise
        try:
            self.chain = Chain(self.config, self.state_db, self.block_store)
            self.mempool = Mempool(self.chain)
            await self._startup_txindex_sweep()
            self.network = P2PServer(self.config, self.chain, self.mempool)
            await self.network.start()
            self._initialize_time_sync()
            self._initialize_mining_components()
            await self._initialize_wallet()
            self._initialize_rpc()
            if self.stratum:
                await self.stratum.start()
            if self.rpc_server:
                await self.rpc_server.start()
        except (ChainError, BlockStoreError, OSError) as exc:
            self.log.error("Chain or network initialization failed: %s", exc)
            raise
        self._stop_event.clear()
        self._started = True
        self._tasks["payouts"] = asyncio.create_task(self._payout_task(), name="payouts")
        self._tasks["wallet-maint"] = asyncio.create_task(self._wallet_task(), name="wallet-maint")
        self._tasks["time-monitor"] = asyncio.create_task(self._time_monitor_task(), name="time-monitor")
        self._tasks["ibd-tune"] = asyncio.create_task(self._ibd_tune_task(), name="ibd-tune")
        self._tasks["loop-monitor"] = asyncio.create_task(self._loop_monitor_task(), name="loop-monitor")

    async def stop(self) -> None:
        if not self._started:
            self._shutdown_requested = True
            return
        self._shutdown_requested = True
        self.log.info("Stopping node...")
        self._stop_event.set()
        for name, task in list(self._tasks.items()):
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            self.log.debug("Service %s stopped", name)
        self._tasks.clear()
        self.log.debug("Node stopped")
        if self.rpc_server:
            await self.rpc_server.stop()
            self.rpc_server = None
        if self.stratum:
            await self.stratum.stop()
            self.stratum = None
        if self.time_manager:
            with contextlib.suppress(Exception):
                self.time_manager.stop()
            set_time_manager(None)
            self.time_manager = None
        if self.wallet:
            self.wallet.stop_background_sync()
            self.wallet.stop()
        if self.network:
            await self.network.stop()
        if self.mempool:
            self.mempool.close()
        with contextlib.suppress(Exception):
            self.block_store.flush()
        self._started = False

    def close(self) -> None:
        """Release long-lived resources."""
        try:
            self.state_db.close()
        except Exception:
            self.log.exception("Failed to close state DB cleanly")
        self._started = False

    async def run(self) -> None:
        await self.start()
        try:
            await self._stop_event.wait()
        finally:
            await self.stop()

    def request_shutdown(self) -> None:
        self._shutdown_requested = True
        self.log.warning("Shutdown requested")
        self._stop_event.set()

    async def _wait_or_stop(self, timeout: float) -> bool:
        if self._stop_event.is_set():
            return True
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=timeout)
            return True
        except TimeoutError:
            return False

    def _set_storage_mode(self, *, fast: bool, reason: str) -> None:
        if fast:
            sqlite_mode = self.config.storage.fast_sqlite_synchronous
            fsync_interval = self.config.storage.fast_blockstore_fsync_interval
        else:
            sqlite_mode = self.config.storage.sqlite_synchronous
            fsync_interval = self.config.storage.blockstore_fsync_interval

        self.state_db.set_synchronous(sqlite_mode)
        self.block_store.set_fsync_interval(fsync_interval)
        if not fast:
            with contextlib.suppress(Exception):
                self.state_db.checkpoint_wal("PASSIVE")
            with contextlib.suppress(Exception):
                self.block_store.flush()
        self.log.info(
            "Storage mode=%s (%s): sqlite_synchronous=%s blockstore_fsync_interval=%s",
            "fast-ibd" if fast else "safe",
            reason,
            sqlite_mode,
            fsync_interval,
        )

    def _best_peer_height(self) -> int:
        if not self.network:
            return 0
        best = 0
        for peer in self.network.peers.values():
            rv = peer.remote_version
            if not rv:
                continue
            try:
                height = int(rv.get("height", 0))
            except Exception:
                continue
            best = max(best, height)
        best = max(best, int(getattr(self.network.sync, "max_seen_remote_height", 0) or 0))
        best = max(best, int(getattr(self.network.sync, "sync_remote_height", 0) or 0))
        return best

    async def _ibd_tune_task(self) -> None:
        if not getattr(self.config.storage, "auto_fast_ibd", False):
            return
        fast_enabled = False
        last_toggle = 0.0
        check_every = float(self.config.storage.auto_fast_ibd_check_interval)
        enable_delta = int(self.config.storage.auto_fast_ibd_enable_delta)
        disable_delta = int(self.config.storage.auto_fast_ibd_disable_delta)
        min_toggle = float(self.config.storage.auto_fast_ibd_min_seconds_between_toggles)
        while True:
            should_stop = await self._wait_or_stop(check_every)
            if should_stop:
                return
            if not self.network:
                continue
            local_height = int(self.network.best_height())
            remote_height = int(self._best_peer_height())
            if remote_height <= 0:
                continue
            delta = remote_height - local_height
            now = time.time()
            if now - last_toggle < min_toggle:
                continue
            if not fast_enabled and delta >= enable_delta and enable_delta > 0:
                self._set_storage_mode(fast=True, reason=f"ibd delta={delta}")
                fast_enabled = True
                last_toggle = now
            elif fast_enabled and delta <= disable_delta:
                self._set_storage_mode(fast=False, reason=f"caught up delta={delta}")
                fast_enabled = False
                last_toggle = now

    async def _payout_task(self) -> None:
        try:
            while not await self._wait_or_stop(5):
                await self._run_payout_cycle()
        except asyncio.CancelledError:  # pragma: no cover - shutdown path
            pass

    async def _wallet_task(self) -> None:
        try:
            while not await self._wait_or_stop(5):
                if not self.wallet:
                    continue
                try:
                    self.wallet.request_sync(max_blocks=200)
                    self.wallet.tick()
                except Exception:  # pragma: no cover - defensive
                    self.log.exception("Wallet maintenance loop failed")
        except asyncio.CancelledError:  # pragma: no cover - shutdown path
            pass

    async def _time_monitor_task(self) -> None:
        try:
            while not await self._wait_or_stop(10):
                self._monitor_time_sync()
        except asyncio.CancelledError:  # pragma: no cover - shutdown path
            pass

    async def _loop_monitor_task(self) -> None:
        loop = asyncio.get_running_loop()
        try:
            while not self._stop_event.is_set():
                start = loop.time()
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=LOOP_MONITOR_INTERVAL)
                    return
                except TimeoutError:
                    pass
                elapsed = loop.time() - start
                lag = max(0.0, elapsed - LOOP_MONITOR_INTERVAL)
                task_count = len(asyncio.all_tasks())
                peers = len(self.network.peers) if self.network else 0
                stratum_sessions = len(self.stratum.sessions) if self.stratum else 0
                mempool_stats = "n/a"
                if self.mempool:
                    acquired = self.mempool.lock.acquire(timeout=0)
                    if acquired:
                        try:
                            mempool_stats = (
                                f"{len(self.mempool.entries)} tx ({len(self.mempool.orphans)} orphans) "
                                f"weight={self.mempool.total_weight}"
                            )
                        finally:
                            self.mempool.lock.release()
                    else:
                        mempool_stats = "busy"
                rpc_state = "up" if self.rpc_server and self.rpc_server.server else "down"
                self.log.debug(
                    "Load: lag=%.3fs tasks=%d peers=%d stratum=%d mempool=%s rpc=%s",
                    lag,
                    task_count,
                    peers,
                    stratum_sessions,
                    mempool_stats,
                    rpc_state,
                )
                if lag >= LOOP_LAG_WARNING_SECONDS:
                    self.log.warning(
                        "Event loop lag %.3fs (tasks=%d peers=%d stratum=%d mempool=%s rpc=%s)",
                        lag,
                        task_count,
                        peers,
                        stratum_sessions,
                        mempool_stats,
                        rpc_state,
                    )
        except asyncio.CancelledError:  # pragma: no cover - shutdown path
            pass

    def _initialize_time_sync(self) -> None:
        """Initialize NTP time synchronization."""
        ntp_client = NTPClient(
            servers=list(self.config.ntp.servers),
            timeout=self.config.ntp.timeout
        )
        manager = TimeManager(
            ntp_client=ntp_client,
            sync_interval=self.config.ntp.sync_interval
        )
        self.time_manager = manager
        set_time_manager(manager)
        if not self.config.ntp.enabled:
            manager.disable()
            self.log.info("NTP synchronization disabled")
            return
        manager.start()
        self.log.info("NTP synchronization enabled with servers: %s", self.config.ntp.servers)

    def _initialize_mining_components(self) -> None:
        if not (self.chain and self.mempool):
            return
        pool_key = self.config.mining.pool_private_key
        if not pool_key:
            self.log.info("Mining components disabled (pool_private_key not configured)")
            return
        pool_privkey = parse_pool_private_key(pool_key)
        pool_pubkey = crypto.generate_pubkey(pool_privkey)
        pool_script = b"\x76\xa9\x14" + crypto.hash160(pool_pubkey) + b"\x88\xac"
        payouts_path = self.config.data_dir / "payouts" / "ledger.json"
        self.payout_tracker = PayoutTracker(
            payouts_path,
            pool_privkey,
            pool_pubkey,
            pool_script,
            maturity=self.config.mining.coinbase_maturity,
            min_payout=self.config.mining.min_payout,
            pool_fee_percent=self.config.mining.pool_fee_percent,
        )
        try:
            reconcile = self.payout_tracker.reconcile_balances(self.state_db, apply=True)
            shortfall = reconcile.get("shortfall", 0)
            if shortfall > 0:
                self.log.warning(
                    "Payout ledger reconciled at startup: spendable=%s, owed=%s, shortfall=%s, applied=%s",
                    reconcile.get("spendable_total"),
                    reconcile.get("owed_total"),
                    shortfall,
                    reconcile.get("applied"),
                )
        except Exception:
            self.log.exception("Payout ledger reconciliation failed at startup")
        self.template_builder = TemplateBuilder(self.chain, self.mempool, pool_script)
        self.stratum = StratumServer(
            self.config,
            self.chain,
            self.mempool,
            self.template_builder,
            self.payout_tracker,
            network=self.network,
        )

    async def _initialize_wallet(self) -> None:
        if not (self.chain and self.mempool):
            return
        if self.wallet:
            return
        wallet_path = self.config.data_dir / "wallet" / "wallet.json"
        wallet_path.parent.mkdir(parents=True, exist_ok=True)
        self.wallet = WalletManager(
            wallet_path,
            self.state_db,
            self.block_store,
            self.mempool,
            self.network,
            wallet_notify=self.config.walletnotify,
        )
        await asyncio.to_thread(self.wallet.sync_chain, self._wallet_should_abort)
        self.wallet.start_background_sync(self._wallet_should_abort)
        self.wallet.request_sync()

    def _initialize_rpc(self) -> None:
        if not (self.chain and self.mempool):
            return
        if self.rpc_server:
            return
        handlers = RPCHandlers(
            self.chain,
            self.mempool,
            self.block_store,
            self.template_builder,
            self.network,
            self.wallet,
            self.time_manager,
            payout_tracker=self.payout_tracker,
            stratum_server=self.stratum,
        )
        self.rpc_handlers = handlers
        self.rpc_server = RPCServer(self.config, handlers)

    async def _startup_txindex_sweep(self) -> None:
        if not self.chain:
            return
        try:
            start_height = self.state_db.txindex_rebuild_start()
            if start_height is None:
                return
            blocks, txs = await asyncio.to_thread(
                self.state_db.rebuild_tx_index_from_blocks,
                self.block_store,
                start_height=start_height,
            )
            if blocks:
                self.log.info(
                    "Tx index backfill complete: start_height=%s blocks=%s txs=%s",
                    start_height,
                    blocks,
                    txs,
                )
        except Exception:
            self.log.exception("Tx index sweep failed")

    async def _run_payout_cycle(self) -> None:
        if not (self.chain and self.payout_tracker and self.mempool):
            return
        try:
            tx = await asyncio.to_thread(self._prepare_payout_transaction)
            if tx is None:
                return
            await asyncio.to_thread(self._accept_payout_tx, tx)
            self.log.info("Queued payout transaction %s", tx.txid())
        except Exception:
            self.log.exception("Payout processing failed")

    def _accept_payout_tx(self, tx) -> None:
        if not self.mempool:
            return
        try:
            self.mempool.accept_transaction(tx, peer_id="payout")
        except MempoolError as exc:
            self.log.warning("Failed to enqueue payout tx %s: %s", tx.txid(), exc)

    def _prepare_payout_transaction(self):
        assert self.chain and self.payout_tracker
        best = self.chain.state_db.get_best_tip()
        if not best:
            return None
        self.payout_tracker.process_maturity(best[1])
        return self.payout_tracker.create_payout_transaction(self.chain.state_db)

    def _monitor_time_sync(self) -> None:
        """Monitor time synchronization status and log warnings."""
        if not self.time_manager or not self.config.ntp.enabled:
            return

        status = self.time_manager.get_sync_status()

        # Warn about large offsets
        if abs(status["offset"]) > self.config.ntp.max_offset_warning:
            self.log.warning(
                "Large time offset detected: %.3fs (system time may be incorrect)",
                status["offset"]
            )

        # Warn if synchronization is stale
        if not status["synchronized"]:
            self.log.warning(
                "Time synchronization is stale (last sync: %.1fs ago)",
                status.get("time_since_sync", 0)
            )

    def _wallet_should_abort(self) -> bool:
        return self._shutdown_requested
