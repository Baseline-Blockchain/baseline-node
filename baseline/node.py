"""
Baseline node orchestration.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging

from .config import NodeConfig, parse_pool_private_key
from .core import crypto
from .core.chain import Chain, ChainError
from .mempool import Mempool, MempoolError
from .mining import PayoutTracker, StratumServer, TemplateBuilder
from .net import P2PServer
from .rpc import RPCHandlers, RPCServer
from .storage import BlockStore, BlockStoreError, StateDB, StateDBError
from .time_sync import NTPClient, TimeManager
from .wallet import WalletManager


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
        self.block_store = BlockStore(blocks_dir)
        self.state_db = StateDB(chainstate_path)
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
            self.network = P2PServer(self.config, self.chain, self.mempool)
            await self.network.start()
            self._initialize_time_sync()
            self._initialize_mining_components()
            self._initialize_wallet()
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
            self.time_manager.stop()
            self.time_manager = None
        if self.wallet:
            self.wallet.stop_background_sync()
        if self.network:
            await self.network.stop()
        if self.mempool:
            self.mempool.close()
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

    def _initialize_time_sync(self) -> None:
        """Initialize NTP time synchronization."""
        if not self.config.ntp.enabled:
            self.log.info("NTP synchronization disabled")
            return

        ntp_client = NTPClient(
            servers=list(self.config.ntp.servers),
            timeout=self.config.ntp.timeout
        )
        self.time_manager = TimeManager(
            ntp_client=ntp_client,
            sync_interval=self.config.ntp.sync_interval
        )
        self.time_manager.start()
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
        self.template_builder = TemplateBuilder(self.chain, self.mempool, pool_script)
        self.stratum = StratumServer(
            self.config,
            self.chain,
            self.mempool,
            self.template_builder,
            self.payout_tracker,
            network=self.network,
        )

    def _initialize_wallet(self) -> None:
        if not (self.chain and self.mempool):
            return
        if self.wallet:
            return
        wallet_path = self.config.data_dir / "wallet" / "wallet.json"
        wallet_path.parent.mkdir(parents=True, exist_ok=True)
        self.wallet = WalletManager(wallet_path, self.state_db, self.block_store, self.mempool)
        self.wallet.sync_chain(self._wallet_should_abort)
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
        )
        self.rpc_handlers = handlers
        self.rpc_server = RPCServer(self.config, handlers)

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
