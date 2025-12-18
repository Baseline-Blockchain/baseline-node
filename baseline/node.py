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
from .wallet import WalletManager


class BaselineNode:
    """Coordinates the different networking and chain services."""

    def __init__(self, config: NodeConfig) -> None:
        self.config = config
        self.log = logging.getLogger("baseline.node")
        self._tasks: dict[str, asyncio.Task] = {}
        self._stop_event = asyncio.Event()
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

    async def start(self) -> None:
        if self._started:
            return
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
        self._tasks["housekeeping"] = asyncio.create_task(self._housekeeping(), name="housekeeping")

    async def stop(self) -> None:
        if not self._started:
            return
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
        if self.network:
            await self.network.stop()

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
        self.log.warning("Shutdown requested")
        self._stop_event.set()

    async def _housekeeping(self) -> None:
        while not self._stop_event.is_set():
            await asyncio.sleep(5)
            await self._run_payout_cycle()
            if self.wallet:
                await asyncio.to_thread(self.wallet.sync_chain)
                self.wallet.tick()

    def _initialize_mining_components(self) -> None:
        if not (self.chain and self.mempool):
            return
        pool_privkey = parse_pool_private_key(self.config.mining.pool_private_key)
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
        self.wallet.sync_chain()

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
        )
        self.rpc_handlers = handlers
        self.rpc_server = RPCServer(self.config, handlers)

    async def _run_payout_cycle(self) -> None:
        if not (self.chain and self.payout_tracker and self.mempool):
            return
        try:
            best = self.chain.state_db.get_best_tip()
            if not best:
                return
            self.payout_tracker.process_maturity(best[1])
            tx = self.payout_tracker.create_payout_transaction(self.chain.state_db)
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
