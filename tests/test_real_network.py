import asyncio
import contextlib
import os
import tempfile
import time
import unittest
from pathlib import Path

from baseline.config import NodeConfig
from baseline.core import difficulty
from baseline.node import BaselineNode


class RealNetworkIntegrationTests(unittest.TestCase):
    """Exercise a mini network of actual Baseline nodes."""

    BASE_P2P_PORT = 19440
    BASE_RPC_PORT = 18400
    BASE_STRATUM_PORT = 20400
    INITIAL_BLOCKS = 12
    POST_SYNC_BLOCKS = 6

    def test_three_node_cluster_sync_and_resync(self) -> None:
        asyncio.run(self._run_cluster_flow())

    async def _run_cluster_flow(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            nodes: list[BaselineNode] = []
            try:
                seed = await self._start_node(self._build_config(base_dir, 0, ()))
                nodes.append(seed)
                seed_addr = self._node_address(0)

                follower = await self._start_node(self._build_config(base_dir, 1, (seed_addr,)))
                nodes.append(follower)

                miner = await self._start_node(self._build_config(base_dir, 2, (seed_addr,)))
                nodes.append(miner)

                await self._wait_for(lambda: len(seed.network.peers) >= 2, 10.0, "seed node missing inbound peers")
                await self._wait_for(lambda: len(follower.network.peers) >= 1, 10.0, "follower missing peer")
                await self._wait_for(lambda: len(miner.network.peers) >= 1, 10.0, "miner missing peer")

                await self._mine_blocks(miner, self.INITIAL_BLOCKS, nodes[:3])
                self._assert_all_tips_match(nodes[:3])

                seed_tip = seed.chain.state_db.get_best_tip()
                follower_tip = follower.chain.state_db.get_best_tip()
                miner_tip = miner.chain.state_db.get_best_tip()
                self.assertIsNotNone(seed_tip)
                self.assertIsNotNone(follower_tip)
                self.assertIsNotNone(miner_tip)
                hashes = {seed_tip[0], follower_tip[0], miner_tip[0]}
                self.assertEqual(len(hashes), 1, "All nodes should agree on tip hash")

                newcomer_seeds = (seed_addr, self._node_address(2))
                newcomer = await self._start_node(self._build_config(base_dir, 3, newcomer_seeds))
                nodes.append(newcomer)
                await self._wait_for(lambda: len(newcomer.network.peers) >= 1, 10.0, "newcomer failed to connect")

                target_height = self._node_height(miner)
                peer_states = [
                    (
                        peer.address,
                        peer.remote_version.get("height") if peer.remote_version else None,
                        peer.outbound,
                    )
                    for peer in newcomer.network.peers.values()
                ]
                self.assertTrue(peer_states, "newcomer should have at least one peer")
                self.assertTrue(
                    any(height is not None and height >= target_height for _, height, _ in peer_states),
                    f"connected peers lack advertised height: {peer_states}",
                )
                await self._wait_for(
                    lambda: newcomer.network.header_sync_active
                    or newcomer.network.sync_active
                    or self._node_height(newcomer) >= target_height,
                    5.0,
                    f"newcomer never initiated synchronization; peers={peer_states}",
                )
                try:
                    await self._wait_for_height([newcomer], target_height, 20.0)
                except AssertionError as exc:  # pragma: no cover - debug helper
                    debug = {
                        "header_sync_active": newcomer.network.header_sync_active,
                        "sync_active": newcomer.network.sync_active,
                        "remote_height": newcomer.network.sync_remote_height,
                        "peers": [
                            {
                                "address": peer.address,
                                "height": peer.remote_version.get("height") if peer.remote_version else None,
                                "outbound": peer.outbound,
                            }
                            for peer in newcomer.network.peers.values()
                        ],
                    }
                    raise AssertionError(f"{exc} debug={debug}") from exc
                newcomer_tip = newcomer.chain.state_db.get_best_tip()
                self.assertIsNotNone(newcomer_tip)
                self.assertEqual(newcomer_tip[0], seed_tip[0], "newcomer failed to sync existing chain")

                await self._mine_blocks(miner, self.POST_SYNC_BLOCKS, nodes)
                self._assert_all_tips_match(nodes)
            finally:
                await self._stop_nodes(nodes)

    async def _mine_blocks(self, miner: BaselineNode, count: int, synced_nodes: list[BaselineNode]) -> None:
        for _ in range(count):
            block_hash, height = await asyncio.to_thread(self._mine_single_block, miner)
            self.assertTrue(block_hash)
            await self._wait_for_height(synced_nodes, height, 20.0)

    def _mine_single_block(self, miner: BaselineNode) -> tuple[str, int]:
        if miner.chain is None or miner.template_builder is None or miner.network is None:
            raise AssertionError("Miner node not fully initialized")
        extranonce1 = os.urandom(miner.template_builder.extranonce1_size)
        max_extranonce2 = 1 << (miner.template_builder.extranonce2_size * 8)
        while True:
            template = miner.template_builder.build_template()
            timestamp = max(int(time.time()), template.timestamp)
            for extranonce2_value in range(max_extranonce2):
                extranonce2 = extranonce2_value.to_bytes(miner.template_builder.extranonce2_size, "little")
                for nonce in range(0, 0xFFFFFFFF):
                    block = miner.template_builder.assemble_block(template, extranonce1, extranonce2, nonce, timestamp)
                    block_hash = block.block_hash()
                    if difficulty.check_proof_of_work(block_hash, block.header.bits):
                        raw = block.serialize()
                        result = miner.chain.add_block(block, raw)
                        status = result.get("status")
                        height = result.get("height")
                        if status in {"connected", "reorganized"} and isinstance(height, int):
                            miner.network.announce_block(block_hash)
                            return block_hash, height
                timestamp = max(timestamp + 1, int(time.time()))
            extranonce1 = os.urandom(miner.template_builder.extranonce1_size)

    def _build_config(self, base_dir: Path, node_id: int, seeds: tuple[str, ...]) -> NodeConfig:
        config = NodeConfig()
        config.data_dir = base_dir / f"node{node_id}"
        config.ensure_data_layout()
        config.network.host = "127.0.0.1"
        config.network.port = self.BASE_P2P_PORT + node_id
        config.network.max_peers = 8
        config.network.min_peers = 0
        config.network.target_outbound = max(1, len(seeds))
        config.network.seeds = tuple(seeds)
        config.rpc.host = "127.0.0.1"
        config.rpc.port = self.BASE_RPC_PORT + node_id
        config.stratum.host = "127.0.0.1"
        config.stratum.port = self.BASE_STRATUM_PORT + node_id
        config.mining.allow_consensus_overrides = True
        config.ntp.enabled = False
        config.mining.coinbase_maturity = 1
        config.mining.block_interval_target = 1
        config.mining.initial_bits = 0x207FFFFF
        config.ensure_data_layout()
        return config

    def _node_address(self, node_id: int) -> str:
        return f"127.0.0.1:{self.BASE_P2P_PORT + node_id}"

    async def _start_node(self, config: NodeConfig) -> BaselineNode:
        node = BaselineNode(config)
        await node.start()
        return node

    async def _stop_nodes(self, nodes: list[BaselineNode]) -> None:
        for node in reversed(nodes):
            if node is None:
                continue
            with contextlib.suppress(Exception):
                await node.stop()
            node.close()

    def _node_height(self, node: BaselineNode) -> int:
        best = node.chain.state_db.get_best_tip() if node.chain else None
        return best[1] if best else 0

    def _assert_all_tips_match(self, nodes: list[BaselineNode]) -> None:
        tips = []
        for node in nodes:
            best = node.chain.state_db.get_best_tip()
            self.assertIsNotNone(best, f"{node} missing best tip")
            tips.append(best[0])
        self.assertEqual(len(set(tips)), 1, f"tip mismatch across nodes: {tips}")

    async def _wait_for(self, predicate, timeout: float, message: str) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if predicate():
                return
            await asyncio.sleep(0.1)
        raise AssertionError(message)

    async def _wait_for_height(self, nodes: list[BaselineNode], height: int, timeout: float) -> None:
        deadline = time.time() + timeout
        last_heights: list[int] = []
        while time.time() < deadline:
            last_heights = [self._node_height(node) for node in nodes]
            if all(current >= height for current in last_heights):
                return
            await asyncio.sleep(0.1)
        raise AssertionError(f"nodes failed to reach height {height}, got {last_heights}")
