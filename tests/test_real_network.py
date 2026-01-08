import asyncio
import contextlib
import os
import tempfile
import time
import unittest
from pathlib import Path

from baseline.config import NodeConfig
from baseline.core import crypto, difficulty
from baseline.core.address import script_from_address
from baseline.core.tx import Transaction, TxInput, TxOutput
from baseline.node import BaselineNode


class RealNetworkIntegrationTests(unittest.TestCase):
    """Exercise a mini network of actual Baseline nodes."""

    BASE_P2P_PORT = 19440
    BASE_RPC_PORT = 18400
    BASE_STRATUM_PORT = 20400
    INITIAL_BLOCKS = 6
    POST_SYNC_BLOCKS = 3

    def test_three_node_cluster_sync_and_resync(self) -> None:
        asyncio.run(self._run_cluster_flow())

    def test_pool_payout_flow(self) -> None:
        asyncio.run(self._run_payout_flow())

    def test_tx_propagation_across_peers(self) -> None:
        asyncio.run(self._run_tx_propagation_flow())

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

    async def _run_payout_flow(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            config = self._build_config(base_dir, 0, ())
            config.mining.min_payout = 1_000_000
            node = await self._start_node(config)
            try:
                self.assertIsNotNone(node.wallet, "wallet not initialized")
                self.assertIsNotNone(node.payout_tracker, "payout tracker missing")
                assert node.wallet is not None
                assert node.payout_tracker is not None
                worker_address = node.wallet.get_new_address("worker")
                node.payout_tracker.record_share("worker-1", worker_address, difficulty=1.0)
                block, height = await asyncio.to_thread(self._mine_block_full, node)
                coinbase = block.transactions[0]
                assert node.payout_tracker is not None
                pool_script = node.payout_tracker.pool_script
                pool_vout = next(
                    (idx for idx, output in enumerate(coinbase.outputs) if output.script_pubkey == pool_script),
                    None,
                )
                self.assertIsNotNone(pool_vout, "Pool payout output missing")
                assert pool_vout is not None
                reward = coinbase.outputs[pool_vout].value
                node.payout_tracker.record_block(height, coinbase.txid(), reward, pool_vout)
                await asyncio.to_thread(self._mine_block_full, node)
                best = node.chain.state_db.get_best_tip()
                assert best is not None
                node.payout_tracker.process_maturity(best[1])
                self.assertTrue(node.payout_tracker.matured_utxos, "No matured coinbase outputs recorded")
                worker_state = node.payout_tracker.workers.get("worker-1")
                self.assertIsNotNone(worker_state)
                assert worker_state is not None
                self.assertGreater(worker_state.balance, 0, "Worker balance never credited")
                payout_tx = node.payout_tracker.create_payout_transaction(node.chain.state_db)
                self.assertIsNotNone(payout_tx, "Payout transaction was not created")
                assert payout_tx is not None
                await asyncio.to_thread(node._accept_payout_tx, payout_tx)
                self.assertTrue(node.mempool.contains(payout_tx.txid()), "Payout transaction missing from mempool")
                self.assertEqual(worker_state.balance, 0)
            finally:
                await self._stop_nodes([node])

    async def _run_tx_propagation_flow(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            nodes: list[BaselineNode] = []
            try:
                seed = await self._start_node(self._build_config(base_dir, 0, ()))
                nodes.append(seed)
                follower = await self._start_node(self._build_config(base_dir, 1, (self._node_address(0),)))
                nodes.append(follower)
                # Wait for connectivity
                await self._wait_for(lambda: len(seed.network.peers) >= 1, 10.0, "seed missing peers")
                await self._wait_for(lambda: len(follower.network.peers) >= 1, 10.0, "follower missing peers")

                # Mine two blocks to unlock a coinbase and ensure chain sync
                await asyncio.to_thread(self._mine_single_block, seed)
                _, height2 = await asyncio.to_thread(self._mine_single_block, seed)
                await self._wait_for_height([follower], height2, 20.0)

                self.assertIsNotNone(seed.payout_tracker, "Payout tracker missing")
                assert seed.payout_tracker is not None
                pool_script = seed.payout_tracker.pool_script
                pool_priv = seed.payout_tracker.pool_privkey
                spendable_utxos = seed.chain.state_db.get_utxos_by_scripts([pool_script])
                self.assertTrue(spendable_utxos, "No spendable pool UTXOs found")
                base_utxo = max(spendable_utxos, key=lambda record: record.height)

                # First transaction: spend coinbase reward to a wallet address and mine it into a block
                addr1 = seed.wallet.get_new_address("send-1") if seed.wallet else crypto.address_from_pubkey(crypto.generate_pubkey(3))
                script1 = script_from_address(addr1)
                tx1 = Transaction(
                    version=1,
                    inputs=[TxInput(prev_txid=base_utxo.txid, prev_vout=base_utxo.vout, script_sig=b"", sequence=0xFFFFFFFF)],
                    outputs=[TxOutput(value=max(1, base_utxo.amount - 10_000), script_pubkey=script1)],
                    lock_time=0,
                )
                sighash1 = tx1.signature_hash(0, base_utxo.script_pubkey, 0x01)
                pub1 = crypto.generate_pubkey(pool_priv)
                sig1 = crypto.sign(sighash1, pool_priv) + b"\x01"
                tx1.inputs[0].script_sig = bytes([len(sig1)]) + sig1 + bytes([len(pub1)]) + pub1
                tx1.validate_basic()
                res1 = seed.mempool.accept_transaction(tx1, peer_id="local")
                self.assertEqual(res1.get("status"), "accepted", f"seed mempool reject tx1: {res1}")
                block_with_tx1, height3 = await asyncio.to_thread(self._mine_single_block, seed)
                self.assertTrue(block_with_tx1)
                await self._wait_for_height([follower], height3, 20.0)

                # Second transaction: spend the confirmed P2PKH output (non-coinbase) and leave it in mempool
                spend_utxo = seed.chain.state_db.get_utxo(tx1.txid(), 0)
                self.assertIsNotNone(spend_utxo, "Failed to locate tx1 output")
                assert spend_utxo is not None
                addr2 = seed.wallet.get_new_address("send-2") if seed.wallet else crypto.address_from_pubkey(crypto.generate_pubkey(5))
                script2 = script_from_address(addr2)
                spend_priv = seed.wallet._lookup_privkey(addr1) if seed.wallet else 5
                tx2 = Transaction(
                    version=1,
                    inputs=[TxInput(prev_txid=spend_utxo.txid, prev_vout=spend_utxo.vout, script_sig=b"", sequence=0xFFFFFFFF)],
                    outputs=[TxOutput(value=max(1, spend_utxo.amount - 5_000), script_pubkey=script2)],
                    lock_time=0,
                )
                sighash2 = tx2.signature_hash(0, spend_utxo.script_pubkey, 0x01)
                pub2 = crypto.generate_pubkey(spend_priv)
                sig2 = crypto.sign(sighash2, spend_priv) + b"\x01"
                tx2.inputs[0].script_sig = bytes([len(sig2)]) + sig2 + bytes([len(pub2)]) + pub2
                tx2.validate_basic()
                tx2id = tx2.txid()
                res2 = seed.mempool.accept_transaction(tx2, peer_id="local")
                self.assertEqual(res2.get("status"), "accepted", f"seed mempool reject tx2: {res2}")

                # Existing peers should receive the mempool tx immediately
                await self._wait_for(lambda: follower.mempool.contains(tx2id), 10.0, "follower did not receive tx2")
                self.assertTrue(follower.mempool.contains(tx2id))

                # New peers should learn about the mempool inventory on connect
                newcomer = await self._start_node(self._build_config(base_dir, 2, (self._node_address(0),)))
                nodes.append(newcomer)
                await self._wait_for(lambda: len(newcomer.network.peers) >= 1, 10.0, "newcomer failed to connect")
                await self._wait_for_height([newcomer], height3, 20.0)
                await self._wait_for(lambda: newcomer.mempool.contains(tx2id), 10.0, "newcomer did not receive tx2 on connect")
                self.assertTrue(newcomer.mempool.contains(tx2id))
            finally:
                await self._stop_nodes(nodes)

    async def _mine_blocks(self, miner: BaselineNode, count: int, synced_nodes: list[BaselineNode]) -> None:
        for _ in range(count):
            block_hash, height = await asyncio.to_thread(self._mine_single_block, miner)
            self.assertTrue(block_hash)
            await self._wait_for_height(synced_nodes, height, 20.0)

    def _mine_single_block(self, miner: BaselineNode) -> tuple[str, int]:
        block, height = self._mine_block_full(miner)
        return block.block_hash(), height

    def _mine_block_full(self, miner: BaselineNode):
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
                            miner.network._best_hash_cache = block_hash
                            miner.network._best_height_cache = height
                            return block, height
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
        config.mining.pool_private_key = "1337" * 16
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
