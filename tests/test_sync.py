import asyncio
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from baseline.config import NodeConfig
from baseline.core import crypto, difficulty
from baseline.core.block import Block, BlockHeader, merkle_root_hash
from baseline.core.chain import Chain
from baseline.core.tx import Transaction, TxInput, TxOutput
from baseline.mempool import Mempool
from baseline.net.peer import Peer
from baseline.net.server import P2PServer
from baseline.storage import BlockStore, HeaderData, StateDB


class DummyPeer:
    def __init__(self):
        self.sent = []
        self.address = ("127.0.0.1", 0)
        self.peer_id = "dummy"
        self.remote_version = {"height": 0}
        self.outbound = True
        self.known_inventory = set()
        self.closed = False

    async def send_message(self, payload):
        self.sent.append(payload)


class SyncTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        data_dir = Path(self.tmpdir.name)
        self.config = NodeConfig()
        self.config.data_dir = data_dir
        self.config.mining.allow_consensus_overrides = True
        self.config.mining.coinbase_maturity = 1
        self.config.ensure_data_layout()
        self.block_store = BlockStore(data_dir / "blocks")
        self.state_db = StateDB(data_dir / "chainstate" / "state.sqlite3")
        self.chain = Chain(self.config, self.state_db, self.block_store)
        self.mempool = Mempool(self.chain)
        self.server = P2PServer(self.config, self.chain, self.mempool)
        prev = self.chain.genesis_hash
        for _ in range(3):
            block = self._mine_block(prev)
            self.chain.add_block(block, block.serialize())
            prev = block.block_hash()

    async def asyncSetUp(self) -> None:
        self.server.loop = asyncio.get_running_loop()

    def tearDown(self) -> None:
        self.state_db.close()
        self.tmpdir.cleanup()

    def _make_coinbase(self, height: int) -> Transaction:
        height_bytes = height.to_bytes((height.bit_length() + 7) // 8 or 1, "little")
        script_sig = len(height_bytes).to_bytes(1, "little") + height_bytes
        subsidy = self.chain._block_subsidy(height)
        foundation = self.chain._foundation_reward(subsidy)
        outputs = []
        if foundation:
            outputs.append(TxOutput(value=foundation, script_pubkey=self.chain.foundation_script))
        miner_script = b"\x76\xa9\x14" + crypto.hash160(crypto.generate_pubkey(1)) + b"\x88\xac"
        outputs.append(TxOutput(value=subsidy - foundation, script_pubkey=miner_script))
        return Transaction(
            version=1,
            inputs=[TxInput(prev_txid="00" * 32, prev_vout=0xFFFFFFFF, script_sig=script_sig, sequence=0xFFFFFFFF)],
            outputs=outputs,
            lock_time=0,
        )

    def _mine_block(self, prev_hash: str) -> Block:
        parent = self.state_db.get_header(prev_hash)
        height = (parent.height + 1) if parent else 1
        coinbase = self._make_coinbase(height)
        block_txs = [coinbase]
        base_time = parent.timestamp if parent else self.chain.genesis_block.header.timestamp
        header = BlockHeader(
            version=1,
            prev_hash=prev_hash,
            merkle_root=merkle_root_hash(block_txs),
            timestamp=base_time + self.config.mining.block_interval_target,
            bits=self.config.mining.initial_bits,
            nonce=0,
        )
        block = Block(header=header, transactions=block_txs)
        while not difficulty.check_proof_of_work(block.block_hash(), block.header.bits):
            block.header.nonce += 1
        return block

    def _make_header(self, prev_hash: str) -> BlockHeader:
        parent = self.state_db.get_header(prev_hash)
        base_time = parent.timestamp if parent else self.chain.genesis_block.header.timestamp
        header = BlockHeader(
            version=1,
            prev_hash=prev_hash,
            merkle_root="00" * 32,
            timestamp=base_time + self.config.mining.block_interval_target,
            bits=self.config.mining.initial_bits,
            nonce=0,
        )
        while not difficulty.check_proof_of_work(header.hash(), header.bits):
            header.nonce += 1
        return header

    async def test_handle_getblocks_returns_inventory(self) -> None:
        peer = DummyPeer()
        locator = [self.chain.genesis_hash]
        await self.server.handle_getblocks(peer, {"locator": locator})
        self.assertTrue(peer.sent)
        payload = peer.sent[0]
        self.assertEqual(payload["type"], "inv")
        hashes = [item["hash"] for item in payload["items"]]
        best = self.state_db.get_best_tip()[0]
        self.assertIn(best, hashes)

    async def test_block_locator_includes_tip(self) -> None:
        locator = await self.server._build_block_locator_async()
        best = self.state_db.get_best_tip()[0]
        self.assertEqual(locator[0], best)
        self.assertIn(self.chain.genesis_hash, locator)

    async def test_handle_headers_triggers_block_sync(self) -> None:
        peer = DummyPeer()
        peer.remote_version = {"height": 10}
        self.server.header_peer = peer
        self.server.header_sync_active = True
        best = self.state_db.get_best_tip()[0]
        header = self._make_header(best)
        payload = {
            "headers": [
                {
                    "version": header.version,
                    "prev_hash": header.prev_hash,
                    "merkle_root": header.merkle_root,
                    "timestamp": header.timestamp,
                    "bits": header.bits,
                    "nonce": header.nonce,
                }
            ]
        }
        await self.server.handle_headers(peer, payload)
        self.assertFalse(self.server.header_sync_active)
        self.assertTrue(self.server.sync_active)

    async def test_addr_gossip_populates_discovery(self) -> None:
        self.assertIs(self.server.known_addresses, self.server.discovery.address_book.addresses)
        host = "203.0.113.5"
        port = 9333
        payload = {
            "type": "addr",
            "peers": [{"host": host, "port": port, "last_seen": int(time.time())}],
        }
        self.server.discovery.handle_addr_message(payload, "198.51.100.10")
        self.assertIn((host, port), self.server.discovery.address_book.addresses)
        self.assertIn((host, port), self.server.known_addresses)
        discovered = await self.server.discovery.discover_peers(1)
        self.assertIn((host, port), discovered)

    async def test_outbound_allowed_when_already_inbound(self) -> None:
        inbound = DummyPeer()
        inbound.outbound = False
        inbound.address = ("203.0.113.9", 9333)
        self.server.peers["inbound"] = inbound
        captured: list[tuple[str, int]] = []
        spawned: list[Peer] = []

        class DummyReader:
            pass

        class DummyWriter:
            def close(self):  # pragma: no cover - noop
                pass

            async def wait_closed(self):  # pragma: no cover - noop
                pass

        async def fake_open_connection(host: str, port: int, **_):
            captured.append((host, port))
            return DummyReader(), DummyWriter()

        self.server._run_peer = lambda peer: spawned.append(peer)
        with mock.patch("baseline.net.server.asyncio.open_connection", fake_open_connection):
            await self.server._connect_outbound(*inbound.address)

        self.assertIn(inbound.address, captured)
        self.assertEqual(len(spawned), 1)
        self.assertTrue(spawned[0].outbound)

    async def test_outbound_maintains_network_diversity(self) -> None:
        self.server.target_outbound = 2
        self.server._allow_non_seed_outbound = False
        captured: list[tuple[str, int]] = []

        async def fake_connect(host: str, port: int) -> None:
            captured.append((host, port))

        candidates = [
            ("192.0.2.1", 9333),
            ("192.0.2.55", 9333),
            ("198.51.100.5", 9333),
        ]
        with mock.patch.object(self.server, "_connect_outbound", fake_connect), mock.patch.object(
            self.server.discovery, "discover_peers", return_value=candidates
        ):
            await self.server._maintain_outbound()

        self.assertEqual(len(captured), 2)
        buckets = {self.server._network_bucket(host) for host, _ in captured}
        self.assertGreaterEqual(len(buckets), 2)

    async def test_missing_parent_rotation_moves_to_new_peer(self) -> None:
        peer = DummyPeer()
        peer.peer_id = "sync"
        self.server.sync_peer = peer
        self.server.sync_active = True

        orphan_block = self._mine_block("11" * 32)
        payload = {"raw": orphan_block.serialize().hex(), "hash": orphan_block.block_hash()}

        for _ in range(3):
            await self.server.handle_block(peer, payload)

        self.assertFalse(self.server.sync_active)
        self.assertIsNone(self.server.sync_peer)

    async def test_invalid_block_marked_and_penalized(self) -> None:
        peer = DummyPeer()
        peer.peer_id = "bad"

        tip = self.state_db.get_best_tip()[0]
        block = self._mine_block(tip)
        block.transactions = []
        payload = {"raw": block.serialize().hex(), "hash": block.block_hash()}

        await self.server.handle_block(peer, payload)

        self.assertTrue(self.server._is_block_invalid_fast(block.block_hash()))
        self.assertIn(peer.peer_id, self.server._bad_block_counts)

    async def test_ephemeral_outbound_skipped(self) -> None:
        captured: list[tuple[str, int]] = []

        class DummyReader:
            pass

        class DummyWriter:
            def close(self):  # pragma: no cover - noop
                pass

            async def wait_closed(self):  # pragma: no cover - noop
                pass

        async def fake_open_connection(host: str, port: int, **_):
            captured.append((host, port))
            return DummyReader(), DummyWriter()

        with mock.patch("baseline.net.server.asyncio.open_connection", fake_open_connection):
            await self.server._connect_outbound("203.0.113.9", 36054)

        self.assertEqual(captured, [("203.0.113.9", self.server.listen_port)])

    async def test_inv_floods_truncated_for_non_sync_peer(self) -> None:
        peer = DummyPeer()
        items = [{"type": "block", "hash": f"{i:064x}"} for i in range(300)]
        for _ in range(7):
            await self.server.handle_inv(peer, {"items": items})
        # Should only respond a few times, then rate-limit further floods.
        self.assertLessEqual(len(peer.sent), 5)
        if peer.sent:
            self.assertEqual(len(peer.sent[0]["items"]), 200)
        self.assertNotIn(peer.peer_id, self.server._bad_block_counts)

    async def test_missing_parent_does_not_ban_peer(self) -> None:
        peer = DummyPeer()
        peer.peer_id = "no-sync"
        orphan_block = self._mine_block("22" * 32)
        payload = {"raw": orphan_block.serialize().hex(), "hash": orphan_block.block_hash()}

        for _ in range(5):
            await self.server.handle_block(peer, payload)

        # Missing parents should not ban/penalize the peer; we just rotate sync sources.
        self.assertNotIn(peer.peer_id, self.server._bad_block_counts)

    async def test_parent_data_missing_requests_parent_not_ban(self) -> None:
        peer = DummyPeer()
        peer.peer_id = "parent-miss"
        # Insert a fake header without storing the raw block to trigger "Parent block data missing".
        fake_prev = "ab" * 32
        header = HeaderData(
            hash=fake_prev,
            prev_hash=self.chain.genesis_hash,
            height=1,
            bits=self.config.mining.initial_bits,
            nonce=0,
            timestamp=self.chain.genesis_block.header.timestamp + self.config.mining.block_interval_target,
            merkle_root="00" * 32,
            chainwork="1",
            version=1,
            status=0,
        )
        self.state_db.store_header(header)

        block = self._mine_block(fake_prev)
        payload = {"raw": block.serialize().hex(), "hash": block.block_hash()}

        await self.server.handle_block(peer, payload)

        self.assertNotIn(peer.peer_id, self.server._bad_block_counts)
        self.assertFalse(self.server._is_block_invalid_fast(block.block_hash()))

    async def test_missing_referenced_output_does_not_ban_peer(self) -> None:
        peer = DummyPeer()
        peer.peer_id = "utxo-miss"
        tip = self.state_db.get_best_tip()[0]
        coinbase = self._make_coinbase(4)
        bogus_tx = Transaction(
            version=1,
            inputs=[
                TxInput(prev_txid="ff" * 32, prev_vout=0, script_sig=b"", sequence=0xFFFFFFFF),
            ],
            outputs=[TxOutput(value=1, script_pubkey=b"\x51")],
            lock_time=0,
        )
        block = self._mine_block(tip)
        block.transactions = [coinbase, bogus_tx]
        block.header.merkle_root = merkle_root_hash(block.transactions)
        while not difficulty.check_proof_of_work(block.block_hash(), block.header.bits):
            block.header.nonce += 1
        payload = {"raw": block.serialize().hex(), "hash": block.block_hash()}

        await self.server.handle_block(peer, payload)

        self.assertNotIn(peer.peer_id, self.server._bad_block_counts)
