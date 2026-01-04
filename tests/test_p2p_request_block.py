import asyncio
import os
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
from baseline.net import protocol
from baseline.net.server import P2PServer
from baseline.storage import BlockStore, StateDB
from baseline.storage.blockstore import INDEX_STRUCT


class DummyStateDB:
    def get_best_tip(self):
        return None


class DummyChain:
    def __init__(self):
        self.config = NodeConfig()
        self.state_db = DummyStateDB()
        self.max_target = 0x00000000FFFF0000000000000000000000000000000000000000000000000000
        self.genesis_hash = "00" * 32


class DummyMempool:
    def register_listener(self, _):
        return


class DummyPeer:
    def __init__(self):
        self.closed = False
        self.sent = []

    async def send_message(self, payload):
        self.sent.append(payload)


class P2PRequestBlockTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.chain = DummyChain()
        self.mempool = DummyMempool()
        self.server = P2PServer(self.chain.config, self.chain, self.mempool)
        self.server.loop = asyncio.get_running_loop()
        self.peer1 = DummyPeer()
        self.peer2 = DummyPeer()
        self.server.peers = {"p1": self.peer1, "p2": self.peer2}

    async def test_request_block_broadcasts_once(self):
        block_hash = "ab" * 32
        loop = asyncio.get_running_loop()

        def _run_threadsafe(coro, _loop):
            # Execute immediately in this loop using a task.
            return loop.create_task(coro)

        with mock.patch("baseline.net.server.asyncio.run_coroutine_threadsafe", _run_threadsafe):
            sent = self.server.request_block(block_hash)
            self.assertTrue(sent)
            await asyncio.sleep(0)
            self.assertEqual(len(self.peer1.sent), 1)
            self.assertEqual(len(self.peer2.sent), 1)

            # Second call within backoff should do nothing.
            sent_again = self.server.request_block(block_hash)
            self.assertFalse(sent_again)
            await asyncio.sleep(0)
            self.assertEqual(len(self.peer1.sent), 1)
            self.assertEqual(len(self.peer2.sent), 1)

            # After backoff expires, it should send again.
            self.server._block_request_backoff[block_hash] = time.time() - 11
            sent_third = self.server.request_block(block_hash)
            self.assertTrue(sent_third)
            await asyncio.sleep(0)
            self.assertEqual(len(self.peer1.sent), 2)
            self.assertEqual(len(self.peer2.sent), 2)


class P2PRepairFlowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        base_dir = Path(self.tmpdir.name)
        self.config = NodeConfig()
        self.config.data_dir = base_dir
        self.config.mining.allow_consensus_overrides = True
        self.config.mining.coinbase_maturity = 1
        self.config.ensure_data_layout()
        self.block_store = BlockStore(base_dir / "blocks")
        self.state_db = StateDB(base_dir / "chainstate" / "state.sqlite3")
        self.chain = Chain(self.config, self.state_db, self.block_store)
        self.mempool = Mempool(self.chain)
        self.server = P2PServer(self.config, self.chain, self.mempool)
        self.server.loop = asyncio.get_running_loop()

    async def asyncTearDown(self) -> None:
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
        base_time = parent.timestamp if parent else self.chain.genesis_block.header.timestamp
        header = BlockHeader(
            version=1,
            prev_hash=prev_hash,
            merkle_root=merkle_root_hash([coinbase]),
            timestamp=base_time + self.config.mining.block_interval_target,
            bits=self.config.mining.initial_bits,
            nonce=0,
        )
        block = Block(header=header, transactions=[coinbase])
        while not difficulty.check_proof_of_work(block.block_hash(), block.header.bits):
            block.header.nonce += 1
        return block

    def _remove_block_from_store(self, block_hash: str) -> None:
        entry = self.block_store._index.pop(block_hash, None)
        self.assertIsNotNone(entry, "Block missing from index")
        offset, _ = entry
        with self.block_store.data_path.open("r+b") as fh:
            fh.truncate(offset)
        with self.block_store.index_path.open("r+b") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            fh.truncate(max(0, size - INDEX_STRUCT.size))

    async def test_request_block_heals_missing_data(self) -> None:
        prev = self.chain.genesis_hash
        block = self._mine_block(prev)
        raw = block.serialize()
        result = self.chain.add_block(block, raw)
        self.assertEqual(result.get("status"), "connected")
        missing_hash = block.block_hash()
        self.assertTrue(self.block_store.has_block(missing_hash))

        # Simulate corruption: drop the block from the store but keep headers/state.
        self._remove_block_from_store(missing_hash)
        self.assertFalse(self.block_store.has_block(missing_hash))

        loop = asyncio.get_running_loop()
        server = self.server

        class HealingPeer:
            def __init__(self, block_hash: str, raw_hex: str):
                self.peer_id = "healer"
                self.address = ("127.0.0.1", 0)
                self.remote_version = {"height": 2}
                self.outbound = True
                self.known_inventory = set()
                self.closed = False
                self.sent = []
                self._block_hash = block_hash
                self._raw_hex = raw_hex

            async def send_message(self, payload):
                self.sent.append(payload)
                if payload.get("type") == "getdata":
                    items = payload.get("items") or []
                    if any(item.get("type") == "block" and item.get("hash") == self._block_hash for item in items):
                        await server.handle_block(
                            self,
                            protocol.block_payload(self._block_hash, self._raw_hex),
                        )

        peer = HealingPeer(missing_hash, raw.hex())
        self.server.peers = {"p": peer}

        def _run_threadsafe(coro, _loop):
            return loop.create_task(coro)

        with mock.patch("baseline.net.server.asyncio.run_coroutine_threadsafe", _run_threadsafe):
            sent = self.server.request_block(missing_hash)
            self.assertTrue(sent)
            deadline = time.time() + 2.0
            while not self.block_store.has_block(missing_hash) and time.time() < deadline:
                await asyncio.sleep(0.01)
            self.assertTrue(self.block_store.has_block(missing_hash))
            self.assertTrue(any(payload.get("type") == "getdata" for payload in peer.sent))
