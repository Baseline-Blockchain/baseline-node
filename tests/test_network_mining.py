import tempfile
import unittest
from pathlib import Path

from baseline.config import NodeConfig
from baseline.core import crypto, difficulty
from baseline.core.block import Block, BlockHeader, merkle_root_hash
from baseline.core.chain import Chain
from baseline.core.tx import COIN, Transaction, TxInput, TxOutput
from baseline.storage import BlockStore, StateDB


class NodeSim:
    def __init__(self, root: Path, node_id: int):
        self.config = NodeConfig()
        self.config.data_dir = root / f"node{node_id}"
        self.config.mining.coinbase_maturity = 1
        self.config.ensure_data_layout()
        self.block_store = BlockStore(self.config.data_dir / "blocks")
        self.state_db = StateDB(self.config.data_dir / "chainstate" / "state.sqlite3")
        self.chain = Chain(self.config, self.state_db, self.block_store)
        self.pubkey = crypto.generate_pubkey(node_id + 2)
        self.script = b"\x76\xa9\x14" + crypto.hash160(self.pubkey) + b"\x88\xac"

    def close(self) -> None:
        self.state_db.close()

    def _make_coinbase(self, height: int) -> Transaction:
        height_bytes = height.to_bytes((height.bit_length() + 7) // 8 or 1, "little")
        script_sig = len(height_bytes).to_bytes(1, "little") + height_bytes
        return Transaction(
            version=1,
            inputs=[TxInput(prev_txid="00" * 32, prev_vout=0xFFFFFFFF, script_sig=script_sig, sequence=0xFFFFFFFF)],
            outputs=[TxOutput(value=50 * COIN, script_pubkey=self.script)],
            lock_time=0,
        )

    def mine_block(self) -> Block:
        tip = self.state_db.get_best_tip()
        prev_hash = tip[0] if tip else self.chain.genesis_hash
        parent = self.state_db.get_header(prev_hash)
        height = (parent.height + 1) if parent else 1
        coinbase = self._make_coinbase(height)
        txs = [coinbase]
        base_time = parent.timestamp if parent else self.chain.genesis_block.header.timestamp
        header = BlockHeader(
            version=1,
            prev_hash=prev_hash,
            merkle_root=merkle_root_hash(txs),
            timestamp=base_time + self.config.mining.block_interval_target,
            bits=self.config.mining.initial_bits,
            nonce=0,
        )
        block = Block(header=header, transactions=txs)
        while not difficulty.check_proof_of_work(block.block_hash(), block.header.bits):
            block.header.nonce += 1
        return block


class NetworkMiningTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        root = Path(self.tmpdir.name)
        self.nodes = [NodeSim(root, idx) for idx in range(10)]

    def tearDown(self) -> None:
        for node in self.nodes:
            node.close()
        self.tmpdir.cleanup()

    def test_simulated_network_many_miners(self) -> None:
        rounds = 6
        for round_idx in range(rounds):
            miner_index = round_idx % 9  # 9 miners, 1 passive node
            block = self.nodes[miner_index].mine_block()
            raw = block.serialize()
            for node in self.nodes:
                result = node.chain.add_block(block, raw)
                self.assertIn(result["status"], {"connected", "reorganized", "duplicate"})
        tips = {node.state_db.get_best_tip() for node in self.nodes}
        self.assertEqual(len(tips), 1)
        final_hash, final_height = tips.pop()
        self.assertGreaterEqual(final_height, rounds)
        for node in self.nodes:
            best = node.state_db.get_best_tip()
            self.assertEqual(best[0], final_hash)
