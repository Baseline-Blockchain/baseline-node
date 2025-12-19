import tempfile
import unittest
from pathlib import Path

from baseline.config import NodeConfig
from baseline.core import crypto, difficulty
from baseline.core.chain import Chain
from baseline.mempool import Mempool
from baseline.mining.templates import EXTRANONCE2_SIZE, TemplateBuilder
from baseline.rpc.handlers import RPCHandlers
from baseline.storage import BlockStore, StateDB


class DummyNetwork:
    def __init__(self) -> None:
        self.peers = {}


class SmokeTests(unittest.TestCase):
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
        pool_pub = crypto.generate_pubkey(7)
        self.pool_script = b"\x76\xa9\x14" + crypto.hash160(pool_pub) + b"\x88\xac"
        self.builder = TemplateBuilder(self.chain, self.mempool, self.pool_script)

    def tearDown(self) -> None:
        self.state_db.close()
        self.tmpdir.cleanup()

    def _mine_single_block(self) -> str:
        template = self.builder.build_template()
        extranonce1 = b"\x00" * self.builder.extranonce1_size
        extranonce2 = (1).to_bytes(EXTRANONCE2_SIZE, "little")
        ntime = template.timestamp
        for nonce in range(0, 1_000_000):
            block = self.builder.assemble_block(template, extranonce1, extranonce2, nonce, ntime)
            if difficulty.check_proof_of_work(block.block_hash(), block.header.bits):
                result = self.chain.add_block(block, block.serialize())
                self.assertIn(result["status"], {"connected", "reorganized"})
                return block.block_hash()
        self.fail("Failed to mine block within nonce range")

    def test_block_production_rpc_and_persistence(self) -> None:
        mined_hash = self._mine_single_block()
        handlers = RPCHandlers(
            self.chain,
            self.mempool,
            self.block_store,
            self.builder,
            DummyNetwork(),
        )
        best_height = self.state_db.get_best_tip()[1]
        block_hash = handlers.dispatch("getblockhash", [best_height])
        self.assertEqual(block_hash, mined_hash)
        block_info = handlers.dispatch("getblock", [block_hash, True])
        self.assertEqual(block_info["hash"], block_hash)
        self.assertEqual(block_info["height"], best_height)
        # Persistence round-trip: reopen state DB and ensure tip matches.
        self.state_db.close()
        reopened = StateDB(self.config.data_dir / "chainstate" / "state.sqlite3")
        self.state_db = reopened
        best = reopened.get_best_tip()
        self.assertIsNotNone(best)
        self.assertEqual(best[0], mined_hash)
