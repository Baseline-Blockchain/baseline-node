import tempfile
import unittest
from pathlib import Path

from baseline.config import NodeConfig
from baseline.core import crypto
from baseline.core.chain import Chain
from baseline.core.tx import COIN
from baseline.mempool import Mempool
from baseline.mining.payout import PayoutTracker
from baseline.mining.templates import EXTRANONCE2_SIZE, TemplateBuilder
from baseline.storage import BlockStore, StateDB, UTXORecord


class TemplateBuilderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        data_dir = Path(self.tmpdir.name)
        self.config = NodeConfig()
        self.config.data_dir = data_dir
        self.config.ensure_data_layout()
        self.block_store = BlockStore(data_dir / "blocks")
        self.state_db = StateDB(data_dir / "chainstate" / "state.sqlite3")
        self.chain = Chain(self.config, self.state_db, self.block_store)
        self.mempool = Mempool(self.chain)
        pool_pub = crypto.generate_pubkey(2)
        self.pool_script = b"\x76\xa9\x14" + crypto.hash160(pool_pub) + b"\x88\xac"

    def tearDown(self) -> None:
        self.state_db.close()
        self.tmpdir.cleanup()

    def test_coinbase_assembly_includes_extranonce(self) -> None:
        builder = TemplateBuilder(self.chain, self.mempool, self.pool_script)
        template = builder.build_template()
        self.assertEqual(template.height, 1)
        self.assertEqual(template.coinbase_value, self.chain._block_subsidy(template.height))
        extranonce1 = b"\x01\x02\x03\x04"
        extranonce2 = (12345678).to_bytes(EXTRANONCE2_SIZE, "little")
        block = builder.assemble_block(
            template,
            extranonce1,
            extranonce2,
            nonce=0,
            ntime=template.timestamp,
        )
        coinbase = block.transactions[0]
        script_sig = coinbase.inputs[0].script_sig
        total_extranonce = len(extranonce1) + len(extranonce2)
        self.assertTrue(script_sig.endswith(extranonce1 + extranonce2))
        self.assertEqual(script_sig[-(total_extranonce + 1)], total_extranonce)
        self.assertEqual(coinbase.outputs[0].script_pubkey, self.pool_script)
        self.assertEqual(len(template.merkle_branches), 0)


class DummyState:
    def __init__(self, utxos):
        self.utxos = utxos

    def get_utxo(self, txid: str, vout: int):
        return self.utxos.get((txid, vout))


class PayoutTrackerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.ledger = Path(self.tmpdir.name) / "ledger.json"
        self.pool_priv = 3
        self.pool_pub = crypto.generate_pubkey(self.pool_priv)
        self.pool_script = b"\x76\xa9\x14" + crypto.hash160(self.pool_pub) + b"\x88\xac"
        self.tracker = PayoutTracker(
            self.ledger,
            self.pool_priv,
            self.pool_pub,
            self.pool_script,
            maturity=2,
            min_payout=50_000_000,
            pool_fee_percent=1.0,
        )
        worker_pub = crypto.generate_pubkey(5)
        self.worker_address = crypto.address_from_pubkey(worker_pub)
        self.worker_id = "worker-1"

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_payout_transaction_created_after_maturity(self) -> None:
        reward = 50 * COIN
        txid = "ab" * 32
        self.tracker.record_share(self.worker_id, self.worker_address, difficulty=1.0)
        self.tracker.record_block(height=5, coinbase_txid=txid, reward=reward)
        self.tracker.process_maturity(best_height=7)
        balance_before = self.tracker.workers[self.worker_id].balance
        self.assertGreater(balance_before, 0)
        utxo = UTXORecord(
            txid=txid,
            vout=0,
            amount=reward,
            script_pubkey=self.pool_script,
            height=5,
            coinbase=True,
        )
        dummy_state = DummyState({(txid, 0): utxo})
        payout_tx = self.tracker.create_payout_transaction(dummy_state)
        self.assertIsNotNone(payout_tx)
        assert payout_tx is not None
        self.assertEqual(payout_tx.outputs[0].value, balance_before)
        self.assertEqual(self.tracker.workers[self.worker_id].balance, 0)
        self.assertEqual(len(self.tracker.matured_utxos), 0)
