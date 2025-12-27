import hashlib
import tempfile
import time
import unittest
from pathlib import Path

from baseline.config import NodeConfig
from baseline.core import crypto
from baseline.core.chain import Chain
from baseline.core.tx import COIN, Transaction, TxInput, TxOutput
from baseline.mempool import Mempool
from baseline.mining.templates import TemplateBuilder
from baseline.policy import MIN_RELAY_FEE_RATE
from baseline.storage import BlockStore, StateDB, UTXORecord


class TPSTestCase(unittest.TestCase):
    MIN_EXPECTED_TPS = 5.0
    TX_COUNT = 40

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        node_dir = Path(self.temp_dir.name) / "node"
        config = NodeConfig()
        config.data_dir = node_dir
        config.ensure_data_layout()
        config.mining.allow_consensus_overrides = True
        config.mining.coinbase_maturity = 1
        self.block_store = BlockStore(node_dir / "blocks")
        self.state_db = StateDB(node_dir / "chainstate" / "state.sqlite3")
        self.chain = Chain(config, self.state_db, self.block_store)
        self.mempool = Mempool(self.chain)
        payout_privkey = 0xBADCAFE
        self._privkey = payout_privkey
        self._pubkey = crypto.generate_pubkey(payout_privkey)
        self._script_pubkey = b"\x76\xa9\x14" + crypto.hash160(self._pubkey) + b"\x88\xac"
        self.template_builder = TemplateBuilder(self.chain, self.mempool, self._script_pubkey)

    def tearDown(self) -> None:
        self.state_db.close()
        self.temp_dir.cleanup()

    def test_transaction_throughput_acceptance(self) -> None:
        utxos = self._seed_utxos(self.TX_COUNT)
        txs = [self._build_signed_transaction(record) for record in utxos]
        start = time.perf_counter()
        for tx in txs:
            result = self.mempool.accept_transaction(tx, peer_id="tps-test", propagate=False)
            self.assertEqual(result["status"], "accepted")
        duration = time.perf_counter() - start
        tps = len(txs) / duration if duration else float("inf")
        # Basic sanity: mempool should retain every transaction.
        self.assertEqual(len(self.mempool.entries), self.TX_COUNT)
        self.assertGreaterEqual(tps, self.MIN_EXPECTED_TPS, f"Transaction throughput below expectation: {tps:.2f} TPS")
        print(f"[TPS benchmark] accepted {len(txs)} txs in {duration:.3f}s -> {tps:.2f} TPS")
        # Ensure block template construction handles the high load without errors.
        template = self.template_builder.build_template()
        self.assertGreaterEqual(template.height, 1)
        self.assertEqual(len(template.transactions), self.TX_COUNT)

    def _seed_utxos(self, count: int) -> list[UTXORecord]:
        utxos: list[UTXORecord] = []
        value = int(0.5 * COIN)
        for index in range(count):
            txid = hashlib.sha256(f"tps-utxo-{index}".encode()).hexdigest()
            record = UTXORecord(
                txid=txid,
                vout=0,
                amount=value,
                script_pubkey=self._script_pubkey,
                height=1,
                coinbase=False,
            )
            utxos.append(record)
        self.state_db.apply_utxo_changes([], utxos)
        return utxos

    def _build_signed_transaction(self, utxo: UTXORecord) -> Transaction:
        fee = MIN_RELAY_FEE_RATE
        output_value = utxo.amount - fee
        tx = Transaction(
            version=1,
            inputs=[
                TxInput(
                    prev_txid=utxo.txid,
                    prev_vout=utxo.vout,
                    script_sig=b"",
                    sequence=0xFFFFFFFF,
                )
            ],
            outputs=[TxOutput(value=output_value, script_pubkey=self._script_pubkey)],
            lock_time=0,
        )
        sighash = tx.signature_hash(0, self._script_pubkey, 0x01)
        signature = crypto.sign(sighash, self._privkey) + b"\x01"
        script_sig = bytes([len(signature)]) + signature + bytes([len(self._pubkey)]) + self._pubkey
        tx.inputs[0].script_sig = script_sig
        return tx
