import tempfile
import unittest
from pathlib import Path

from baseline.config import NodeConfig
from baseline.core import crypto, difficulty
from baseline.core.block import Block, BlockHeader, merkle_root_hash
from baseline.core.chain import GENESIS_PRIVKEY, GENESIS_PUBKEY, Chain
from baseline.core.tx import COIN, Transaction, TxInput, TxOutput
from baseline.policy import MIN_RELAY_FEE_RATE
from baseline.storage import BlockStore, StateDB


class ChainTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        data_dir = Path(self.tmpdir.name)
        self.config = NodeConfig()
        self.config.data_dir = data_dir
        self.config.mining.allow_consensus_overrides = True
        self.config.mining.coinbase_maturity = 2
        self.config.ensure_data_layout()
        self.block_store = BlockStore(data_dir / "blocks")
        self.state_db = StateDB(data_dir / "chainstate" / "state.sqlite3")
        self.chain = Chain(self.config, self.state_db, self.block_store)
        self.script_pubkey = b"\x76\xa9\x14" + crypto.hash160(GENESIS_PUBKEY) + b"\x88\xac"

    def tearDown(self) -> None:
        self.state_db.close()
        self.tmpdir.cleanup()

    def test_genesis_initializes(self) -> None:
        best = self.state_db.get_best_tip()
        self.assertIsNotNone(best)
        self.assertEqual(best[1], 0)
        self.assertEqual(best[0], self.chain.genesis_hash)
        self.assertTrue(self.block_store.has_block(self.chain.genesis_hash))

    def test_block_connection_and_spend(self) -> None:
        prev_hash = self.chain.genesis_hash
        for height in (1, 2):
            coinbase = self._make_coinbase(height)
            block = self._mine_block(prev_hash, height, [coinbase])
            res = self.chain.add_block(block)
            self.assertEqual(res["status"], "connected")
            prev_hash = block.block_hash()
        spend_block = self._build_spend_block(prev_hash, height=3)
        res = self.chain.add_block(spend_block)
        self.assertEqual(res["status"], "connected")
        best = self.state_db.get_best_tip()
        self.assertEqual(best[1], 3)

    def _make_coinbase(self, height: int, value: int = 50 * COIN) -> Transaction:
        height_bytes = height.to_bytes((height.bit_length() + 7) // 8 or 1, "little")
        script_sig = len(height_bytes).to_bytes(1, "little") + height_bytes + b"\x01"
        tx = Transaction(
            version=1,
            inputs=[
                TxInput(
                    prev_txid="00" * 32,
                    prev_vout=0xFFFFFFFF,
                    script_sig=script_sig,
                    sequence=0xFFFFFFFF,
                )
            ],
            outputs=[TxOutput(value=value, script_pubkey=self.script_pubkey)],
            lock_time=0,
        )
        return tx

    def _mine_block(self, prev_hash: str, height: int, transactions: list[Transaction]) -> Block:
        parent_header = self.state_db.get_header(prev_hash)
        timestamp = (parent_header.timestamp if parent_header else self.chain.genesis_block.header.timestamp) + self.config.mining.block_interval_target
        header = BlockHeader(
            version=1,
            prev_hash=prev_hash,
            merkle_root=merkle_root_hash(transactions),
            timestamp=timestamp,
            bits=self.config.mining.initial_bits,
            nonce=0,
        )
        block = Block(header=header, transactions=transactions)
        while not difficulty.check_proof_of_work(block.block_hash(), block.header.bits):
            block.header.nonce += 1
        return block

    def _build_spend_block(self, prev_hash: str, height: int) -> Block:
        genesis_txid = self.chain.genesis_block.transactions[0].txid()
        spend_tx = Transaction(
            version=1,
            inputs=[TxInput(prev_txid=genesis_txid, prev_vout=0, script_sig=b"", sequence=0xFFFFFFFF)],
            outputs=[TxOutput(value=50 * COIN - MIN_RELAY_FEE_RATE, script_pubkey=self.script_pubkey)],
            lock_time=0,
        )
        sighash = spend_tx.signature_hash(0, self.script_pubkey, 0x01)
        signature = crypto.sign(sighash, GENESIS_PRIVKEY) + b"\x01"
        pubkey = GENESIS_PUBKEY
        script_sig = len(signature).to_bytes(1, "little") + signature + len(pubkey).to_bytes(1, "little") + pubkey
        spend_tx.inputs[0].script_sig = script_sig
        spend_tx.validate_basic()
        coinbase = self._make_coinbase(height)
        block = self._mine_block(prev_hash, height, [coinbase, spend_tx])
        return block
