import tempfile
import unittest
from pathlib import Path

from baseline.config import NodeConfig
from baseline.core import crypto, difficulty
from baseline.core.block import Block, BlockHeader, merkle_root_hash
from baseline.core.chain import GENESIS_PRIVKEY, GENESIS_PUBKEY, Chain
from baseline.core.tx import COIN, Transaction, TxInput, TxOutput
from baseline.mempool import Mempool, MempoolError
from baseline.policy import MIN_RELAY_FEE_RATE
from baseline.storage import BlockStore, StateDB, UTXORecord


class MempoolTests(unittest.TestCase):
    GENESIS_ADDRESS = crypto.address_from_pubkey(GENESIS_PUBKEY)
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        data_dir = Path(self.tmpdir.name)
        self.config = NodeConfig()
        self.config.data_dir = data_dir
        self.config.mining.allow_consensus_overrides = True
        self.config.mining.coinbase_maturity = 2
        self.config.mining.foundation_address = self.GENESIS_ADDRESS
        self.config.ensure_data_layout()
        self.block_store = BlockStore(data_dir / "blocks")
        self.state_db = StateDB(data_dir / "chainstate" / "state.sqlite3")
        self.chain = Chain(self.config, self.state_db, self.block_store)
        self.mempool = Mempool(self.chain)
        self.script_pubkey = b"\x76\xa9\x14" + crypto.hash160(GENESIS_PUBKEY) + b"\x88\xac"
        self._mine_initial_blocks()

    def tearDown(self) -> None:
        self.state_db.close()
        self.tmpdir.cleanup()

    def _mine_initial_blocks(self) -> None:
        prev = self.chain.genesis_hash
        for height in (1, 2):
            coinbase = self._make_coinbase(height)
            block = self._mine_block(prev, height, [coinbase])
            res = self.chain.add_block(block)
            self.assertEqual(res["status"], "connected")
            prev = block.block_hash()

    def _make_coinbase(self, height: int, value: int | None = None) -> Transaction:
        if value is None:
            value = self.chain._block_subsidy(height)
        height_bytes = height.to_bytes((height.bit_length() + 7) // 8 or 1, "little")
        script_sig = len(height_bytes).to_bytes(1, "little") + height_bytes + b"\x01"
        foundation = self.chain._foundation_reward(value)
        outputs = []
        if foundation:
            outputs.append(TxOutput(value=foundation, script_pubkey=self.chain.foundation_script))
        outputs.append(TxOutput(value=value - foundation, script_pubkey=self.script_pubkey))
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
            outputs=outputs,
            lock_time=0,
        )
        return tx

    def _mine_block(self, prev_hash: str, height: int, transactions: list[Transaction]) -> Block:
        parent_header = self.state_db.get_header(prev_hash)
        timestamp = (
            parent_header.timestamp if parent_header else self.chain.genesis_block.header.timestamp
        ) + self.config.mining.block_interval_target
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

    def _signed_spend(self, value: int = 50 * COIN - MIN_RELAY_FEE_RATE, *, lock_time: int = 0) -> Transaction:
        genesis_txid = self.chain.genesis_block.transactions[0].txid()
        tx = Transaction(
            version=1,
            inputs=[TxInput(prev_txid=genesis_txid, prev_vout=0, script_sig=b"", sequence=0xFFFFFFFF)],
            outputs=[TxOutput(value=value, script_pubkey=self.script_pubkey)],
            lock_time=lock_time,
        )
        sighash = tx.signature_hash(0, self.script_pubkey, 0x01)
        signature = crypto.sign(sighash, GENESIS_PRIVKEY) + b"\x01"
        pubkey = GENESIS_PUBKEY
        script_sig = len(signature).to_bytes(1, "little") + signature + len(pubkey).to_bytes(1, "little") + pubkey
        tx.inputs[0].script_sig = script_sig
        tx.validate_basic()
        return tx

    def _add_utxo(self, txid: str, vout: int, amount: int, script_pubkey: bytes) -> None:
        record = UTXORecord(
            txid=txid,
            vout=vout,
            amount=amount,
            script_pubkey=script_pubkey,
            height=0,
            coinbase=False,
        )
        self.state_db.add_utxo(record)

    def _signed_spend_from(
        self,
        prev_txid: str,
        prev_vout: int,
        amount: int,
        *,
        fee: int,
        priv: int,
        script_pubkey: bytes,
    ) -> Transaction:
        tx = Transaction(
            version=1,
            inputs=[TxInput(prev_txid=prev_txid, prev_vout=prev_vout, script_sig=b"", sequence=0xFFFFFFFF)],
            outputs=[TxOutput(value=amount - fee, script_pubkey=script_pubkey)],
            lock_time=0,
        )
        sighash = tx.signature_hash(0, script_pubkey, 0x01)
        signature = crypto.sign(sighash, priv) + b"\x01"
        pubkey = crypto.generate_pubkey(priv)
        tx.inputs[0].script_sig = len(signature).to_bytes(1, "little") + signature + len(pubkey).to_bytes(1, "little") + pubkey
        tx.validate_basic()
        return tx

    def test_accept_transaction(self) -> None:
        tx = self._signed_spend()
        res = self.mempool.accept_transaction(tx)
        self.assertEqual(res["status"], "accepted")
        self.assertIn(tx.txid(), self.mempool.transaction_ids())

    def test_double_spend_rejected(self) -> None:
        tx1 = self._signed_spend(value=50 * COIN - 2_000)
        tx2 = self._signed_spend(value=50 * COIN - 3_000)
        self.mempool.accept_transaction(tx1)
        with self.assertRaises(MempoolError):
            self.mempool.accept_transaction(tx2)

    def test_orphan_then_parent(self) -> None:
        parent = self._signed_spend(value=30 * COIN)
        child = Transaction(
            version=1,
            inputs=[TxInput(prev_txid=parent.txid(), prev_vout=0, script_sig=b"", sequence=0xFFFFFFFF)],
            outputs=[TxOutput(value=30 * COIN - MIN_RELAY_FEE_RATE, script_pubkey=self.script_pubkey)],
            lock_time=0,
        )
        sighash = child.signature_hash(0, self.script_pubkey, 0x01)
        signature = crypto.sign(sighash, GENESIS_PRIVKEY) + b"\x01"
        pubkey = GENESIS_PUBKEY
        child.inputs[0].script_sig = len(signature).to_bytes(1, "little") + signature + len(pubkey).to_bytes(1, "little") + pubkey
        orphan_res = self.mempool.accept_transaction(child)
        self.assertEqual(orphan_res["status"], "orphan")
        parent_res = self.mempool.accept_transaction(parent)
        self.assertEqual(parent_res["status"], "accepted")
        self.assertIn(child.txid(), self.mempool.transaction_ids())

    def test_accepts_future_lock_time(self) -> None:
        best = self.state_db.get_best_tip()
        height = best[1] if best else 0
        future_lock = height + 5
        tx = self._signed_spend(lock_time=future_lock)
        res = self.mempool.accept_transaction(tx, peer_id="test")
        self.assertEqual(res["status"], "accepted")
        self.assertIn(tx.txid(), self.mempool.transaction_ids())

    def test_rejects_nonstandard_script_pubkey(self) -> None:
        genesis_txid = self.chain.genesis_block.transactions[0].txid()
        tx = Transaction(
            version=1,
            inputs=[TxInput(prev_txid=genesis_txid, prev_vout=0, script_sig=b"", sequence=0xFFFFFFFF)],
            outputs=[TxOutput(value=50 * COIN - MIN_RELAY_FEE_RATE, script_pubkey=b"\x51")],
            lock_time=0,
        )
        sighash = tx.signature_hash(0, self.script_pubkey, 0x01)
        signature = crypto.sign(sighash, GENESIS_PRIVKEY) + b"\x01"
        pubkey = GENESIS_PUBKEY
        script_sig = len(signature).to_bytes(1, "little") + signature + len(pubkey).to_bytes(1, "little") + pubkey
        tx.inputs[0].script_sig = script_sig
        tx.validate_basic()
        with self.assertRaises(MempoolError):
            self.mempool.accept_transaction(tx, peer_id="test")

    def test_mempool_evicts_low_fee_when_full(self) -> None:
        priv = 7777
        pub = crypto.generate_pubkey(priv)
        script_pubkey = b"\x76\xa9\x14" + crypto.hash160(pub) + b"\x88\xac"
        txid_low = "ab" * 32
        txid_high = "cd" * 32
        amount = 10 * COIN
        self._add_utxo(txid_low, 0, amount, script_pubkey)
        self._add_utxo(txid_high, 0, amount, script_pubkey)
        pool = Mempool(self.chain, max_weight=1200)
        low_fee_tx = self._signed_spend_from(txid_low, 0, amount, fee=5_000, priv=priv, script_pubkey=script_pubkey)
        high_fee_tx = self._signed_spend_from(txid_high, 0, amount, fee=20_000, priv=priv, script_pubkey=script_pubkey)
        pool.accept_transaction(low_fee_tx, peer_id="test")
        pool.accept_transaction(high_fee_tx, peer_id="test")
        self.assertIn(high_fee_tx.txid(), pool.transaction_ids())
        self.assertNotIn(low_fee_tx.txid(), pool.transaction_ids())
