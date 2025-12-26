import tempfile
import unittest
from pathlib import Path

from baseline.config import NodeConfig
from baseline.core import crypto, difficulty
from baseline.core.block import Block, BlockHeader, merkle_root_hash
from baseline.core.chain import GENESIS_PUBKEY, GENESIS_PRIVKEY, Chain
from baseline.core.tx import COIN, Transaction, TxInput, TxOutput
from baseline.mempool import Mempool, MempoolError
from baseline.policy import MIN_RELAY_FEE_RATE
from baseline.storage import BlockStore, StateDB, UTXORecord


class StressTests(unittest.TestCase):
    GENESIS_ADDRESS = crypto.address_from_pubkey(GENESIS_PUBKEY)

    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        data_dir = Path(self.tmpdir.name)
        config = NodeConfig()
        config.data_dir = data_dir
        config.mining.allow_consensus_overrides = True
        config.mining.foundation_address = self.GENESIS_ADDRESS
        config.mining.coinbase_maturity = 1
        config.ensure_data_layout()
        self.block_store = BlockStore(data_dir / "blocks")
        self.state_db = StateDB(data_dir / "chainstate" / "state.sqlite3")
        self.chain = Chain(config, self.state_db, self.block_store)
        self.mempool = Mempool(self.chain)
        self.script_pubkey = b"\x76\xa9\x14" + crypto.hash160(GENESIS_PUBKEY) + b"\x88\xac"

    def tearDown(self) -> None:
        self.state_db.close()
        self.tmpdir.cleanup()
        self.mempool.close()

    def _mine_block(self, prev_hash: str, transactions: list[Transaction]) -> Block:
        parent_header = self.state_db.get_header(prev_hash)
        height = (parent_header.height + 1) if parent_header else 1
        height_bytes = height.to_bytes((height.bit_length() + 7) // 8 or 1, "little")
        coinbase_script = len(height_bytes).to_bytes(1, "little") + height_bytes + b"\x01"
        coinbase = Transaction(
            version=1,
            inputs=[
                TxInput(
                    prev_txid="00" * 32,
                    prev_vout=0xFFFFFFFF,
                    script_sig=coinbase_script,
                    sequence=0xFFFFFFFF,
                )
            ],
            outputs=[TxOutput(value=self.chain._block_subsidy(height), script_pubkey=self.script_pubkey)],
            lock_time=0,
        )
        block_txs = [coinbase] + transactions
        timestamp = self.chain.genesis_block.header.timestamp + (1 << 20) + height * 100
        header = BlockHeader(
            version=1,
            prev_hash=prev_hash,
            merkle_root=merkle_root_hash(block_txs),
            timestamp=timestamp,
            bits=self.chain.config.mining.initial_bits,
            nonce=0,
        )
        block = Block(header=header, transactions=block_txs)
        while not difficulty.check_proof_of_work(block.block_hash(), block.header.bits):
            block.header.nonce += 1
        return block

    def _add_utxo(self, txid: str, amount: int) -> None:
        record = UTXORecord(
            txid=txid,
            vout=0,
            amount=amount,
            script_pubkey=self.script_pubkey,
            height=0,
            coinbase=False,
        )
        self.state_db.add_utxo(record)

    def _build_signed_tx(self, prev_txid: str, amount: int, fee: int) -> Transaction:
        tx = Transaction(
            version=1,
            inputs=[TxInput(prev_txid=prev_txid, prev_vout=0, script_sig=b"", sequence=0xFFFFFFFF)],
            outputs=[TxOutput(value=amount - fee, script_pubkey=self.script_pubkey)],
            lock_time=0,
        )
        sighash = tx.signature_hash(0, self.script_pubkey, 0x01)
        signature = crypto.sign(sighash, GENESIS_PRIVKEY) + b"\x01"
        script_sig = bytes([len(signature)]) + signature + bytes([len(GENESIS_PUBKEY)]) + GENESIS_PUBKEY
        tx.inputs[0].script_sig = script_sig
        tx.validate_basic()
        return tx

    def test_reorg_storm_high_work_branch(self) -> None:
        prev = self.chain.genesis_hash
        # build base chain 4 blocks deep
        for height in range(1, 5):
            block = self._mine_block(prev, [])
            res = self.chain.add_block(block)
            self.assertIn(res["status"], {"connected", "reorganized"})
            prev = block.block_hash()

        fork_prev = self.chain.genesis_hash
        # create a longer branch to force a reorg
        newer_tip = fork_prev
        for height in range(1, 7):
            block = self._mine_block(newer_tip, [])
            res = self.chain.add_block(block)
            newer_tip = block.block_hash()
        best_tip = self.state_db.get_best_tip()
        self.assertEqual(best_tip[0], newer_tip)

    def test_mempool_flood_and_eviction(self) -> None:
        for idx in range(50):
            txid = f"{idx:064x}"
            self._add_utxo(txid, COIN)
            tx = self._build_signed_tx(txid, COIN, fee=MIN_RELAY_FEE_RATE)
            try:
                self.mempool.accept_transaction(tx, peer_id=f"peer-{idx}")
            except MempoolError:
                break
        self.assertLessEqual(len(self.mempool.transaction_ids()), 50)
        self.assertGreater(len(self.mempool.transaction_ids()), 0)
