import hashlib
import tempfile
import unittest
from pathlib import Path

from baseline.storage import (
    BlockStore,
    BlockStoreError,
    HeaderData,
    StateDB,
    UTXORecord,
)


class BlockStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.store = BlockStore(Path(self.tmpdir.name))

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_append_and_read_block(self) -> None:
        block_hash = hashlib.sha256(b"block-1").digest()
        payload = b"\x01\x02payload"
        self.store.append_block(block_hash, payload)
        self.assertTrue(self.store.has_block(block_hash))
        self.assertEqual(self.store.get_block(block_hash), payload)
        self.assertEqual(self.store.block_count(), 1)
        self.store.check()

    def test_duplicate_block_rejected(self) -> None:
        block_hash = hashlib.sha256(b"duplicate").digest()
        self.store.append_block(block_hash, b"abc")
        with self.assertRaises(BlockStoreError):
            self.store.append_block(block_hash, b"abc")


class StateDBTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "state.sqlite3"
        self.state = StateDB(self.db_path)

    def tearDown(self) -> None:
        self.state.close()
        self.tmpdir.cleanup()

    def test_header_storage_and_tip_tracking(self) -> None:
        header = HeaderData(
            hash="00" * 32,
            prev_hash=None,
            height=0,
            bits=0x1f00ffff,
            nonce=1,
            timestamp=1234567890,
            merkle_root="11" * 32,
            chainwork="1",
            status=0,
        )
        self.state.store_header(header)
        loaded = self.state.get_header(header.hash)
        self.assertEqual(loaded, header)
        self.state.set_best_tip(header.hash, header.height)
        self.state.upsert_chain_tip(header.hash, header.height, header.chainwork)
        tips = self.state.list_chain_tips()
        self.assertEqual(len(tips), 1)
        self.assertEqual(tips[0][0], header.hash)
        best = self.state.get_best_tip()
        self.assertIsNotNone(best)
        self.assertEqual(best[0], header.hash)

    def test_utxo_lifecycle(self) -> None:
        utxo = UTXORecord(
            txid="aa" * 32,
            vout=0,
            amount=5000000000,
            script_pubkey=b"\x51",
            height=1,
            coinbase=True,
        )
        self.state.add_utxo(utxo)
        fetched = self.state.get_utxo(utxo.txid, utxo.vout)
        self.assertEqual(fetched, utxo)
        self.state.apply_utxo_changes([(utxo.txid, utxo.vout)], [])
        self.assertIsNone(self.state.get_utxo(utxo.txid, utxo.vout))
