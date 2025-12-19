import tempfile
import time
import unittest
from pathlib import Path

from baseline.config import NodeConfig
from baseline.core import crypto
from baseline.core.address import script_from_address
from baseline.core.block import Block, BlockHeader, merkle_root_hash
from baseline.core.chain import Chain
from baseline.core.tx import COIN, Transaction, TxInput, TxOutput
from baseline.mempool import Mempool
from baseline.storage import BlockStore, HeaderData, StateDB, UTXORecord
from baseline.wallet import WalletLockedError, WalletManager


class WalletTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        data_dir = Path(self.tmpdir.name)
        config = NodeConfig()
        config.data_dir = data_dir
        config.ensure_data_layout()
        self.block_store = BlockStore(data_dir / "blocks")
        self.state_db = StateDB(data_dir / "chainstate" / "state.sqlite3")
        self.chain = Chain(config, self.state_db, self.block_store)
        self.mempool = Mempool(self.chain)
        self.wallet_path = data_dir / "wallet" / "wallet.json"

    def tearDown(self) -> None:
        self.state_db.close()
        self.tmpdir.cleanup()

    def test_addresses_are_deterministic(self) -> None:
        wallet = WalletManager(self.wallet_path, self.state_db, self.block_store, self.mempool)
        addr1 = wallet.get_new_address()
        wallet = WalletManager(self.wallet_path, self.state_db, self.block_store, self.mempool)
        priv = wallet._lookup_privkey(addr1)
        self.assertTrue(1 <= priv < crypto.SECP_N)
        addr2 = wallet.get_new_address()
        self.assertNotEqual(addr1, addr2)

    def test_wallet_encryption_and_autolock(self) -> None:
        wallet = WalletManager(self.wallet_path, self.state_db, self.block_store, self.mempool)
        wallet.get_new_address()
        wallet.encrypt_wallet("secret-pass")
        self.assertTrue(wallet.is_encrypted())
        with self.assertRaises(WalletLockedError):
            wallet.get_new_address()
        wallet.unlock_wallet("secret-pass", 1)
        wallet.get_new_address()
        wallet._unlock_until = time.time() - 1
        wallet.tick()
        with self.assertRaises(WalletLockedError):
            wallet.get_new_address()

    def test_dump_and_import_wallet_roundtrip(self) -> None:
        wallet = WalletManager(self.wallet_path, self.state_db, self.block_store, self.mempool)
        first_addr = wallet.get_new_address()
        backup_path = Path(self.tmpdir.name) / "wallet-backup.json"
        exported_seed = wallet.export_seed()
        wallet.dump_wallet(backup_path)
        imported_path = Path(self.tmpdir.name) / "wallet" / "wallet2.json"
        wallet_clone = WalletManager(imported_path, self.state_db, self.block_store, self.mempool)
        wallet_clone.import_wallet(backup_path, rescan=False)
        self.assertEqual(wallet_clone.export_seed(), exported_seed)
        self.assertIn(first_addr, wallet_clone.data["addresses"])

    def test_import_address_creates_watch_only_entry(self) -> None:
        wallet = WalletManager(self.wallet_path, self.state_db, self.block_store, self.mempool)
        pub = crypto.generate_pubkey(12345)
        addr = crypto.address_from_pubkey(pub)
        wallet.import_address(addr, label="watch", rescan=False)
        entry = wallet.data["addresses"][addr]
        self.assertTrue(entry["watch_only"])
        self.assertEqual(entry["label"], "watch")

    def test_wallet_info_tracks_encryption_state(self) -> None:
        wallet = WalletManager(self.wallet_path, self.state_db, self.block_store, self.mempool)
        info = wallet.wallet_info()
        self.assertFalse(info["encrypted"])
        wallet.encrypt_wallet("pass")
        info = wallet.wallet_info()
        self.assertTrue(info["encrypted"])
        self.assertTrue(info["locked"])

    def test_list_addresses_returns_known_entries(self) -> None:
        wallet = WalletManager(self.wallet_path, self.state_db, self.block_store, self.mempool)
        addr = wallet.get_new_address("label-1")
        entries = wallet.list_addresses()
        self.assertTrue(any(entry["address"] == addr and entry["label"] == "label-1" for entry in entries))

    def test_address_balances_reports_spendable_and_watch_only(self) -> None:
        wallet = WalletManager(self.wallet_path, self.state_db, self.block_store, self.mempool)
        spend_addr = wallet.get_new_address("spendable")
        watch_pub = crypto.generate_pubkey(9999)
        watch_addr = crypto.address_from_pubkey(watch_pub)
        wallet.import_address(watch_addr, label="watch", rescan=False)
        record = UTXORecord(
            txid="11" * 32,
            vout=0,
            amount=5 * COIN,
            script_pubkey=script_from_address(spend_addr),
            height=0,
            coinbase=False,
        )
        self.state_db.add_utxo(record)
        balances = wallet.address_balances()
        spend_entry = next(entry for entry in balances if entry["address"] == spend_addr)
        watch_entry = next(entry for entry in balances if entry["address"] == watch_addr)
        self.assertAlmostEqual(spend_entry["balance"], 5.0)
        self.assertFalse(spend_entry["watch_only"])
        self.assertTrue(watch_entry["watch_only"])

    def _make_wif(self, priv_int: int, compressed: bool = False) -> str:
        priv_bytes = priv_int.to_bytes(32, "big")
        payload = b"\x80" + priv_bytes + (b"\x01" if compressed else b"")
        return crypto.base58check_encode(payload)

    def test_import_private_key_plain(self) -> None:
        wallet = WalletManager(self.wallet_path, self.state_db, self.block_store, self.mempool)
        priv = 123456789
        wif = self._make_wif(priv)
        addr = wallet.import_private_key(wif, label="imported", rescan=False)
        entry = wallet.data["addresses"][addr]
        self.assertFalse(entry["watch_only"])
        self.assertEqual(entry["label"], "imported")
        looked_up = wallet._lookup_privkey(addr)
        self.assertEqual(looked_up, priv)

    def test_import_private_key_requires_unlock_when_encrypted(self) -> None:
        wallet = WalletManager(self.wallet_path, self.state_db, self.block_store, self.mempool)
        wallet.encrypt_wallet("secret")
        wif = self._make_wif(987654321, compressed=True)
        with self.assertRaises(WalletLockedError):
            wallet.import_private_key(wif, rescan=False)
        wallet.unlock_wallet("secret", 10)
        addr = wallet.import_private_key(wif, label="hot", rescan=False)
        wallet.lock_wallet()
        with self.assertRaises(WalletLockedError):
            wallet._lookup_privkey(addr)
        wallet.unlock_wallet("secret", 10)
        self.assertEqual(wallet._lookup_privkey(addr), 987654321)

    def test_send_transaction_preserves_comments_across_sync(self) -> None:
        wallet = WalletManager(self.wallet_path, self.state_db, self.block_store, self.mempool)
        source_addr = wallet.get_new_address("source")
        dest_addr = wallet.get_new_address("dest")
        utxo = UTXORecord(
            txid="cc" * 32,
            vout=0,
            amount=10 * COIN,
            script_pubkey=script_from_address(source_addr),
            height=0,
            coinbase=False,
        )
        self.state_db.add_utxo(utxo)
        txid = wallet.send_to_address(dest_addr, 1, comment="gift memo", comment_to="friend")
        tx_entry = wallet.get_transaction(txid)
        self.assertIsNotNone(tx_entry)
        self.assertEqual(tx_entry["comment"], "gift memo")
        self.assertEqual(tx_entry["comment_to"], "friend")
        listed = wallet.list_transactions(count=1)[0]
        self.assertEqual(listed["comment"], "gift memo")
        self.assertEqual(listed["comment_to"], "friend")
        # Ensure persistence across wallet reload
        reloaded = WalletManager(self.wallet_path, self.state_db, self.block_store, self.mempool)
        reloaded_entry = reloaded.get_transaction(txid)
        self.assertIsNotNone(reloaded_entry)
        self.assertEqual(reloaded_entry["comment"], "gift memo")
        self.assertEqual(reloaded_entry["comment_to"], "friend")
        # Mine the transaction into a block and resync; comments should remain intact
        self._mine_transaction_into_block(txid)
        reloaded.sync_chain()
        synced_entry = reloaded.get_transaction(txid)
        self.assertIsNotNone(synced_entry)
        self.assertEqual(synced_entry["comment"], "gift memo")
        self.assertEqual(synced_entry["comment_to"], "friend")

    def _mine_transaction_into_block(self, txid: str) -> None:
        tx = self.mempool.get(txid)
        self.assertIsNotNone(tx, "transaction must exist in mempool")
        best = self.state_db.get_best_tip()
        prev_hash, prev_height = best if best else (self.chain.genesis_hash, 0)
        height = prev_height + 1
        coinbase = Transaction(
            version=1,
            inputs=[
                TxInput(
                    prev_txid="00" * 32,
                    prev_vout=0xFFFFFFFF,
                    script_sig=b"\x51",
                    sequence=0xFFFFFFFF,
                )
            ],
            outputs=[TxOutput(value=50 * COIN, script_pubkey=b"\x51")],
            lock_time=0,
        )
        block_txs = [coinbase, tx]
        header = BlockHeader(
            version=1,
            prev_hash=prev_hash,
            merkle_root=merkle_root_hash(block_txs),
            timestamp=int(time.time()),
            bits=self.chain.config.mining.initial_bits,
            nonce=0,
        )
        block = Block(header=header, transactions=block_txs)
        block_hash = block.block_hash()
        self.block_store.append_block(block_hash, block.serialize())
        current_work = int(self.state_db.get_meta("best_work") or "0")
        new_work = str(current_work + 1)
        header_record = HeaderData(
            hash=block_hash,
            prev_hash=prev_hash,
            height=height,
            bits=header.bits,
            nonce=header.nonce,
            timestamp=header.timestamp,
            merkle_root=header.merkle_root,
            chainwork=new_work,
            status=0,
        )
        self.state_db.store_header(header_record)
        self.state_db.set_best_tip(block_hash, height)
        self.state_db.upsert_chain_tip(block_hash, height, new_work)
        self.state_db.set_meta("best_work", new_work)
        spent = [(vin.prev_txid, vin.prev_vout) for vin in tx.inputs]
        created = [
            UTXORecord(
                txid=txid,
                vout=idx,
                amount=txout.value,
                script_pubkey=txout.script_pubkey,
                height=height,
                coinbase=False,
            )
            for idx, txout in enumerate(tx.outputs)
        ]
        self.state_db.apply_utxo_changes(spent, created)
