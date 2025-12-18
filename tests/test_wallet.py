import tempfile
import time
import unittest
from pathlib import Path

from baseline.config import NodeConfig
from baseline.core import crypto
from baseline.core.chain import Chain
from baseline.mempool import Mempool
from baseline.storage import BlockStore, StateDB
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
