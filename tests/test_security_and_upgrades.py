"""
Tests for P2P security enhancements and upgrade mechanisms.
"""

import asyncio
import logging
import tempfile
import time
import unittest
from pathlib import Path

from baseline.core.block import BlockHeader
from baseline.core.upgrade import UpgradeDefinition, UpgradeManager, UpgradeState
from baseline.net.discovery import AddressBook, DNSSeeder, PeerAddress
from baseline.net.security import (
    BanManager,
    ConnectionLimiter,
    MessageValidator,
    P2PSecurity,
    PeerReputation,
    RateLimiter,
)
from baseline.storage.state import HeaderData, StateDB
from baseline.net.peer import Peer


class SecurityTests(unittest.TestCase):
    """Test P2P security features."""

    def test_rate_limiter_basic_functionality(self):
        """Test basic rate limiter functionality."""
        limiter = RateLimiter(max_tokens=5, refill_rate=1.0)

        # Should be able to consume initial tokens
        for _ in range(5):
            self.assertTrue(limiter.consume())

        # Should be rate limited now
        self.assertFalse(limiter.consume())

        # Wait for refill
        time.sleep(1.1)
        self.assertTrue(limiter.consume())

    def test_peer_reputation_scoring(self):
        """Test peer reputation system."""
        reputation = PeerReputation()

        # Start with neutral score
        self.assertEqual(reputation.score, 100)
        self.assertFalse(reputation.is_banned())

        # Record violations
        reputation.record_violation(severity=2)
        self.assertEqual(reputation.score, 80)

        # Record invalid messages
        reputation.record_invalid_message()
        self.assertEqual(reputation.score, 75)

        # Record good behavior
        for _ in range(100):
            reputation.record_message()
        self.assertEqual(reputation.score, 76)  # Should improve by 1

    def test_connection_limiter(self):
        """Test connection limiting."""
        limiter = ConnectionLimiter(max_per_ip=2, max_total=5)

        # Should accept connections within limits
        self.assertTrue(limiter.add_connection("192.168.1.1"))
        self.assertTrue(limiter.add_connection("192.168.1.1"))
        self.assertTrue(limiter.add_connection("192.168.1.2"))

        # Should reject third connection from same IP
        self.assertFalse(limiter.add_connection("192.168.1.1"))

        # Should accept from different IP
        self.assertTrue(limiter.add_connection("192.168.1.3"))
        self.assertTrue(limiter.add_connection("192.168.1.4"))

        # Should reject when total limit reached
        self.assertFalse(limiter.add_connection("192.168.1.5"))

    def test_ban_manager(self):
        """Test ban management."""
        ban_manager = BanManager()

        # Ban an IP
        ban_manager.ban_ip("192.168.1.1", 3600)  # 1 hour
        self.assertTrue(ban_manager.is_ip_banned("192.168.1.1"))
        self.assertFalse(ban_manager.is_ip_banned("192.168.1.2"))

        # Ban a peer
        ban_manager.ban_peer("peer123", 1800)  # 30 minutes
        self.assertTrue(ban_manager.is_peer_banned("peer123"))
        self.assertFalse(ban_manager.is_peer_banned("peer456"))

    def test_message_validator(self):
        """Test message validation."""
        # Valid version message
        valid_version = {
            "type": "version",
            "network": "test-net",
            "services": 1,
            "height": 100,
            "port": 9333,
        }
        is_valid, error = MessageValidator.validate_message(valid_version)
        self.assertTrue(is_valid)
        self.assertEqual(error, "")

        # Invalid version message (missing field)
        invalid_version = {
            "type": "version",
            "network": "test-net",
            "services": 1,
            # Missing height and port
        }
        is_valid, error = MessageValidator.validate_message(invalid_version)
        self.assertFalse(is_valid)
        self.assertIn("Missing required field", error)

        # Invalid inventory message (too many items)
        invalid_inv = {
            "type": "inv",
            "inventory": [{"type": "tx", "hash": f"hash{i}"} for i in range(1001)]
        }
        is_valid, error = MessageValidator.validate_message(invalid_inv)
        self.assertFalse(is_valid)
        self.assertIn("Too many inventory items", error)

    def test_message_validator_rejects_invalid_lock_time(self):
        """Test lock_time validation in tx messages."""
        base_tx = {
            "type": "tx",
            "transaction": {
                "version": 1,
                "inputs": [{"prev_txid": "00" * 32, "prev_vout": 0}],
                "outputs": [{"value": 1, "script_pubkey": "51"}],
                "lock_time": 0,
            },
        }

        too_large = dict(base_tx)
        too_large["transaction"] = dict(base_tx["transaction"])
        too_large["transaction"]["lock_time"] = 0x1_0000_0000
        is_valid, error = MessageValidator.validate_message(too_large)
        self.assertFalse(is_valid)
        self.assertIn("lock_time", error)

        negative = dict(base_tx)
        negative["transaction"] = dict(base_tx["transaction"])
        negative["transaction"]["lock_time"] = -1
        is_valid, error = MessageValidator.validate_message(negative)
        self.assertFalse(is_valid)
        self.assertIn("lock_time", error)

    def test_p2p_security_integration(self):
        """Test integrated P2P security."""
        security = P2PSecurity()

        # Should accept initial connections
        self.assertTrue(security.can_accept_connection("192.168.1.1"))
        self.assertTrue(security.add_connection("192.168.1.1", "peer1"))

        # Should validate messages
        valid_msg = {"type": "ping", "nonce": 12345}
        should_accept, error = security.should_accept_message("peer1", valid_msg)
        self.assertTrue(should_accept)

        # Should reject invalid messages
        invalid_msg = {"type": "version"}  # Missing required fields
        should_accept, error = security.should_accept_message("peer1", invalid_msg)
        self.assertFalse(should_accept)


class BannedPeerSpamTests(unittest.IsolatedAsyncioTestCase):
    """Ensure banned peers are closed without log spam."""

    async def test_banned_peer_closes_once(self):
        class DummyWriter:
            def close(self):  # pragma: no cover - noop
                pass

            async def wait_closed(self):  # pragma: no cover - noop
                pass

        class DummyManager:
            def __init__(self, logger):
                self.security = P2PSecurity()
                self.log = logger
                self.closed_called = False

            def should_skip_rate_limit(self, *_args, **_kwargs):
                return False

            async def on_peer_closed(self, _peer):
                self.closed_called = True

        # Capture warnings on the peer logger to assert we only log once.
        peer_logger = logging.getLogger("baseline.peer[203.0.113.9:9333]")
        peer_logger.setLevel(logging.WARNING)
        peer_logger.propagate = False
        records: list[logging.LogRecord] = []

        class ListHandler(logging.Handler):
            def emit(self, record):  # pragma: no cover - simple sink
                records.append(record)

        handler = ListHandler(level=logging.WARNING)
        peer_logger.addHandler(handler)

        manager = DummyManager(peer_logger)
        manager.security.ban_manager.ban_peer("peer-ban", 60)
        peer = Peer(
            reader=None,
            writer=DummyWriter(),
            manager=manager,
            address=("203.0.113.9", 9333),
            outbound=False,
            peer_id="peer-ban",
        )

        await peer.handle_message({"type": "ping", "nonce": 1})
        await peer.handle_message({"type": "ping", "nonce": 2})

        self.assertTrue(peer.closed)
        self.assertTrue(manager.closed_called)
        warning_count = sum(1 for rec in records if rec.levelno == logging.WARNING)
        self.assertEqual(warning_count, 1)


class PeerDiscoveryTests(unittest.TestCase):
    """Test peer discovery features."""

    def test_peer_address_reliability_scoring(self):
        """Test peer address reliability scoring."""
        addr = PeerAddress("192.168.1.1", 9333, time.time(), source="manual")

        # Initial score should be neutral
        self.assertEqual(addr.reliability_score(), 0.5)

        # Record successes and failures
        addr.record_success()
        addr.record_success()
        addr.record_failure()

        # Score should be (2 + 2) / (2 + 1 + 4) = 4/7 ≈ 0.571
        self.assertAlmostEqual(addr.reliability_score(), 4/7, places=2)

    def test_address_book_persistence(self):
        """Test address book persistence."""
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "peers.json"

            # Create address book and add addresses
            book1 = AddressBook(path)
            addr1 = PeerAddress("192.168.1.1", 9333, time.time(), source="manual")
            addr2 = PeerAddress("192.168.1.2", 9333, time.time(), source="dns")
            book1.add_address(addr1)
            book1.add_address(addr2)
            book1.save()

            # Load in new instance
            book2 = AddressBook(path)
            addresses = book2.get_addresses(10)
            self.assertEqual(len(addresses), 2)

            # Check addresses are preserved
            hosts = {addr.host for addr in addresses}
            self.assertEqual(hosts, {"192.168.1.1", "192.168.1.2"})

    def test_dns_seeder_ip_filtering(self):
        """Test DNS seeder IP filtering."""
        seeder = DNSSeeder([])

        # Test routable IP detection
        self.assertTrue(seeder._is_routable_ip("8.8.8.8"))      # Public
        self.assertTrue(seeder._is_routable_ip("1.1.1.1"))      # Public
        self.assertFalse(seeder._is_routable_ip("127.0.0.1"))   # Localhost
        self.assertFalse(seeder._is_routable_ip("192.168.1.1")) # Private
        self.assertFalse(seeder._is_routable_ip("10.0.0.1"))    # Private
        self.assertFalse(seeder._is_routable_ip("172.16.0.1"))  # Private


class UpgradeTests(unittest.TestCase):
    """Test network upgrade mechanisms."""

    def setUp(self):
        """Set up test database."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test.db"
        self.state_db = StateDB(self.db_path)
        self.upgrade_manager = UpgradeManager(self.state_db)

    def tearDown(self):
        """Clean up test database."""
        self.state_db.close()
        self.temp_dir.cleanup()

    def test_upgrade_definition_bit_checking(self):
        """Test upgrade definition bit checking."""
        upgrade = UpgradeDefinition(
            name="test_upgrade",
            bit=5,
            start_time=int(time.time()),
            timeout=int(time.time()) + 3600,
            threshold=75,
            period=100,
            min_activation_height=0,
            description="Test upgrade"
        )

        # Test bit checking
        self.assertTrue(upgrade.is_signaling_bit_set(1 << 5))  # Bit 5 set
        self.assertFalse(upgrade.is_signaling_bit_set(1 << 4)) # Bit 4 set
        self.assertTrue(upgrade.is_signaling_bit_set((1 << 5) | (1 << 3))) # Multiple bits

    def test_version_bits_state_transitions(self):
        """Test version bits state transitions."""
        # Create a test upgrade
        current_time = int(time.time())
        upgrade = UpgradeDefinition(
            name="test_upgrade",
            bit=0,
            start_time=current_time - 100,  # Started
            timeout=current_time + 3600,    # Not timed out
            threshold=75,                   # 75% threshold
            period=100,                     # 100 block periods
            min_activation_height=0,
            description="Test upgrade"
        )

        # Add the test upgrade
        self.upgrade_manager.add_custom_upgrade(upgrade)

        # Should be in STARTED state
        state = self.upgrade_manager.version_tracker.get_upgrade_state(
            "test_upgrade", 50, current_time
        )
        self.assertEqual(state, UpgradeState.STARTED)

        # Test DEFINED state (before start time)
        state = self.upgrade_manager.version_tracker.get_upgrade_state(
            "test_upgrade", 50, current_time - 200
        )
        self.assertEqual(state, UpgradeState.DEFINED)

        # Test FAILED state (after timeout)
        state = self.upgrade_manager.version_tracker.get_upgrade_state(
            "test_upgrade", 50, current_time + 4000
        )
        self.assertEqual(state, UpgradeState.FAILED)

    def test_block_version_creation(self):
        """Test block version creation with signaling."""
        current_time = int(time.time())

        # Add a test upgrade that should be signaled
        upgrade = UpgradeDefinition(
            name="test_upgrade",
            bit=3,
            start_time=current_time - 100,
            timeout=current_time + 3600,
            threshold=75,
            period=100,
            min_activation_height=0,
            description="Test upgrade"
        )
        self.upgrade_manager.add_custom_upgrade(upgrade)

        # Create version for block
        version = self.upgrade_manager.create_block_version(50, current_time)

        # Should have bit 3 set (signaling for test_upgrade)
        self.assertTrue(version & (1 << 3))

    def test_upgrade_activation_tracking(self):
        """Test upgrade activation tracking."""
        # Record an upgrade activation
        self.state_db.set_upgrade_activation_height("test_feature", 1000)

        # Retrieve activation height
        height = self.state_db.get_upgrade_activation_height("test_feature")
        self.assertEqual(height, 1000)

        # Non-existent upgrade should return None
        height = self.state_db.get_upgrade_activation_height("nonexistent")
        self.assertIsNone(height)

    def test_backward_compatibility_validation(self):
        """Test backward compatibility validation."""
        compatibility = self.upgrade_manager.compatibility

        # Valid block version
        header = BlockHeader(
            version=1,
            prev_hash="0" * 64,
            merkle_root="0" * 64,
            timestamp=int(time.time()),
            bits=0x1d00ffff,
            nonce=0
        )

        is_valid, error = compatibility.validate_block_version(header)
        self.assertTrue(is_valid)
        self.assertEqual(error, "")

        # Invalid block version (too low)
        header.version = 0
        is_valid, error = compatibility.validate_block_version(header)
        self.assertFalse(is_valid)
        self.assertIn("must be at least 1", error)

    def test_custom_upgrade_addition(self):
        """Test adding custom upgrades."""
        custom_upgrade = UpgradeDefinition(
            name="custom_feature",
            bit=10,
            start_time=int(time.time()) + 1000,
            timeout=int(time.time()) + 10000,
            threshold=80,
            period=144,
            min_activation_height=5000,
            description="Custom feature upgrade"
        )

        # Should be able to add custom upgrade
        self.upgrade_manager.add_custom_upgrade(custom_upgrade)
        self.assertIn("custom_feature", self.upgrade_manager.version_tracker.upgrades)

        # Should reject duplicate upgrade
        with self.assertRaises(ValueError):
            self.upgrade_manager.add_custom_upgrade(custom_upgrade)

        # Should reject upgrade with used bit
        duplicate_bit_upgrade = UpgradeDefinition(
            name="another_feature",
            bit=10,  # Same bit as custom_feature
            start_time=int(time.time()) + 1000,
            timeout=int(time.time()) + 10000,
            threshold=80,
            period=144,
            min_activation_height=5000,
            description="Another feature"
        )

        with self.assertRaises(ValueError):
            self.upgrade_manager.add_custom_upgrade(duplicate_bit_upgrade)

    def test_upgrade_locks_in_and_activates(self):
        """Test upgrade signaling transitions from STARTED to LOCKED_IN to ACTIVE."""
        current_time = int(time.time())
        upgrade = UpgradeDefinition(
            name="feature_signal",
            bit=2,
            start_time=current_time - 100,
            timeout=current_time + 10_000,
            threshold=3,
            period=4,
            min_activation_height=0,
            description="Feature signaling test"
        )
        self.upgrade_manager.add_custom_upgrade(upgrade)

        def store_header(height: int, version: int) -> None:
            header = BlockHeader(
                version=version,
                prev_hash=f"{max(0, height - 1):064x}",
                merkle_root="00" * 32,
                timestamp=current_time,
                bits=0x1d00ffff,
                nonce=0,
            )
            header_record = HeaderData(
                hash=f"{height:064x}",
                prev_hash=header.prev_hash,
                height=height,
                bits=header.bits,
                nonce=header.nonce,
                timestamp=header.timestamp,
                merkle_root=header.merkle_root,
                chainwork=str(height + 1),
                version=header.version,
                status=0,
            )
            self.state_db.store_header(header_record)
            self.upgrade_manager.process_new_block(header, height)

        signal_version = 1 | (1 << upgrade.bit)
        for height in range(0, 4):
            version = signal_version if height < 3 else 1
            store_header(height, version)

        state = self.upgrade_manager.version_tracker.get_upgrade_state("feature_signal", 4, current_time)
        self.assertEqual(state, UpgradeState.LOCKED_IN)

        for height in range(4, 9):
            store_header(height, 1)

        state = self.upgrade_manager.version_tracker.get_upgrade_state("feature_signal", 8, current_time)
        self.assertEqual(state, UpgradeState.ACTIVE)
        activation_height = self.state_db.get_upgrade_activation_height("feature_signal")
        self.assertEqual(activation_height, 8)


if __name__ == "__main__":
    unittest.main()
