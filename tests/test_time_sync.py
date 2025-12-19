"""
Tests for NTP time synchronization functionality.
"""

import asyncio
import socket
import time
import unittest
from unittest.mock import MagicMock, patch

from baseline.config import NTPConfig, ConfigError
from baseline.time_sync import NTPClient, NTPResponse, NTPError, TimeManager, synchronized_time_int


class TestNTPConfig(unittest.TestCase):
    """Test NTP configuration."""

    def test_default_config(self):
        """Test default NTP configuration values."""
        config = NTPConfig()
        self.assertTrue(config.enabled)
        self.assertIn("pool.ntp.org", config.servers)
        self.assertEqual(config.sync_interval, 300.0)
        self.assertEqual(config.timeout, 5.0)
        self.assertEqual(config.max_offset_warning, 60.0)

    def test_config_validation(self):
        """Test NTP configuration validation."""
        config = NTPConfig()
        config.validate()  # Should not raise

        # Test invalid sync interval
        config.sync_interval = -1
        with self.assertRaises(ConfigError):
            config.validate()

        # Test invalid timeout
        config.sync_interval = 300
        config.timeout = 0
        with self.assertRaises(ConfigError):
            config.validate()


class TestNTPClient(unittest.IsolatedAsyncioTestCase):
    """Test NTP client functionality."""

    def setUp(self):
        self.client = NTPClient(servers=["test.ntp.org"], timeout=1.0)

    @patch("socket.socket")
    async def test_query_server_success(self, mock_socket):
        """Test successful NTP server query."""
        # Mock socket behavior
        mock_sock = MagicMock()
        mock_socket.return_value = mock_sock
        
        # Mock NTP response (simplified)
        ntp_time = time.time() + 2208988800  # Convert to NTP epoch
        response_data = bytearray(48)
        # Set transmit timestamp (bytes 40-47)
        struct_time = int(ntp_time)
        frac_time = int((ntp_time - struct_time) * (2**32))
        response_data[40:44] = struct_time.to_bytes(4, 'big')
        response_data[44:48] = frac_time.to_bytes(4, 'big')
        
        mock_sock.recvfrom.return_value = (bytes(response_data), ("test.ntp.org", 123))
        
        response = await self.client.query_server("test.ntp.org")
        
        self.assertIsInstance(response, NTPResponse)
        self.assertEqual(response.server, "test.ntp.org")
        self.assertIsInstance(response.offset, float)
        self.assertIsInstance(response.delay, float)

    async def test_query_server_timeout(self):
        """Test NTP server query timeout."""
        with patch("socket.socket") as mock_socket:
            mock_sock = MagicMock()
            mock_socket.return_value = mock_sock
            mock_sock.recvfrom.side_effect = socket.timeout()
            
            with self.assertRaises(NTPError):
                await self.client.query_server("nonexistent.ntp.org")

    async def test_sync_time_multiple_servers(self):
        """Test time synchronization with multiple servers."""
        client = NTPClient(servers=["server1.ntp.org", "server2.ntp.org"], timeout=1.0)
        
        # Mock successful responses from both servers
        responses = [
            NTPResponse(offset=0.1, delay=0.05, server="server1.ntp.org", timestamp=time.time()),
            NTPResponse(offset=0.15, delay=0.06, server="server2.ntp.org", timestamp=time.time())
        ]
        
        with patch.object(client, "query_server", side_effect=responses):
            result = await client.sync_time(max_servers=2)
            
            self.assertIsInstance(result, NTPResponse)
            # Should return the response with lowest delay
            self.assertEqual(result.server, "server1.ntp.org")


class TestTimeManager(unittest.TestCase):
    """Test TimeManager functionality."""

    def setUp(self):
        self.mock_client = MagicMock(spec=NTPClient)
        self.manager = TimeManager(ntp_client=self.mock_client, sync_interval=1.0)

    def test_initialization(self):
        """Test TimeManager initialization."""
        self.assertTrue(self.manager._enabled)  # Enabled by default
        self.assertIsNone(self.manager._sync_task)  # Not started yet
        self.assertEqual(self.manager.sync_interval, 1.0)

    def test_enable_disable(self):
        """Test enabling and disabling time synchronization."""
        self.manager.enable()
        self.assertTrue(self.manager._enabled)
        
        self.manager.disable()
        self.assertFalse(self.manager._enabled)

    def test_time_without_sync(self):
        """Test time methods when not synchronized."""
        # Should return system time when not synchronized
        sys_time = time.time()
        manager_time = self.manager.time()
        
        self.assertAlmostEqual(manager_time, sys_time, delta=0.1)

    def test_time_with_offset(self):
        """Test time methods with offset applied."""
        # Simulate a successful sync with offset
        response = NTPResponse(
            offset=0.5, 
            delay=0.1, 
            server="test.ntp.org", 
            timestamp=time.time()
        )
        self.manager._update_offset(response)
        
        # Time should now include the offset
        sys_time = time.time()
        manager_time = self.manager.time()
        
        self.assertAlmostEqual(manager_time, sys_time + 0.5, delta=0.1)

    def test_get_sync_status(self):
        """Test sync status reporting."""
        status = self.manager.get_sync_status()
        
        self.assertIn('synchronized', status)
        self.assertIn('offset', status)
        self.assertIn('last_sync', status)
        self.assertFalse(status['synchronized'])  # Not synced initially

    def test_drift_rate_calculation(self):
        """Test clock drift rate calculation."""
        # Simulate two sync points
        now = time.time()
        
        response1 = NTPResponse(offset=0.1, delay=0.05, server="test.ntp.org", timestamp=now)
        self.manager._update_offset(response1)
        
        # Simulate time passing and another sync
        response2 = NTPResponse(offset=0.2, delay=0.05, server="test.ntp.org", timestamp=now + 100)
        self.manager._update_offset(response2)
        
        drift_rate = self.manager.get_drift_rate()
        self.assertIsNotNone(drift_rate)
        # Drift rate should be approximately (0.2 - 0.1) / 100 = 0.001
        self.assertAlmostEqual(drift_rate, 0.001, delta=0.0001)


class TestSynchronizedTimeFunctions(unittest.TestCase):
    """Test global synchronized time functions."""

    def test_synchronized_time_int(self):
        """Test synchronized_time_int function."""
        # Should return an integer timestamp
        timestamp = synchronized_time_int()
        self.assertIsInstance(timestamp, int)
        
        # Should be close to current time
        current_time = int(time.time())
        self.assertAlmostEqual(timestamp, current_time, delta=2)


if __name__ == "__main__":
    unittest.main()
