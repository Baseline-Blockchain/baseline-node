"""
Test that BaselineNode wires its TimeManager into the global singleton used by
synchronized_time_int().
"""

import time
import unittest
from unittest.mock import patch

from baseline.config import NodeConfig
from baseline.node import BaselineNode
from baseline.time_sync import NTPResponse, TimeManager, get_time_manager, set_time_manager, synchronized_time_int


class TestNodeSetsGlobalTimeManager(unittest.TestCase):
    def tearDown(self) -> None:
        # Ensure global state doesn't leak into other tests
        set_time_manager(None)

    def test_node_sets_global_time_manager(self) -> None:
        # Arrange
        cfg = NodeConfig()
        cfg.ntp.enabled = True
        cfg.ntp.servers = ["test.ntp.org"]
        cfg.ntp.timeout = 0.1
        cfg.ntp.sync_interval = 9999.0  # irrelevant, we won't start the loop in this test

        node = BaselineNode(cfg)

        # Prevent background sync loop from being scheduled (no event loop needed).
        with patch.object(TimeManager, "start", autospec=True) as mock_start:
            # Act
            node._initialize_time_sync()

            # Assert: node creates and installs the global time manager
            self.assertIsNotNone(node.time_manager)
            self.assertIs(get_time_manager(), node.time_manager)
            mock_start.assert_called_once()

            # Make the manager "synced" with a known offset
            node.time_manager._update_offset(
                NTPResponse(
                    offset=7.0,
                    delay=0.01,
                    server="test.ntp.org",
                    timestamp=time.time(),
                )
            )

            # synchronized_time_int() must now reflect that offset
            sys_now = int(time.time())
            synced_now = synchronized_time_int()

            # allow a bit of slack for integer rounding and time passing
            self.assertAlmostEqual(synced_now, sys_now + 7, delta=3)


if __name__ == "__main__":
    unittest.main()
