"""
NTP time synchronization for Baseline blockchain nodes.

This module provides NTP client functionality to synchronize node clocks
with network time servers, reducing clock drift issues in distributed
blockchain networks.
"""

from __future__ import annotations

import asyncio
import logging
import socket
import struct
import time
from dataclasses import dataclass
from functools import lru_cache


class NTPError(Exception):
    """Raised when NTP synchronization fails."""


@dataclass
class NTPResponse:
    """NTP server response data."""
    offset: float  # Time offset from local clock (seconds)
    delay: float   # Round-trip delay (seconds)
    server: str    # NTP server address
    timestamp: float  # Local timestamp when response received


class NTPClient:
    """Simple NTP client for time synchronization."""

    # NTP packet format constants
    NTP_PACKET_SIZE = 48
    NTP_EPOCH_OFFSET = 2208988800  # Seconds between 1900 and 1970

    def __init__(self, servers: list[str] | None = None, timeout: float = 5.0):
        """Initialize NTP client.

        Args:
            servers: List of NTP server addresses. Defaults to common public servers.
            timeout: Network timeout for NTP requests in seconds.
        """
        self.servers = servers or [
            "pool.ntp.org",
            "time.nist.gov",
            "time.google.com",
            "time.cloudflare.com"
        ]
        self.timeout = timeout
        self.log = logging.getLogger("baseline.ntp")

    async def query_server(self, server: str) -> NTPResponse:
        """Query a single NTP server for time information.

        Args:
            server: NTP server hostname or IP address.

        Returns:
            NTPResponse with time offset and delay information.

        Raises:
            NTPError: If the query fails or times out.
        """
        try:
            # Create NTP request packet
            packet = bytearray(self.NTP_PACKET_SIZE)
            packet[0] = 0x1B  # LI=0, VN=3, Mode=3 (client)

            # Record local time before sending
            local_send_time = time.time()

            # Send UDP packet to NTP server
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(self.timeout)

            try:
                sock.sendto(packet, (server, 123))
                response, _ = sock.recvfrom(self.NTP_PACKET_SIZE)
                local_recv_time = time.time()
            finally:
                sock.close()

            if len(response) < self.NTP_PACKET_SIZE:
                raise NTPError(f"Invalid NTP response size from {server}")

            # Parse NTP response
            # Transmit timestamp is at bytes 40-47 (8 bytes, big-endian)
            transmit_timestamp_raw = struct.unpack("!Q", response[40:48])[0]

            # Convert NTP timestamp to Unix timestamp
            transmit_timestamp = (transmit_timestamp_raw >> 32) - self.NTP_EPOCH_OFFSET
            transmit_fraction = (transmit_timestamp_raw & 0xFFFFFFFF) / (2**32)
            server_time = transmit_timestamp + transmit_fraction

            # Calculate offset and delay
            # Offset = ((server_time - local_send_time) + (server_time - local_recv_time)) / 2
            # Delay = (local_recv_time - local_send_time) - (server_time - server_time)
            # Simplified: offset ≈ server_time - local_time, delay = round_trip_time
            local_time = (local_send_time + local_recv_time) / 2
            offset = server_time - local_time
            delay = local_recv_time - local_send_time

            return NTPResponse(
                offset=offset,
                delay=delay,
                server=server,
                timestamp=local_recv_time
            )

        except TimeoutError as exc:
            raise NTPError(f"Timeout querying NTP server {server}") from exc
        except socket.gaierror as exc:
            raise NTPError(f"DNS resolution failed for {server}: {exc}") from exc
        except Exception as exc:
            raise NTPError(f"Failed to query NTP server {server}: {exc}") from exc

    async def sync_time(self, max_servers: int = 3) -> NTPResponse:
        """Synchronize time with multiple NTP servers.

        Args:
            max_servers: Maximum number of servers to query.

        Returns:
            Best NTPResponse based on lowest delay.

        Raises:
            NTPError: If all servers fail or no valid responses received.
        """
        responses: list[NTPResponse] = []
        errors: list[str] = []

        # Query multiple servers concurrently
        tasks = []
        for server in self.servers[:max_servers]:
            task = asyncio.create_task(self.query_server(server))
            tasks.append((server, task))

        # Collect responses
        for server, task in tasks:
            try:
                response = await task
                responses.append(response)
                self.log.debug(f"NTP response from {server}: offset={response.offset:.3f}s, delay={response.delay:.3f}s")
            except NTPError as e:
                errors.append(str(e))
                self.log.warning(f"NTP query failed: {e}")

        if not responses:
            raise NTPError(f"All NTP servers failed: {'; '.join(errors)}")

        # Return response with lowest delay (most accurate)
        best_response = min(responses, key=lambda r: r.delay)
        self.log.info(f"NTP sync successful: offset={best_response.offset:.3f}s from {best_response.server}")

        return best_response


class TimeManager:
    """Manages synchronized time for the blockchain node."""

    def __init__(self, ntp_client: NTPClient | None = None, sync_interval: float = 300.0):
        """Initialize time manager.

        Args:
            ntp_client: NTP client instance. Creates default if None.
            sync_interval: How often to sync with NTP servers (seconds).
        """
        self.ntp_client = ntp_client or NTPClient()
        self.sync_interval = sync_interval
        self.log = logging.getLogger("baseline.time")

        # Time synchronization state
        self._offset = 0.0  # Current time offset from system clock
        self._last_sync = 0.0  # When we last synced
        self._sync_task: asyncio.Task | None = None
        self._enabled = True

        # Clock drift monitoring
        self._drift_history: list[tuple[float, float]] = []  # (timestamp, offset)
        self.max_drift_history = 100

    def start(self) -> None:
        """Start automatic NTP synchronization."""
        if self._sync_task is None or self._sync_task.done():
            self._sync_task = asyncio.create_task(self._sync_loop())
            self.log.info("NTP synchronization started")

    def stop(self) -> None:
        """Stop automatic NTP synchronization."""
        if self._sync_task and not self._sync_task.done():
            self._sync_task.cancel()
            self.log.info("NTP synchronization stopped")

    def enable(self) -> None:
        """Enable NTP time synchronization."""
        self._enabled = True
        self.log.info("NTP synchronization enabled")

    def disable(self) -> None:
        """Disable NTP time synchronization (use system time)."""
        self._enabled = False
        self._offset = 0.0
        self.log.info("NTP synchronization disabled")

    def time(self) -> float:
        """Get current synchronized time.

        Returns:
            Current time as Unix timestamp, adjusted for NTP offset.
        """
        if not self._enabled:
            return time.time()
        return time.time() + self._offset

    def time_int(self) -> int:
        """Get current synchronized time as integer.

        Returns:
            Current time as Unix timestamp integer, adjusted for NTP offset.
        """
        return int(self.time())

    def get_offset(self) -> float:
        """Get current time offset from system clock.

        Returns:
            Time offset in seconds (positive means system clock is slow).
        """
        return self._offset

    def get_drift_rate(self) -> float | None:
        """Calculate clock drift rate from recent history.

        Returns:
            Drift rate in seconds per second, or None if insufficient data.
        """
        if len(self._drift_history) < 2:
            return None

        # Calculate drift rate from first and last measurements
        first_time, first_offset = self._drift_history[0]
        last_time, last_offset = self._drift_history[-1]

        time_span = last_time - first_time
        if time_span <= 0:
            return None

        offset_change = last_offset - first_offset
        return offset_change / time_span

    def is_synchronized(self, max_age: float = 600.0) -> bool:
        """Check if time is recently synchronized.

        Args:
            max_age: Maximum age of last sync in seconds.

        Returns:
            True if synchronized within max_age seconds.
        """
        if not self._enabled:
            return False
        return (time.time() - self._last_sync) <= max_age

    def get_sync_status(self) -> dict[str, any]:
        """Get detailed synchronization status.

        Returns:
            Dictionary with sync status information.
        """
        return {
            "enabled": self._enabled,
            "offset": self._offset,
            "last_sync": self._last_sync,
            "time_since_sync": time.time() - self._last_sync if self._last_sync > 0 else None,
            "synchronized": self.is_synchronized(),
            "drift_rate": self.get_drift_rate(),
            "system_time": time.time(),
            "sync_time": self.time(),
        }

    async def force_sync(self) -> bool:
        """Force immediate NTP synchronization.

        Returns:
            True if synchronization successful, False otherwise.
        """
        if not self._enabled:
            self.log.warning("Cannot sync: NTP synchronization is disabled")
            return False

        try:
            response = await self.ntp_client.sync_time()
            self._update_offset(response)
            return True
        except NTPError as e:
            self.log.error(f"Force sync failed: {e}")
            return False

    def _update_offset(self, response: NTPResponse) -> None:
        """Update time offset from NTP response."""
        old_offset = self._offset
        self._offset = response.offset
        self._last_sync = response.timestamp

        # Record drift history
        self._drift_history.append((response.timestamp, response.offset))
        if len(self._drift_history) > self.max_drift_history:
            self._drift_history.pop(0)

        # Log significant changes
        offset_change = abs(self._offset - old_offset)
        if offset_change > 1.0:  # More than 1 second change
            self.log.warning(f"Large time offset change: {offset_change:.3f}s")

        # Warn about large offsets
        if abs(self._offset) > 60.0:  # More than 1 minute off
            self.log.warning(f"Large time offset detected: {self._offset:.3f}s")

    async def _sync_loop(self) -> None:
        """Background task for periodic NTP synchronization."""
        while True:
            try:
                if self._enabled:
                    response = await self.ntp_client.sync_time()
                    self._update_offset(response)

                await asyncio.sleep(self.sync_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.log.error(f"NTP sync loop error: {e}")
                await asyncio.sleep(min(self.sync_interval, 60.0))  # Retry sooner on error


@lru_cache(maxsize=1)
def get_time_manager() -> TimeManager:
    """Get the global time manager instance."""
    return TimeManager()


def synchronized_time() -> float:
    """Get current synchronized time (convenience function)."""
    return get_time_manager().time()


def synchronized_time_int() -> int:
    """Get current synchronized time as integer (convenience function)."""
    return get_time_manager().time_int()
