"""
Shared peer address dataclass used by the networking stack.
"""

from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass
class PeerAddress:
    """Peer metadata tracked by discovery and the P2P server."""

    host: str
    port: int
    last_seen: float
    services: int = 1
    attempts: int = 0
    last_attempt: float = 0.0
    success_count: int = 0
    failure_count: int = 0
    source: str = "unknown"  # "seed", "dns", "peer", "manual"

    def key(self) -> tuple[str, int]:
        return self.host, self.port

    def is_stale(self, max_age: float = 86400 * 7) -> bool:
        """True if we have not seen the peer in max_age seconds."""
        # Treat zero as "unknown" so bootstrap files with last_seen=0 are dialed.
        if self.last_seen <= 0:
            return False
        return time.time() - self.last_seen > max_age

    def retry_delay(self, base: float = 30.0, cap: float = 3600.0) -> float:
        # backoff based on consecutive-ish failures; use failure_count as a proxy
        delay = base * (2 ** min(self.failure_count, 7))  # max 30*128=3840
        return min(delay, cap)

    def should_retry(self, min_retry_delay: float = 30.0) -> bool:
        if self.last_attempt == 0:
            return True
        delay = max(min_retry_delay, self.retry_delay())
        return time.time() - self.last_attempt > delay

    def record_attempt(self) -> None:
        self.attempts += 1
        self.last_attempt = time.time()

    def record_success(self) -> None:
        self.success_count += 1
        self.last_seen = time.time()

    def record_failure(self) -> None:
        self.failure_count += 1
        self.last_attempt = time.time()

    def reliability_score(self) -> float:
        # Beta(2,2) prior -> starts at 0.5, less jumpy
        return (self.success_count + 2) / (self.success_count + self.failure_count + 4)
