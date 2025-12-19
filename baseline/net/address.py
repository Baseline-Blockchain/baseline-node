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
        return time.time() - self.last_seen > max_age

    def should_retry(self, min_retry_delay: float = 30) -> bool:
        """True if enough time has passed since the last attempt."""
        if self.last_attempt == 0:
            return True
        return time.time() - self.last_attempt > min_retry_delay

    def record_attempt(self) -> None:
        self.attempts += 1
        self.last_attempt = time.time()

    def record_success(self) -> None:
        self.success_count += 1
        self.last_seen = time.time()

    def record_failure(self) -> None:
        self.failure_count += 1

    def reliability_score(self) -> float:
        total = self.success_count + self.failure_count
        if total == 0:
            return 0.5
        return self.success_count / total
