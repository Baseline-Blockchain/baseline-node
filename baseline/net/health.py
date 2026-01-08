"""
Peer health and connection tracking helpers.
"""

from __future__ import annotations

import time


class PeerHealthManager:
    """Track per-peer health, invalid inventory, and address cooldowns."""

    def __init__(self, log):
        self.log = log
        self.invalid_blocks: dict[str, float] = {}
        self.bad_block_counts: dict[str, int] = {}
        self.missing_parent_counts: dict[str, int] = {}
        self.inv_flood_counts: dict[str, int] = {}
        self.inv_rate: dict[str, tuple[float, int]] = {}
        self.invalid_inv_counts: dict[str, int] = {}
        self.address_cooldowns: dict[tuple[str, int], float] = {}
        self.handshake_failures: dict[str, tuple[int, float]] = {}
        self.orphan_log_state: dict[str, tuple[float, int]] = {}

    def reset_peer(self, peer_id: str) -> None:
        self.bad_block_counts.pop(peer_id, None)
        self.missing_parent_counts.pop(peer_id, None)
        self.inv_flood_counts.pop(peer_id, None)
        self.inv_rate.pop(peer_id, None)
        self.invalid_inv_counts.pop(peer_id, None)
        self.orphan_log_state.pop(peer_id, None)

    def record_success(self, peer_id: str) -> None:
        """Clear strike counters after a successful block from a peer."""
        self.bad_block_counts.pop(peer_id, None)
        self.missing_parent_counts.pop(peer_id, None)
        self.inv_flood_counts.pop(peer_id, None)
        self.invalid_inv_counts.pop(peer_id, None)

    def cleanup(self) -> None:
        """Drop expired state to keep memory bounded."""
        now = time.time()
        expired_invalid = [h for h, exp in self.invalid_blocks.items() if exp < now]
        for h in expired_invalid:
            self.invalid_blocks.pop(h, None)
        cooled = [addr for addr, exp in self.address_cooldowns.items() if exp < now]
        for addr in cooled:
            self.address_cooldowns.pop(addr, None)

    def is_block_invalid(self, block_hash: str) -> bool:
        expiry = self.invalid_blocks.get(block_hash)
        if expiry is None:
            return False
        if expiry < time.time():
            self.invalid_blocks.pop(block_hash, None)
            return False
        return True

    def mark_block_invalid(self, block_hash: str | None, reason: str, *, ttl: float = 1800.0) -> None:
        if block_hash:
            self.invalid_blocks[block_hash] = time.time() + ttl
        self.log.debug("Marked block %s invalid: %s", block_hash, reason)

    def clear_invalid_block(self, block_hash: str | None) -> None:
        if block_hash:
            self.invalid_blocks.pop(block_hash, None)

    def increment_bad_block(self, peer_id: str, severity: int = 1) -> int:
        count = self.bad_block_counts.get(peer_id, 0) + max(1, severity)
        self.bad_block_counts[peer_id] = count
        return count

    def increment_missing_parent(self, peer_id: str) -> int:
        count = self.missing_parent_counts.get(peer_id, 0) + 1
        self.missing_parent_counts[peer_id] = count
        return count

    def log_orphan_event(self, peer_id: str, block_hash: str, prev_hash: str) -> None:
        """Reduce log spam for orphan storms by batching per-peer logs over short windows."""
        now = time.time()
        last, suppressed = self.orphan_log_state.get(peer_id, (0.0, 0))
        if now - last > 5.0:
            if suppressed:
                self.log.debug("Suppressed %d orphan logs from %s in last window", suppressed, peer_id)
            self.orphan_log_state[peer_id] = (now, 0)
            self.log.debug("Orphan block %s from %s (missing parent %s)", block_hash, peer_id, prev_hash)
        else:
            self.orphan_log_state[peer_id] = (last, suppressed + 1)

    def inv_rate_limited(self, peer_id: str, limit: int = 5, window: float = 10.0) -> bool:
        now = time.time()
        start, count = self.inv_rate.get(peer_id, (now, 0))
        if now - start > window:
            start, count = now, 0
        count += 1
        self.inv_rate[peer_id] = (start, count)
        return count > limit

    def record_invalid_inv(self, peer_id: str, block_hash: str, *, max_count: int = 15) -> bool:
        """Record advertisement of an invalid block. Returns True if peer should be disconnected."""
        cnt = self.invalid_inv_counts.get(peer_id, 0) + 1
        self.invalid_inv_counts[peer_id] = cnt
        if cnt == 1 or cnt % 5 == 0:
            self.log.debug("Peer %s advertising known invalid block %s (cnt=%d)", peer_id, block_hash, cnt)
        return cnt >= max_count

    def address_on_cooldown(self, key: tuple[str, int]) -> bool:
        expiry = self.address_cooldowns.get(key)
        return bool(expiry and expiry > time.time())

    def set_address_cooldown(self, key: tuple[str, int], seconds: float) -> None:
        self.address_cooldowns[key] = time.time() + seconds

    def record_handshake_failure(self, host: str, port: int, *, cooldown_after: int = 3, cooldown_seconds: float = 180.0) -> None:
        now = time.time()
        count, last = self.handshake_failures.get(host, (0, 0.0))
        if now - last > 300:
            count = 0
        count += 1
        self.handshake_failures[host] = (count, now)
        if count >= cooldown_after:
            self.set_address_cooldown((host, port), cooldown_seconds)
