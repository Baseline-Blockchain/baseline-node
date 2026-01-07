"""
Sync orchestration helpers for P2PServer.
"""

from __future__ import annotations

import time
from collections import deque
from typing import Any

from . import protocol


class SyncManager:
    """Manage header/block sync state and pacing."""

    def __init__(self, server):
        self.server = server
        self.header_peer = None
        self.header_sync_active = False
        self._header_last_time = 0.0
        self._header_timeout = 30.0
        self.sync_peer = None
        self.sync_active = False
        self.sync_remote_height = 0
        self.sync_batch = 500
        self.sync_download_window = 128
        self.sync_header_batch = 2000
        best = self.server.chain.state_db.get_best_tip()
        self._block_last_height = best[1] if best else 0
        self._block_last_time = time.time()
        self._block_timeout = 60.0
        self._block_queue: deque[str] = deque()
        self._block_queue_set: set[str] = set()
        self._inflight_blocks: set[str] = set()
        self._sync_inv_requested = False
        self._header_peer_cooldowns: dict[str, float] = {}
        self._sync_peer_cooldowns: dict[str, float] = {}
        self._header_received: int = 0
        self.max_seen_remote_height: int = 0
        self._stale_height_slack = 2000
        self._last_high_seen: float = 0.0

    def _record_remote_height(self, remote_height: int) -> None:
        if remote_height > self.max_seen_remote_height:
            self.max_seen_remote_height = remote_height
            self._last_high_seen = time.time()

    def _best_connected_height(self) -> int:
        best = 0
        for peer in self.server.peers.values():
            if not peer.remote_version:
                continue
            try:
                height = int(peer.remote_version.get("height", 0))
            except Exception:
                continue
            if height > best:
                best = height
        return best

    def _is_peer_stale(self, remote_height: int, peer=None) -> bool:
        now = time.time()
        ceiling = self.max_seen_remote_height
        # Decay the ceiling after a minute without seeing a better peer, so we can fall back.
        if self._last_high_seen and now - self._last_high_seen > 60.0:
            ceiling = max(ceiling, self._best_connected_height(), self.server.best_height())
            self.max_seen_remote_height = ceiling
        if ceiling and remote_height + self._stale_height_slack < ceiling:
            self.server.log.debug(
                "Skipping sync with %s: remote height %s far below best seen %s",
                getattr(peer, "peer_id", "peer"),
                remote_height,
                ceiling,
            )
            return True
        return False

    # Header sync
    def maybe_start_header_sync(self, peer) -> None:
        if self.server._stop_event.is_set():
            return
        if self.header_sync_active or not peer.remote_version:
            return
        cooldown_until = self._header_peer_cooldowns.get(peer.peer_id)
        if cooldown_until and time.time() < cooldown_until:
            return
        self._header_peer_cooldowns.pop(peer.peer_id, None)
        remote_height = int(peer.remote_version.get("height", 0))
        self._record_remote_height(remote_height)
        if self._is_peer_stale(remote_height, peer):
            return
        local_height = self.server.best_height()
        if remote_height <= local_height:
            return
        self.header_peer = peer
        self.header_sync_active = True
        self._header_received = 0
        self.sync_remote_height = max(self.sync_remote_height, remote_height)
        self._header_last_time = time.time()
        self.server.log.info(
            "Starting header sync with %s remote_height=%s local_height=%s",
            peer.peer_id,
            remote_height,
            local_height,
        )
        self.server._schedule(self.send_getheaders(peer))

    def try_start_header_sync(self) -> None:
        if self.server._stop_event.is_set():
            return
        if self.header_sync_active:
            return
        peers = list(self.server.peers.values())
        import random

        random.shuffle(peers)
        for peer in peers:
            self.maybe_start_header_sync(peer)
            if self.header_sync_active:
                break

    async def send_getheaders(self, peer) -> None:
        if peer.closed:
            return
        locator = self.server._build_block_locator()
        # If we have no chain yet, add a zero-hash sentinel to encourage peers to start from genesis.
        if len(locator) == 1 and locator[0] == self.server.chain.genesis_hash:
            locator.append("00" * 32)
        await peer.send_message(protocol.getheaders_payload(locator))

    def complete_header_sync(self, peer) -> None:
        if self.server._stop_event.is_set():
            return
        self.header_sync_active = False
        self.header_peer = None
        if self._header_received == 0 and self.sync_remote_height > self.server.best_height():
            self.server.log.info(
                "Header sync with %s yielded no headers despite higher height; retrying with another peer",
                peer.peer_id,
            )
            self.try_start_header_sync()
            return
        if not peer.closed:
            self.start_block_sync(peer)
        if not self.sync_active:
            self.try_start_header_sync()   # or pick another for blocks
        if not self.sync_active:
            # No block sync needed; consider ourselves caught up for outbound expansion.
            self.server._allow_non_seed_outbound = True

    def handle_header_timeout(self) -> None:
        if self.server._stop_event.is_set():
            return
        now = time.time()
        if self.header_sync_active and now - self._header_last_time > self._header_timeout:
            self.server.log.warning("Header sync stalled; restarting")
            if self.header_peer:
                self._header_peer_cooldowns[self.header_peer.peer_id] = now + 60.0
            self.header_sync_active = False
            self.header_peer = None
            self._header_received = 0
            self.try_start_header_sync()

    def record_headers_received(self, count: int) -> None:
        self._header_received += count
        self._header_last_time = time.time()

    # Block sync
    def _reset_block_sync_state(self) -> None:
        self._block_queue.clear()
        self._block_queue_set.clear()
        self._inflight_blocks.clear()
        self._sync_inv_requested = False

    def _sync_backlog_size(self) -> int:
        return len(self._block_queue) + len(self._inflight_blocks)

    def enqueue_block_hashes(self, block_hashes: list[str]) -> int:
        added = 0
        for block_hash in block_hashes:
            if not isinstance(block_hash, str):
                continue
            if self.server.health.is_block_invalid(block_hash):
                continue
            if self.server.chain.block_store.has_block(block_hash):
                continue
            if block_hash in self._inflight_blocks or block_hash in self._block_queue_set:
                continue
            self._block_queue.append(block_hash)
            self._block_queue_set.add(block_hash)
            added += 1
        return added

    def _maybe_request_more_inventory(self, peer) -> None:
        if not self.sync_active or peer is not self.sync_peer or peer.closed:
            return
        if self._sync_inv_requested:
            return
        backlog = self._sync_backlog_size()
        target_backlog = max(self.sync_download_window * 2, self.sync_batch // 2)
        if backlog >= target_backlog:
            return
        local_height = self.server.best_height()
        if local_height + backlog >= self.sync_remote_height:
            return
        self.request_block_inventory(peer)

    async def _pump_block_downloads(self, peer) -> None:
        if not self.sync_active or peer is not self.sync_peer or peer.closed:
            return
        available = self.sync_download_window - len(self._inflight_blocks)
        if available <= 0:
            return
        batch = []
        while self._block_queue and available > 0:
            block_hash = self._block_queue.popleft()
            self._block_queue_set.discard(block_hash)
            if self.server.chain.block_store.has_block(block_hash):
                continue
            if block_hash in self._inflight_blocks:
                continue
            self._inflight_blocks.add(block_hash)
            batch.append({"type": "block", "hash": block_hash})
            available -= 1
        if batch:
            await peer.send_message(protocol.getdata_payload(batch))

    def _sync_cooldown_key(self, peer) -> str:
        host, port = self.server._canonical_remote_address(peer)
        return f"{host}:{port}"

    def start_block_sync(self, peer) -> None:
        if self.server._stop_event.is_set():
            return
        if self.sync_active or not peer.remote_version:
            return
        key = self._sync_cooldown_key(peer)
        cooldown_until = self._sync_peer_cooldowns.get(key)
        if cooldown_until and time.time() < cooldown_until:
            return
        self._sync_peer_cooldowns.pop(key, None)
        remote_height = int(peer.remote_version.get("height", 0))
        self._record_remote_height(remote_height)
        if self._is_peer_stale(remote_height, peer):
            return
        local_height = self.server.best_height()
        if remote_height <= local_height:
            return
        self.sync_peer = peer
        self.sync_active = True
        self.sync_remote_height = max(self.sync_remote_height, remote_height)
        self._reset_block_sync_state()
        self._block_last_height = local_height
        self._block_last_time = time.time()
        self.server.log.info(
            "Starting block download from %s remote_height=%s local_height=%s",
            peer.peer_id,
            remote_height,
            local_height,
        )
        self.request_block_inventory(peer)

    def on_block_connected(self, height: int) -> None:
        self._block_last_height = max(self._block_last_height, height)
        self._block_last_time = time.time()
        if self.sync_active and height >= self.sync_remote_height:
            self.server.log.info("Block download caught up height=%s", height)
            self.sync_active = False
            self.sync_peer = None
            self._reset_block_sync_state()
            self.server._allow_non_seed_outbound = True

    def rotate_sync_peer(self, reason: str, cooldown: float = 90.0) -> None:
        """Drop the current sync peer and try another source."""
        if self.server._stop_event.is_set():
            return
        if self.sync_peer:
            self.server.log.info("Rotating sync peer from %s: %s", self.sync_peer.peer_id, reason)
            key = self._sync_cooldown_key(self.sync_peer)
            self._sync_peer_cooldowns[key] = time.time() + cooldown
        self.sync_active = False
        self.sync_peer = None
        self._reset_block_sync_state()
        # Broaden outbound attempts after a failed sync source.
        self.server._allow_non_seed_outbound = True
        self.try_start_header_sync()

    def handle_block_timeout(self) -> None:
        if self.server._stop_event.is_set():
            return
        now = time.time()
        if self.sync_active and now - self._block_last_time > self._block_timeout:
            self.server.log.warning("Block sync stalled; restarting header sync")
            self.rotate_sync_peer("stall detected", cooldown=60.0)

    def request_block_inventory(self, peer) -> None:
        if self.server._stop_event.is_set():
            return
        if peer.closed:
            return
        if self.sync_active and self._sync_inv_requested:
            return
        if self.sync_active:
            self._sync_inv_requested = True
        self.server._schedule(self.server._send_getblocks(peer))

    def maybe_request_more_inventory(self, peer) -> None:
        if self.server._stop_event.is_set():
            return
        self._maybe_request_more_inventory(peer)

    async def pump_block_downloads(self, peer) -> None:
        if self.server._stop_event.is_set():
            return
        await self._pump_block_downloads(peer)
