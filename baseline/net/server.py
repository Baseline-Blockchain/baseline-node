"""
Async P2P server coordinating peer connections.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import time
import ipaddress
from collections import deque
from dataclasses import asdict
from typing import Any

from ..config import NodeConfig
from ..core import difficulty
from ..core.block import Block, BlockHeader
from ..core.chain import Chain, ChainError
from ..core.tx import Transaction
from ..mempool import Mempool, MempoolError
from ..storage import BlockStoreError, HeaderData, StateDBError
from . import protocol
from .address import PeerAddress
from .discovery import PeerDiscovery
from .health import PeerHealthManager
from .peer import Peer
from .sync import SyncManager
from .security import P2PSecurity

MAINNET_NETWORK_ID = "baseline-mainnet-2025-12-28-r1"


class P2PServer:
    def __init__(self, config: NodeConfig, chain: Chain, mempool: Mempool):
        self.config = config
        self.chain = chain
        self.mempool = mempool
        self.log = logging.getLogger("baseline.p2p")
        self.host = config.network.host
        self.listen_port = config.network.port
        self.max_peers = config.network.max_peers
        self.target_outbound = config.network.target_outbound
        self.services = 1
        self.network_id = (
            MAINNET_NETWORK_ID
            if not self.config.mining.allow_consensus_overrides
            else f"baseline-dev-{self.chain.genesis_hash[:8]}"
        )
        self.handshake_timeout = config.network.handshake_timeout
        self.idle_timeout = config.network.idle_timeout

        # Enhanced security and discovery
        self.security = P2PSecurity()
        self.discovery = PeerDiscovery(
            data_dir=config.data_dir,
            dns_seeds=getattr(config.network, 'dns_seeds', []),
            manual_seeds=config.network.seeds
        )
        self.seeds = tuple(config.network.seeds)
        self.peers: dict[str, Peer] = {}
        self.peer_tasks: set[asyncio.Task] = set()
        self._stop_event = asyncio.Event()
        self.server: asyncio.AbstractServer | None = None
        self.loop: asyncio.AbstractEventLoop | None = None
        self._broadcast_sem = asyncio.Semaphore(500)  # tune
        self.known_addresses = self.discovery.address_book.addresses
        self._peer_seq = 0
        self._tasks: list[asyncio.Task] = []
        self.sync = SyncManager(self)
        self.bytes_sent = 0
        self.bytes_received = 0
        self._local_addresses: set[tuple[str, int]] = set()
        # If only one or zero seeds are configured, allow outbound discovery immediately.
        self._allow_non_seed_outbound = len(self.seeds) <= 1
        self._pending_outbound: set[tuple[str, int]] = set()
        self._missing_block_log: dict[str, float] = {}
        self._block_request_backoff: dict[str, float] = {}
        self.health = PeerHealthManager(self.log)
        # Legacy references used in tests
        self._bad_block_counts = self.health.bad_block_counts
        self._missing_parent_counts = self.health.missing_parent_counts
        self._inv_flood_counts = self.health.inv_flood_counts
        self._invalid_inv_counts = self.health.invalid_inv_counts
        self._seed_ports: set[int] = {self.listen_port}
        for seed in self.seeds:
            try:
                port = int(seed.rsplit(":", 1)[1])
                self._seed_ports.add(port)
            except Exception:
                continue
        self._init_local_addresses()
        self._load_known_addresses()
        self.mempool.register_listener(self._on_local_tx)

    # Compatibility shims for legacy sync/header attributes used in tests and callers.
    @property
    def sync_active(self) -> bool:
        return self.sync.sync_active

    @sync_active.setter
    def sync_active(self, value: bool) -> None:
        self.sync.sync_active = value

    @property
    def sync_peer(self):
        return self.sync.sync_peer

    @sync_peer.setter
    def sync_peer(self, value) -> None:
        self.sync.sync_peer = value

    @property
    def header_peer(self):
        return self.sync.header_peer

    @header_peer.setter
    def header_peer(self, value) -> None:
        self.sync.header_peer = value

    @property
    def header_sync_active(self) -> bool:
        return self.sync.header_sync_active

    @header_sync_active.setter
    def header_sync_active(self, value: bool) -> None:
        self.sync.header_sync_active = value

    @property
    def sync_remote_height(self) -> int:
        return self.sync.sync_remote_height

    @sync_remote_height.setter
    def sync_remote_height(self, value: int) -> None:
        self.sync.sync_remote_height = value

    @property
    def sync_batch(self) -> int:
        return self.sync.sync_batch

    @sync_batch.setter
    def sync_batch(self, value: int) -> None:
        self.sync.sync_batch = value

    @property
    def sync_download_window(self) -> int:
        return self.sync.sync_download_window

    @sync_download_window.setter
    def sync_download_window(self, value: int) -> None:
        self.sync.sync_download_window = value

    @property
    def sync_header_batch(self) -> int:
        return self.sync.sync_header_batch

    @sync_header_batch.setter
    def sync_header_batch(self, value: int) -> None:
        self.sync.sync_header_batch = value

    def _init_local_addresses(self) -> None:
        """Initialize the set of local addresses to prevent self-connection."""
        import socket
        # Add configured listen address
        self._local_addresses.add((self.host, self.listen_port))
        # Add common localhost variants
        for local_ip in ("127.0.0.1", "::1", "localhost"):
            self._local_addresses.add((local_ip, self.listen_port))
        # Try to detect local network interfaces
        try:
            hostname = socket.gethostname()
            for info in socket.getaddrinfo(hostname, None, socket.AF_UNSPEC, socket.SOCK_STREAM):
                ip = info[4][0]
                self._local_addresses.add((ip, self.listen_port))
        except (socket.gaierror, OSError):
            pass
        # Also try to get external IP by connecting to a public server
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.settimeout(1.0)
                s.connect(("8.8.8.8", 80))
                external_ip = s.getsockname()[0]
                self._local_addresses.add((external_ip, self.listen_port))
        except OSError:
            pass
        self.log.debug("Local addresses for self-connection prevention: %s", self._local_addresses)

    def is_local_address(self, host: str, port: int) -> bool:
        """Check if the given address is a local address (self-connection)."""
        return (host, port) in self._local_addresses

    def add_local_address(self, host: str, port: int) -> None:
        """Add an address to the local addresses set (e.g., known public IP)."""
        self._local_addresses.add((host, port))
        self.log.debug("Added local address: %s:%s", host, port)

    async def start(self) -> None:
        self.loop = asyncio.get_running_loop()
        self.server = await asyncio.start_server(self._handle_inbound, self.host, self.listen_port)
        self.log.info("P2P server listening on %s:%s", self.host, self.listen_port)
        self._stop_event.clear()
        self._tasks.append(asyncio.create_task(self._dialer_loop(), name="p2p-dialer"))
        self._tasks.append(asyncio.create_task(self._heartbeat_loop(), name="p2p-heartbeat"))
        self._tasks.append(asyncio.create_task(self._sync_watchdog_loop(), name="p2p-sync-watchdog"))
        self._tasks.append(asyncio.create_task(self._cleanup_loop(), name="p2p-cleanup"))

    async def stop(self) -> None:
        self._stop_event.set()

        # 1) Stop accepting NEW inbound connections immediately
        if self.server:
            self.server.close()
            with contextlib.suppress(Exception):
                await self.server.wait_closed()

        # 2) Cancel background loops
        for task in self._tasks:
            task.cancel()

        # 3) Cancel peer tasks (peer.run)
        for task in list(self.peer_tasks):
            task.cancel()

        # 4) Close all peers (idempotent)
        close_peers = [peer.close() for peer in list(self.peers.values())]

        # 5) Await everything, but don't deadlock shutdown
        async def _drain():
            await asyncio.gather(*close_peers, return_exceptions=True)
            await asyncio.gather(*self._tasks, return_exceptions=True)
            await asyncio.gather(*list(self.peer_tasks), return_exceptions=True)

        try:
            await asyncio.wait_for(_drain(), timeout=10.0)
        except TimeoutError:
            self.log.warning("Shutdown timed out; forcing exit with pending tasks")

        self._save_known_addresses()


    async def _handle_inbound(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if self._stop_event.is_set():
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            return

        peername = writer.get_extra_info("peername")
        if not peername:
            writer.close()
            return
        host, port = peername[:2]

        # Security check for inbound connections
        if not self.security.can_accept_connection(host):
            self.log.debug("Rejected connection from %s (security policy)", host)
            writer.close()
            return

        peer = self._build_peer(reader, writer, (host, port), outbound=False)

        # Add connection to security manager
        if not self.security.add_connection(host, peer.peer_id):
            self.log.debug("Rejected connection from %s (connection limit)", host)
            writer.close()
            return

        self._run_peer(peer)

    def _build_peer(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        address: tuple[str, int],
        *,
        outbound: bool,
    ) -> Peer:
        peer_id = f"P{self._peer_seq}"
        self._peer_seq += 1
        return Peer(
            reader=reader,
            writer=writer,
            manager=self,
            address=address,
            outbound=outbound,
            peer_id=peer_id,
        )

    def _canonical_remote_address(self, peer: Peer) -> tuple[str, int]:
        """Prefer the advertised listening port over the ephemeral socket port."""
        host, port = peer.address
        if peer.remote_version:
            try:
                advertised = int(peer.remote_version.get("port", port))
                if 1 <= advertised <= 65535:
                    port = advertised
            except (TypeError, ValueError):
                pass
        return host, port

    def _sync_cooldown_key(self, peer: Peer) -> str:
        host, port = self._canonical_remote_address(peer)
        return f"{host}:{port}"

    def _run_peer(self, peer: Peer) -> None:
        task = asyncio.create_task(peer.run(), name=f"peer-{peer.peer_id}")
        self.peer_tasks.add(task)

        def _cleanup(t: asyncio.Task) -> None:
            self.peer_tasks.discard(t)
            try:
                exc = t.exception()
            except asyncio.CancelledError:
                return
            except Exception:
                self.log.exception("Failed reading peer task exception")
                return

            if exc is not None:
                # keep it low-noise unless you’re debugging
                self.log.warning("Peer task %s ended with exception: %r", t.get_name(), exc, exc_info=False)

        task.add_done_callback(_cleanup)

    def _schedule(self, coro) -> None:
        if not self.loop:
            return
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None

        if running is self.loop:
            asyncio.create_task(coro)
        else:
            asyncio.run_coroutine_threadsafe(coro, self.loop)

    def request_block(self, block_hash: str) -> bool:
        """
        Ask connected peers for a specific block (broadcast with backoff).
        Returns True if at least one request was enqueued, False otherwise.
        """
        if not self.loop:
            return False
        if self._is_block_invalid(block_hash):
            return False
        now = time.time()
        last = self._block_request_backoff.get(block_hash, 0.0)
        if now - last < 10.0:
            return False
        self._block_request_backoff[block_hash] = now
        payload = protocol.getdata_payload([{"type": "block", "hash": block_hash}])
        any_sent = False
        for peer in list(self.peers.values()):
            if peer.closed:
                continue
            self._schedule(peer.send_message(payload))
            any_sent = True
        return any_sent

    def record_bytes_sent(self, count: int) -> None:
        self.bytes_sent += count

    def record_bytes_received(self, count: int) -> None:
        self.bytes_received += count

    def should_skip_rate_limit(self, peer: Peer, msg_type: str | None) -> bool:
        """Return True when rate limiting should be skipped for this message."""
        if msg_type is None:
            return False
        if msg_type == "headers":
            return self.sync.header_sync_active and self.sync.header_peer is peer
        if msg_type in {"block", "inv"}:
            return self.sync.sync_active and self.sync.sync_peer is peer
        return False

    async def _dialer_loop(self) -> None:
        try:
            while not self._stop_event.is_set():
                try:
                    await self._maintain_outbound()
                except Exception:
                    self.log.exception("Dialer loop error")
                await asyncio.sleep(5)
        except asyncio.CancelledError:
            pass

    async def _maintain_outbound(self) -> None:
        needed = self.target_outbound - self.outbound_count()
        if needed <= 0:
            return
        seeds_only = not self._allow_non_seed_outbound
        diversity_target = max(2, self.target_outbound // 2 or 1)
        # If we are too concentrated on a single network, expand beyond seeds immediately.
        if seeds_only and len(self._active_buckets(outbound_only=True)) < diversity_target:
            seeds_only = False
            self._allow_non_seed_outbound = True

        request_count = max(needed * 2, diversity_target)
        candidates = await self.discovery.discover_peers(request_count, seeds_only=seeds_only)
        filtered = [
            (host, port)
            for host, port in candidates
            if not self.security.ban_manager.is_ip_banned(host)
        ]
        diversified = self._diversify_candidates(filtered, needed)
        for host, port in diversified:
            await self._connect_outbound(host, port)

    async def _connect_outbound(self, host: str, port: int) -> None:
        if self._stop_event.is_set():
            return
        targets: list[tuple[str, int]] = [(host, port)]
        if port != self.listen_port:
            derived = (host, self.listen_port)
            if derived not in targets:
                targets.append(derived)

        for tgt_host, tgt_port in targets:
            if self._stop_event.is_set():
                return
            if self.security.ban_manager.is_ip_banned(tgt_host):
                self.log.debug("Skipping banned outbound target %s:%s", tgt_host, tgt_port)
                continue
            if self._should_skip_outbound_port(tgt_port):
                continue
            key = (tgt_host, tgt_port)
            if self.health.address_on_cooldown(key):
                continue
            if key in self._pending_outbound:
                continue
            if key in self.active_addresses():
                # Allow an outbound even if we already have only inbound connections
                existing = [peer for peer in self.peers.values() if peer.address == key]
                if any(peer.outbound for peer in existing):
                    continue
            # Prevent self-connection
            if self.is_local_address(tgt_host, tgt_port):
                self.log.debug("Skipping self-connection to %s:%s", tgt_host, tgt_port)
                continue

            self.discovery.record_connection_attempt(tgt_host, tgt_port)
            self._pending_outbound.add(key)

            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(tgt_host, tgt_port), timeout=5
                )
            except (TimeoutError, OSError) as exc:
                self.log.debug("Failed to dial %s:%s: %s", tgt_host, tgt_port, exc)
                self.discovery.record_connection_failure(tgt_host, tgt_port)
                self._pending_outbound.discard(key)
                continue

            peer = self._build_peer(reader, writer, (tgt_host, tgt_port), outbound=True)

            if not self.security.add_connection(tgt_host, peer.peer_id):
                self.log.debug("Rejected outbound connection to %s:%s (connection limit)", tgt_host, tgt_port)
                writer.close()
                self.discovery.record_connection_failure(tgt_host, tgt_port)
                self._pending_outbound.discard(key)
                continue

            self.discovery.record_connection_success(tgt_host, tgt_port)
            self._run_peer(peer)
            self._pending_outbound.discard(key)
            return

    def outbound_count(self) -> int:
        return sum(1 for peer in self.peers.values() if peer.outbound)

    def active_addresses(self) -> set[tuple[str, int]]:
        return {peer.address for peer in self.peers.values()}

    def _network_bucket(self, host: str) -> str | None:
        """Group hosts into coarse network buckets to reduce eclipse risk."""
        try:
            addr = ipaddress.ip_address(host)
        except ValueError:
            return None
        if isinstance(addr, ipaddress.IPv4Address):
            return str(ipaddress.ip_network(f"{addr}/16", strict=False))
        return str(ipaddress.ip_network(f"{addr}/32", strict=False))

    def _active_buckets(self, outbound_only: bool = False) -> set[str]:
        """Return network buckets currently connected or pending."""
        buckets: set[str] = set()
        for addr in self.active_addresses().union(self._pending_outbound):
            bucket = self._network_bucket(addr[0])
            if bucket:
                if outbound_only and not any(
                    peer.outbound and peer.address == addr for peer in self.peers.values()
                ):
                    continue
                buckets.add(bucket)
        return buckets

    def _diversify_candidates(self, candidates: list[tuple[str, int]], target: int) -> list[tuple[str, int]]:
        """
        Prefer candidates from new network buckets to avoid concentrating on one ASN/IP range.
        """
        selected: list[tuple[str, int]] = []
        used_buckets = self._active_buckets()
        overflow: list[tuple[str, int]] = []
        for host, port in candidates:
            bucket = self._network_bucket(host)
            if bucket and bucket not in used_buckets:
                selected.append((host, port))
                used_buckets.add(bucket)
            else:
                overflow.append((host, port))
            if len(selected) >= target:
                break
        for host, port in overflow:
            if len(selected) >= target:
                break
            if (host, port) not in selected:
                selected.append((host, port))
        return selected

    @staticmethod
    def _is_ephemeral_port(port: int) -> bool:
        return port >= 32768

    def _should_skip_outbound_port(self, port: int) -> bool:
        """
        Avoid dialing obvious ephemeral ports learned from inbound peers.
        Still allow the peer's advertised/listen port.
        """
        if port == self.listen_port:
            return False
        if port in self._seed_ports:
            return False
        # Skip anything outside our known/listen/seed ports to avoid junk dial targets.
        return True

    def _pick_addresses(self, count: int) -> list[tuple[str, int]]:
        available = [addr for addr in self.known_addresses.values()]
        if not available:
            available = [PeerAddress(host=host, port=port, last_seen=0) for host, port in self._seed_hosts()]
        random.shuffle(available)
        selected: list[tuple[str, int]] = []
        for addr in available:
            if addr.key() in self.active_addresses():
                continue
            selected.append(addr.key())
            if len(selected) >= count:
                break
        return selected

    def _seed_hosts(self) -> list[tuple[str, int]]:
        hosts = []
        for seed in self.seeds:
            host, port = seed.split(":")
            hosts.append((host.strip(), int(port)))
        return hosts

    async def _heartbeat_loop(self) -> None:
        try:
            while not self._stop_event.is_set():
                now = time.time()
                for peer in list(self.peers.values()):
                    if now - peer.last_message > self.idle_timeout:
                        self.log.info("Peer %s timed out", peer.peer_id)
                        await peer.close()
                        continue
                    if peer.ping_nonce is None:
                        await peer.ping()
                await asyncio.sleep(15)
        except asyncio.CancelledError:
            pass

    async def _sync_watchdog_loop(self) -> None:
        try:
            while not self._stop_event.is_set():
                now = time.time()
                self.sync.handle_header_timeout()
                self.sync.handle_block_timeout()
                await asyncio.sleep(10)
        except asyncio.CancelledError:
            pass

    async def on_peer_ready(self, peer: Peer) -> None:
        if self._stop_event.is_set():
            await peer.close()
            return
        if len(self.peers) >= self.max_peers:
            self.log.warning("Max peers reached, disconnecting %s", peer.peer_id)
            await peer.close()
            return
        self.peers[peer.peer_id] = peer
        host, port = self._canonical_remote_address(peer)
        services = 1
        if peer.remote_version:
            try:
                services = int(peer.remote_version.get("services", services))
            except (TypeError, ValueError):
                services = 1
        addr = PeerAddress(
            host=host,
            port=port,
            last_seen=time.time(),
            services=services,
            source="outbound" if peer.outbound else "inbound",
        )

        # Preserve existing attempt/health counters when refreshing an address.
        existing = self.discovery.address_book.addresses.get(addr.key())
        if existing:
            addr.attempts = existing.attempts
            addr.last_attempt = existing.last_attempt
            addr.success_count = existing.success_count
            addr.failure_count = existing.failure_count

        self.discovery.address_book.add_address(addr)
        if peer.outbound:
            self.discovery.address_book.record_success(addr.host, addr.port)

        self.log.info("Peer %s connected (addr=%s:%s outbound=%s)", peer.peer_id, host, port, peer.outbound)
        await self._send_addr(peer)
        await self._request_addr(peer)
        self._maybe_start_header_sync(peer)
        await self._send_mempool_inventory(peer)

    async def _send_mempool_inventory(self, peer: Peer, limit: int = 200) -> None:
        if not self.mempool:
            return
        with self.mempool.lock:
            txids = list(self.mempool.entries.keys())[:limit]
        if not txids:
            return
        items = [{"type": "tx", "hash": txid} for txid in txids]
        await peer.send_message(protocol.inv_payload(items))

    async def on_peer_closed(self, peer: Peer) -> None:
        self.peers.pop(peer.peer_id, None)
        with contextlib.suppress(Exception):
            self.security.remove_connection(peer.address[0], peer.peer_id)
        self.health.reset_peer(peer.peer_id)
        host, port = self._canonical_remote_address(peer)
        if not peer.handshake_complete:
            # Cooldown addresses that repeatedly close during handshake.
            self.discovery.record_connection_failure(host, port)
            self.health.record_handshake_failure(host, port)
        self.log.info("Peer %s disconnected", peer.peer_id)
        if self.sync.sync_peer is peer:
            self.sync.sync_peer = None
            self.sync.sync_active = False
            self.sync._reset_block_sync_state()
        if self.sync.header_peer is peer:
            self.sync.header_peer = None
            self.sync.header_sync_active = False
        self.sync.try_start_header_sync()

    def best_height(self) -> int:
        try:
            tip = self.chain.state_db.get_best_tip()
        except StateDBError:
            self.log.debug("StateDB unavailable during best_height lookup")
            return 0
        return tip[1] if tip else 0

    async def _send_addr(self, peer: Peer) -> None:
        peers = list(self.known_addresses.values())[:32]
        await peer.send_message(protocol.addr_payload([asdict(addr) for addr in peers]))

    async def _request_addr(self, peer: Peer) -> None:
        await peer.send_message(protocol.getaddr_payload())

    async def handle_inv(self, peer: Peer, message: dict[str, Any]) -> None:
        items = message.get("items", [])
        tx_requests: list[dict[str, str]] = []
        block_requests: list[dict[str, str]] = []
        block_hashes: list[str] = []
        for entry in items:
            obj_type = entry.get("type")
            obj_hash = entry.get("hash")
            if obj_hash is None:
                continue
            if obj_type == "block" and self._is_block_invalid(obj_hash):
                exceeded = self.health.record_invalid_inv(peer.peer_id, obj_hash, max_count=50)
                if exceeded and peer not in {self.sync.sync_peer, self.sync.header_peer}:
                    host, port = self._canonical_remote_address(peer)
                    self.health.set_address_cooldown((host, port), 180.0)
                    self.log.info("Disconnecting %s for repeated invalid block inventory", peer.peer_id)
                    await peer.close()
                    return
                continue
            peer.known_inventory.add(obj_hash)
            if obj_type == "tx":
                if not self.mempool.contains(obj_hash):
                    tx_requests.append(entry)
            elif obj_type == "block":
                block_hashes.append(obj_hash)
                if not self.sync.sync_active or peer is not self.sync.sync_peer:
                    if (
                        not self.chain.block_store.has_block(obj_hash)
                        and obj_hash not in self.sync._inflight_blocks
                        and obj_hash not in self.sync._block_queue_set
                    ):
                        block_requests.append(entry)
        self.log.debug(
            "Received inv from %s items=%d tx_requests=%d block_hashes=%d",
            peer.peer_id,
            len(items),
            len(tx_requests),
            len(block_hashes),
        )
        if (not self.sync.sync_active or peer is not self.sync.sync_peer) and len(block_hashes) > 200:
            if self._inv_rate_limited(peer.peer_id):
                if self._inv_flood_counts.get(peer.peer_id, 0) % 10 == 0:
                    self.log.debug("Dropping repeated large inv from %s (rate limited)", peer.peer_id)
                return
        if (not self.sync.sync_active or peer is not self.sync.sync_peer) and len(block_hashes) > 200:
            floods = self._inv_flood_counts.get(peer.peer_id, 0) + 1
            self._inv_flood_counts[peer.peer_id] = floods
            if floods == 1 or floods % 5 == 0:
                self.log.debug("Truncating large inv from %s (count=%s)", peer.peer_id, floods)
            block_hashes = block_hashes[:200]
            block_requests = block_requests[:200]
        if tx_requests:
            await peer.send_message(protocol.getdata_payload(tx_requests))
        sync_inv = self.sync.sync_active and peer is self.sync.sync_peer
        if sync_inv:
            self.sync._sync_inv_requested = False
        if sync_inv and block_hashes:
            added = self._enqueue_block_hashes(block_hashes)
            if added:
                self.log.debug(
                    "Queued %d blocks from %s (queue=%d inflight=%d)",
                    added,
                    peer.peer_id,
                    len(self.sync._block_queue),
                    len(self.sync._inflight_blocks),
                )
            await self._pump_block_downloads(peer)
        if sync_inv:
            self._maybe_request_more_inventory(peer)
        elif block_requests:
            await peer.send_message(protocol.getdata_payload(block_requests))

    async def handle_getdata(self, peer: Peer, message: dict[str, Any]) -> None:
        items = message.get("items", [])
        for entry in items:
            obj_type = entry.get("type")
            obj_hash = entry.get("hash")
            if obj_type == "tx":
                tx = self.mempool.get(obj_hash)
                if tx:
                    await peer.send_message(protocol.tx_payload(obj_hash, tx.serialize().hex()))
            elif obj_type == "block":
                try:
                    raw = self.chain.block_store.get_block(obj_hash)
                except BlockStoreError:
                    continue
                await peer.send_message(protocol.block_payload(obj_hash, raw.hex()))

    async def handle_tx(self, peer: Peer, message: dict[str, Any]) -> None:
        txid = message.get("txid")
        raw = message.get("raw")
        if not isinstance(raw, str) or not isinstance(txid, str):
            return
        try:
            tx_bytes = bytes.fromhex(raw)
            tx = Transaction.parse(tx_bytes)
            if tx.txid() != txid:
                self.log.debug("TXID mismatch from peer %s", peer.peer_id)
                return
        except Exception as exc:
            self.log.warning("Invalid tx from %s: %s", peer.peer_id, exc)
            return
        if self.mempool.contains(txid):
            return
        try:
            result = await asyncio.to_thread(self.mempool.accept_transaction, tx, peer_id=peer.peer_id)
        except MempoolError as exc:
            self.log.debug("Rejected tx %s: %s", txid, exc)
            return
        if result.get("status") == "accepted":
            peer.known_inventory.add(txid)
            await self.broadcast_inv("tx", txid, exclude={peer.peer_id})

    async def handle_block(self, peer: Peer, message: dict[str, Any]) -> None:
        raw = message.get("raw")
        requested_hash = message.get("hash")
        requested_hash = requested_hash if isinstance(requested_hash, str) else None
        block_hash = None
        if not isinstance(raw, str):
            if requested_hash:
                self.sync._inflight_blocks.discard(requested_hash)
            return
        try:
            block_bytes = bytes.fromhex(raw)
            block = Block.parse(block_bytes)
            block_hash = block.block_hash()
            if requested_hash and requested_hash != block_hash:
                self.log.debug(
                    "Block hash mismatch from %s expected=%s got=%s",
                    peer.peer_id,
                    requested_hash,
                    block_hash,
                )
                self.sync._inflight_blocks.discard(requested_hash)
                self.sync._inflight_blocks.discard(block_hash)
                return
            if block_hash and self._is_block_invalid(block_hash):
                if requested_hash:
                    self.sync._inflight_blocks.discard(requested_hash)
                self.sync._inflight_blocks.discard(block_hash)
                self.log.debug("Dropping known invalid block %s from %s", block_hash, peer.peer_id)
                return
        except Exception as exc:
            if requested_hash:
                self.sync._inflight_blocks.discard(requested_hash)
            self.log.warning("Invalid block payload: %s", exc)
            self._mark_block_invalid(requested_hash or block_hash, str(exc), peer, severity=1)
            return
        result: dict[str, Any] | None = None
        try:
            result = await asyncio.to_thread(self.chain.add_block, block, block_bytes)
        except BlockStoreError as exc:
            msg = str(exc)
            if "already stored" in msg:
                self.log.debug("Duplicate block %s from %s; already stored", block.block_hash(), peer.peer_id)
                result = {"status": "duplicate", "hash": block.block_hash()}
            else:
                now = time.time()
                last = self._missing_block_log.get(block_hash)
                if not last or now - last > 30:
                    self.log.warning("Block store miss for %s from %s: %s", block.block_hash(), peer.peer_id, exc)
                    self._missing_block_log[block_hash] = now
                # Ask for the missing parent to heal the gap.
                missing = [{"type": "block", "hash": block.header.prev_hash}]
                await peer.send_message(protocol.getdata_payload(missing))
                self.request_block(block.header.prev_hash)
        except ChainError as exc:
            msg = str(exc)
            if "Unknown parent block" in msg:
                # Treat as an out-of-order block; keep it as an orphan and fetch the parent.
                orphan_hash = block.block_hash()
                self._log_orphan_event(peer, orphan_hash, block.header.prev_hash)
                self.chain.fork_manager.detector.orphan_manager.add_orphan(block, peer.peer_id)
                await self._request_missing_parent(peer, block.header.prev_hash)
                self.request_block(block.header.prev_hash)
                count = self.health.increment_missing_parent(peer.peer_id)
                if self.sync.sync_active and peer is self.sync.sync_peer and count >= 3:
                    self._rotate_sync_peer("missing parents from sync peer", cooldown=120.0)
                # Kick sync to get ordered blocks instead of a long orphan chain.
                if not self.sync.header_sync_active:
                    self._maybe_start_header_sync(peer)
                if not self.sync.sync_active:
                    self._request_block_inventory(peer)
            elif "Parent block data missing" in msg:
                # We have the header but not the raw parent block; request it instead of banning.
                parent_hash = block.header.prev_hash
                await self._request_missing_parent(peer, parent_hash)
                self.request_block(parent_hash)
                count = self.health.increment_missing_parent(peer.peer_id)
                if self.sync.sync_active and peer is self.sync.sync_peer and count >= 3:
                    self._rotate_sync_peer("parent data missing from sync peer", cooldown=120.0)
                return
            else:
                penalize = "Missing referenced output" not in msg
                self._mark_block_invalid(block_hash or requested_hash, msg, peer, severity=2, penalize=penalize)
                if self.sync.sync_active and peer is self.sync.sync_peer:
                    self._rotate_sync_peer("invalid block during sync", cooldown=180.0)
            return
        finally:
            if requested_hash:
                self.sync._inflight_blocks.discard(requested_hash)
            if block_hash:
                self.sync._inflight_blocks.discard(block_hash)
        if result is None:
            if self.sync.sync_active and peer is self.sync.sync_peer:
                await self._pump_block_downloads(peer)
                self._maybe_request_more_inventory(peer)
            return
        status = result.get("status")
        self.health.record_success(peer.peer_id)
        peer.known_inventory.add(block_hash)
        if status in {"connected", "reorganized"}:
            # Successful block from this peer: forgive prior bad-block strikes.
            self._bad_block_counts.pop(peer.peer_id, None)
            if self.mempool:
                try:
                    self.mempool.remove_confirmed(block.transactions)
                except Exception as exc:
                    self.log.debug("Failed to prune mempool for block %s: %s", block_hash, exc)
            height = result.get("height")
            if isinstance(height, int):
                self._on_block_connected(height)
            await self.broadcast_inv("block", block_hash, exclude={peer.peer_id})
            await self._process_orphans(block_hash, peer)
        if self.sync.sync_active and peer is self.sync.sync_peer:
            await self._pump_block_downloads(peer)
            self._maybe_request_more_inventory(peer)

    async def _request_missing_parent(self, peer: Peer, parent_hash: str) -> None:
        """Ask the peer for a missing parent block when we see an orphan."""
        if peer.closed:
            return
        payload = protocol.getdata_payload([{"type": "block", "hash": parent_hash}])
        await peer.send_message(payload)

    async def _process_orphans(self, parent_hash: str, source_peer: Peer | None = None) -> None:
        """Try to connect any orphans that now have their parent."""
        orphans = self.chain.fork_manager.detector.process_orphans(parent_hash)
        if not orphans:
            return
        self.log.debug("Processing %d orphan blocks linking to %s", len(orphans), parent_hash)
        for orphan in orphans:
            orphan_hash = orphan.block_hash()
            try:
                result = await asyncio.to_thread(self.chain.add_block, orphan)
            except ChainError as exc:
                msg = str(exc)
                if "Unknown parent block" in msg:
                    # Still missing a parent further back; keep it as an orphan and try to fetch again.
                    self.chain.fork_manager.detector.orphan_manager.add_orphan(
                        orphan, getattr(source_peer, "peer_id", "unknown")
                    )
                    if source_peer:
                        await self._request_missing_parent(source_peer, orphan.header.prev_hash)
                else:
                    self.log.debug("Orphan block %s rejected: %s", orphan_hash, exc)
                continue

            status = result.get("status")
            if status in {"connected", "reorganized"}:
                if self.mempool:
                    try:
                        self.mempool.remove_confirmed(orphan.transactions)
                    except Exception as exc:
                        self.log.debug("Failed to prune mempool for orphan %s: %s", orphan_hash, exc)
                height = result.get("height")
                if isinstance(height, int):
                    self._on_block_connected(height)
                exclude = {source_peer.peer_id} if source_peer else None
                await self.broadcast_inv("block", orphan_hash, exclude=exclude)

    async def handle_addr(self, _peer: Peer, _message: dict[str, Any]) -> None:
        return

    async def handle_getheaders(self, peer: Peer, message: dict[str, Any]) -> None:
        locator = message.get("locator")
        if not isinstance(locator, list):
            return
        best = self.chain.state_db.get_best_tip()
        if not best:
            return
        # Ensure our main chain view is coherent before serving headers.
        try:
            if self.chain.state_db.has_main_chain_gap():
                repaired = self.chain.state_db.rebuild_main_headers_from_blocks(self.chain.block_store)
                if repaired:
                    self.log.warning("Rebuilt %s missing headers before responding to getheaders", repaired)
            # If continuity is broken, re-anchor from the highest chainwork tip.
            highest = self.chain.state_db.get_highest_main_header()
            if highest is None or highest.height < best[1]:
                path_len, final_height = self.chain.state_db.reanchor_main_chain()
                if path_len:
                    self.log.warning(
                        "Re-anchored main chain before getheaders; path=%s final_height=%s",
                        path_len,
                        final_height,
                    )
        except Exception as exc:  # noqa: BLE001
            self.log.warning("Failed to repair headers before getheaders: %s", exc)
        start_height = self._find_locator_height(locator)
        stop_hash = message.get("stop")
        headers: list[dict[str, int]] = []
        height = start_height + 1
        prev_hash: str | None = None
        if start_height >= 0:
            prev = self.chain.state_db.get_main_header_at_height(start_height)
            prev_hash = prev.hash if prev else None
        while height <= best[1] and len(headers) < self.sync_header_batch:
            header = self.chain.state_db.get_main_header_at_height(height)
            if header is None:
                # Try once to re-anchor and retry the fetch at this height.
                repaired = self.chain.state_db.reanchor_main_chain()
                if repaired[0] > 0:
                    self.log.warning("Re-anchored main chain during getheaders; path=%s height=%s", *repaired)
                    header = self.chain.state_db.get_main_header_at_height(height)
                if header is None:
                    break
            if prev_hash and header.prev_hash != prev_hash:
                self.log.warning(
                    "Detected header continuity break at height %s (prev=%s actual=%s); re-anchoring",
                    height,
                    prev_hash,
                    header.prev_hash,
                )
                repaired = self.chain.state_db.reanchor_main_chain()
                if repaired[0] > 0:
                    header = self.chain.state_db.get_main_header_at_height(height)
                if header is None or (prev_hash and header.prev_hash != prev_hash):
                    break
            headers.append(self._header_to_dict(header))
            prev_hash = header.hash
            if stop_hash and header.hash == stop_hash:
                break
            height += 1
        await peer.send_message(protocol.headers_payload(headers))

    async def handle_getblocks(self, peer: Peer, message: dict[str, Any]) -> None:
        locator = message.get("locator")
        if not isinstance(locator, list):
            return
        best = self.chain.state_db.get_best_tip()
        if not best:
            return
        start_height = self._find_locator_height(locator)
        stop_hash = message.get("stop")
        inv_items = []
        height = start_height + 1
        while height <= best[1] and len(inv_items) < self.sync_batch:
            header = self.chain.state_db.get_main_header_at_height(height)
            if header is None:
                break
            inv_items.append({"type": "block", "hash": header.hash})
            if stop_hash and header.hash == stop_hash:
                break
            height += 1
        if inv_items:
            await peer.send_message(protocol.inv_payload(inv_items))

    async def handle_headers(self, peer: Peer, message: dict[str, Any]) -> None:
        headers = message.get("headers")
        if not isinstance(headers, list):
            return
        if peer is not self.sync.header_peer or not self.sync.header_sync_active:
            return
        if not headers:
            if self.sync.sync_remote_height > self.best_height():
                self.log.warning(
                    "Peer %s advertised height %s but returned no headers; rotating",
                    peer.peer_id,
                    self.sync.sync_remote_height,
                )
                self.sync.rotate_sync_peer("no headers from peer", cooldown=120.0)
            else:
                self._complete_header_sync(peer)
            return
        last_height = None
        for entry in headers:
            try:
                header = BlockHeader(
                    version=int(entry["version"]),
                    prev_hash=str(entry["prev_hash"]),
                    merkle_root=str(entry["merkle_root"]),
                    timestamp=int(entry["timestamp"]),
                    bits=int(entry["bits"]),
                    nonce=int(entry["nonce"]),
                )
            except (KeyError, ValueError, TypeError):
                self.log.warning("Malformed header received")
                return
            parent = self.chain.state_db.get_header(header.prev_hash)
            if parent is None:
                self.log.warning(
                    "Header parent unknown from %s: header=%s prev=%s (genesis=%s)",
                    peer.peer_id,
                    header.hash(),
                    header.prev_hash,
                    self.chain.genesis_hash,
                )
                # If we are still effectively at genesis, clear any cached headers
                # so we can restart cleanly rather than getting stuck on orphans.
                best = self.chain.state_db.get_best_tip()
                if not best or best[1] == 0:
                    self.log.info("Resetting cached headers to genesis and restarting header sync")
                    with contextlib.suppress(Exception):
                        self.chain.state_db.reset_headers_to_genesis(self.chain.genesis_hash)
                    self.sync.header_sync_active = False
                    self.sync.header_peer = None
                    self.sync.try_start_header_sync()
                return
            height = parent.height + 1
            expected_bits = self.chain._expected_bits(height, parent)
            if header.bits != expected_bits:
                self.log.warning(
                    "Header bits mismatch at height %s from %s: got=%s expected=%s prev=%s",
                    height,
                    peer.peer_id,
                    header.bits,
                    expected_bits,
                    header.prev_hash,
                )
                return
            if not difficulty.check_proof_of_work(header.hash(), header.bits):
                self.log.debug("Invalid POW for header %s", header.hash())
                return
            header_hash = header.hash()
            chainwork_int = int(parent.chainwork) + difficulty.block_work(header.bits)
            header_record = HeaderData(
                hash=header_hash,
                prev_hash=header.prev_hash,
                height=height,
                bits=header.bits,
                nonce=header.nonce,
                timestamp=header.timestamp,
                merkle_root=header.merkle_root,
                chainwork=str(chainwork_int),
                version=header.version,
                status=1,
            )
            self.chain.state_db.store_header(header_record)
            last_height = height
        self.sync.record_headers_received(len(headers))
        if last_height is not None:
            self.sync.sync_remote_height = max(self.sync.sync_remote_height, last_height)
        if len(headers) < self.sync.sync_header_batch:
            self._complete_header_sync(peer)
        else:
            asyncio.create_task(self._send_getheaders(peer))

    async def _bounded_send(self, peer: Peer, payload: dict[str, Any]) -> None:
        async with self._broadcast_sem:
            await peer.send_message(payload)
            
    async def broadcast_inv(self, obj_type: str, obj_hash: str, exclude: set[str] | None = None) -> None:
        payload = protocol.inv_payload([{"type": obj_type, "hash": obj_hash}])
        for peer in list(self.peers.values()):
            if exclude and peer.peer_id in exclude:
                continue
            if obj_hash in peer.known_inventory:
                continue
            peer.known_inventory.add(obj_hash)
            asyncio.create_task(self._bounded_send(peer, payload))


    def _fire_and_forget(self, coro) -> None:
        task = asyncio.create_task(coro)

        def _ignore(result: asyncio.Task) -> None:
            with contextlib.suppress(Exception):
                result.result()

        task.add_done_callback(_ignore)

    def _on_local_tx(self, tx: Transaction) -> None:
        if not self.loop:
            return
        asyncio.run_coroutine_threadsafe(self.broadcast_inv("tx", tx.txid()), self.loop)

    def announce_block(self, block_hash: str) -> None:
        if not self.loop:
            return
        asyncio.run_coroutine_threadsafe(self.broadcast_inv("block", block_hash), self.loop)

    def _penalize_peer(self, peer: Peer, reason: str, *, severity: int = 1, ban_seconds: int = 600) -> None:
        """
        Penalize a peer for providing bad data. After a few strikes, temporarily ban and disconnect.
        """
        count = self.health.increment_bad_block(peer.peer_id, severity)
        if count >= 3:
            self.log.warning("Banning peer %s for repeated bad data: %s", peer.peer_id, reason)
            with contextlib.suppress(Exception):
                self.security.ban_manager.ban_peer(peer.peer_id, ban_seconds)
                self.security.ban_manager.ban_ip(peer.address[0], ban_seconds)
            asyncio.create_task(peer.close())

    def _is_block_invalid(self, block_hash: str) -> bool:
        if not block_hash:
            return False
        # If we already have the block (or its header) stored, do not treat it as invalid.
        try:
            if self.chain.block_store.has_block(block_hash):
                return False
            if self.chain.state_db.get_header(block_hash):
                return False
        except Exception:
            pass
        return self.health.is_block_invalid(block_hash)

    def _mark_block_invalid(
        self,
        block_hash: str | None,
        reason: str,
        peer: Peer | None = None,
        *,
        severity: int = 1,
        penalize: bool = True,
    ) -> None:
        self.health.mark_block_invalid(block_hash, reason)
        if "Missing referenced output" in reason:
            # Do not persist a missing-output failure; allow re-requests as parents arrive.
            self.health.clear_invalid_block(block_hash)
            penalize = False
        if penalize and peer:
            self._penalize_peer(peer, reason, severity=severity)

    def _log_orphan_event(self, peer: Peer, block_hash: str, prev_hash: str) -> None:
        self.health.log_orphan_event(peer.peer_id, block_hash, prev_hash)

    def _inv_rate_limited(self, peer_id: str, limit: int = 5, window: float = 10.0) -> bool:
        """Return True if peer sent too many large invs in a short window."""
        return self.health.inv_rate_limited(peer_id, limit=limit, window=window)

    def _load_known_addresses(self) -> None:
        legacy_path = self.config.data_dir / "peers" / "known_peers.json"
        if not legacy_path.exists():
            return
        try:
            data = json.loads(legacy_path.read_text("utf-8"))
        except Exception:
            self.log.warning("Failed to load legacy peer store")
            return
        added = 0
        for entry in data:
            try:
                addr = PeerAddress(
                    host=entry["host"],
                    port=int(entry["port"]),
                    last_seen=float(entry.get("last_seen", 0)),
                    source="legacy",
                )
            except (KeyError, ValueError, TypeError):
                continue
            if self.discovery.address_book.add_address(addr):
                added += 1
        if added:
            self.log.info("Imported %d legacy peer addresses", added)

    def _save_known_addresses(self) -> None:
        self.discovery.save_address_book()

    def _build_block_locator(self, limit: int = 32) -> list[str]:
        tip = self.chain.state_db.get_best_tip()
        if not tip:
            return [self.chain.genesis_hash]
        locator: list[str] = []
        current_hash = tip[0]
        while current_hash and len(locator) < limit:
            locator.append(current_hash)
            header = self.chain.state_db.get_header(current_hash)
            if header is None or header.prev_hash is None:
                break
            current_hash = header.prev_hash
        if self.chain.genesis_hash not in locator:
            locator.append(self.chain.genesis_hash)
        return locator

    def _header_to_dict(self, header) -> dict[str, int]:
        return {
            "version": int(getattr(header, "version", header.version if hasattr(header, "version") else 1)),
            "prev_hash": header.prev_hash,
            "merkle_root": header.merkle_root,
            "timestamp": int(header.timestamp),
            "bits": int(header.bits),
            "nonce": int(header.nonce),
        }

    def _find_locator_height(self, locator: list[str]) -> int:
        for block_hash in locator:
            header = self.chain.state_db.get_header(block_hash)
            if header and header.status == 0:
                return header.height
        return -1

    def _maybe_start_header_sync(self, peer: Peer) -> None:
        self.sync.maybe_start_header_sync(peer)

    def _try_start_header_sync(self) -> None:
        self.sync.try_start_header_sync()

    def _rotate_sync_peer(self, reason: str, cooldown: float = 90.0) -> None:
        self.sync.rotate_sync_peer(reason, cooldown=cooldown)

    def _sync_backlog_size(self) -> int:
        return self.sync._sync_backlog_size()

    def _enqueue_block_hashes(self, block_hashes: list[str]) -> int:
        return self.sync.enqueue_block_hashes(block_hashes)

    def _request_block_inventory(self, peer: Peer) -> None:
        self.sync.request_block_inventory(peer)

    def _maybe_request_more_inventory(self, peer: Peer) -> None:
        self.sync.maybe_request_more_inventory(peer)

    async def _pump_block_downloads(self, peer: Peer) -> None:
        await self.sync.pump_block_downloads(peer)

    async def _send_getblocks(self, peer: Peer) -> None:
        if peer.closed:
            return
        locator = self._build_block_locator()
        await peer.send_message(protocol.getblocks_payload(locator))

    async def _send_getheaders(self, peer: Peer) -> None:
        if peer.closed:
            return
        locator = self._build_block_locator()
        # If we have no chain yet, add a zero-hash sentinel to encourage peers to start from genesis.
        if len(locator) == 1 and locator[0] == self.chain.genesis_hash:
            locator.append("00" * 32)
        await peer.send_message(protocol.getheaders_payload(locator))

    def _on_block_connected(self, height: int) -> None:
        self.sync.on_block_connected(height)

    def _complete_header_sync(self, peer: Peer) -> None:
        self.sync.complete_header_sync(peer)

    def _start_block_sync(self, peer: Peer) -> None:
        self.sync.start_block_sync(peer)

    async def _cleanup_loop(self) -> None:
        """Periodic cleanup of expired data."""
        try:
            while not self._stop_event.is_set():
                try:
                    # Clean up security-related data
                    self.security.cleanup()

                    # Clean up peer discovery data
                    self.discovery.cleanup()

                    # Clean up health-related caches
                    self.health.cleanup()

                    self.log.debug("Completed periodic cleanup")
                except Exception as exc:
                    self.log.error("Error during cleanup: %s", exc)

                # Run cleanup every 5 minutes
                await asyncio.sleep(300)
        except asyncio.CancelledError:
            pass
