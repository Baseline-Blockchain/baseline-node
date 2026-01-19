"""
Async P2P server coordinating peer connections.

This version avoids blocking the event loop on:
- SQLite (StateDB) reads/writes
- disk IO (BlockStore reads)
- heavier/optional repair work

Key changes:
- All StateDB calls inside async handlers go through asyncio.to_thread()
- BlockStore.get_block() goes through asyncio.to_thread()
- best_height() is cached (no DB in handshake paths)
- getheaders/getblocks build responses using batched DB reads (range queries) off-thread
"""

from __future__ import annotations

import asyncio
import contextlib
import ipaddress
import json
import logging
import time
from collections.abc import Callable
from dataclasses import asdict
from typing import Any

from ..config import NodeConfig
from ..core import difficulty
from ..core.block import Block, BlockHeader
from ..core.chain import Chain, ChainError
from ..core.tx import Transaction
from ..mempool import Mempool, MempoolError
from ..storage import BlockStoreError, HeaderData
from . import protocol
from .address import PeerAddress
from .discovery import PeerDiscovery
from .health import PeerHealthManager
from .peer import Peer
from .security import P2PSecurity
from .sync import SyncManager

MAINNET_NETWORK_ID = "baseline-mainnet-2025-12-28-r1"
SNAPSHOT_TIMEOUT = 2.0


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
            dns_seeds=getattr(config.network, "dns_seeds", []),
            manual_seeds=config.network.seeds,
        )
        self.seeds = tuple(config.network.seeds)
        self.peers: dict[str, Peer] = {}
        self.peer_tasks: set[asyncio.Task] = set()
        self._stop_event = asyncio.Event()
        self.server: asyncio.AbstractServer | None = None
        self.loop: asyncio.AbstractEventLoop | None = None

        self.write_timeout = 5.0
        self.known_addresses = self.discovery.address_book.addresses
        self._peer_seq = 0
        self._tasks: list[asyncio.Task] = []
        self.sync = SyncManager(self)
        self.bytes_sent = 0
        self.bytes_received = 0
        self._local_addresses: set[tuple[str, int]] = set()

        # Cached chain tip (avoid DB reads in hot paths like handshake)
        self._best_height_cache: int = 0
        self._best_hash_cache: str | None = None

        # If only one or zero seeds are configured, allow outbound discovery immediately.
        self._allow_non_seed_outbound = len(self.seeds) <= 1
        self._pending_outbound: set[tuple[str, int]] = set()
        self._missing_block_log: dict[str, float] = {}
        self._block_request_backoff: dict[str, float] = {}
        self.health = PeerHealthManager(self.log)
        self._block_apply_lock = asyncio.Lock()

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

    # -------------------------------------------------------------------------
    # Thread offload helpers (avoid blocking the event loop)
    # -------------------------------------------------------------------------

    async def _to_thread(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        return await asyncio.to_thread(fn, *args, **kwargs)

    async def _db(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        # StateDB is thread-safe for readers; its writes are already serialized internally.
        return await asyncio.to_thread(fn, *args, **kwargs)

    async def _refresh_best_tip_cache(self) -> None:
        """Refresh cached tip values from the DB (off-thread)."""
        try:
            tip = await self._db(self.chain.state_db.get_best_tip)
        except Exception:
            return
        if tip:
            self._best_hash_cache, self._best_height_cache = tip[0], int(tip[1])

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

    # -------------------------------------------------------------------------

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

        # Initialize cached tip without blocking the loop.
        await self._refresh_best_tip_cache()

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
        async def _drain() -> None:
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

    def _call_on_loop(self, fn: Callable[[], Any], default: Any) -> Any:
        loop = self.loop
        if not loop:
            try:
                return fn()
            except Exception:
                return default
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is loop:
            return fn()

        async def _runner() -> Any:
            return fn()

        try:
            future = asyncio.run_coroutine_threadsafe(_runner(), loop)
            return future.result(timeout=SNAPSHOT_TIMEOUT)
        except Exception:
            self.log.debug("Failed to capture P2P snapshot", exc_info=True)
            return default

    def snapshot_peers(self) -> list[dict[str, Any]]:
        def _capture() -> list[dict[str, Any]]:
            peers: list[dict[str, Any]] = []
            for peer in self.peers.values():
                peers.append(
                    {
                        "peer_id": peer.peer_id,
                        "address": peer.address,
                        "outbound": peer.outbound,
                        "last_send": peer.last_send,
                        "last_message": peer.last_message,
                        "bytes_sent": peer.bytes_sent,
                        "bytes_received": peer.bytes_received,
                        "latency": peer.latency,
                        "remote_version": peer.remote_version or {},
                    }
                )
            return peers

        return self._call_on_loop(_capture, [])

    def snapshot_network_state(self) -> dict[str, Any]:
        def _capture() -> dict[str, Any]:
            connections = len(self.peers)
            inbound = sum(1 for peer in self.peers.values() if not peer.outbound)
            outbound = connections - inbound
            localaddresses = [
                {"address": addr.host, "port": addr.port, "score": 1}
                for addr in list(self.known_addresses.values())[:5]
            ]
            return {
                "connections": connections,
                "inbound": inbound,
                "outbound": outbound,
                "localaddresses": localaddresses,
                "networkactive": not self._stop_event.is_set(),
                "listen_host": self.host,
                "listen_port": self.listen_port,
            }

        default = {
            "connections": 0,
            "inbound": 0,
            "outbound": 0,
            "localaddresses": [],
            "networkactive": False,
            "listen_host": self.host,
            "listen_port": self.listen_port,
        }
        return self._call_on_loop(_capture, default)

    def request_block(self, block_hash: str) -> bool:
        """
        Ask connected peers for a specific block (broadcast with backoff).
        This must not block the loop; it only uses in-memory checks.
        """
        if not self.loop:
            return False
        if self._is_block_invalid_fast(block_hash):
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
        filtered = [(host, port) for host, port in candidates if not self.security.ban_manager.is_ip_banned(host)]
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
                reader, writer = await asyncio.wait_for(asyncio.open_connection(tgt_host, tgt_port), timeout=5)
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
                if outbound_only and not any(peer.outbound and peer.address == addr for peer in self.peers.values()):
                    continue
                buckets.add(bucket)
        return buckets

    def _diversify_candidates(self, candidates: list[tuple[str, int]], target: int) -> list[tuple[str, int]]:
        """Prefer candidates from new network buckets to avoid concentrating on one ASN/IP range."""
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

    def _should_skip_outbound_port(self, port: int) -> bool:
        """Avoid dialing junk ports; only allow listen/seed ports."""
        if port == self.listen_port:
            return False
        if port in self._seed_ports:
            return False
        return True

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

        existing = self.discovery.address_book.addresses.get(addr.key())
        if existing:
            addr.attempts = existing.attempts
            addr.last_attempt = existing.last_attempt
            addr.success_count = existing.success_count
            addr.failure_count = existing.failure_count

        self.discovery.address_book.add_address(addr)
        if peer.outbound:
            self.discovery.address_book.record_success(addr.host, addr.port)
        rv = peer.remote_version or {}
        if rv.get("network") != self.network_id:
            self.log.info(
                "Disconnecting %s: wrong network remote=%r local=%r",
                peer.peer_id, rv.get("network"), self.network_id
            )
            await peer.close()
            return

        # optional extra hard check if you include this in version:
        if rv.get("genesis_hash") and rv["genesis_hash"] != self.chain.genesis_hash:
            self.log.info("Disconnecting %s: wrong genesis_hash", peer.peer_id)
            await peer.close()
            return
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

        if not self._stop_event.is_set():
            self.sync.try_start_header_sync()

    # -------------------------------------------------------------------------
    # Hot path height lookup: cached (no DB)
    # -------------------------------------------------------------------------

    def best_height(self) -> int:
        return int(self._best_height_cache or 0)

    async def _send_addr(self, peer: Peer) -> None:
        peers = list(self.known_addresses.values())[:32]
        await peer.send_message(protocol.addr_payload([asdict(addr) for addr in peers]))

    async def _request_addr(self, peer: Peer) -> None:
        await peer.send_message(protocol.getaddr_payload())

    # -------------------------------------------------------------------------
    # INV / GETDATA / BLOCK / TX
    # -------------------------------------------------------------------------

    async def handle_inv(self, peer: Peer, message: dict[str, Any]) -> None:
        sync_active = self.sync.sync_active
        sync_peer = self.sync.sync_peer

        items = message.get("items", [])
        tx_requests: list[dict[str, str]] = []
        block_requests: list[dict[str, str]] = []
        block_hashes: list[str] = []

        for entry in items:
            obj_type = entry.get("type")
            obj_hash = entry.get("hash")
            if obj_hash is None:
                continue

            if obj_type == "block" and self._is_block_invalid_fast(obj_hash):
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

                if sync_active and peer is not sync_peer:
                    continue

                if not self.sync.sync_active or peer is not self.sync.sync_peer:
                    # NOTE: has_block() is in-memory index lookup (fast, no disk IO).
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
                self.log.debug(
                    "Truncating large inv from %s (items=%d flood_events=%d)",
                    peer.peer_id,
                    len(block_hashes),
                    floods,
                )
            block_hashes = block_hashes[:200]
            block_requests = block_requests[:200]

        if tx_requests:
            await peer.send_message(protocol.getdata_payload(tx_requests))

        sync_inv = sync_active and (peer is sync_peer)
        if sync_inv:
            self.sync._sync_inv_requested = False
            if block_hashes:
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
            self._maybe_request_more_inventory(peer)
            return

        if sync_active:
            return

        if block_requests:
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
                    raw: bytes = await self._to_thread(self.chain.block_store.get_block, obj_hash)
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
        requested_hash = message.get("hash")
        requested_hash = requested_hash if isinstance(requested_hash, str) else None
        raw = message.get("raw")

        if self.sync.sync_active and peer is not self.sync.sync_peer:
            if not requested_hash or requested_hash not in self.sync._inflight_blocks:
                return

        block_hash: str | None = None
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

            if block_hash and self._is_block_invalid_fast(block_hash):
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
            async with self._block_apply_lock:
                # add_block may touch disk/DB heavily -> off-thread
                result = await asyncio.to_thread(self.chain.add_block, block, block_bytes)

        except BlockStoreError as exc:
            msg = str(exc)
            if "already stored" in msg:
                self.log.debug("Duplicate block %s from %s; already stored", block.block_hash(), peer.peer_id)
                result = {"status": "duplicate", "hash": block.block_hash()}
            else:
                now = time.time()
                last = self._missing_block_log.get(block_hash or "")
                if not last or now - last > 30:
                    self.log.warning("Block store miss for %s from %s: %s", block.block_hash(), peer.peer_id, exc)
                    self._missing_block_log[block_hash or ""] = now
                missing = [{"type": "block", "hash": block.header.prev_hash}]
                await peer.send_message(protocol.getdata_payload(missing))
                self.request_block(block.header.prev_hash)

        except ChainError as exc:
            msg = str(exc)
            if "Unknown parent block" in msg:
                orphan_hash = block.block_hash()
                self._log_orphan_event(peer, orphan_hash, block.header.prev_hash)
                self.chain.fork_manager.detector.orphan_manager.add_orphan(block, peer.peer_id)
                await self._request_missing_parent(peer, block.header.prev_hash)
                self.request_block(block.header.prev_hash)

                count = self.health.increment_missing_parent(peer.peer_id)
                if self.sync.sync_active and peer is self.sync.sync_peer and count >= 3:
                    self._rotate_sync_peer("missing parents from sync peer", cooldown=120.0)

                if not self.sync.header_sync_active:
                    self._maybe_start_header_sync(peer)
                if not self.sync.sync_active:
                    self._request_block_inventory(peer)

            elif "Parent block data missing" in msg:
                parent_hash = block.header.prev_hash
                await self._request_missing_parent(peer, parent_hash)
                self.request_block(parent_hash)
                count = self.health.increment_missing_parent(peer.peer_id)
                if self.sync.sync_active and peer is self.sync.sync_peer and count >= 3:
                    self._rotate_sync_peer("parent data missing from sync peer", cooldown=120.0)
                return

            elif "Missing referenced output" in msg:
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
        if block_hash:
            peer.known_inventory.add(block_hash)

        if status in {"connected", "reorganized"}:
            self._bad_block_counts.pop(peer.peer_id, None)

            if self.mempool:
                with contextlib.suppress(Exception):
                    self.mempool.remove_confirmed(block.transactions)

            height = result.get("height")
            if isinstance(height, int):
                self._on_block_connected(height)

            # After a reorganization, refresh the cached tip from the DB so
            # handshake/reporting paths don't advertise a stale height/hash.
            if status == "reorganized" and not self._stop_event.is_set():
                self._schedule(self._refresh_best_tip_cache())

            if block_hash:
                if not self.sync.sync_active:
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
                    with contextlib.suppress(Exception):
                        self.mempool.remove_confirmed(orphan.transactions)

                height = result.get("height")
                if isinstance(height, int):
                    self._on_block_connected(height)

                exclude = {source_peer.peer_id} if source_peer else None
                if not self.sync.sync_active:
                    await self.broadcast_inv("block", orphan_hash, exclude=exclude)

    async def handle_addr(self, _peer: Peer, _message: dict[str, Any]) -> None:
        return

    # -------------------------------------------------------------------------
    # GETHEADERS / GETBLOCKS: batch DB read off-thread, no per-height DB loop
    # -------------------------------------------------------------------------

    async def _find_locator_height_async(self, locator: list[str]) -> int:
        def _work() -> int:
            for block_hash in locator:
                header = self.chain.state_db.get_header(block_hash)
                if header and getattr(header, "status", 0) == 0:
                    return int(header.height)
            return -1

        return int(await self._db(_work))

    async def handle_getheaders(self, peer: Peer, message: dict[str, Any]) -> None:
        locator = message.get("locator")
        if not isinstance(locator, list):
            return

        stop_hash = message.get("stop")
        stop_hash = stop_hash if isinstance(stop_hash, str) else None

        # Optional: schedule repair in background (off-thread) without blocking response.
        # (If you want "always repair before serving", wrap this in wait_for)
        self._schedule(self._repair_headers_best_effort())

        start_height = await self._find_locator_height_async(locator)

        def _work() -> tuple[int, list[HeaderData]]:
            best = self.chain.state_db.get_best_tip()
            if not best:
                return 0, []
            best_height = int(best[1])

            from_h = start_height + 1
            to_h = min(best_height, from_h + int(self.sync_header_batch) - 1)
            if to_h < from_h:
                return best_height, []

            headers = self.chain.state_db.get_headers_range(from_h, to_h)

            # If stop_hash is in-range, truncate up to it.
            if stop_hash:
                trimmed: list[HeaderData] = []
                for h in headers:
                    trimmed.append(h)
                    if h.hash == stop_hash:
                        break
                headers = trimmed

            # Continuity sanity: if broken, truncate at first break.
            if headers:
                prev = self.chain.state_db.get_main_header_at_height(start_height) if start_height >= 0 else None
                expected_prev = prev.hash if prev else None
                if expected_prev:
                    ok: list[HeaderData] = []
                    for h in headers:
                        if h.prev_hash != expected_prev:
                            break
                        ok.append(h)
                        expected_prev = h.hash
                    headers = ok

            return best_height, headers

        try:
            _best_height, hdrs = await self._db(_work)
        except Exception as exc:
            self.log.debug("getheaders DB error: %s", exc)
            return

        payload_headers = [self._header_to_dict(h) for h in hdrs]
        await peer.send_message(protocol.headers_payload(payload_headers))

    async def handle_getblocks(self, peer: Peer, message: dict[str, Any]) -> None:
        locator = message.get("locator")
        if not isinstance(locator, list):
            return

        stop_hash = message.get("stop")
        stop_hash = stop_hash if isinstance(stop_hash, str) else None

        start_height = await self._find_locator_height_async(locator)

        def _work() -> list[str]:
            best = self.chain.state_db.get_best_tip()
            if not best:
                return []
            best_height = int(best[1])

            from_h = start_height + 1
            to_h = min(best_height, from_h + int(self.sync_batch) - 1)
            if to_h < from_h:
                return []

            headers = self.chain.state_db.get_headers_range(from_h, to_h)
            hashes: list[str] = []
            for h in headers:
                hashes.append(h.hash)
                if stop_hash and h.hash == stop_hash:
                    break
            return hashes

        try:
            block_hashes: list[str] = await self._db(_work)
        except Exception as exc:
            self.log.debug("getblocks DB error: %s", exc)
            return

        if block_hashes:
            inv_items = [{"type": "block", "hash": h} for h in block_hashes]
            await peer.send_message(protocol.inv_payload(inv_items))

    # -------------------------------------------------------------------------
    # HEADERS (header sync)
    # -------------------------------------------------------------------------

    async def handle_headers(self, peer: Peer, message: dict[str, Any]) -> None:
        headers = message.get("headers")
        if not isinstance(headers, list):
            return
        if peer is not self.sync.header_peer or not self.sync.header_sync_active:
            return

        if not headers:
            local_header_height: int | None = None
            try:
                header_tip = await self._db(self.chain.state_db.get_highest_chainwork_header)
            except Exception:
                header_tip = None
            if header_tip is not None:
                local_header_height = int(header_tip.height)
            local_height = self.best_height()
            compare_height = local_header_height if local_header_height is not None else local_height
            if self.sync.sync_remote_height > compare_height:
                self.log.warning(
                    "Peer %s advertised height %s but returned no headers; rotating",
                    peer.peer_id,
                    self.sync.sync_remote_height,
                )
                self.sync.rotate_sync_peer("no headers from peer", cooldown=120.0)
            else:
                self._complete_header_sync(peer)
            return

        # Anchor on the connection point the peer is extending (Bitcoin-style getheaders).
        first_prev = headers[0].get("prev_hash")
        if not isinstance(first_prev, str):
            self.sync.rotate_sync_peer("malformed headers (no prev_hash)", cooldown=120.0)
            return
        try:
            parent = await self._db(self.chain.state_db.get_header, first_prev)
        except Exception:
            parent = None
        if parent is None:
            self.log.warning(
                "Peer %s sent headers that don't connect to our chain (unknown prev=%s)",
                peer.peer_id,
                first_prev,
            )
            self.sync.rotate_sync_peer("headers do not connect", cooldown=180.0)
            return

        expected_prev = parent.hash
        height = int(parent.height)

        batch: list[HeaderData] = []
        overlay: dict[str, HeaderData] = {parent.hash: parent}

        def get_hdr(h: str) -> HeaderData | None:
            return overlay.get(h) or self.chain.state_db.get_header(h)

        def expected_bits_lwma(height: int, parent_header: HeaderData) -> int:
            # same logic as Chain._expected_bits_lwma but using get_hdr()
            from ..core.chain import LWMA_SOLVETIME_CLAMP_FACTOR, LWMA_WINDOW

            if height <= 1:
                return self.chain.config.mining.initial_bits
            if height < 3:
                return parent_header.bits

            target_spacing = max(1, self.chain.config.mining.block_interval_target)
            window = min(LWMA_WINDOW, height - 1)
            if window < 2:
                return parent_header.bits

            path: list[HeaderData] = []
            current: HeaderData | None = parent_header
            for _ in range(window + 1):
                if current is None:
                    break
                path.append(current)
                if current.prev_hash is None:
                    break
                current = get_hdr(current.prev_hash)

            if len(path) < window + 1:
                return parent_header.bits

            path.reverse()
            actual_window = len(path) - 1
            sum_targets = 0
            sum_weighted_solvetime = 0
            max_solvetime = LWMA_SOLVETIME_CLAMP_FACTOR * target_spacing
            weight_sum = actual_window * (actual_window + 1) // 2

            for i in range(1, actual_window + 1):
                prev = path[i - 1]
                curr = path[i]
                solvetime = curr.timestamp - prev.timestamp
                solvetime = max(1, min(max_solvetime, solvetime))
                sum_weighted_solvetime += solvetime * i
                sum_targets += difficulty.compact_to_target(curr.bits)

            denom = actual_window * weight_sum * target_spacing
            next_target = (sum_targets * sum_weighted_solvetime) // max(1, denom)
            next_target = max(1, min(self.chain.max_target, next_target))
            return difficulty.target_to_compact(next_target)

        for entry in headers:
            try:
                hdr = BlockHeader(
                    version=int(entry["version"]),
                    prev_hash=str(entry["prev_hash"]),
                    merkle_root=str(entry["merkle_root"]),
                    timestamp=int(entry["timestamp"]),
                    bits=int(entry["bits"]),
                    nonce=int(entry["nonce"]),
                )
            except (KeyError, ValueError, TypeError):
                self.log.warning("Malformed header received from %s", peer.peer_id)
                self.sync.rotate_sync_peer("malformed headers", cooldown=120.0)
                return

            hdr_hash = hdr.hash()

            if hdr.prev_hash != expected_prev:
                self.log.warning(
                    "Header continuity break from %s at local_next_height=%d: prev=%s expected=%s header=%s",
                    peer.peer_id,
                    height + 1,
                    hdr.prev_hash,
                    expected_prev,
                    hdr_hash,
                )
                self.sync.rotate_sync_peer("header continuity break", cooldown=120.0)
                return

            next_height = height + 1

            with contextlib.suppress(Exception):
                self.chain.upgrade_manager.process_new_block(hdr, next_height)

            # expected bits might consult DB; do it off-thread
            try:
                expected_bits =  await asyncio.to_thread(expected_bits_lwma, next_height, parent)
            except Exception as exc:
                self.log.warning("Failed computing expected bits at height %s: %s", next_height, exc)
                self.sync.rotate_sync_peer("expected bits failure", cooldown=60.0)
                return

            if hdr.bits != expected_bits:
                self.log.warning(
                    "Header bits mismatch at height %s from %s: got=%s expected=%s prev=%s header=%s",
                    next_height,
                    peer.peer_id,
                    hdr.bits,
                    expected_bits,
                    hdr.prev_hash,
                    hdr_hash,
                )
                self.sync.rotate_sync_peer("header bits mismatch", cooldown=180.0)
                return

            if not difficulty.check_proof_of_work(hdr_hash, hdr.bits):
                self.log.warning("Invalid POW for header %s from %s", hdr_hash, peer.peer_id)
                self.sync.rotate_sync_peer("invalid header pow", cooldown=180.0)
                return

            chainwork_int = int(parent.chainwork) + difficulty.block_work(hdr.bits)

            batch.append(
                HeaderData(
                    hash=hdr_hash,
                    prev_hash=hdr.prev_hash,
                    height=next_height,
                    bits=hdr.bits,
                    nonce=hdr.nonce,
                    timestamp=hdr.timestamp,
                    merkle_root=hdr.merkle_root,
                    chainwork=str(chainwork_int),
                    version=hdr.version,
                    status=1,
                )
            )
            if next_height % 1000 == 0:
                self.log.info(
                    "Header sync from %s at height %d hash=%s",
                    peer.peer_id,
                    next_height,
                    hdr_hash,
                )
            overlay[hdr_hash] = batch[-1]
            parent = batch[-1]
            expected_prev = hdr_hash
            height = next_height

        # Bulk-store (already off-thread)
        try:
            if hasattr(self.chain.state_db, "store_headers_bulk"):
                await asyncio.to_thread(self.chain.state_db.store_headers_bulk, batch)
            else:
                for h in batch:
                    await asyncio.to_thread(self.chain.state_db.store_header, h)
        except Exception as exc:
            self.log.warning("Failed storing headers batch: %s", exc)
            self.sync.rotate_sync_peer("header store failure", cooldown=60.0)
            return

        self.sync.record_headers_received(len(batch))
        self.sync.sync_remote_height = max(self.sync.sync_remote_height, height)

        if len(headers) < self.sync.sync_header_batch:
            self._complete_header_sync(peer)
        elif not self._stop_event.is_set():
            self._schedule(self._send_getheaders(peer))

    # -------------------------------------------------------------------------
    # Broadcast: bounded tasks
    # -------------------------------------------------------------------------

    async def broadcast_inv(self, obj_type: str, obj_hash: str, exclude: set[str] | None = None) -> None:
        if self._stop_event.is_set():
            return

        if obj_type == "block" and self.sync.sync_active:
            return

        payload = protocol.inv_payload([{"type": obj_type, "hash": obj_hash}])

        for peer in list(self.peers.values()):
            if exclude and peer.peer_id in exclude:
                continue
            if peer.closed:
                continue
            if obj_hash in peer.known_inventory:
                continue

            self.log.debug("Broadcasting inv %s to peer %s", obj_hash, peer.peer_id)
            peer.known_inventory.add(obj_hash)
            peer.send_message_background(payload)

    def _on_local_tx(self, tx: Transaction) -> None:
        if not self.loop:
            return
        asyncio.run_coroutine_threadsafe(self.broadcast_inv("tx", tx.txid()), self.loop)

    def announce_block(self, block_hash: str) -> None:
        if not self.loop:
            return
        asyncio.run_coroutine_threadsafe(self.broadcast_inv("block", block_hash), self.loop)

    # -------------------------------------------------------------------------
    # Health / security helpers
    # -------------------------------------------------------------------------

    def _penalize_peer(self, peer: Peer, reason: str, *, severity: int = 1, ban_seconds: int = 600) -> None:
        count = self.health.increment_bad_block(peer.peer_id, severity)
        if count >= 3:
            self.log.warning("Banning peer %s for repeated bad data: %s", peer.peer_id, reason)
            with contextlib.suppress(Exception):
                self.security.ban_manager.ban_peer(peer.peer_id, ban_seconds)
                self.security.ban_manager.ban_ip(peer.address[0], ban_seconds)
            asyncio.create_task(peer.close())

    def _is_block_invalid_fast(self, block_hash: str) -> bool:
        """
        Non-blocking invalid check.
        - block_store.has_block() is in-memory (safe)
        - health cache is in-memory
        """
        if not block_hash:
            return False
        try:
            if self.chain.block_store.has_block(block_hash):
                return False
        except Exception:  # noqa: S110
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
            penalize = False
        if penalize and peer:
            self._penalize_peer(peer, reason, severity=severity)

    def _log_orphan_event(self, peer: Peer, block_hash: str, prev_hash: str) -> None:
        self.health.log_orphan_event(peer.peer_id, block_hash, prev_hash)

    def _inv_rate_limited(self, peer_id: str, limit: int = 5, window: float = 10.0) -> bool:
        return self.health.inv_rate_limited(peer_id, limit=limit, window=window)

    # -------------------------------------------------------------------------
    # Known peers persistence (called on init/stop; not on hot loop paths)
    # -------------------------------------------------------------------------

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

    # -------------------------------------------------------------------------
    # Locator building: async DB access
    # -------------------------------------------------------------------------

    async def _build_block_locator_async(self, limit: int = 32) -> list[str]:
        def _work() -> list[str]:
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

        try:
            return await self._db(_work)
        except Exception:
            return [self.chain.genesis_hash]

    def _header_to_dict(self, header: HeaderData) -> dict[str, int]:
        return {
            "version": int(getattr(header, "version", 1)),
            "prev_hash": header.prev_hash,
            "merkle_root": header.merkle_root,
            "timestamp": int(header.timestamp),
            "bits": int(header.bits),
            "nonce": int(header.nonce),
        }

    def _maybe_start_header_sync(self, peer: Peer) -> None:
        self.sync.maybe_start_header_sync(peer)

    def _rotate_sync_peer(self, reason: str, cooldown: float = 90.0) -> None:
        self.sync.rotate_sync_peer(reason, cooldown=cooldown)

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
        locator = await self._build_block_locator_async()
        await peer.send_message(protocol.getblocks_payload(locator))

    async def _send_getheaders(self, peer: Peer) -> None:
        if peer.closed:
            return
        locator = await self._build_header_locator_async()
        if len(locator) == 1 and locator[0] == self.chain.genesis_hash:
            locator.append("00" * 32)
        await peer.send_message(protocol.getheaders_payload(locator))

    def _on_block_connected(self, height: int) -> None:
        # Update cache cheaply.
        # Note: reorganizations can move the tip to a lower height; don't assume
        # monotonically increasing.
        self._best_height_cache = int(height)
        self.sync.on_block_connected(height)

    def _complete_header_sync(self, peer: Peer) -> None:
        self.sync.complete_header_sync(peer)

        async def _reanchor() -> None:
            try:
                await self._refresh_best_tip_cache()
            except Exception as exc:
                self.log.debug("reanchor_main_chain failed: %s", exc)

        if not self._stop_event.is_set():
            self._schedule(_reanchor())

    async def _build_header_locator_async(self, limit: int = 32) -> list[str]:
        def _work() -> list[str]:
            tip = self.chain.state_db.get_highest_chainwork_header()
            if not tip:
                return [self.chain.genesis_hash]
            locator: list[str] = []
            current_hash = tip.hash
            while current_hash and len(locator) < limit:
                locator.append(current_hash)
                header = self.chain.state_db.get_header(current_hash)
                if header is None or header.prev_hash is None:
                    break
                current_hash = header.prev_hash
            if self.chain.genesis_hash not in locator:
                locator.append(self.chain.genesis_hash)
            return locator

        try:
            return await self._db(_work)
        except Exception:
            return [self.chain.genesis_hash]


    # -------------------------------------------------------------------------
    # Best-effort header repair (runs off-thread, never blocks responses)
    # -------------------------------------------------------------------------

    async def _repair_headers_best_effort(self) -> None:
        def _work() -> None:
            try:
                if self.chain.state_db.has_main_chain_gap():
                    self.chain.state_db.rebuild_main_headers_from_blocks(self.chain.block_store)
                best = self.chain.state_db.get_best_tip()
                if best:
                    highest = self.chain.state_db.get_highest_main_header()
                    if highest is None or int(highest.height) < int(best[1]):
                        self.chain.state_db.reanchor_main_chain()
            except Exception:
                return

        # Avoid stacking a bunch of repair jobs
        if getattr(self, "_repair_inflight", False):
            return
        self._repair_inflight = True
        try:
            await asyncio.to_thread(_work)
        finally:
            self._repair_inflight = False

    # -------------------------------------------------------------------------

    async def _cleanup_loop(self) -> None:
        """Periodic cleanup of expired data."""
        try:
            while not self._stop_event.is_set():
                try:
                    self.security.cleanup()
                    self.discovery.cleanup()
                    self.health.cleanup()
                    self.log.debug("Completed periodic cleanup")
                except Exception as exc:
                    self.log.error("Error during cleanup: %s", exc)
                await asyncio.sleep(300)
        except asyncio.CancelledError:
            pass
