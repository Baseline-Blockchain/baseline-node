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
from .peer import Peer
from .security import P2PSecurity

MAINNET_NETWORK_ID = "baseline-mainnet-2025-12-28"


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
        self.known_addresses: dict[tuple[str, int], PeerAddress] = {}
        self.peer_file = config.data_dir / "peers" / "known_peers.json"
        self.peer_file.parent.mkdir(parents=True, exist_ok=True)
        self._peer_seq = 0
        self._tasks: list[asyncio.Task] = []
        self.sync_peer: Peer | None = None
        self.sync_active = False
        self.sync_remote_height = 0
        self.sync_batch = 500
        self.header_peer: Peer | None = None
        self.header_sync_active = False
        self.sync_header_batch = 2000
        self._header_last_time = 0.0
        self._header_timeout = 30.0
        best = self.chain.state_db.get_best_tip()
        self._block_last_height = best[1] if best else 0
        self._block_last_time = time.time()
        self._block_timeout = 60.0
        self.bytes_sent = 0
        self.bytes_received = 0
        self._local_addresses: set[tuple[str, int]] = set()
        self._init_local_addresses()
        self._load_known_addresses()
        self.mempool.register_listener(self._on_local_tx)

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
        for task in self._tasks:
            task.cancel()
        for task in list(self.peer_tasks):
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.gather(*self._tasks, return_exceptions=True)
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        for peer in list(self.peers.values()):
            await peer.close()
        self._save_known_addresses()

    async def _handle_inbound(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
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

    def _run_peer(self, peer: Peer) -> None:
        task = asyncio.create_task(peer.run(), name=f"peer-{peer.peer_id}")
        self.peer_tasks.add(task)

        def _cleanup(_):
            self.peer_tasks.discard(task)

        task.add_done_callback(_cleanup)

    def record_bytes_sent(self, count: int) -> None:
        self.bytes_sent += count

    def record_bytes_received(self, count: int) -> None:
        self.bytes_received += count

    def should_skip_rate_limit(self, peer: Peer, msg_type: str | None) -> bool:
        """Return True when rate limiting should be skipped for this message."""
        if msg_type is None:
            return False
        if msg_type == "headers":
            return self.header_sync_active and self.header_peer is peer
        if msg_type in {"block", "inv"}:
            return self.sync_active and self.sync_peer is peer
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

        # Use enhanced peer discovery
        candidates = await self.discovery.discover_peers(needed)
        for host, port in candidates:
            await self._connect_outbound(host, port)

    async def _connect_outbound(self, host: str, port: int) -> None:
        if self._stop_event.is_set():
            return
        key = (host, port)
        if key in self.active_addresses():
            return
        # Prevent self-connection
        if self.is_local_address(host, port):
            self.log.debug("Skipping self-connection to %s:%s", host, port)
            return

        # Record connection attempt
        self.discovery.record_connection_attempt(host, port)

        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=5)
        except (TimeoutError, OSError) as exc:
            self.log.debug("Failed to dial %s:%s: %s", host, port, exc)
            self.discovery.record_connection_failure(host, port)
            return

        peer = self._build_peer(reader, writer, (host, port), outbound=True)

        # Add connection to security manager
        if not self.security.add_connection(host, peer.peer_id):
            self.log.debug("Rejected outbound connection to %s:%s (connection limit)", host, port)
            writer.close()
            self.discovery.record_connection_failure(host, port)
            return

        self.discovery.record_connection_success(host, port)
        self._run_peer(peer)

    def outbound_count(self) -> int:
        return sum(1 for peer in self.peers.values() if peer.outbound)

    def active_addresses(self) -> set[tuple[str, int]]:
        return {peer.address for peer in self.peers.values()}

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
                if self.header_sync_active and now - self._header_last_time > self._header_timeout:
                    self.log.warning("Header sync stalled; restarting")
                    self.header_sync_active = False
                    if self.header_peer:
                        self.header_peer = None
                    self._try_start_header_sync()
                if self.sync_active and now - self._block_last_time > self._block_timeout:
                    self.log.warning("Block sync stalled; restarting header sync")
                    self.sync_active = False
                    self.sync_peer = None
                    self._try_start_header_sync()
                await asyncio.sleep(10)
        except asyncio.CancelledError:
            pass

    async def on_peer_ready(self, peer: Peer) -> None:
        if len(self.peers) >= self.max_peers:
            self.log.warning("Max peers reached, disconnecting %s", peer.peer_id)
            await peer.close()
            return
        self.peers[peer.peer_id] = peer
        addr = PeerAddress(host=peer.address[0], port=peer.address[1], last_seen=time.time())
        self.known_addresses[addr.key()] = addr
        self.log.info("Peer %s connected (%s:%s)", peer.peer_id, *peer.address)
        await self._send_addr(peer)
        self._maybe_start_header_sync(peer)

    async def on_peer_closed(self, peer: Peer) -> None:
        self.peers.pop(peer.peer_id, None)
        with contextlib.suppress(Exception):
            self.security.remove_connection(peer.address[0], peer.peer_id)
        self.log.info("Peer %s disconnected", peer.peer_id)
        if self.sync_peer is peer:
            self.sync_peer = None
            self.sync_active = False
        if self.header_peer is peer:
            self.header_peer = None
            self.header_sync_active = False
        self._try_start_header_sync()

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

    async def handle_inv(self, peer: Peer, message: dict[str, Any]) -> None:
        items = message.get("items", [])
        needed = []
        for entry in items:
            obj_type = entry.get("type")
            obj_hash = entry.get("hash")
            if obj_hash is None:
                continue
            peer.known_inventory.add(obj_hash)
            if obj_type == "tx":
                if not self.mempool.contains(obj_hash):
                    needed.append(entry)
            elif obj_type == "block":
                if not self.chain.block_store.has_block(obj_hash):
                    needed.append(entry)
        self.log.debug(
            "Received inv from %s items=%d requesting=%d",
            peer.peer_id,
            len(items),
            len(needed),
        )
        if needed:
            await peer.send_message(protocol.getdata_payload(needed))
        self._handle_sync_inv(peer, items)

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
        if not isinstance(raw, str):
            return
        try:
            block_bytes = bytes.fromhex(raw)
            block = Block.parse(block_bytes)
        except Exception as exc:
            self.log.warning("Invalid block payload: %s", exc)
            return
        try:
            result = await asyncio.to_thread(self.chain.add_block, block, block_bytes)
        except ChainError as exc:
            self.log.debug("Block rejected: %s", exc)
            return
        status = result.get("status")
        block_hash = block.block_hash()
        peer.known_inventory.add(block_hash)
        if status in {"connected", "reorganized"}:
            if self.mempool:
                try:
                    self.mempool.remove_confirmed(block.transactions)
                except Exception as exc:
                    self.log.debug("Failed to prune mempool for block %s: %s", block_hash, exc)
            height = result.get("height")
            if isinstance(height, int):
                self._on_block_connected(height)
            await self.broadcast_inv("block", block_hash, exclude={peer.peer_id})

    async def handle_addr(self, _peer: Peer, message: dict[str, Any]) -> None:
        peers = message.get("peers", [])
        for entry in peers:
            try:
                host = entry["host"]
                port = int(entry["port"])
                last_seen = float(entry.get("last_seen", time.time()))
            except (KeyError, ValueError, TypeError):
                continue
            addr = PeerAddress(host=host, port=port, last_seen=last_seen)
            self.known_addresses[addr.key()] = addr

    async def handle_getheaders(self, peer: Peer, message: dict[str, Any]) -> None:
        locator = message.get("locator")
        if not isinstance(locator, list):
            return
        best = self.chain.state_db.get_best_tip()
        if not best:
            return
        start_height = self._find_locator_height(locator)
        stop_hash = message.get("stop")
        headers: list[dict[str, int]] = []
        height = start_height + 1
        while height <= best[1] and len(headers) < self.sync_header_batch:
            header = self.chain.state_db.get_main_header_at_height(height)
            if header is None:
                break
            headers.append(self._header_to_dict(header))
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
        if peer is not self.header_peer or not self.header_sync_active:
            return
        if not headers:
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
                self.log.debug("Header parent %s unknown", header.prev_hash)
                return
            height = parent.height + 1
            expected_bits = self.chain._expected_bits(height, parent)
            if header.bits != expected_bits:
                self.log.debug("Header bits mismatch at height %s", height)
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
        if last_height is not None:
            self.sync_remote_height = max(self.sync_remote_height, last_height)
        self._header_last_time = time.time()
        if len(headers) < self.sync_header_batch:
            self._complete_header_sync(peer)
        else:
            asyncio.create_task(self._send_getheaders(peer))

    async def broadcast_inv(self, obj_type: str, obj_hash: str, exclude: set[str] | None = None) -> None:
        payload = protocol.inv_payload([{"type": obj_type, "hash": obj_hash}])
        for peer in list(self.peers.values()):
            if exclude and peer.peer_id in exclude:
                continue
            if obj_hash in peer.known_inventory:
                continue
            peer.known_inventory.add(obj_hash)
            self._fire_and_forget(peer.send_message(payload))

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

    def _load_known_addresses(self) -> None:
        if not self.peer_file.exists():
            for host, port in self._seed_hosts():
                addr = PeerAddress(host=host, port=port, last_seen=0)
                self.known_addresses[addr.key()] = addr
            return
        try:
            data = json.loads(self.peer_file.read_text("utf-8"))
        except Exception:
            self.log.warning("Failed to load peer store")
            return
        for entry in data:
            try:
                addr = PeerAddress(
                    host=entry["host"],
                    port=int(entry["port"]),
                    last_seen=float(entry.get("last_seen", 0)),
                )
            except (KeyError, ValueError, TypeError):
                continue
            self.known_addresses[addr.key()] = addr

    def _save_known_addresses(self) -> None:
        try:
            serialized = [asdict(addr) for addr in self.known_addresses.values()]
            self.peer_file.write_text(json.dumps(serialized, indent=2), encoding="utf-8")
        except Exception:
            self.log.exception("Failed to store peer addresses")

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
        if self.header_sync_active:
            return
        if not peer.remote_version:
            return
        remote_height = int(peer.remote_version.get("height", 0))
        local_height = self.best_height()
        if remote_height <= local_height:
            return
        self.header_peer = peer
        self.header_sync_active = True
        self.sync_remote_height = max(self.sync_remote_height, remote_height)
        self._header_last_time = time.time()
        self.log.info(
            "Starting header sync with %s remote_height=%s local_height=%s",
            peer.peer_id,
            remote_height,
            local_height,
        )
        asyncio.create_task(self._send_getheaders(peer))

    def _try_start_header_sync(self) -> None:
        if self.header_sync_active:
            return
        for peer in list(self.peers.values()):
            self._maybe_start_header_sync(peer)
            if self.header_sync_active:
                break

    async def _send_getblocks(self, peer: Peer) -> None:
        if peer.closed:
            return
        locator = self._build_block_locator()
        await peer.send_message(protocol.getblocks_payload(locator))

    async def _send_getheaders(self, peer: Peer) -> None:
        if peer.closed:
            return
        locator = self._build_block_locator()
        await peer.send_message(protocol.getheaders_payload(locator))

    def _handle_sync_inv(self, peer: Peer, items: list[dict[str, str]]) -> None:
        if peer is not self.sync_peer or not self.sync_active:
            return
        block_count = sum(1 for entry in items if entry.get("type") == "block")
        if block_count == 0:
            return
        local_height = self.best_height()
        if local_height >= self.sync_remote_height:
            self.log.info("Block sync complete height=%s", local_height)
            self.sync_active = False
            self.sync_peer = None
            return
        if block_count >= self.sync_batch:
            asyncio.create_task(self._send_getblocks(peer))

    def _on_block_connected(self, height: int) -> None:
        self._block_last_height = max(self._block_last_height, height)
        self._block_last_time = time.time()
        if self.sync_active and height >= self.sync_remote_height:
            self.log.info("Block download caught up height=%s", height)
            self.sync_active = False
            self.sync_peer = None

    def _complete_header_sync(self, peer: Peer) -> None:
        self.header_sync_active = False
        self.header_peer = None
        self._start_block_sync(peer)

    def _start_block_sync(self, peer: Peer) -> None:
        if self.sync_active:
            return
        if not peer.remote_version:
            return
        remote_height = int(peer.remote_version.get("height", 0))
        local_height = self.best_height()
        if remote_height <= local_height:
            return
        self.sync_peer = peer
        self.sync_active = True
        self.sync_remote_height = max(self.sync_remote_height, remote_height)
        self._block_last_height = local_height
        self._block_last_time = time.time()
        self.log.info(
            "Starting block download from %s remote_height=%s local_height=%s",
            peer.peer_id,
            remote_height,
            local_height,
        )
        asyncio.create_task(self._send_getblocks(peer))

    async def _cleanup_loop(self) -> None:
        """Periodic cleanup of expired data."""
        try:
            while not self._stop_event.is_set():
                try:
                    # Clean up security-related data
                    self.security.cleanup()

                    # Clean up peer discovery data
                    self.discovery.cleanup()

                    self.log.debug("Completed periodic cleanup")
                except Exception as exc:
                    self.log.error("Error during cleanup: %s", exc)

                # Run cleanup every 5 minutes
                await asyncio.sleep(300)
        except asyncio.CancelledError:
            pass
