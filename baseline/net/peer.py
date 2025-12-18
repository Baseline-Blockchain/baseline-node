"""
Peer connection management.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from typing import TYPE_CHECKING, Any

from . import protocol

if TYPE_CHECKING:
    from .server import P2PServer


class Peer:
    def __init__(
        self,
        *,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        manager: P2PServer,
        address: tuple[str, int],
        outbound: bool,
        peer_id: str,
    ):
        self.reader = reader
        self.writer = writer
        self.manager = manager
        self.address = address
        self.outbound = outbound
        self.peer_id = peer_id
        self.log = logging.getLogger(f"baseline.peer[{address[0]}:{address[1]}]")
        self.remote_version: dict[str, Any] | None = None
        self.handshake_complete = False
        self.got_version = False
        self.got_verack = False
        self.sent_verack = False
        self.closed = False
        self.ping_nonce: int | None = None
        self.last_message = time.time()
        self.last_ping = 0.0
        self.known_inventory: set[str] = set()
        self.latency = None
        self._lock = asyncio.Lock()

    async def run(self) -> None:
        try:
            await self._perform_handshake()
            await self.manager.on_peer_ready(self)
            while not self.closed:
                msg = await protocol.read_message(self.reader, timeout=self.manager.idle_timeout)
                self.last_message = time.time()
                await self.handle_message(msg)
        except (TimeoutError, asyncio.IncompleteReadError, ConnectionError, protocol.ProtocolError) as exc:
            self.log.info("Connection closed: %s", exc)
        finally:
            await self.close()

    async def _perform_handshake(self) -> None:
        if self.outbound:
            await self.send_message(protocol.version_payload(
                network_id=self.manager.network_id,
                services=self.manager.services,
                height=self.manager.best_height(),
                listen_port=self.manager.listen_port,
            ))
        timeout = self.manager.handshake_timeout
        while not self.handshake_complete:
            msg = await protocol.read_message(self.reader, timeout=timeout)
            await self.handle_message(msg)
        self.last_message = time.time()

    async def handle_message(self, message: dict[str, Any]) -> None:
        msg_type = message.get("type")
        if msg_type == "version":
            await self._handle_version(message)
            return
        if msg_type == "verack":
            self.got_verack = True
            self.handshake_complete = self.sent_verack and self.got_verack and self.got_version
            return
        if not self.handshake_complete:
            raise protocol.ProtocolError("Handshaking not complete")
        elif msg_type == "ping":
            await self.send_message(protocol.pong_payload(message.get("nonce")))
        elif msg_type == "pong":
            if self.ping_nonce and message.get("nonce") == self.ping_nonce:
                self.latency = time.time() - self.last_ping
                self.ping_nonce = None
        elif msg_type == "inv":
            await self.manager.handle_inv(self, message)
        elif msg_type == "getdata":
            await self.manager.handle_getdata(self, message)
        elif msg_type == "tx":
            await self.manager.handle_tx(self, message)
        elif msg_type == "block":
            await self.manager.handle_block(self, message)
        elif msg_type == "addr":
            await self.manager.handle_addr(self, message)
        elif msg_type == "getheaders":
            await self.manager.handle_getheaders(self, message)
        elif msg_type == "getblocks":
            await self.manager.handle_getblocks(self, message)
        else:
            self.log.debug("Ignoring message type %s", msg_type)

    async def _handle_version(self, message: dict[str, Any]) -> None:
        if self.remote_version is not None:
            raise protocol.ProtocolError("Duplicate version message")
        if message.get("network") != self.manager.network_id:
            raise protocol.ProtocolError("Network identifier mismatch")
        self.remote_version = message
        self.got_version = True
        if not self.outbound:
            await self.send_message(
                protocol.version_payload(
                    network_id=self.manager.network_id,
                    services=self.manager.services,
                    height=self.manager.best_height(),
                    listen_port=self.manager.listen_port,
                )
            )
        await self._send_verack()

    async def _send_verack(self) -> None:
        if self.sent_verack:
            return
        await self.send_message(protocol.verack_payload())
        self.sent_verack = True
        self.handshake_complete = self.sent_verack and self.got_verack and self.got_version

    async def send_message(self, payload: dict[str, Any]) -> None:
        data = protocol.encode_message(payload)
        async with self._lock:
            self.writer.write(data)
            await self.writer.drain()

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        try:
            self.writer.close()
            with contextlib.suppress(Exception):
                await self.writer.wait_closed()
        except Exception:  # noqa: BLE001
            self.manager.log.exception("Peer close failed for peer_id=%s", self.peer_id)
        await self.manager.on_peer_closed(self)

    async def ping(self) -> None:
        self.ping_nonce = int(time.time() * 1000) & 0xFFFFFFFFFFFFFFFF
        self.last_ping = time.time()
        await self.send_message(protocol.ping_payload(self.ping_nonce))
