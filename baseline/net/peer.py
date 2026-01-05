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
        self.bytes_sent = 0
        self.bytes_received = 0
        self.last_send = 0.0
        self._lock = asyncio.Lock()
        self._ban_notice_sent = False

    async def run(self) -> None:
        try:
            await self._perform_handshake()
            await self.manager.on_peer_ready(self)
            while not self.closed:
                msg, byte_len = await protocol.read_message(
                    self.reader, timeout=self.manager.idle_timeout, include_bytes=True
                )
                self.last_message = time.time()
                self.bytes_received += byte_len
                self.manager.record_bytes_received(byte_len)
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
            msg, byte_len = await protocol.read_message(self.reader, timeout=timeout, include_bytes=True)
            self.bytes_received += byte_len
            self.manager.record_bytes_received(byte_len)
            await self.handle_message(msg)
        self.last_message = time.time()

    async def handle_message(self, message: dict[str, Any]) -> None:
        msg_type = message.get("type")
        skip_rate_limit = self.manager.should_skip_rate_limit(self, msg_type)

        # Security validation
        should_accept, error_reason = self.manager.security.should_accept_message(
            self.peer_id,
            message,
            skip_rate_limit=skip_rate_limit,
        )
        if not should_accept:
            if "banned" in error_reason.lower():
                if not self._ban_notice_sent:
                    self.log.warning("Peer banned; closing connection (%s)", error_reason)
                    self._ban_notice_sent = True
                await self.close()
                return
            self.log.warning("Message rejected: %s", error_reason)
            # Do not penalize peers for rate limiting; just drop the message.
            if "rate limit exceeded" not in error_reason.lower():
                self.manager.security.record_violation(self.peer_id)
            return

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
        elif msg_type == "addr":
            self.manager.discovery.handle_addr_message(message, self.address[0])
            await self.manager.handle_addr(self, message)
        elif msg_type == "getaddr":
            addr_msg = self.manager.discovery.create_addr_message()
            await self.send_message(addr_msg)
        elif msg_type == "inv":
            await self.manager.handle_inv(self, message)
        elif msg_type == "getdata":
            await self.manager.handle_getdata(self, message)
        elif msg_type == "tx":
            await self.manager.handle_tx(self, message)
        elif msg_type == "block":
            await self.manager.handle_block(self, message)
        elif msg_type == "getheaders":
            await self.manager.handle_getheaders(self, message)
        elif msg_type == "headers":
            await self.manager.handle_headers(self, message)
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
        if self.closed:
            return
        transport = getattr(self.writer, "transport", None)
        if transport and transport.is_closing():
            return
        data = protocol.encode_message(payload)
        try:
            async with self._lock:
                self.writer.write(data)
                await self.writer.drain()
        except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError):
            self.log.debug("Send failed to %s; closing peer", self.peer_id)
            await self.close()
            return
        except Exception:
            self.log.exception("Unexpected send failure to %s", self.peer_id)
            await self.close()
            return
        self.bytes_sent += len(data)
        self.last_send = time.time()
        self.manager.record_bytes_sent(len(data))

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
