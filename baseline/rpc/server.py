"""
Minimal JSON-RPC server with HTTP framing and basic authentication.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import time
from typing import Any

from ..config import NodeConfig
from .handlers import RPCError, RPCHandlers


class RPCServer:
    def __init__(self, config: NodeConfig, handlers: RPCHandlers):
        self.config = config
        self.handlers = handlers
        self.log_prefix = "baseline.rpc"
        self.host = config.rpc.host
        self.port = config.rpc.port
        self.max_request_bytes = config.rpc.max_request_bytes
        self.username = config.rpc.username
        self.password = config.rpc.password
        self.server: asyncio.AbstractServer | None = None
        self._stop_event = asyncio.Event()
        self._start_time = time.time()

    async def start(self) -> None:
        if self.server:
            return
        self._stop_event.clear()
        self.server = await asyncio.start_server(self._handle_client, self.host, self.port)

    async def stop(self) -> None:
        if not self.server:
            return
        self._stop_event.set()
        self.server.close()
        await self.server.wait_closed()
        self.server = None

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request_line = await self._read_line(reader)
            if request_line is None:
                await self._write_response(writer, 400, b"Bad Request", b"")
                return
            parts = request_line.decode("utf-8", "replace").strip().split()
            if len(parts) != 3:
                await self._write_response(writer, 400, b"Bad Request", b"")
                return
            method, _path, _version = parts
            method = method.upper()
            headers = await self._read_headers(reader)
            if headers is None:
                await self._write_response(writer, 400, b"Bad Request", b"")
                return
            if method == "GET":
                if not self._check_auth(headers.get("authorization")):
                    await self._write_response(
                        writer,
                        401,
                        b"Unauthorized",
                        b"",
                        extra_headers={"WWW-Authenticate": 'Basic realm="baseline"'},
                    )
                    return
                panel = self._render_status_panel()
                await self._write_response(writer, 200, b"OK", panel, content_type="text/plain; charset=utf-8")
                return
            if method != "POST":
                await self._write_response(writer, 405, b"Method Not Allowed", b"")
                return
            if not self._check_auth(headers.get("authorization")):
                await self._write_response(
                    writer,
                    401,
                    b"Unauthorized",
                    b"",
                    extra_headers={"WWW-Authenticate": 'Basic realm="baseline"'},
                )
                return
            try:
                content_length = int(headers.get("content-length", "0"))
            except ValueError:
                await self._write_response(writer, 411, b"Length Required", b"")
                return
            if content_length < 0 or content_length > self.max_request_bytes:
                await self._write_response(writer, 413, b"Request Entity Too Large", b"")
                return
            body = await reader.readexactly(content_length) if content_length else b""
            response_payload = await self._handle_payload(body)
            await self._write_response(writer, 200, b"OK", response_payload, content_type="application/json")
        except asyncio.IncompleteReadError:
            await self._write_response(writer, 400, b"Bad Request", b"")
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def _handle_payload(self, body: bytes) -> bytes:
        try:
            request = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError:
            return json.dumps(self._error_response(None, -32700, "Parse error")).encode("utf-8")
        if isinstance(request, list):
            responses = [await self._handle_call(item) for item in request]
            return json.dumps(responses).encode("utf-8")
        return json.dumps(await self._handle_call(request)).encode("utf-8")

    async def _handle_call(self, message: Any) -> dict[str, Any]:
        if not isinstance(message, dict):
            return self._error_response(None, -32600, "Invalid Request")
        msg_id = message.get("id")
        method = message.get("method")
        params = message.get("params", [])
        if not isinstance(method, str):
            return self._error_response(msg_id, -32600, "Invalid Request")
        if not isinstance(params, list):
            return self._error_response(msg_id, -32602, "Invalid params")
        try:
            result = await asyncio.to_thread(self.handlers.dispatch, method, params)
        except RPCError as exc:
            return self._error_response(msg_id, exc.code, exc.message)
        except Exception as exc:  # pragma: no cover - defensive
            return self._error_response(msg_id, -32603, f"Internal error: {exc}")
        return {"jsonrpc": "2.0", "result": result, "error": None, "id": msg_id}

    async def _read_line(self, reader: asyncio.StreamReader) -> bytes | None:
        line = await reader.readline()
        if not line:
            return None
        if len(line) > self.max_request_bytes:
            return None
        return line

    async def _read_headers(self, reader: asyncio.StreamReader) -> dict[str, str] | None:
        headers: dict[str, str] = {}
        while True:
            line = await reader.readline()
            if not line:
                return None
            if line in (b"\r\n", b"\n"):
                return headers
            if len(line) > self.max_request_bytes:
                return None
            try:
                name, value = line.decode("utf-8").split(":", 1)
            except ValueError:
                return None
            headers[name.strip().lower()] = value.strip()

    def _check_auth(self, header: str | None) -> bool:
        if not header or not header.startswith("Basic "):
            return False
        token = header[6:]
        try:
            decoded = base64.b64decode(token).decode("utf-8")
        except Exception:
            return False
        username, _, password = decoded.partition(":")
        return username == self.username and password == self.password

    def _error_response(self, msg_id: Any, code: int, message: str) -> dict[str, Any]:
        return {"jsonrpc": "2.0", "result": None, "error": {"code": code, "message": message}, "id": msg_id}

    async def _write_response(
        self,
        writer: asyncio.StreamWriter,
        status_code: int,
        reason: bytes,
        body: bytes,
        *,
        content_type: str = "text/plain",
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        headers = {
            "Content-Length": str(len(body)),
            "Content-Type": content_type,
            "Connection": "close",
        }
        if extra_headers:
            headers.update(extra_headers)
        status_line = f"HTTP/1.1 {status_code} {reason.decode('ascii')}\r\n".encode("ascii")
        writer.write(status_line)
        for name, value in headers.items():
            writer.write(f"{name}: {value}\r\n".encode("ascii"))
        writer.write(b"\r\n")
        writer.write(body)
        await writer.drain()

    def _render_status_panel(self) -> bytes:
        stats = self._collect_status_metrics()
        rows = [
            ("Height", f"{stats['height']:,}"),
            ("Best Block", stats["best_block"]),
            ("Chain Tip Age", stats["last_block_age"]),
            ("Mempool", f"{stats['mempool']} tx ({stats['orphans']} orphans)"),
            ("Peers", f"{stats['peers']} (in {stats['inbound']}/out {stats['outbound']})"),
            ("Sync State", stats["sync_state"]),
            ("RPC Uptime", stats["uptime"]),
            ("Wallet", "enabled" if stats["wallet_enabled"] else "disabled"),
            ("NTP", stats["ntp_status"]),
        ]
        label_width = max(len(label) for label, _ in rows)
        value_width = max(len(value) for _, value in rows)
        inner_width = label_width + 3 + value_width  # "label : value"
        title = " Baseline RPC Status "
        box_width = max(inner_width, len(title))
        border = "+" + "-" * (box_width + 2) + "+"
        title_line = f"| {title.center(box_width)} |"
        content_lines = []
        for label, value in rows:
            line = f"{label.ljust(label_width)} : {value}"
            content_lines.append(f"| {line.ljust(box_width)} |")
        panel = "\n".join([border, title_line, border] + content_lines + [border])
        return panel.encode("utf-8")

    def _collect_status_metrics(self) -> dict[str, Any]:
        state_db = self.handlers.state_db
        best = state_db.get_best_tip()
        height = best[1] if best else 0
        best_hash = best[0] if best else self.handlers.chain.genesis_hash
        best_header = state_db.get_header(best_hash)
        now = time.time()
        if best_header:
            age_seconds = max(0, now - best_header.timestamp)
            last_block_age = self._format_duration(age_seconds)
        else:
            last_block_age = "n/a"
        with self.handlers.mempool.lock:
            mempool_size = len(self.handlers.mempool.entries)
            orphan_size = len(self.handlers.mempool.orphans)
        network = getattr(self.handlers, "network", None)
        peers = len(network.peers) if network else 0
        outbound = sum(1 for peer in network.peers.values() if peer.outbound) if network else 0
        inbound = max(0, peers - outbound)
        sync_state = "offline"
        if network:
            if network.header_sync_active:
                sync_state = f"headers ({network.sync_remote_height:,})"
            elif network.sync_active:
                sync_state = f"blocks ({network.sync_remote_height:,})"
            else:
                sync_state = "steady"
        uptime = self._format_duration(now - self._start_time)
        wallet_enabled = self.handlers.wallet is not None
        ntp = self.handlers.time_manager.get_sync_status() if self.handlers.time_manager else None
        ntp_status = "disabled"
        if ntp:
            if ntp["synchronized"]:
                ntp_status = f"sync'd ({ntp['offset']:+.3f}s)"
            else:
                ntp_status = f"drift {ntp['offset']:+.3f}s"
        short_hash = f"{best_hash[:16]}...{best_hash[-4:]}" if len(best_hash) > 24 else best_hash
        return {
            "height": height,
            "best_block": short_hash,
            "last_block_age": last_block_age,
            "mempool": mempool_size,
            "orphans": orphan_size,
            "peers": peers,
            "inbound": inbound,
            "outbound": outbound,
            "sync_state": sync_state,
            "uptime": uptime,
            "wallet_enabled": wallet_enabled,
            "ntp_status": ntp_status,
        }

    def _format_duration(self, seconds: float) -> str:
        seconds = int(seconds)
        days, rem = divmod(seconds, 86400)
        hours, rem = divmod(rem, 3600)
        minutes, secs = divmod(rem, 60)
        parts = []
        if days:
            parts.append(f"{days}d")
        if hours:
            parts.append(f"{hours}h")
        if minutes:
            parts.append(f"{minutes}m")
        if not parts:
            parts.append(f"{secs}s")
        return " ".join(parts)
