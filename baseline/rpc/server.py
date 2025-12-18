"""
Minimal JSON-RPC server with HTTP framing and basic authentication.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
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
            headers = await self._read_headers(reader)
            if headers is None:
                await self._write_response(writer, 400, b"Bad Request", b"")
                return
            if method.upper() != "POST":
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
