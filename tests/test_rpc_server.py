import asyncio
import base64
import json
import time
import unittest

from baseline.config import NodeConfig
from baseline.rpc.server import RPCServer


class DummyHandlers:
    def __init__(self, delay: float = 0.0, result: object = None):
        self.delay = delay
        self.result = result if result is not None else {"status": "ok"}

    def dispatch(self, method: str, params: list[object]):
        if self.delay:
            time.sleep(self.delay)
        if method == "slow":
            time.sleep(self.delay or 0.0)
        return self.result


class RPCServerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self) -> None:
        if getattr(self, "server", None):
            await self.server.stop()
            self.server = None

    async def _start_server(self, handler: DummyHandlers, **rpc_overrides) -> None:
        config = NodeConfig()
        config.rpc.host = "127.0.0.1"
        config.rpc.port = 0
        for key, value in rpc_overrides.items():
            setattr(config.rpc, key, value)
        server = RPCServer(config, handler)
        await server.start()
        sock = next(iter(server.server.sockets))  # type: ignore[arg-type]
        self._rpc_host = config.rpc.host
        self._rpc_port = sock.getsockname()[1]
        self._rpc_auth = base64.b64encode(
            f"{config.rpc.username}:{config.rpc.password}".encode("utf-8")
        ).decode("ascii")
        self.server = server
        self.config = config

    async def _rpc_call(self, payload: dict[str, object]) -> tuple[int, bytes]:
        body = json.dumps(payload).encode("utf-8")
        reader, writer = await asyncio.open_connection(self._rpc_host, self._rpc_port)
        request = (
            "POST / HTTP/1.1\r\n"
            f"Host: {self._rpc_host}:{self._rpc_port}\r\n"
            "Content-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Authorization: Basic {self._rpc_auth}\r\n"
            "\r\n"
        ).encode("utf-8") + body
        writer.write(request)
        await writer.drain()
        status_line = await reader.readline()
        status_parts = status_line.decode("utf-8").split()
        status_code = int(status_parts[1])
        headers: dict[str, str] = {}
        while True:
            line = await reader.readline()
            if line in (b"\r\n", b"\n", b""):
                break
            name, value = line.decode("utf-8").split(":", 1)
            headers[name.strip().lower()] = value.strip()
        content_length = int(headers.get("content-length", "0"))
        body_bytes = await reader.readexactly(content_length) if content_length else b""
        writer.close()
        await writer.wait_closed()
        return status_code, body_bytes

    async def test_rate_limit_returns_429(self) -> None:
        handler = DummyHandlers()
        await self._start_server(handler, max_requests_per_minute=1)
        payload = {"jsonrpc": "2.0", "id": 1, "method": "ping", "params": []}
        status, _ = await self._rpc_call(payload)
        self.assertEqual(status, 200)
        status, _ = await self._rpc_call(payload)
        self.assertEqual(status, 429)

    async def test_slow_handler_hits_timeout(self) -> None:
        handler = DummyHandlers(delay=0.2)
        await self._start_server(handler, request_timeout=0.05)
        payload = {"jsonrpc": "2.0", "id": 99, "method": "slow", "params": []}
        status, body = await self._rpc_call(payload)
        self.assertEqual(status, 200)
        response = json.loads(body.decode("utf-8"))
        self.assertIsNotNone(response["error"])
        self.assertIn("timed out", response["error"]["message"])
