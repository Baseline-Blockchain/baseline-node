"""
Minimal JSON-RPC server with HTTP framing and basic authentication.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hmac
import ipaddress
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from urllib.parse import parse_qs, urlsplit

from ..config import NodeConfig
from ..core.tx import COIN
from .handlers import RPCError, RPCHandlers
from .rendering import DashboardRenderer


class _TokenBucket:
    """Simple token bucket used for per-client rate limiting."""

    __slots__ = ("capacity", "tokens", "refill_rate", "updated")

    def __init__(self, capacity: int, refill_rate: float):
        self.capacity = max(1, capacity)
        self.tokens = float(self.capacity)
        self.refill_rate = refill_rate
        self.updated = time.monotonic()

    def consume(self, amount: float = 1.0) -> bool:
        now = time.monotonic()
        elapsed = now - self.updated
        self.updated = now
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        if self.tokens >= amount:
            self.tokens -= amount
            return True
        return False


class RPCServer:
    def __init__(self, config: NodeConfig, handlers: RPCHandlers):
        self.config = config
        self.handlers = handlers
        self.log_prefix = "baseline.rpc"
        self.host = config.rpc.host
        self.port = config.rpc.port
        self.max_request_bytes = config.rpc.max_request_bytes
        self.request_timeout = config.rpc.request_timeout
        self.username = config.rpc.username
        self.password = config.rpc.password
        self.server: asyncio.AbstractServer | None = None
        self._stop_event = asyncio.Event()
        self._start_time = time.time()
        self._worker_threads = max(1, config.rpc.worker_threads)
        self._max_batch_size = config.rpc.max_batch_size
        self._max_batch_concurrency = config.rpc.max_batch_concurrency
        self._max_requests_per_minute = config.rpc.max_requests_per_minute
        self._rate_limit_exempt_loopback = config.rpc.rate_limit_exempt_loopback
        self._executor: ThreadPoolExecutor | None = None
        self._rate_limits: dict[str, _TokenBucket] = {}
        self._body_chunk = 64 * 1024
        self.dashboard_renderer = DashboardRenderer()
        self.public_methods = {
            "getblockcount",
            "getbestblockhash",
            "getblockchaininfo",
            "getblockhash",
            "getblockheader",
            "getblock",
            "getrawtransaction",
            "gettxout",
            "getrichlist",
            "getaddressutxos",
            "getaddressbalance",
            "getaddresstxids",
            "estimatesmartfee",
            "getmempoolinfo",
            "sendrawtransaction",
            "listscheduledtx",
            "getschedule",
            "getpoolstats",
            "getpoolworkers",
            "getpoolpendingblocks",
            "getpoolmatured",
            "getpoolpayoutpreview",
            "getstratumsessions",
        }

    async def start(self) -> None:
        if self.server:
            return
        if self._executor is None:
            # Bump worker count relative to available CPUs for better throughput
            max_workers = self._worker_threads or max(4, (os.cpu_count() or 4))
            self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="baseline-rpc")
        self._stop_event.clear()
        self.server = await asyncio.start_server(self._handle_client, self.host, self.port)

    async def stop(self) -> None:
        if not self.server:
            return
        self._stop_event.set()
        self.server.close()
        await self.server.wait_closed()
        self.server = None
        self._rate_limits.clear()
        if self._executor:
            self._executor.shutdown(wait=False)
            self._executor = None

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername") or ("unknown", 0)
        client_ip = peer[0] if isinstance(peer, tuple) else str(peer)
        try:
            while not self._stop_event.is_set():
                request_line = await self._read_line(reader)
                if request_line is None:
                    break
                if not request_line.strip():
                    continue
                parts = request_line.decode("utf-8", "replace").strip().split()
                if len(parts) != 3:
                    await self._write_response(writer, 400, b"Bad Request", b"", keep_alive=False)
                    break
                method, raw_path, version = parts
                method = method.upper()
                parsed_path = urlsplit(raw_path)
                path = parsed_path.path or "/"
                headers = await self._read_headers(reader)
                if headers is None:
                    await self._write_response(writer, 400, b"Bad Request", b"", keep_alive=False)
                    break
                client_keep_alive = self._client_wants_keep_alive(headers, version)
                auth_header = headers.get("authorization")
                if method == "GET":
                    if not self._consume_rate_limit(client_ip):
                        await self._write_response(
                            writer,
                            429,
                            b"Too Many Requests",
                            b"Rate limit exceeded",
                            keep_alive=False,
                            extra_headers={"Retry-After": "1"},
                        )
                        break
                    loop = asyncio.get_running_loop()
                    if path == "/pool":
                        if self._executor:
                            panel = await loop.run_in_executor(
                                self._executor, 
                                self._render_pool_panel, 
                                parsed_path, 
                                headers.get("host")
                            )
                        else:
                             panel = self._render_pool_panel(parsed_path, headers.get("host"))
                    else:
                        # Status panel is also somewhat heavy
                        if self._executor:
                            panel = await loop.run_in_executor(
                                self._executor,
                                self._render_status_panel
                            )
                        else:
                            panel = self._render_status_panel()

                    await self._write_response(
                        writer,
                        200,
                        b"OK",
                        panel,
                        keep_alive=client_keep_alive,
                        content_type="text/html; charset=utf-8" if path == "/pool" else "text/plain; charset=utf-8",
                    )
                    if not client_keep_alive:
                        break
                    continue
                if method != "POST":
                    await self._write_response(writer, 405, b"Method Not Allowed", b"", keep_alive=False)
                    break
                if not self._consume_rate_limit(client_ip):
                    await self._write_response(
                        writer,
                        429,
                        b"Too Many Requests",
                        b"Rate limit exceeded",
                        keep_alive=False,
                        extra_headers={"Retry-After": "1"},
                    )
                    break
                try:
                    content_length = int(headers.get("content-length", "0"))
                except ValueError:
                    await self._write_response(writer, 411, b"Length Required", b"", keep_alive=False)
                    break
                if content_length < 0 or content_length > self.max_request_bytes:
                    await self._write_response(writer, 413, b"Request Entity Too Large", b"", keep_alive=False)
                    break
                body = await self._read_body(reader, content_length)
                if body is None:
                    await self._write_response(writer, 400, b"Bad Request", b"", keep_alive=False)
                    break
                try:
                    req_obj = json.loads(body.decode("utf-8"))
                except json.JSONDecodeError:
                    response_payload = json.dumps(self._error_response(None, -32700, "Parse error")).encode("utf-8")
                    await self._write_response(
                        writer,
                        200,
                        b"OK",
                        response_payload,
                        keep_alive=client_keep_alive,
                        content_type="application/json",
                    )
                    if not client_keep_alive:
                        break
                    continue

                # Auth gate only if required
                if self._request_requires_auth(req_obj) and not self._check_auth(auth_header):
                    await self._write_response(
                        writer,
                        401,
                        b"Unauthorized",
                        b"",
                        keep_alive=False,
                        extra_headers={"WWW-Authenticate": 'Basic realm="baseline"'},
                    )
                    break

                # Now process normally
                response_payload = await self._handle_payload(body)
                await self._write_response(
                    writer,
                    200,
                    b"OK",
                    response_payload,
                    keep_alive=client_keep_alive,
                    content_type="application/json",
                )
                if not client_keep_alive:
                    break
        except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError, ConnectionAbortedError):
            await self._write_response(writer, 400, b"Bad Request", b"", keep_alive=False)
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
            if len(request) > self._max_batch_size:
                return json.dumps(self._error_response(None, -32600, "Batch too large")).encode("utf-8")
            responses = await self._handle_batch(request)
            return json.dumps(responses).encode("utf-8")
        return json.dumps(await self._handle_call(request)).encode("utf-8")

    async def _handle_batch(self, batch: list[Any]) -> list[dict[str, Any]]:
        if not batch:
            return []
        concurrency = max(1, self._max_batch_concurrency)
        semaphore = asyncio.Semaphore(concurrency)

        async def run(message: Any) -> dict[str, Any]:
            async with semaphore:
                return await self._handle_call(message)

        return await asyncio.gather(*(run(entry) for entry in batch))

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
        loop = asyncio.get_running_loop()
        executor = self._executor
        if executor is None:
            return self._error_response(msg_id, -32603, "RPC executor unavailable")
        try:
            future = loop.run_in_executor(executor, self.handlers.dispatch, method, params)
            result = await asyncio.wait_for(future, timeout=self.request_timeout)
        except RPCError as exc:
            return self._error_response(msg_id, exc.code, exc.message)
        except TimeoutError:
            return self._error_response(msg_id, -32603, "RPC handler timed out")
        except Exception as exc:  # pragma: no cover - defensive
            return self._error_response(msg_id, -32603, f"Internal error: {exc}")
        return {"jsonrpc": "2.0", "result": result, "error": None, "id": msg_id}

    async def _read_line(self, reader: asyncio.StreamReader) -> bytes | None:
        try:
            line = await asyncio.wait_for(reader.readline(), timeout=self.request_timeout)
        except TimeoutError:
            return None
        if not line:
            return None
        if len(line) > self.max_request_bytes:
            return None
        return line

    async def _read_headers(self, reader: asyncio.StreamReader) -> dict[str, str] | None:
        headers: dict[str, str] = {}
        while True:
            try:
                line = await asyncio.wait_for(reader.readline(), timeout=self.request_timeout)
            except TimeoutError:
                return None
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

    async def _read_body(self, reader: asyncio.StreamReader, total: int) -> bytes | None:
        if total <= 0:
            return b""
        remaining = total
        chunks: list[bytes] = []
        while remaining > 0:
            chunk_size = min(self._body_chunk, remaining)
            try:
                chunk = await asyncio.wait_for(reader.read(chunk_size), timeout=self.request_timeout)
            except TimeoutError:
                return None
            if not chunk:
                return None
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def _request_requires_auth(self, req: object) -> bool:
        # If no allowlist configured, keep current behavior: everything needs auth
        if not self.public_methods:
            return True

        methods = []
        if isinstance(req, dict):
            m = req.get("method")
            if isinstance(m, str):
                methods.append(m)
        elif isinstance(req, list):
            for item in req:
                if isinstance(item, dict):
                    m = item.get("method")
                    if isinstance(m, str):
                        methods.append(m)

        # require auth if ANY requested method is not public
        return any(m not in self.public_methods for m in methods)

    def _check_auth(self, header: str | None) -> bool:
        if not header or not header.startswith("Basic "):
            return False
        token = header[6:]
        try:
            decoded = base64.b64decode(token).decode("utf-8")
        except Exception:
            return False
        username, _, password = decoded.partition(":")
        return hmac.compare_digest(username, self.username) and hmac.compare_digest(password, self.password)

    def _error_response(self, msg_id: Any, code: int, message: str) -> dict[str, Any]:
        return {"jsonrpc": "2.0", "result": None, "error": {"code": code, "message": message}, "id": msg_id}

    async def _write_response(
        self,
        writer: asyncio.StreamWriter,
        status_code: int,
        reason: bytes,
        body: bytes,
        *,
        keep_alive: bool = False,
        content_type: str = "text/plain",
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        headers = {
            "Content-Length": str(len(body)),
            "Content-Type": content_type,
            "Connection": "keep-alive" if keep_alive else "close",
        }
        if extra_headers:
            headers.update(extra_headers)
        status_line = f"HTTP/1.1 {status_code} {reason.decode('ascii')}\r\n".encode("ascii")
        writer.write(status_line)
        for name, value in headers.items():
            writer.write(f"{name}: {value}\r\n".encode("ascii"))
        writer.write(b"\r\n")
        writer.write(body)
        try:
            await writer.drain()
        except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError):
            # Client bailed; treat as a normal disconnect instead of crashing the server.
            return

    def _client_wants_keep_alive(self, headers: dict[str, str], version: str) -> bool:
        connection = (headers.get("connection") or "").lower()
        version = version.upper()
        if version == "HTTP/1.0":
            return connection == "keep-alive"
        if connection == "close":
            return False
        return True

    def _consume_rate_limit(self, client_ip: str) -> bool:
        if self._rate_limit_exempt_loopback:
            try:
                if ipaddress.ip_address(client_ip).is_loopback:
                    return True
            except ValueError:
                pass
        limit = self._max_requests_per_minute
        if limit <= 0:
            return True
        bucket = self._rate_limits.get(client_ip)
        if bucket is None:
            bucket = _TokenBucket(limit, limit / 60.0)
            self._rate_limits[client_ip] = bucket
        return bucket.consume()

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
            ("Wallet", stats["wallet_status"]),
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

    def _render_pool_panel(self, parsed_path, request_host: str | None = None) -> bytes:
        return self.dashboard_renderer.render(self.handlers, parsed_path.path, request_host)

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
        wallet_status = "disabled"
        if self.handlers.wallet:
            status = self.handlers.wallet.sync_status()
            phase = "syncing" if status["syncing"] else "ready"
            processed = status["processed_height"]
            wallet_status = f"{phase} ({processed:,}/{height:,})" if height else phase
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
            "wallet_status": wallet_status,
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

    def _format_hashrate(self, hashes: float) -> str:
        if hashes >= 1e18:
            return f"{hashes / 1e18:.2f} EH/s"
        if hashes >= 1e15:
            return f"{hashes / 1e15:.2f} PH/s"
        if hashes >= 1e12:
            return f"{hashes / 1e12:.2f} TH/s"
        if hashes >= 1e9:
            return f"{hashes / 1e9:.2f} GH/s"
        if hashes >= 1e6:
            return f"{hashes / 1e6:.2f} MH/s"
        if hashes >= 1e3:
            return f"{hashes / 1e3:.2f} kH/s"
        return f"{hashes:.2f} H/s"
