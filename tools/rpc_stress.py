"""
Simple RPC stress tester that hammers the JSON-RPC endpoint with concurrent clients.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import time
from collections.abc import Sequence


async def rpc_request(
    host: str,
    port: int,
    auth: str,
    payload: dict[str, object],
) -> tuple[int, float]:
    body = json.dumps(payload).encode("utf-8")
    reader, writer = await asyncio.open_connection(host, port)
    request = (
        "POST / HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        "Content-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n"
        f"Authorization: Basic {auth}\r\n"
        "\r\n"
    ).encode("utf-8") + body
    start = time.perf_counter()
    writer.write(request)
    await writer.drain()
    status_line = await reader.readline()
    parts = status_line.decode("utf-8").split()
    status_code = int(parts[1])
    headers: dict[str, str] = {}
    while True:
        line = await reader.readline()
        if line in (b"\r\n", b"\n", b""):
            break
        name, value = line.decode("utf-8").split(":", 1)
        headers[name.strip().lower()] = value.strip()
    content_length = int(headers.get("content-length", "0"))
    if content_length:
        await reader.readexactly(content_length)
    writer.close()
    await writer.wait_closed()
    duration = time.perf_counter() - start
    return status_code, duration


async def worker(
    name: int,
    iterations: int,
    host: str,
    port: int,
    auth: str,
    method: str,
    params: Sequence[object],
    stats: dict[str, list[float] | int],
) -> None:
    payload = {"jsonrpc": "2.0", "id": name, "method": method, "params": list(params)}
    errors = 0
    latencies: list[float] = []
    for _ in range(iterations):
        status, duration = await rpc_request(host, port, auth, payload)
        if status != 200:
            errors += 1
        latencies.append(duration)
    stats["errors"] += errors
    stats["latencies"].extend(latencies)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Hammer the JSON-RPC endpoint with concurrent requests.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8832)
    parser.add_argument("--username", default="rpcuser")
    parser.add_argument("--password", default="rpcpass")
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--requests", type=int, default=200, help="Total requests to send")
    parser.add_argument("--method", default="getblockcount")
    parser.add_argument("--params", default="[]", help="JSON array of RPC params")
    args = parser.parse_args()

    try:
        params = json.loads(args.params)
        if not isinstance(params, list):
            raise ValueError
    except ValueError as exc:
        raise SystemExit("--params must be a JSON array") from exc

    auth = base64.b64encode(f"{args.username}:{args.password}".encode("utf-8")).decode("ascii")
    per_worker = max(1, args.requests // args.concurrency)
    stats = {"errors": 0, "latencies": []}  # type: ignore[var-annotated]
    tasks = [
        worker(i, per_worker, args.host, args.port, auth, args.method, params, stats)
        for i in range(args.concurrency)
    ]
    start = time.perf_counter()
    await asyncio.gather(*tasks)
    elapsed = time.perf_counter() - start
    latencies = stats["latencies"]  # type: ignore[assignment]
    latencies.sort()
    total = len(latencies)
    p50 = latencies[int(total * 0.5)] if total else 0.0
    p95 = latencies[int(total * 0.95)] if total else 0.0
    print(f"Completed {total} requests in {elapsed:.2f}s ({total / max(0.0001, elapsed):.1f} rps)")
    print(f"Errors: {stats['errors']}")
    print(f"Latency p50={p50*1000:.1f}ms p95={p95*1000:.1f}ms max={latencies[-1]*1000:.1f}ms" if latencies else "No samples")


if __name__ == "__main__":
    asyncio.run(main())
