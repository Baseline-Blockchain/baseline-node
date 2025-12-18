"""
Message framing and helpers for the Baseline P2P protocol.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from typing import Any

MAX_PAYLOAD = 8 * 1024 * 1024
LEN_FIELD = 4
CHECKSUM_FIELD = 4

AGENT = "Baseline/0.1"


class ProtocolError(Exception):
    """Raised when inbound messages fail validation."""


def sha256d(data: bytes) -> bytes:
    return hashlib.sha256(hashlib.sha256(data).digest()).digest()


def encode_message(message: dict[str, Any]) -> bytes:
    payload = json.dumps(message, separators=(",", ":"), sort_keys=True).encode("utf-8")
    if len(payload) > MAX_PAYLOAD:
        raise ProtocolError("Message payload too large")
    checksum = sha256d(payload)[:CHECKSUM_FIELD]
    length = len(payload).to_bytes(LEN_FIELD, "big")
    return length + checksum + payload


async def read_message(reader: asyncio.StreamReader, timeout: float = 10.0) -> dict[str, Any]:
    header = await asyncio.wait_for(reader.readexactly(LEN_FIELD + CHECKSUM_FIELD), timeout)
    length = int.from_bytes(header[:LEN_FIELD], "big")
    if length <= 0 or length > MAX_PAYLOAD:
        raise ProtocolError(f"Invalid length {length}")
    checksum = header[LEN_FIELD:]
    payload = await asyncio.wait_for(reader.readexactly(length), timeout)
    if sha256d(payload)[:CHECKSUM_FIELD] != checksum:
        raise ProtocolError("Checksum mismatch")
    try:
        message = json.loads(payload.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise ProtocolError("Invalid JSON payload") from exc
    if not isinstance(message, dict):
        raise ProtocolError("Message must be a JSON object")
    if "type" not in message:
        raise ProtocolError("Missing message type")
    return message


def version_payload(
    *,
    network_id: str,
    services: int,
    height: int,
    listen_port: int,
    agent: str = AGENT,
) -> dict[str, Any]:
    return {
        "type": "version",
        "version": 1,
        "network": network_id,
        "services": services,
        "timestamp": int(time.time()),
        "height": height,
        "port": listen_port,
        "agent": agent,
        "nonce": int.from_bytes(os.urandom(8), "big"),
    }


def verack_payload() -> dict[str, Any]:
    return {"type": "verack"}


def ping_payload(nonce: int | None = None) -> dict[str, Any]:
    if nonce is None:
        nonce = int.from_bytes(os.urandom(8), "big")
    return {"type": "ping", "nonce": nonce}


def pong_payload(nonce: int) -> dict[str, Any]:
    return {"type": "pong", "nonce": nonce}


def inv_payload(items: list[dict[str, str]]) -> dict[str, Any]:
    return {"type": "inv", "items": items}


def getdata_payload(items: list[dict[str, str]]) -> dict[str, Any]:
    return {"type": "getdata", "items": items}


def getheaders_payload(locator: list[str], stop: str | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {"type": "getheaders", "locator": locator}
    if stop:
        payload["stop"] = stop
    return payload


def headers_payload(headers: list[dict[str, int]]) -> dict[str, Any]:
    return {"type": "headers", "headers": headers}


def tx_payload(txid: str, raw_hex: str) -> dict[str, Any]:
    return {"type": "tx", "txid": txid, "raw": raw_hex}


def block_payload(block_hash: str, raw_hex: str) -> dict[str, Any]:
    return {"type": "block", "hash": block_hash, "raw": raw_hex}


def addr_payload(peers: list[dict[str, Any]]) -> dict[str, Any]:
    return {"type": "addr", "peers": peers}


def getblocks_payload(locator: list[str], stop: str | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {"type": "getblocks", "locator": locator}
    if stop:
        payload["stop"] = stop
    return payload
