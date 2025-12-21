"""Shared wallet helper utilities."""

from __future__ import annotations

from typing import Any

from .client import RPCClient


def fetch_wallet_info(client: RPCClient) -> dict[str, Any]:
    return client.call("walletinfo")
