"""RPC helpers shared by wallet interfaces."""

from __future__ import annotations

import base64
import http.client
import json
from typing import Any


class RPCClient:
    """Thin HTTP client for Baseline JSON-RPC."""

    def __init__(self, host: str, port: int, username: str, password: str, timeout: float = 15.0):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.timeout = timeout

    def call(self, method: str, params: list[Any] | None = None) -> Any:
        payload = json.dumps(
            {
                "jsonrpc": "1.0",
                "id": "wallet-cli",
                "method": method,
                "params": params or [],
            }
        )
        auth_token = base64.b64encode(f"{self.username}:{self.password}".encode("utf-8")).decode("ascii")
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Basic {auth_token}",
        }
        conn = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
        conn.request("POST", "/", body=payload, headers=headers)
        response = conn.getresponse()
        body = response.read().decode("utf-8")
        if response.status != 200:
            raise SystemExit(f"RPC HTTP error {response.status}: {body}")
        data = json.loads(body)
        if data.get("error"):
            err = data["error"]
            raise SystemExit(f"RPC error {err.get('code')}: {err.get('message')}")
        return data["result"]
