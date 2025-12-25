"""RPC helpers shared by wallet interfaces."""

from __future__ import annotations

import base64
import http.client
import json
from typing import Any


class RPCError(Exception):
    """Base exception for RPC problems."""


class RPCRequestError(RPCError):
    """Raised for HTTP-level failures."""

    def __init__(self, status: int, body: str):
        super().__init__(f"RPC HTTP error {status}: {body}")
        self.status = status
        self.body = body


class RateLimitError(RPCRequestError):
    """Raised when the RPC server rejects the request due to rate limiting."""


class RPCResponseError(RPCError):
    """Raised when the JSON-RPC response includes an error."""

    def __init__(self, code: int | None, message: str | None):
        super().__init__(f"RPC error {code}: {message}")
        self.code = code
        self.message = message


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
            if response.status == 429:
                raise RateLimitError(response.status, body)
            raise RPCRequestError(response.status, body)
        data = json.loads(body)
        error = data.get("error")
        if error:
            raise RPCResponseError(error.get("code"), error.get("message"))
        return data["result"]
