"""
Shared RPC error types.
"""

from __future__ import annotations


class RPCError(Exception):
    """Raised when an RPC request cannot be satisfied."""

    def __init__(self, code: int, message: str):
        super().__init__(message)
        self.code = code
        self.message = message
