"""
Address helpers shared by wallet/mining subsystems.
"""

from __future__ import annotations

from . import crypto
from .tx import TxInput  # noqa: F401  # imported for type checkers


def script_from_address(address: str) -> bytes:
    payload = crypto.base58check_decode(address)
    if len(payload) != 21:
        raise ValueError("Unsupported address payload")
    version = payload[0]
    if version not in (0x35, 0x00):  # support Baseline + Bitcoin main prefixes
        raise ValueError("Unsupported address version")
    return b"\x76\xa9\x14" + payload[1:] + b"\x88\xac"


def address_from_script(script: bytes, version: int = 0x35) -> str | None:
    if len(script) == 25 and script[0] == 0x76 and script[1] == 0xA9 and script[2] == 0x14 and script[-2:] == b"\x88\xac":
        payload = bytes([version]) + script[3:-2]
        return crypto.base58check_encode(payload)
    return None
