"""
Bitcoin-style script interpreter (restricted to P2PKH) for Baseline.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from . import crypto

if TYPE_CHECKING:
    from .tx import Transaction


MAX_SCRIPT_SIZE = 10_000
MAX_STACK_ITEMS = 1_000
SIG_MAX_SIZE = 72


class ScriptError(Exception):
    pass


OP_DUP = 0x76
OP_HASH160 = 0xA9
OP_EQUALVERIFY = 0x88
OP_CHECKSIG = 0xAC


def parse_script(data: bytes) -> list:
    if len(data) > MAX_SCRIPT_SIZE:
        raise ScriptError("Script too large")
    i = 0
    program = []
    while i < len(data):
        opcode = data[i]
        i += 1
        if opcode <= 75:
            if i + opcode > len(data):
                raise ScriptError("PUSHDATA exceeds script length")
            program.append(data[i : i + opcode])
            i += opcode
        else:
            program.append(opcode)
    return program


@dataclass
class ExecutionContext:
    transaction: Transaction
    input_index: int


def run_script(script_sig: bytes, script_pubkey: bytes, ctx: ExecutionContext) -> bool:
    stack: list[bytes] = []
    alt_programs = [parse_script(script_sig), parse_script(script_pubkey)]
    for program in alt_programs:
        for op in program:
            if isinstance(op, bytes):
                _push(stack, op)
                continue
            if op == OP_DUP:
                if not stack:
                    raise ScriptError("OP_DUP empty stack")
                _push(stack, stack[-1])
            elif op == OP_HASH160:
                if not stack:
                    raise ScriptError("OP_HASH160 empty stack")
                _push(stack, crypto.hash160(stack.pop()))
            elif op == OP_EQUALVERIFY:
                if len(stack) < 2:
                    raise ScriptError("OP_EQUALVERIFY requires two items")
                a = stack.pop()
                b = stack.pop()
                if a != b:
                    raise ScriptError("OP_EQUALVERIFY failed")
            elif op == OP_CHECKSIG:
                if len(stack) < 2:
                    raise ScriptError("OP_CHECKSIG requires pubkey+sig")
                pubkey = stack.pop()
                sig = stack.pop()
                if len(sig) < 2:
                    raise ScriptError("Signature too short")
                sighash_type = sig[-1]
                signature = sig[:-1]
                if sighash_type != 0x01:
                    raise ScriptError("Only SIGHASH_ALL supported")
                msg_hash = ctx.transaction.signature_hash(ctx.input_index, script_pubkey, sighash_type)
                if not crypto.verify(msg_hash, signature, pubkey):
                    raise ScriptError("Signature check failed")
                _push(stack, b"\x01")
            else:
                raise ScriptError(f"Unsupported opcode {op}")
    if not stack:
        return False
    return stack.pop() not in (b"", b"\x00")


def _push(stack: list[bytes], value: bytes) -> None:
    if len(stack) >= MAX_STACK_ITEMS:
        raise ScriptError("Stack overflow")
    if len(value) > 520 and isinstance(value, bytes):
        raise ScriptError("Push size limit exceeded")
    stack.append(value)
