"""
Difficulty and proof-of-work helpers.
"""

from __future__ import annotations


class DifficultyError(Exception):
    pass


def compact_to_target(bits: int) -> int:
    exponent = bits >> 24
    mantissa = bits & 0xFFFFFF
    if exponent <= 3:
        target = mantissa >> (8 * (3 - exponent))
    else:
        target = mantissa << (8 * (exponent - 3))
    return target


def target_to_compact(target: int) -> int:
    size = (target.bit_length() + 7) // 8
    if size <= 3:
        mantissa = target << (8 * (3 - size))
    else:
        mantissa = target >> (8 * (size - 3))
    if mantissa & 0x800000:
        mantissa >>= 8
        size += 1
    return (size << 24) | (mantissa & 0xFFFFFF)


def block_work(bits: int) -> int:
    target = compact_to_target(bits)
    if target <= 0:
        raise DifficultyError("Target must be positive")
    return (1 << 256) // (target + 1)


def check_proof_of_work(block_hash: str, bits: int) -> bool:
    hash_int = int.from_bytes(bytes.fromhex(block_hash)[::-1], "big")
    target = compact_to_target(bits)
    return hash_int <= target


def calculate_new_bits(prev_bits: int, actual_timespan: int, target_timespan: int, max_target: int) -> int:
    lower_bound = target_timespan // 4
    upper_bound = target_timespan * 4
    actual_timespan = max(lower_bound, min(actual_timespan, upper_bound))
    prev_target = compact_to_target(prev_bits)
    new_target = prev_target * actual_timespan // target_timespan
    new_target = min(new_target, max_target)
    return target_to_compact(new_target)
