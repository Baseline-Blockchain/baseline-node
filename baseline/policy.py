"""
Policy rules applied before admitting transactions to the mempool.
"""

from __future__ import annotations

from .core.tx import Transaction

MIN_RELAY_FEE_RATE = 5_000  # liners per kB
MAX_STANDARD_TX_SIZE = 100_000
MAX_SCRIPT_SIG_SIZE = 1_650
MAX_SCRIPTPUBKEY_SIZE = 10_000


class PolicyError(Exception):
    """Raised when a transaction violates relay policy."""


def required_fee(size: int, fee_rate: int = MIN_RELAY_FEE_RATE) -> int:
    return (size * fee_rate + 999) // 1_000


def is_p2pkh(script_pubkey: bytes) -> bool:
    return (
        len(script_pubkey) == 25
        and script_pubkey[0] == 0x76
        and script_pubkey[1] == 0xA9
        and script_pubkey[2] == 0x14
        and script_pubkey[-2] == 0x88
        and script_pubkey[-1] == 0xAC
    )


def check_standard_tx(tx: Transaction, fee: int, size: int, min_fee_rate: int = MIN_RELAY_FEE_RATE) -> None:
    if size > MAX_STANDARD_TX_SIZE:
        raise PolicyError("Transaction exceeds standard size limit")
    if tx.is_coinbase():
        raise PolicyError("Coinbase transactions are not relayed")
    for txin in tx.inputs:
        if len(txin.script_sig) > MAX_SCRIPT_SIG_SIZE:
            raise PolicyError("scriptSig too large")
    for txout in tx.outputs:
        if len(txout.script_pubkey) > MAX_SCRIPTPUBKEY_SIZE:
            raise PolicyError("scriptPubKey too large")
        if not is_p2pkh(txout.script_pubkey):
            raise PolicyError("Non-standard scriptPubKey")
    min_fee = required_fee(size, min_fee_rate)
    if fee < min_fee:
        raise PolicyError("Insufficient fee")
