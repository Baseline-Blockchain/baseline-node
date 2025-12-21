#!/usr/bin/env python3
"""Wallet CLI entrypoint."""

from __future__ import annotations

import argparse
import getpass
import json
import os
import secrets
from pathlib import Path
from typing import Any

from baseline.core import crypto

from .client import RPCClient
from .config import apply_config_defaults
from .helpers import fetch_wallet_info


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Baseline wallet CLI")
    parser.add_argument("--config", type=Path, help="Path to node config JSON (optional)")
    parser.add_argument("--rpc-host", default="127.0.0.1")
    parser.add_argument("--rpc-port", type=int, default=8832)
    parser.add_argument("--rpc-user", default="rpcuser")
    parser.add_argument("--rpc-password", default="rpcpass")
    parser.add_argument("--timeout", type=float, default=15.0)

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("generate-key", help="Generate a fresh 32-byte hex key + address for mining payouts")

    sub.add_parser("newaddress", help="Generate a new receiving address").add_argument("label", nargs="?", default="")

    sub.add_parser("listaddresses", help="List wallet-managed (and watch-only) addresses")

    sub.add_parser("balance", help="Show wallet balance").add_argument("--minconf", type=int, default=1)

    balances = sub.add_parser("balances", help="Show balance for each wallet address")
    balances.add_argument("--minconf", type=int, default=1)

    lu = sub.add_parser("listunspent", help="List spendable UTXOs")
    lu.add_argument("--minconf", type=int, default=1)
    lu.add_argument("--maxconf", type=int, default=9999999)
    lu.add_argument("--address", action="append", dest="addresses", help="Filter by address (repeatable)")

    setup = sub.add_parser("setup", help="Initialize wallet UX flow")
    setup.add_argument("--encrypt", action="store_true", help="Prompt to encrypt wallet if not already encrypted")

    send = sub.add_parser("send", help="Send coins to an address")
    send.add_argument("address")
    send.add_argument("amount", type=float)
    send.add_argument("--passphrase", help="Wallet passphrase (omit to be prompted)")
    send.add_argument("--unlock-time", type=int, default=90, help="Seconds to keep wallet unlocked for signing")
    send.add_argument("--from-address", action="append", dest="from_addresses", help="Restrict inputs to this address (repeatable)")
    send.add_argument("--change-address", help="Force change to return to this wallet address")
    send.add_argument("--fee", type=float, help="Custom fee in BLINE (defaults to 0.00001)")

    tx = sub.add_parser("tx", help="Inspect a wallet transaction")
    tx.add_argument("txid")

    lt = sub.add_parser("listtx", help="List recent wallet transactions")
    lt.add_argument("--label", default="*")
    lt.add_argument("--count", type=int, default=10)
    lt.add_argument("--skip", type=int, default=0)
    lt.add_argument("--watchonly", action="store_true")

    enc = sub.add_parser("encrypt", help="Encrypt the wallet with a passphrase")
    enc.add_argument("passphrase", nargs="?", help="Provide passphrase or omit to be prompted")

    unlock = sub.add_parser("unlock", help="Unlock encrypted wallet for limited time")
    unlock.add_argument("passphrase")
    unlock.add_argument("timeout", type=int)

    sub.add_parser("lock", help="Lock the wallet")

    dump = sub.add_parser("dump", help="Export wallet backup")
    dump.add_argument("path")
    dump.add_argument("--passphrase", help="Wallet passphrase (omit to prompt if encrypted)")
    dump.add_argument(
        "--unlock-time",
        type=int,
        default=120,
        help="Seconds to keep wallet unlocked while exporting",
    )

    import_wallet = sub.add_parser("importwallet", help="Import wallet backup")
    import_wallet.add_argument("path")
    import_wallet.add_argument("--rescan", action="store_true", default=False)
    import_wallet.add_argument("--passphrase", help="Wallet passphrase (omit to prompt if encrypted)")
    import_wallet.add_argument("--unlock-time", type=int, default=180, help="Seconds to keep wallet unlocked during import")

    ia = sub.add_parser("importaddress", help="Import watch-only address")
    ia.add_argument("address")
    ia.add_argument("--label", default="")
    ia.add_argument("--rescan", action="store_true", default=False)

    ipk = sub.add_parser("importprivkey", help="Import a WIF private key")
    ipk.add_argument("key")
    ipk.add_argument("--label", default="")
    ipk.add_argument("--rescan", action="store_true", default=False)
    ipk.add_argument("--passphrase", help="Wallet passphrase (omit to prompt if encrypted)")
    ipk.add_argument("--unlock-time", type=int, default=300)

    sub.add_parser("rescan", help="Force a full wallet rescan")

    return parser


def generate_key() -> None:
    priv_bytes = secrets.token_bytes(32)
    priv_hex = priv_bytes.hex()
    priv_int = int.from_bytes(priv_bytes, "big")
    pubkey = crypto.generate_pubkey(priv_int)
    address = crypto.address_from_pubkey(pubkey)
    wif_payload = b"\x80" + priv_bytes + b"\x01"
    wif = crypto.base58check_encode(wif_payload)
    print("Private key (hex):", priv_hex)
    print("Private key (WIF):", wif)
    print("Payout address   :", address)


def prompt_new_passphrase() -> str:
    phrase1 = getpass.getpass("New wallet passphrase: ")
    phrase2 = getpass.getpass("Confirm passphrase: ")
    if phrase1 != phrase2 or not phrase1:
        raise SystemExit("Passphrases did not match or were empty.")
    return phrase1


def maybe_unlock_for_signing(client: RPCClient, passphrase: str | None, timeout: int) -> bool:
    info = fetch_wallet_info(client)
    if not info.get("encrypted"):
        return False
    if not info.get("locked"):
        return True
    phrase = passphrase or getpass.getpass("Wallet passphrase: ")
    if not phrase:
        raise SystemExit("Passphrase is required to unlock wallet")
    client.call("walletpassphrase", [phrase, timeout])
    return True


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "generate-key":
        generate_key()
        return
    if args.config:
        apply_config_defaults(args)
    client = RPCClient(args.rpc_host, args.rpc_port, args.rpc_user, args.rpc_password, args.timeout)

    if args.command == "setup":
        info = fetch_wallet_info(client)
        print(f"Wallet encrypted: {info['encrypted']}")
        print(f"Wallet locked   : {info['locked']}")
        print(f"Addresses       : {info['address_count']}")
        if args.encrypt and not info["encrypted"]:
            phrase = prompt_new_passphrase()
            client.call("encryptwallet", [phrase])
            print("Wallet encrypted. Restart node before continuing.")
        elif info["encrypted"] and info["locked"]:
            print("Wallet is encrypted and locked; use 'unlock' or provide --passphrase when sending.")
        return
    elif args.command == "newaddress":
        result = client.call("getnewaddress", [args.label] if args.label else [])
        print(result)
    elif args.command == "listaddresses":
        result = client.call("listaddresses", [])
        print(json.dumps(result, indent=2))
    elif args.command == "balance":
        params = [] if args.minconf == 1 else [None, args.minconf]
        result = client.call("getbalance", params)
        print(result)
    elif args.command == "balances":
        result = client.call("listaddressbalances", [args.minconf])
        print(json.dumps(result, indent=2))
    elif args.command == "listunspent":
        params = [args.minconf, args.maxconf]
        if args.addresses:
            params.append(args.addresses)
        result = client.call("listunspent", params)
        print(json.dumps(result, indent=2))
    elif args.command == "send":
        unlocked = maybe_unlock_for_signing(client, args.passphrase, args.unlock_time)
        options: dict[str, Any] = {}
        if args.from_addresses:
            options["fromaddresses"] = args.from_addresses
        if args.change_address:
            options["changeaddress"] = args.change_address
        if args.fee is not None:
            options["fee"] = args.fee
        params: list[Any] = [args.address, args.amount]
        if options:
            params.extend(["", "", options])
        txid = client.call("sendtoaddress", params)
        print(txid)
        if unlocked:
            client.call("walletlock")
    elif args.command == "tx":
        result = client.call("gettransaction", [args.txid])
        print(json.dumps(result, indent=2))
    elif args.command == "listtx":
        result = client.call("listtransactions", [args.label, args.count, args.skip, args.watchonly])
        print(json.dumps(result, indent=2))
    elif args.command == "encrypt":
        phrase = args.passphrase or prompt_new_passphrase()
        client.call("encryptwallet", [phrase])
        print("Wallet encrypted. Restart node to continue.")
    elif args.command == "unlock":
        client.call("walletpassphrase", [args.passphrase, args.timeout])
        print(f"Wallet unlocked for {args.timeout} seconds.")
    elif args.command == "lock":
        client.call("walletlock")
        print("Wallet locked.")
    elif args.command == "dump":
        path = os.path.abspath(args.path)
        unlocked = maybe_unlock_for_signing(client, args.passphrase, args.unlock_time)
        try:
            result = client.call("dumpwallet", [path])
            print(f"Wallet dumped to {result}")
        finally:
            if unlocked:
                client.call("walletlock")
    elif args.command == "importwallet":
        unlocked = maybe_unlock_for_signing(client, args.passphrase, args.unlock_time)
        try:
            client.call("importwallet", [os.path.abspath(args.path), args.rescan])
        finally:
            if unlocked:
                client.call("walletlock")
        print("Wallet imported.")
    elif args.command == "importaddress":
        client.call("importaddress", [args.address, args.label, args.rescan])
        print("Address imported.")
    elif args.command == "importprivkey":
        unlocked = maybe_unlock_for_signing(client, args.passphrase, args.unlock_time)
        client.call("importprivkey", [args.key, args.label, args.rescan])
        print("Private key imported.")
        if unlocked:
            client.call("walletlock")
    elif args.command == "rescan":
        client.call("rescanwallet", [])
        print("Wallet rescan started.")
    else:
        parser.error(f"Unknown command {args.command}")


if __name__ == "__main__":
    main()
