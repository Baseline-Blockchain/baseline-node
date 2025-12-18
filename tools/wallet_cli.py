#!/usr/bin/env python3
"""
Thin JSON-RPC wallet helper that wraps common operations in a friendly CLI.
"""

from __future__ import annotations

import argparse
import base64
import getpass
import http.client
import json
import os
import secrets
from pathlib import Path
from typing import Any

from baseline.core import crypto


def load_config_overrides(path: Path | None) -> dict[str, Any]:
    if not path:
        return {}
    cfg_path = Path(path).expanduser()
    if not cfg_path.exists():
        raise SystemExit(f"Config file {cfg_path} not found")
    data = json.loads(cfg_path.read_text(encoding="utf-8"))
    return data


class RPCClient:
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

    tx = sub.add_parser("tx", help="Inspect a wallet transaction")
    tx.add_argument("txid")

    lt = sub.add_parser("listtx", help="List recent wallet transactions")
    lt.add_argument("--label", default="*")
    lt.add_argument("--count", type=int, default=10)
    lt.add_argument("--skip", type=int, default=0)
    lt.add_argument("--watchonly", action="store_true")

    enc = sub.add_parser("encrypt", help="Encrypt the wallet with a passphrase")
    enc.add_argument("passphrase")

    unlock = sub.add_parser("unlock", help="Unlock encrypted wallet for limited time")
    unlock.add_argument("passphrase")
    unlock.add_argument("timeout", type=int)

    sub.add_parser("lock", help="Lock the wallet")

    sub.add_parser("dump", help="Export wallet backup").add_argument("path")
    import_wallet = sub.add_parser("importwallet", help="Import wallet backup")
    import_wallet.add_argument("path")
    import_wallet.add_argument("--rescan", action="store_true", default=False)

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

    return parser


def apply_config_defaults(args: argparse.Namespace) -> None:
    overrides = load_config_overrides(args.config)
    rpc_cfg = overrides.get("rpc", {})
    args.rpc_host = rpc_cfg.get("host", args.rpc_host)
    args.rpc_port = int(rpc_cfg.get("port", args.rpc_port))
    args.rpc_user = rpc_cfg.get("username", args.rpc_user)
    args.rpc_password = rpc_cfg.get("password", args.rpc_password)


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


def fetch_wallet_info(client: RPCClient) -> dict[str, Any]:
    return client.call("walletinfo")


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
            phrase1 = getpass.getpass("New wallet passphrase: ")
            phrase2 = getpass.getpass("Confirm passphrase: ")
            if phrase1 != phrase2 or not phrase1:
                raise SystemExit("Passphrases did not match or were empty.")
            client.call("encryptwallet", [phrase1])
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
        txid = client.call("sendtoaddress", [args.address, args.amount])
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
        client.call("encryptwallet", [args.passphrase])
        print("Wallet encrypted. Restart node to continue.")
    elif args.command == "unlock":
        client.call("walletpassphrase", [args.passphrase, args.timeout])
        print(f"Wallet unlocked for {args.timeout} seconds.")
    elif args.command == "lock":
        client.call("walletlock")
        print("Wallet locked.")
    elif args.command == "dump":
        path = os.path.abspath(args.path)
        result = client.call("dumpwallet", [path])
        print(f"Wallet dumped to {result}")
    elif args.command == "importwallet":
        client.call("importwallet", [os.path.abspath(args.path), args.rescan])
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
    else:
        parser.error(f"Unknown command {args.command}")


if __name__ == "__main__":
    main()
