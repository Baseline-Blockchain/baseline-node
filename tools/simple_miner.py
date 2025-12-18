#!/usr/bin/env python3
"""
Single-process reference miner that hits the JSON-RPC API for block templates.
Intended for local testing; use Stratum for production ASIC deployments.
"""

from __future__ import annotations

import argparse
import base64
import http.client
import json
import secrets
import time
from pathlib import Path
from typing import Any

from baseline.core import difficulty
from baseline.core.block import Block, BlockHeader, merkle_root_hash
from baseline.core.tx import Transaction


class RPCClient:
    def __init__(self, host: str, port: int, username: str, password: str, timeout: float = 15.0):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.timeout = timeout

    def call(self, method: str, params: list[Any] | None = None) -> Any:
        payload = json.dumps({"jsonrpc": "1.0", "id": "miner", "method": method, "params": params or []})
        auth_token = base64.b64encode(f"{self.username}:{self.password}".encode("utf-8")).decode("ascii")
        headers = {"Content-Type": "application/json", "Authorization": f"Basic {auth_token}"}
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
    parser = argparse.ArgumentParser(description="Baseline reference miner")
    parser.add_argument("--config", type=Path, help="Optional node config to read RPC creds")
    parser.add_argument("--rpc-host", default="127.0.0.1")
    parser.add_argument("--rpc-port", type=int, default=8832)
    parser.add_argument("--rpc-user", default="rpcuser")
    parser.add_argument("--rpc-password", default="rpcpass")
    parser.add_argument("--timeout", type=float, default=15.0)
    parser.add_argument("--attempts-per-template", type=int, default=2_000_000, help="Nonce attempts before refreshing template")
    parser.add_argument("--status-interval", type=int, default=250_000, help="Log progress every N nonces")
    return parser


def apply_config_defaults(args: argparse.Namespace) -> None:
    if not args.config:
        return
    cfg_path = Path(args.config).expanduser()
    if not cfg_path.exists():
        raise SystemExit(f"Config file {cfg_path} not found")
    data = json.loads(cfg_path.read_text(encoding="utf-8"))
    rpc_cfg = data.get("rpc", {})
    args.rpc_host = rpc_cfg.get("host", args.rpc_host)
    args.rpc_port = int(rpc_cfg.get("port", args.rpc_port))
    args.rpc_user = rpc_cfg.get("username", args.rpc_user)
    args.rpc_password = rpc_cfg.get("password", args.rpc_password)


def assemble_block(template: dict, extranonce: bytes, nonce: int, timestamp: int) -> Block:
    coinb1 = bytes.fromhex(template["coinb1"])
    coinb2 = bytes.fromhex(template["coinb2"])
    coinbase_serialized = coinb1 + extranonce + coinb2
    coinbase_tx = Transaction.parse(coinbase_serialized)
    txs = [coinbase_tx]
    for entry in template.get("transactions", []):
        txs.append(Transaction.parse(bytes.fromhex(entry["data"])))
    merkle_root = merkle_root_hash(txs)
    header = BlockHeader(
        version=template["version"],
        prev_hash=template["previousblockhash"],
        merkle_root=merkle_root,
        timestamp=timestamp,
        bits=int(template["bits"], 16),
        nonce=nonce,
    )
    return Block(header=header, transactions=txs)


def mine_once(client: RPCClient, attempts: int, status_interval: int) -> None:
    template = client.call("getblocktemplate")
    extranonce_size = template.get("extranonce", {}).get("size", 8)
    extranonce = secrets.token_bytes(extranonce_size)
    base_time = max(template.get("curtime", int(time.time())), int(time.time()))
    nonce = secrets.randbelow(0xFFFFFFFF)
    for attempt in range(attempts):
        timestamp = max(base_time, int(time.time()))
        block = assemble_block(template, extranonce, nonce, timestamp)
        if difficulty.check_proof_of_work(block.block_hash(), block.header.bits):
            block_hex = block.serialize().hex()
            result = client.call("submitblock", [block_hex])
            print(f"Solved block {block.block_hash()} status={result['status']} height={result.get('height')}")
            return
        if attempt and attempt % status_interval == 0:
            print(f"Checked {attempt:,} nonces... still searching (height {template['height']})")
        nonce = (nonce + 1) & 0xFFFFFFFF
        if nonce == 0:
            extranonce = secrets.token_bytes(extranonce_size)
    print("Refreshing template, no solution found.")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    apply_config_defaults(args)
    client = RPCClient(args.rpc_host, args.rpc_port, args.rpc_user, args.rpc_password, args.timeout)
    while True:
        mine_once(client, args.attempts_per_template, args.status_interval)


if __name__ == "__main__":
    main()
