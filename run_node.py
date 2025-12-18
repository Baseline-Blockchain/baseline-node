"""
CLI entry point for running the Baseline node.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from pathlib import Path

from baseline.config import ConfigError, load_config
from baseline.logging import setup_logging
from baseline.node import BaselineNode


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Baseline full node")
    parser.add_argument("--config", type=Path, help="Path to config JSON")
    parser.add_argument("--data-dir", type=Path, help="Override data directory")
    parser.add_argument("--log-level", default="info", help="Log level (debug, info, warning, error)")
    parser.add_argument("--rpc-user", help="Override RPC username")
    parser.add_argument("--rpc-password", help="Override RPC password")
    return parser.parse_args()


def build_overrides(args: argparse.Namespace) -> dict:
    overrides = {}
    if args.data_dir:
        overrides["data_dir"] = str(args.data_dir)
    if args.rpc_user:
        overrides.setdefault("rpc", {})["username"] = args.rpc_user
    if args.rpc_password:
        overrides.setdefault("rpc", {})["password"] = args.rpc_password
    return overrides


def install_signal_handlers(node: BaselineNode) -> None:
    def _handler(signum, _frame) -> None:
        logging.getLogger("baseline").warning("Received signal %s", signum)
        node.request_shutdown()

    for sig_name in ("SIGINT", "SIGTERM"):
        if hasattr(signal, sig_name):
            signal.signal(getattr(signal, sig_name), _handler)


def main() -> None:
    args = parse_args()
    overrides = build_overrides(args)
    try:
        config_path = args.config.resolve() if args.config else None
    except AttributeError:
        config_path = None

    try:
        config = load_config(config_path, overrides=overrides)
    except ConfigError as exc:
        print(f"Config error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    logger = setup_logging(config.log_file or (config.data_dir / "logs" / "node.log"), level=log_level)
    node = BaselineNode(config)
    install_signal_handlers(node)
    logger.info("Node configuration loaded")
    try:
        asyncio.run(node.run())
    except KeyboardInterrupt:
        logger.warning("Interrupted, shutting down...")
    finally:
        node.close()


if __name__ == "__main__":
    main()
