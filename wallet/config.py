"""Config helpers for wallet tools."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_config_overrides(path: Path | None) -> dict[str, Any]:
    if not path:
        return {}
    cfg_path = Path(path).expanduser()
    if not cfg_path.exists():
        raise SystemExit(f"Config file {cfg_path} not found")
    data = json.loads(cfg_path.read_text(encoding="utf-8"))
    return data


def apply_config_defaults(args: Any) -> None:
    overrides = load_config_overrides(args.config)
    rpc_cfg = overrides.get("rpc", {})
    args.rpc_host = rpc_cfg.get("host", args.rpc_host)
    args.rpc_port = int(rpc_cfg.get("port", args.rpc_port))
    args.rpc_user = rpc_cfg.get("username", args.rpc_user)
    args.rpc_password = rpc_cfg.get("password", args.rpc_password)
