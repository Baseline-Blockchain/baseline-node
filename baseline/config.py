"""
Configuration helpers for the Baseline node.

The config loader prefers deterministic defaults, then merges user provided JSON
configuration files and environment overrides prefixed with ``BASELINE_``.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


class ConfigError(Exception):
    """Raised when configuration validation fails."""


def _expand_path(value: str) -> Path:
    return Path(os.path.expandvars(os.path.expanduser(value))).resolve()


def default_data_dir() -> Path:
    base = Path(os.getenv("BASELINE_DATA", Path.home() / ".baseline"))
    return _expand_path(str(base))


def _default_rpc_worker_threads() -> int:
    cpu_count = os.cpu_count() or 4
    return max(8, cpu_count * 2)


def _default_rpc_batch_concurrency() -> int:
    cpu_count = os.cpu_count() or 4
    return max(8, cpu_count)


@dataclass(slots=True)
class P2PConfig:
    host: str = "0.0.0.0"
    port: int = 9333
    seeds: tuple[str, ...] = ()
    dns_seeds: tuple[str, ...] = ()
    max_peers: int = 64
    min_peers: int = 8
    target_outbound: int = 8
    handshake_timeout: float = 5.0
    idle_timeout: float = 90.0

    def validate(self) -> None:
        if not (1 <= self.port <= 65535):
            raise ConfigError(f"Invalid P2P port {self.port}")
        if self.max_peers < self.min_peers:
            raise ConfigError("max_peers must be >= min_peers")
        for seed in self.dns_seeds:
            if not isinstance(seed, str) or not seed.strip():
                raise ConfigError("dns_seeds entries must be non-empty strings")


@dataclass(slots=True)
class RPCConfig:
    host: str = "127.0.0.1"
    port: int = 8832
    username: str = "rpcuser"
    password: str = "rpcpass"
    max_request_bytes: int = 256_000
    request_timeout: float = 15.0
    worker_threads: int = field(default_factory=_default_rpc_worker_threads)
    max_batch_size: int = 32
    max_batch_concurrency: int = field(default_factory=_default_rpc_batch_concurrency)
    # 0 disables rate limiting; wallets tend to burst calls, so keep this high by default.
    max_requests_per_minute: int = 5000
    rate_limit_exempt_loopback: bool = True

    def validate(self) -> None:
        if not (1 <= self.port <= 65535):
            raise ConfigError(f"Invalid RPC port {self.port}")
        if not self.username or not self.password:
            raise ConfigError("RPC username/password must be set")
        if self.max_request_bytes <= 0:
            raise ConfigError("max_request_bytes must be positive")
        if self.request_timeout <= 0:
            raise ConfigError("request_timeout must be positive")
        if self.worker_threads <= 0:
            raise ConfigError("worker_threads must be positive")
        if self.max_batch_size <= 0:
            raise ConfigError("max_batch_size must be positive")
        if self.max_batch_concurrency <= 0:
            raise ConfigError("max_batch_concurrency must be positive")
        if self.max_requests_per_minute < 0:
            raise ConfigError("max_requests_per_minute must be >= 0")


@dataclass(slots=True)
class StratumConfig:
    host: str = "0.0.0.0"
    port: int = 3333
    min_difficulty: float = 4.0
    vardiff_window: int = 30
    session_timeout: float = 120.0
    max_jobs: int = 8

    def validate(self) -> None:
        if not (1 <= self.port <= 65535):
            raise ConfigError(f"Invalid stratum port {self.port}")
        if self.min_difficulty <= 0:
            raise ConfigError("min_difficulty must be > 0")
        if self.vardiff_window <= 0:
            raise ConfigError("vardiff_window must be > 0")
        if self.max_jobs <= 0:
            raise ConfigError("max_jobs must be positive")


@dataclass(slots=True)
class NTPConfig:
    enabled: bool = True
    servers: tuple[str, ...] = (
        "pool.ntp.org",
        "time.nist.gov",
        "time.google.com",
        "time.cloudflare.com"
    )
    sync_interval: float = 300.0  # seconds
    timeout: float = 5.0  # seconds
    max_servers: int = 3
    max_offset_warning: float = 60.0  # seconds

    def validate(self) -> None:
        if self.sync_interval <= 0:
            raise ConfigError("sync_interval must be > 0")
        if self.timeout <= 0:
            raise ConfigError("timeout must be > 0")
        if self.max_servers <= 0:
            raise ConfigError("max_servers must be > 0")
        if not self.servers:
            raise ConfigError("At least one NTP server must be configured")


@dataclass(slots=True)
class StorageConfig:
    # Durability/performance tradeoffs (defaults are safest, but slower for IBD).
    # - blockstore_fsync_interval=N fsyncs every N appended blocks
    # - sqlite_synchronous: OFF | NORMAL | FULL | EXTRA
    blockstore_fsync_interval: int = 1
    sqlite_synchronous: str = "FULL"
    # Automatically speed up initial sync by temporarily relaxing durability settings
    # while the node is far behind the best seen peer height, then restoring them.
    auto_fast_ibd: bool = True
    fast_blockstore_fsync_interval: int = 50
    fast_sqlite_synchronous: str = "NORMAL"
    auto_fast_ibd_check_interval: float = 5.0
    auto_fast_ibd_enable_delta: int = 200
    auto_fast_ibd_disable_delta: int = 10
    auto_fast_ibd_min_seconds_between_toggles: float = 30.0

    def validate(self) -> None:
        if self.blockstore_fsync_interval <= 0:
            raise ConfigError("storage.blockstore_fsync_interval must be > 0")
        if self.fast_blockstore_fsync_interval <= 0:
            raise ConfigError("storage.fast_blockstore_fsync_interval must be > 0")
        if self.auto_fast_ibd_check_interval <= 0:
            raise ConfigError("storage.auto_fast_ibd_check_interval must be > 0")
        if self.auto_fast_ibd_enable_delta < 0:
            raise ConfigError("storage.auto_fast_ibd_enable_delta must be >= 0")
        if self.auto_fast_ibd_disable_delta < 0:
            raise ConfigError("storage.auto_fast_ibd_disable_delta must be >= 0")
        if self.auto_fast_ibd_min_seconds_between_toggles < 0:
            raise ConfigError("storage.auto_fast_ibd_min_seconds_between_toggles must be >= 0")
        if self.auto_fast_ibd_disable_delta > self.auto_fast_ibd_enable_delta and self.auto_fast_ibd_enable_delta > 0:
            raise ConfigError("storage.auto_fast_ibd_disable_delta must be <= storage.auto_fast_ibd_enable_delta")

        mode = str(self.sqlite_synchronous).upper()
        if mode not in {"OFF", "NORMAL", "FULL", "EXTRA"}:
            raise ConfigError("storage.sqlite_synchronous must be one of OFF, NORMAL, FULL, EXTRA")
        self.sqlite_synchronous = mode
        fast_mode = str(self.fast_sqlite_synchronous).upper()
        if fast_mode not in {"OFF", "NORMAL", "FULL", "EXTRA"}:
            raise ConfigError("storage.fast_sqlite_synchronous must be one of OFF, NORMAL, FULL, EXTRA")
        self.fast_sqlite_synchronous = fast_mode


DEFAULT_FOUNDATION_ADDRESS = "NMUrmCNAH5VUrjLSvM4ULu7eNtD1i8qcyK"


@dataclass(slots=True)
class MiningConfig:
    coinbase_maturity: int = 20
    block_interval_target: int = 20  # seconds
    retarget_interval: int = 20  # blocks
    # PoW limit (easiest allowed). This is also used for the genesis block bits field.
    pow_limit_bits: int = 0x207FFFFF
    # Starting difficulty for height 1, before LWMA has a meaningful window.
    # Chosen for a conservative launch difficulty (height 1).
    initial_bits: int = 0x207FFFFF
    subsidy_halving_interval: int = 4_158_884
    pool_fee_percent: float = 1.0
    pool_private_key: str | None = None
    min_payout: int = 50_000_000
    foundation_address: str = DEFAULT_FOUNDATION_ADDRESS
    allow_consensus_overrides: bool = False

    def validate(self) -> None:
        if self.coinbase_maturity <= 0:
            raise ConfigError("coinbase_maturity must be > 0")
        if self.block_interval_target <= 0:
            raise ConfigError("block_interval_target must be > 0")
        if self.pow_limit_bits <= 0:
            raise ConfigError("pow_limit_bits must be > 0")
        if self.initial_bits <= 0:
            raise ConfigError("initial_bits must be > 0")
        if not (0 < self.pool_fee_percent < 100):
            raise ConfigError("pool_fee_percent must be between 0 and 100")
        if self.min_payout <= 0:
            raise ConfigError("min_payout must be positive")
        if self.pool_private_key:
            _ = parse_pool_private_key(self.pool_private_key)
        try:
            from .core.address import script_from_address

            script_from_address(self.foundation_address)
        except Exception as exc:  # noqa: BLE001
            raise ConfigError("foundation_address must be a valid P2PKH address") from exc


@dataclass(slots=True)
class NodeConfig:
    network: P2PConfig = field(default_factory=P2PConfig)
    rpc: RPCConfig = field(default_factory=RPCConfig)
    stratum: StratumConfig = field(default_factory=StratumConfig)
    mining: MiningConfig = field(default_factory=MiningConfig)
    ntp: NTPConfig = field(default_factory=NTPConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    data_dir: Path = field(default_factory=default_data_dir)
    walletnotify: str | None = None
    log_file: Path | None = None

    def ensure_data_layout(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        for name in ("blocks", "chainstate", "peers", "logs"):
            (self.data_dir / name).mkdir(parents=True, exist_ok=True)
        if self.log_file is None:
            self.log_file = self.data_dir / "logs" / "node.log"

    def validate(self) -> None:
        self.network.validate()
        self.rpc.validate()
        self.stratum.validate()
        self.mining.validate()
        self.ntp.validate()
        self.storage.validate()
        if not isinstance(self.data_dir, Path):
            raise ConfigError("data_dir must be a Path")

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["data_dir"] = str(self.data_dir)
        if self.log_file is not None:
            data["log_file"] = str(self.log_file)
        return data


def load_config(path: Path | None = None, *, overrides: dict[str, Any] | None = None) -> NodeConfig:
    """Load configuration from disk and environment overrides."""

    def _merge(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
        for key, value in extra.items():
            if isinstance(value, dict) and isinstance(base.get(key), dict):
                base[key] = _merge(dict(base[key]), value)
            else:
                base[key] = value
        return base

    cfg_path = path or (_expand_path(os.getenv("BASELINE_CONFIG", str(default_data_dir() / "config.json"))))
    base: dict[str, Any] = {}
    if Path(cfg_path).exists():
        with open(cfg_path, "rb") as fh:
            base = json.load(fh)

    env_overrides: dict[str, Any] = {}
    prefix = "BASELINE_"
    for key, value in os.environ.items():
        if not key.startswith(prefix):
            continue
        trimmed = key[len(prefix) :]
        parts = trimmed.lower().split("__")
        target = env_overrides
        for part in parts[:-1]:
            target = target.setdefault(part, {})
        target[parts[-1]] = value

    if overrides:
        env_overrides = _merge(env_overrides, overrides)

    merged = _merge(base, env_overrides)
    config = NodeConfig()
    _apply_dict(config, merged)
    config.ensure_data_layout()
    config.validate()
    return config


def _apply_dict(obj: Any, data: dict[str, Any]) -> None:
    for key, value in data.items():
        if not hasattr(obj, key):
            raise ConfigError(f"Unknown config field {key}")
        current = getattr(obj, key)
        if isinstance(current, Path):
            setattr(obj, key, _expand_path(str(value)))
        elif isinstance(current, (P2PConfig, RPCConfig, StratumConfig, MiningConfig, NTPConfig, StorageConfig)):
            if not isinstance(value, dict):
                raise ConfigError(f"{key} must be a mapping")
            _apply_dict(current, value)
        elif isinstance(value, (str, os.PathLike)) and key.endswith(("dir", "file")):
            setattr(obj, key, _expand_path(str(value)))
        else:
            setattr(obj, key, _coerce_value(current, value))


def _coerce_value(current: Any, value: Any) -> Any:
    target_type = type(current)
    if target_type is bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes"}:
                return True
            if normalized in {"0", "false", "no"}:
                return False
            raise ConfigError(f"Invalid boolean value {value}")
        raise ConfigError(f"Cannot coerce {value!r} to bool")
    if target_type in {int, float}:
        try:
            return target_type(value)
        except (TypeError, ValueError) as exc:
            raise ConfigError(f"Invalid numeric value {value!r}") from exc
    if target_type is str:
        return str(value)
    if isinstance(current, tuple):
        if isinstance(value, str):
            items = [item.strip() for item in value.split(",") if item.strip()]
        else:
            items = list(value)
        return tuple(items)
    return value


def parse_pool_private_key(value: str) -> int:
    """Accept hex, decimal, or WIF-encoded private keys."""

    from .core import crypto  # Imported lazily to avoid circular dependency.

    normalized = value.strip()
    if normalized.startswith("0x"):
        normalized = normalized[2:]
    if all(ch in "0123456789abcdefABCDEF" for ch in normalized) and normalized:
        priv = int(normalized, 16)
    else:
        try:
            data = crypto.base58check_decode(value)
        except crypto.CryptoError as exc:
            if normalized.isdigit():
                priv = int(normalized, 10)
            else:
                raise ConfigError("pool_private_key is not valid hex, decimal, or WIF") from exc
        else:
            if not data:
                raise ConfigError("pool_private_key payload empty")
            version = data[0]
            if version not in (0x80, 0xef, 0x2f, 0x35):
                raise ConfigError("Unsupported pool_private_key WIF prefix")
            if len(data) not in (33, 34):
                raise ConfigError("Unexpected pool_private_key payload length")
            priv_bytes = data[1:33]
            priv = int.from_bytes(priv_bytes, "big")
    if not (1 <= priv < crypto.SECP_N):
        raise ConfigError("pool_private_key outside curve order")
    return priv
