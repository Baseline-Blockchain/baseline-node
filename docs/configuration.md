# Configuration Guide

Baseline loads `config.json`, fills in deterministic defaults from `NodeConfig`, then applies environment overrides (prefixed `BASELINE_`). Every field ultimately lives under these sections:

- `data_dir`: absolute path that stores blocks, chainstate, peers, logs, wallet, payouts. Defaults to `~/.baseline` unless overridden.
- `network`: peer-to-peer settings.
- `rpc`: JSON-RPC listener + auth.
- `stratum`: built-in Stratum v1 pool.
- `mining`: consensus + payout parameters.
- `ntp`: time synchronization.

## Sample Config

```jsonc
{
  "data_dir": "~/baseline-node",
  "network": {
    "host": "0.0.0.0",
    "port": 9333,
    "seeds": [],
    "max_peers": 64,
    "min_peers": 8,
    "target_outbound": 8,
    "handshake_timeout": 5.0,
    "idle_timeout": 90
  },
  "rpc": {
    "host": "127.0.0.1",
    "port": 8832,
    "username": "baseline-rpc",
    "password": "use-a-long-random-pass",
    "max_request_bytes": 256000,
    "request_timeout": 15
  },
  "stratum": {
    "host": "0.0.0.0",
    "port": 3333,
    "min_difficulty": 4.0,
    "vardiff_window": 30,
    "session_timeout": 120,
    "max_jobs": 8
  },
  "mining": {
    "coinbase_maturity": 20,
    "block_interval_target": 20,
    "pow_limit_bits": 545259519,
    "initial_bits": 545259519,
    "subsidy_halving_interval": 4158884,
    "pool_fee_percent": 1.5,
    "pool_private_key": null,
    "min_payout": 100000000,
    "foundation_address": "NMUrmCNAH5VUrjLSvM4ULu7eNtD1i8qcyK",
    "allow_consensus_overrides": false
  },
  "ntp": {
    "enabled": true,
    "servers": [
      "pool.ntp.org",
      "time.nist.gov",
      "time.google.com",
      "time.cloudflare.com"
    ],
    "sync_interval": 300,
    "timeout": 5,
    "max_servers": 3,
    "max_offset_warning": 60
  }
}
```

### Environment Overrides

Any config key can be overwritten via env vars using `BASELINE_<section>__<field>=value`. Examples:

```bash
# point to alternate config file entirely
export BASELINE_CONFIG=/etc/baseline/config.json

# set RPC credentials without editing disk
export BASELINE_RPC__USERNAME=baseline
export BASELINE_RPC__PASSWORD=$(openssl rand -hex 16)

# run dev nodes out of ./data-dev
export BASELINE_DATA_DIR=./data-dev
```

Tuple/list values accept comma-delimited strings (`BASELINE_NETWORK__SEEDS="node1:9333,node2:9333"`). Paths expand `~` and environment variables automatically.

`subsidy_halving_interval` now represents the *half-life* of the smooth exponential subsidy curve. Every `subsidy_halving_interval` blocks (~4.16 million by default, or ~2.6 years) the per-block reward falls by 50% without a discrete cliff, and the geometric decay converges to ~300 million BLINE total supply.

`foundation_address` sets the pay-to-pubkey-hash address that receives the 1% Foundation allocation from every subsidy. Update this to the Foundation-controlled private key before launching a public network; every validator must use the same address or the chain will fork (the value is part of the consensus lock).

### Time Sync

- `ntp.enabled`: toggles the asynchronous NTP client. Disable if the host already enforces strict kernel time discipline and you do not want outbound UDP traffic.
- `ntp.servers`: prioritized list; Baseline queries up to `max_servers` concurrently per sync cycle.
- `ntp.sync_interval`: seconds between background sync attempts.
- `ntp.timeout`: per-server socket timeout.
- `ntp.max_offset_warning`: log warning threshold (seconds).

`baseline-node` exposes `gettimesyncinfo` over RPC so operators can monitor offsets and drift.

### Consensus Overrides

Consensus-critical values (`coinbase_maturity`, `block_interval_target`, `initial_bits`, `subsidy_halving_interval`, `foundation_address`) are **locked** on mainnet. The node compares your config against the compiled defaults and refuses to start if they differ. This prevents accidental forks or runaway inflation.

- `mining.allow_consensus_overrides`: defaults to `false`. Setting it to `true` suppresses the safety check and logs a warning, allowing you to run bespoke devnets or integration tests. Never enable it on public networks; doing so will cause your node to diverge from the canonical chain.

### Pool Key Handling

Leaving `mining.pool_private_key` set to `null` (or omitting it entirely) disables the built-in Stratum server and payout tracker; the node will still validate blocks and serve RPC but mining endpoints return `"Mining not available"`.

`mining.pool_private_key` accepts:

- Hex (with or without `0x` prefix).
- Decimal integers.
- WIF (compressed/uncompressed, mainnet/testnet prefixes).

The value controls the payout UTXO key; keep it offline and import via `baseline-wallet importprivkey` only when sweeping funds.

### Log File

If `log_file` is omitted, the node writes `logs/node.log` under `data_dir`. Override with an absolute path or use `BASELINE_LOG_FILE=/var/log/baseline/node.log` and hook it into logrotate.

Always run `baseline-node --config path/to/config.json --log-level info` to verify the parsed config at startup. Validation errors throw `ConfigError` before touching disk.
