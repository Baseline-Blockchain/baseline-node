# Prerequisites

Baseline is designed to run anywhere Python 3.11+ is available. The node ships with no third-party runtime dependencies; stdlib modules cover networking, sqlite3 storage, and TLS-free RPC.

## Operating System

- **Linux** (Ubuntu/Debian, Fedora/CentOS, Alpine) – preferred for production pool or exchange deployments.
- **macOS** – fine for development/testing.
- **Windows 10/11** – supported; ensure long-running services use PowerShell or NSSM to daemonize.

## Python

- Python 3.11 or newer (3.11/3.12/3.13 tested).
- Install via your OS package manager or `pyenv`.
- Create a dedicated virtualenv per node: `python -m venv .venv && . .venv/bin/activate`.

## Hardware Sizing

| Workload                       | CPU                 | RAM | Disk                                        |
|--------------------------------|---------------------|-----|---------------------------------------------|
| Solo wallet / dev box          | 1 core @ 2 GHz      | 1G  | 1 GB SSD (fast fsync helpful)               |
| Public RPC + Stratum pool      | 2–4 cores @ 3 GHz   | 4G  | 10+ GB SSD (append-only blocks + backups)   |
| Explorer / archival (future)   | 4+ cores            | 8G  | 100 GB SSD (headroom for indexes, logs)     |

Disk layout created under `data_dir`:

```
data_dir/
 ├─ blocks/      # append-only block files (.dat/.idx)
 ├─ chainstate/  # SQLite chainstate (state.sqlite3)
 ├─ peers/       # persisted address book
 ├─ logs/        # node.log + rotated history
 ├─ payouts/     # Stratum payout ledger
 └─ wallet/      # wallet.json + backups
```

Keep the data directory on SSD/NVMe to avoid WAL/FSync stalls.

## Networking & Firewall

- Expose TCP `9333` for inbound P2P peers.
- Expose TCP `3333` for miners (Stratum).
- RPC (`8832`) should stay firewalled and protected by Basic Auth.
- Nodes never accept DNS seeds that resolve to localhost/private subnets; add explicit `network.seeds` entries for lab clusters.

## Time Synchronization

Enable kernel NTP/chrony where possible. Baseline can sync against public servers via its internal client (see `docs/configuration.md`) but system clock discipline is still required to avoid fork penalties.
