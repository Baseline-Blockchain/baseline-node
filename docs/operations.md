# Operations & Maintenance

This guide collects day-two tasks for running a Baseline node in production.

## Process Lifecycle

- **Start**: `baseline-node --config /path/config.json --log-level info`. Consider systemd, launchd, or NSSM for auto-restart.
- **Stop**: send SIGINT/SIGTERM (Ctrl+C). The node drains Stratum, cancels async tasks, closes RPC, and fsyncs wallets/payout ledgers before exiting.
- **Restart after crash**: on startup the node replays append-only block files, runs `PRAGMA quick_check`, and rebuilds the mempool from scratch.

## Logging

- Default log file: `<data_dir>/logs/node.log`.
- Use `--log-level debug` during incident response.
- Integrate with `logrotate` (Linux) or `cronolog`; the file grows slowly but includes Stratum share logging, peer churn, payout events, and wallet actions.

## Backups

| Component  | Location                              | Strategy |
|-----------|----------------------------------------|---------|
| Wallet    | `<data_dir>/wallet/wallet.json`         | Run `baseline-wallet dump /secure/path/wallet-backup.json` after major changes. Encrypt with passphrase + store off-site. |
| Payouts   | `<data_dir>/payouts/ledger.json`        | Periodic rsync/snapshot; contains worker balances and pending rewards. |
| Chainstate| `<data_dir>/chainstate/state.sqlite3`   | Optional (rebuildable via sync). Snapshot while node offline to avoid WAL inconsistency. |
| Blocks    | `<data_dir>/blocks/`                    | Optional for archival nodes. |

For disaster recovery, keep wallet dump + pool private key (hex/WIF) offline, plus `config.json` and payout ledger.

## Resetting Chainstate

Need to resync from scratch without touching wallet/payout data? Run:

```bash
baseline-node --config config.json --reset-chainstate
```

This deletes `blocks/`, `chainstate/`, `peers/`, and `logs/` under the configured `data_dir`, then recreates the directory layout. Wallets, payouts, and custom configs remain untouched. Use this when:

- Upgrading from older builds that lacked the address index.
- Recovering after corruption or to force a clean sync.

Always stop the node first. If you want to keep logs, copy them out before running the reset command.

## Monitoring

- **RPC checks**: poll `getblockchaininfo`, `getnetworkinfo`, and `gettimesyncinfo` to ensure sync, peer counts, and NTP health.
- **Stratum**: alert on sudden drops in `workers` or zero shares logged; inspect `payouts/ledger.json` diff.
- **Time drift**: combine OS-level NTP with Baseline’s `gettimesyncinfo`. Large offsets (>60s) trigger warnings.
- **Disk usage**: track `<data_dir>` growth; append-only block files only grow, so plan for log pruning.

## Upgrades

1. Stop the node cleanly.
2. `git pull && pip install -e .` inside the virtualenv (or upgrade the wheel if packaged).
3. Run `ruff check` + `python -m unittest` if you carry patches.
4. Start the node and tail `node.log` for schema migrations or validation warnings.

## Troubleshooting

- **No peers**: ensure `network.seeds` lists at least one reachable host, and that DNS seeds resolve to public IPs (the resolver rejects `127.0.0.1/192.168.x.x`).
- **RPC 401**: verify `rpc.username/password` and that Basic Auth credentials are correct. Check `rpc.max_request_bytes` if sending large payloads.
- **Stratum idle**: miners may have abandoned the pool. Check firewall rules on port 3333 and confirm `session_timeout` is generous enough for your hardware.

## Maintenance Tasks

- Rotate payout ledger by archiving old worker entries once balances hit zero.
- Reset `known_peers.json` occasionally if you suspect stale addresses.
- Use the wallet CLI’s `rescan` options after restoring from backup to rebuild balances.
- Keep Python + OS security patches current; Baseline uses asyncio and sqlite so kernel bugs can manifest as hangs.
