# Centralized Exchange Integration Guide

Baseline’s JSON-RPC, transaction format, and wallet semantics intentionally mirror Bitcoin Core, so most exchange infrastructure can be reused with only minor tweaks. This document summarizes the pieces operators typically wire up when listing a new coin.

## Node & Network Basics

- **Recommended software**: `baseline-node` from this repository (Python 3.12+).
- **Ports**: P2P `9333`, RPC `8832` (configurable). Firewall the RPC port; expose only to trusted subnets / jump hosts. The node uses Basic Auth exactly like Bitcoin (`rpc.username` / `rpc.password` in `config.json`).
- **Data directory**: defaults to `~/.baseline`. Run on SSDs; expect tens of GB as the chain grows.
- **Systemd/service**: wrap `baseline-node --config /etc/baseline/config.json --log-level info` in your usual process manager. Enable auto-restart and monitor logs for consensus warnings.

## Configuration Checklist

```jsonc
{
  "data_dir": "./data",
  "network": {
    "seeds": [
      "109.104.154.151:9333"
    ]
  },
  "rpc": {
    "username": "localuser",
    "password": "localpass",
    "host": "127.0.0.1",
    "port": 8832
  },
  "stratum": {
    "host": "0.0.0.0",
    "port": 3333,
    "min_difficulty": 0.0001,
    "session_timeout": 120,
    "max_jobs": 8
  },
  "mining": {
    "pool_private_key": null,
    "min_payout": 100000000,
    "pool_fee_percent": 1.0
  }
}
```

- Enable `ntp` so timestamp drift alarms surface early.
- Leave `mining.pool_private_key` unset (`null`) if you are only validating; exchanges rarely need Stratum.

## RPC Endpoints to Integrate

These commands behave like their Bitcoin Core counterparts:

- **Deposits / monitoring**
  - `getblockchaininfo` – sync status, reorg detection.
  - `getblockhash`, `getblock`, `getrawtransaction` – transaction decoding for audit pipelines.
  - `getaddressutxos`, `getaddresstxids`, `getaddressbalance` – Baseline exposes address indexers out of the box; use them to track customer deposit addresses without extra daemons.
  - `getmempoolinfo`, `getrawmempool` – populate pending-deposit queues.

- **Wallet / withdrawals**
  - `getnewaddress` – allocate deposit and change addresses.
  - `listunspent` – build withdrawal inputs.
  - `sendtoaddress` / `sendmany` (via wallet CLI) – broadcast withdrawals; they accept the same parameters and error codes you use on Bitcoin.
  - `walletpassphrase`, `walletlock` - unlock/lock the wallet; the wallet uses PBKDF2-encrypted seeds with integrity checks.
  - `dumpwallet`, `importwallet`, `importprivkey` – cold/offline backups and recovery.

- **Operations**
  - `getnetworkinfo`, `getpeerinfo` – monitor connectivity.
  - `uptime`, `getnettotals` – capacity planning.
  - `estimatesmartfee` – picking withdrawal fees; denominated in BLINE.

Authentication errors, error codes, and JSON structure match Core’s conventions so existing client libraries (Haskell Haskoin, bitcoinlib, etc.) can usually talk to Baseline unchanged—just update RPC credentials and coin units.

## Wallet & Key Management

Baseline ships a deterministic HD wallet (`baseline-wallet`). Best practices for exchanges:

1. Run the hot wallet on the node box, encrypted (`baseline-wallet --config config.json setup --encrypt`).
2. Keep the passphrase in a secrets manager; only unlock just-in-time for withdrawals (`walletpassphrase <pass> <timeout>`).
3. Use `dumpwallet` for cold backups; store WIF exports offline.
4. For cold storage, generate addresses with `baseline-wallet generate-key`, record them in your custody system, and sweep manually when needed.

If you bring your own signing stack, you can disable the built-in wallet and rely on raw transaction RPCs (`createrawtransaction` + `sendrawtransaction` pattern). The node’s mempool/validation logic is independent of the wallet.

## Deposit Flow Pattern

1. Issue unique deposit addresses per user via `getnewaddress` (label them with account IDs).
2. Watch `getaddresstxids` or `getaddressutxos` for activity.
3. Require a confirmation threshold (Baseline defaults to 20-block coinbase maturity; typical exchanges use 20 confirmations for deposits to match that safety margin).
4. Credit user balances once the confirmation threshold passes.

## Withdrawal Flow Pattern

1. Aggregate pending withdrawals; compute desired fee rate (use `estimatesmartfee` or your own policy).
2. Call `sendtoaddress` (single) or craft a batched transaction via `baseline-wallet send --batch`.
3. Monitor returned TXIDs via `gettransaction` / `getrawtransaction` for status; surface to customer support dashboards.
4. If a transaction gets stuck, use RBF by recreating it with a higher fee (Baseline enforces standardness rules similar to Bitcoin; fee bumping is manual for now).

## Monitoring & Alerting

- Scrape the RPC status panel (`GET /` with Basic Auth) for quick metrics (height, peers, mempool, wallet sync).
- Parse logs in `data_dir/logs/node.log`; metrics include sync state, peer bans, and mempool issues.
- Set up alerts for:
  - `getblockchaininfo.initialblockdownload = true` for unexpected IBD.
  - Peer count below a threshold.
  - Divergent best hashes across redundant nodes.
  - Wallet sync lagging far behind chain height.

## Testing / Staging

- Run `baseline-node --config config.json --reset-chainstate` to wipe test deployments.
- Use the built-in Stratum pool (or any internal tooling on a private dev network) to generate mock funds.
- Mirror your production automation against a testnet cluster before mainnet rollout.

## Compatibility Notes

- Transaction format, scripts, sighash rules, and RPC behaviors match Bitcoin unless explicitly documented in [`docs/spec.md`](spec.md). Standard P2PKH only: no segwit or taproot yet.
- Currency units: **BLINE** is the coin; 1 BLINE = 100,000,000 liners (just like satoshis). RPC returns liner amounts in both integer and float form where applicable.
- Consensus parameters: 20-second blocks, per-block LWMA difficulty retarget, smooth exponential subsidy. Exchanges mostly need to be aware of the fast block cadence so confirmation counting aligns with risk appetite.

If your integration stack already supports Bitcoin-like coins, reusing those adapters should be straightforward—point them at Baseline’s RPC endpoint, ensure address formats (`N...` P2PKH) are accepted, and adjust confirmation defaults. For deeper protocol questions, consult `docs/spec.md` or reach out via the project issue tracker.
