# Mining & Payout Operations

Baseline ships with a Stratum v1 server plus a payout tracker so you can operate a pool without extra daemons.

## Stratum Server

- Enabled automatically when `baseline-node` starts.
- Listens on `stratum.host:stratum.port` (defaults to `0.0.0.0:3333`).
- Accepts any `username:password`; usernames identify workers for accounting.
- Implements vardiff: `stratum.min_difficulty`, `vardiff_window`, and `session_timeout` control share targets and session expiry.

### Connecting Miners

```
# ASIC/FPGA example
bfgminer -o stratum+tcp://pool.example.org:3333 -u worker01 -p x

# Reference CPU miner (bundled)
baseline-miner --config config.json --attempts-per-template 500000
```

When a worker submits a share above the network difficulty, the Stratum server calls `TemplateBuilder` to assemble the solved block and hands it to the chain.

## Reward Accounting

`baseline/mining/payout.py` keeps a JSON ledger at `data_dir/payouts/ledger.json`. The flow is:

1. **Share submission** → `record_share(worker_id, address, difficulty)` accumulates per-worker virtual shares.
2. **Block found** → `record_block(height, coinbase_txid, reward)` snapshots shares, subtracts pool fee (`mining.pool_fee_percent`), and adds the entry to `pending_blocks`.
3. **Maturity** → `process_maturity(best_height)` waits `mining.coinbase_maturity` blocks (20 by default) before moving pending rewards to worker balances. Fees + rounding dust accumulate in `pool_balance`.
4. **Payout transaction** → `create_payout_transaction(state_db)` sweeps matured coinbase UTXOs into a multi-output transaction once enough workers exceed `mining.min_payout` (denominated in liners). The tx is signed with `mining.pool_private_key` and broadcast through the mempool.

### Ledger Anatomy

```json
    {
      "workers": {
        "worker01": {
          "address": "NExampleAddr...",
          "script": "76a9...88ac",
          "balance": 250000000
        }
      },
  "round_shares": {
    "worker01": 1234.0
  },
  "pending_blocks": [
    {
      "height": 4200,
      "txid": "...",
      "total_reward": 5000000000,
      "distributable": 4950000000,
      "pool_fee": 50000000,
      "shares": {"worker01": 1234.0, "worker02": 432.0},
      "time": 1734567890.0
    }
  ],
  "matured_utxos": [
    {"txid": "...", "amount": 5000000000}
  ],
  "pool_balance": 100000000
}
```

Monitor this file (or expose it via tooling) to audit payouts.

## Coinbase Maturity & Wallet Integration

- Coinbase outputs require `mining.coinbase_maturity` confirmations (20 by default) before being spendable.
- Once matured, payouts call into the wallet/mempool; set `min_payout` high enough to avoid dust and keep transactions under ~100 kB.
- To monitor rewards, import the pool’s private key into a watch-only wallet or decode the payout transaction via `listtransactions`.

## Operational Tips

- **Dedicated payout key**: keep `pool_private_key` offline. Use the wallet CLI to generate WIF backups.
- **Worker registration**: Stratum auto-registers workers when the first share arrives, using the address they provide in the mining protocol.
- **Fee accounting**: `pool_balance` grows with fees and leftover liners. Periodically sweep it to the operator wallet by crafting a manual transaction.
- **Tx fees**: payout transactions include a flat 5,000 liner fee (matching the network relay floor). Bump it manually (editing `PayoutTracker.tx_fee`) if mempools get congested.
