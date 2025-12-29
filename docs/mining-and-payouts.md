# Mining & Payout Operations

Baseline ships with a Stratum v1 server plus a payout tracker so you can operate a pool without extra daemons.

## Stratum Server

- Enabled automatically when `mining.pool_private_key` is configured; leave it `null` to run a validation-only node.
- Listens on `stratum.host:stratum.port` (defaults to `0.0.0.0:3333`).
- Requires each worker to provide a valid Baseline address (payout target) either as the username, the prefix of `username.worker`, or in the password field. Authorization fails if no address can be parsed. The remaining portion of the username (after `.` or `:`) is used purely for accounting.
- Implements vardiff: `stratum.min_difficulty`, `vardiff_window`, and `session_timeout` control share targets and session expiry. The node samples accepted shares over the last `vardiff_window` seconds and retunes per-worker difficulty toward a ~15 second share interval (with bounded step sizes) so low-hashrate workers stay connected without spamming the server.

### Connecting Miners

```
# cpuminer-multi (https://github.com/tpruvot/cpuminer-multi) using SHA256d
cpuminer-multi -a sha256d -o stratum+tcp://pool.example.org:3333 -u NExampleAddr.worker01 -p x
```

If your hardware or proxy cannot include the address in the username, set the username to a worker label (for example `worker01`) and pass the payout address via the Stratum password (`-p NExampleAddr`). The server tests both fields and authorizes only when it finds a valid Baseline address.

When a worker submits a share above the network difficulty, the Stratum server calls `TemplateBuilder` to assemble the solved block and hands it to the chain.

## Reward Accounting

`baseline/mining/payout.py` keeps a JSON ledger at `data_dir/payouts/ledger.json`. The flow is:

1. **Share submission** → `record_share(worker_id, address, difficulty)` accumulates per-worker virtual shares.
2. **Block found** → `record_block(height, coinbase_txid, reward)` snapshots shares, subtracts pool fee (`mining.pool_fee_percent`), and adds the entry to `pending_blocks`.
3. **Maturity** → `process_maturity(best_height)` waits `mining.coinbase_maturity` blocks (20 by default) before moving pending rewards to worker balances. Fees + rounding dust accumulate in `pool_balance`.
4. **Payout transaction** → `create_payout_transaction(state_db)` sweeps matured coinbase UTXOs into a multi-output transaction once enough workers exceed `mining.min_payout` (denominated in liners). The tx is signed with `mining.pool_private_key` and broadcast through the mempool. If the key is unset, this step (and the background payout task) is skipped entirely.

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
- **Validation-only mode**: set `mining.pool_private_key` to `null` (or remove it) when you need RPC + networking without the built-in pool; `getblocktemplate` will return `"Mining not available"` in this mode.
- **Worker registration**: Stratum auto-registers workers when the first share arrives, using the address they provide in the mining protocol.
- **Fee accounting**: `pool_balance` grows with fees and leftover liners. Periodically sweep it to the operator wallet by crafting a manual transaction.
- **Tx fees**: payout transactions measure their serialized size and include the policy-required fee (currently 5,000 liners/kB minimum). As mempool pressure rises and a larger transaction is crafted, the fee scales automatically instead of relying on a hardcoded constant.
