# JSON-RPC API

Baseline exposes a Bitcoin-like JSON-RPC server over HTTP. The server listens on `config.rpc.host:config.rpc.port` (default `127.0.0.1:8832`).

## Authentication

- Status panel: `GET /` is unauthenticated but rate-limited; keep it on trusted networks.
- JSON-RPC: these read-only/public methods are served without Basic Auth: `getblockcount`, `getbestblockhash`, `getblockchaininfo`, `getblockhash`, `getblockheader`, `getblock`, `getrawtransaction`, `gettxout`, `getrichlist`, `getaddressutxos`, `getaddressbalance`, `getaddresstxids`, `estimatesmartfee`, `getmempoolinfo`, `sendrawtransaction`, `listscheduledtx`, `getschedule`, `getpoolstats`, `getpoolworkers`, `getpoolpendingblocks`, `getpoolmatured`, `getpoolpayoutpreview`, `getstratumsessions`. Any other method—or any batch containing a non-public method—requires Basic Auth (`rpc.username` / `rpc.password`).
- Wallet and mutating methods always require Basic Auth.

## Status Panel

Performing a `GET /` against the RPC endpoint (e.g. `http://127.0.0.1:8832/`) returns a plain-text dashboard instead of JSON. The panel shows the current height, best block hash, peer counts, mempool size, sync status, uptime, wallet + NTP status, and other quick diagnostics. The wallet row reflects background sync state (`ready`, `syncing`, processed height) so you can tell at a glance whether wallet RPC results are current.

## Request Format

```bash
curl --user baseline-rpc:changeme \
  --data '{"jsonrpc":"2.0","id":1,"method":"getblockchaininfo","params":[]}' \
  http://127.0.0.1:8832/
```

Responses follow the standard {"result": ..., "error": null, "id": ...} pattern. Failures return numeric codes aligned with Bitcoin Core where practical (e.g., -32601 method not found, -26 transaction rejected).

## Chain & Network Methods

| Method | Description |
|--------|-------------|
| `getblockcount` | Return the current main-chain height. |
| `getbestblockhash` | Return the hash of the tip block. |
| `getblockhash height` | Return the hash string for a main-chain height. |
| `getblock hash [verbose]` | Fetch a block as raw hex (`verbose=false`) or decoded header/tx list. Includes Core-compatible fields (`weight`, `mediantime`, `chainwork`, etc.). |
| `getblockheader hash [verbose]` | Header-only view with confirmations, chainwork, and an accurate `nTx` count so explorers can page without fetching the full block. |
| `getblockchaininfo` | Summary of height, difficulty, chainwork, disk usage, verification progress, pruning state. |
| `getblockstats hash_or_height [stats]` | Core-style per-block aggregates (fee percentiles, tx counts, subsidy, utxo deltas). Baseline computes these even without rolling indexes so transparent explorers don’t see “data unavailable.” |
| `getchaintxstats [nblocks] [blockhash]` | Rolling transaction throughput stats (window count, interval, txrate). |
| `getdifficulty` | Current PoW difficulty computed from header bits. |
| `getindexinfo` | Reports the status of always-on indexes (`txindex`, `addressindex`, etc.) so tooling can confirm historical lookups will succeed. |
| `getrawmempool [verbose]` | List txids (or detailed entries) currently in the mempool. |
| `getrawtransaction txid [verbose] [blockhash]` | Search mempool/chain for a tx; optional `blockhash` makes lookups O(1). Verbose replies expose per-input `value`/`value_liners` plus `fee`/`fee_liners` to keep explorer math accurate even without extra RPCs. |
| `gettxout txid vout [include_mempool]` | Inspect UTXO set or mempool outputs. |
| `gettxoutsetinfo [hash_type]` | Returns aggregate UTXO stats (height, txouts, total_amount, MuHash/hash_serialized_2 digests). |
| `sendrawtransaction hex` | Broadcast a signed transaction. |
| `getblocktemplate [caps]` | Return work for external miners (Stratum also uses this). |
| `submitblock hex` | Submit solved blocks. |
| `getnetworkinfo` | Peer counts, protocol version, relay fees. |
| `getpeerinfo` | Per-connection diagnostics (addr, version, latencies, bytes sent/received). |
| `getmininginfo` | Mining status (difficulty, blocks, pooled tx count, network hash estimate). |
| `getmempoolinfo` | Current mempool occupancy (tx count, bytes, min relay fee). |
| `getnettotals` | Aggregate bytes sent/received since node startup (needed by explorers' live bandwidth meters). |
| `estimatesmartfee target [mode]` | Samples current mempool fee densities and returns a conservative percentile (falls back to the min relay fee when empty) while preserving Bitcoin Core response shape. |
| `gettimesyncinfo` | Status of the built-in NTP client (enabled flag, offset, drift). |
| `uptime` | Seconds since the RPC server was instantiated (mirrors `bitcoind` uptime). |

## Address Index Helpers

Baseline ships with address/tx indexes enabled so block explorers can stay in sync without extra nodes.

| Method | Description |
|--------|-------------|
| `getaddressutxos {"addresses":[...]} ` | Returns spendable outputs for the provided addresses (liners + scripts + height). |
| `getaddressbalance {"addresses":[...]}` | Aggregate confirmed balance + total received (both in liners and BLINE). |
| `getaddresstxids {"addresses":[...], "include_height":true}` | Lists transaction ids touching any of the addresses. When `include_height` is `true` each entry becomes `{ "txid": "...", "height": 123, "blockhash": "..." }`, which is crucial for explorers that want to fetch block-aware details in one pass. |
| `getrichlist [count] [offset]` | Returns the richest addresses by current UTXO balance. Each entry includes `{ "address": "...", "balance_liners": 123, "balance": 1.23 }` sorted descending. Use `offset` to paginate. |

## Pool & Stratum (built-in)

| Method | Description |
|--------|-------------|
| `getpoolstats` | Summarize pool mode (fee %, min_payout, coinbase_maturity) plus pool balance, worker counts, pending/matured payouts, and Stratum listener/sessions info. |
| `getpoolworkers [offset] [limit] [include_zero]` | List workers with payout address, confirmed balance, and current round shares (paginated). |
| `getpoolpendingblocks` | Show pending block rewards awaiting maturity (height, txid, distributable, pool_fee, shares snapshot). |
| `getpoolmatured` | List matured coinbase UTXOs available for payouts (txid/vout/amount). |
| `getpoolpayoutpreview [max_outputs]` | Dry-run the payout builder to preview payees, spendable matured UTXOs, estimated fee/size, and change without mutating state. |
| `getstratumsessions` | Inspect live Stratum sessions (worker id/address, difficulty, share-rate estimate, idle time, stale/invalid counters, remote endpoint). |

## Wallet Methods (when wallet enabled)

| Method | Notes |
|--------|-------|
| getnewaddress [label] | Derive the next deterministic address. |
| getbalance [account] [min_conf] | Sum confirmed balance (ignores account parameter). |
| getreceivedbyaddress address [min_conf] | Total received on an address with at least min_conf confirmations. |
| listaddresses | Dump address book entries (spendable + watch-only). |
| listaddressbalances [min_conf] | Per-address balances. |
| listunspent [min] [max] [addresses] | Filtered UTXO view. |
| sendtoaddress address amount [comment] [comment_to] [options] | Spend from wallet, optionally recording memo fields and specifying `{ "fromaddresses": [...], "changeaddress": "...", "feerate": 0.00005 }` (BLINE/kB). Use `{ "fee": 0.001 }` for an explicit absolute fee override. |
| createscheduledtx address amount lock_time cancelable [options] | Reserve funds for a future lock-time by signing a transaction (returns `schedule_id`, `txid`, `lock_time`, `cancelable`, `raw_tx`). `lock_time` accepts a block height or UNIX timestamp; the GUI exposes a “Scheduled Date (UTC)” field and converts it to this value. Options match `sendtoaddress` (`fromaddresses`, `changeaddress`, `feerate`, `fee`). |
| listscheduledtx | List active schedules with lock_time, amount, status, cancelable flag, inputs, owner address, raw tx, and an optional `scheduled_at` UTC timestamp for verification. |
| getschedule schedule_id | return the same metadata as `listscheduledtx` but scoped to one schedule. |
| cancelscheduledtx schedule_id | Drop the pending scheduled transaction (must be cancelable) and broadcast the refund tx back to the wallet, updating the schedule’s status. |
| gettransaction txid | Wallet-specific metadata (amount, confirmations, memos). |
| listtransactions [label] [count] [skip] | Recent wallet activity. |
| rescanwallet | Clear cached wallet state and rescan the blockchain. |
| encryptwallet passphrase | Permanently encrypt wallet seed; wallet becomes locked and must be unlocked via `walletpassphrase`. |
| exportseed | Return the wallet deterministic seed (hex). Treat this like your private key; anyone with it can spend all funds. For encrypted wallets, the wallet must be unlocked first. |
| importseed seed [wipe_existing] | Replace the wallet deterministic seed (hex). By default `wipe_existing=true` clears addresses/transactions and restarts wallet scanning. For encrypted wallets, the wallet must be unlocked first. |
| walletpassphrase pass timeout | Temporary unlock for timeout seconds. |
| walletlock | Force-lock encrypted wallet. |
| dumpwallet path | Export deterministic seed + address table for backup. |
| importwallet path [rescan] | Restore from a dump file. |
| importaddress address [label] [rescan] | Add watch-only entries. |
| importprivkey wif [label] [rescan] | Import individual keys (e.g., pool payout key). |
| walletinfo | Encryption + height status plus `syncing`, `last_sync`, and `processed_height` fields that mirror the status panel. |

## Error Handling

The handler raises RPCError(code, message) for user issues (bad params, invalid tx, locked wallet). Unexpected exceptions bubble up as -32603 Internal error with stack traces in `node.log`.

## Security Tips

- Bind RPC to localhost; use SSH tunnels or reverse proxies if remote control is required.
- Rotate credentials regularly and never share them with miners.
- Limit request size with `rpc.max_request_bytes` (defaults to 256 kB) and take advantage of the built-in per-IP rate limiter/timeouts to keep untrusted clients from saturating the server. These knobs live under `[rpc]` in `config.json`.
- Loopback clients are exempt from the rate limiter by default so local explorers can make many parallel calls; set `rpc.rate_limit_exempt_loopback=false` to rate-limit localhost too.
- Monitor logs for repeated authentication failures.

## Tooling

Two helper CLIs wrap RPC:

- baseline-wallet (`wallet/cli.py`) - interactive wallet commands.
- rpc-stress (`tools/rpc_stress.py`) - lightweight load tester that opens multiple concurrent RPC clients and reports aggregate throughput/latencies.

Both respect --config and reuse the RPC credentials defined there.
