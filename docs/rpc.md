# JSON-RPC API

Baseline exposes a Bitcoin-like JSON-RPC server over HTTP with Basic Auth. Configure credentials in `config.rpc`. The server listens on `rpc.host:rpc.port` (default `127.0.0.1:8832`).

## Request Format

```bash
curl --user baseline-rpc:changeme \
     --data '{"jsonrpc":"2.0","id":1,"method":"getblockchaininfo","params":[]}' \
     http://127.0.0.1:8832/
```

Responses follow the standard `{"result": ..., "error": null, "id": ...}` pattern. Failures return numeric codes aligned with Bitcoin Core where practical (e.g., `-32601` method not found, `-26` transaction rejected).

## Chain & Network Methods

| Method               | Description |
|----------------------|-------------|
| `getblockhash height` | Return the hash string for a main-chain height. |
| `getblock hash [verbose]` | Fetch a block as raw hex (`verbose=false`) or decoded header/tx list. |
| `getblockchaininfo` | Summary of height, difficulty, chainwork, and disk usage. |
| `getrawtransaction txid [verbose]` | Search mempool/chain for a tx. |
| `gettxout txid vout [include_mempool]` | Inspect UTXO set or mempool outputs. |
| `sendrawtransaction hex` | Broadcast a signed transaction. |
| `getblocktemplate [caps]` | Return work for external miners (Stratum also uses this). |
| `submitblock hex` | Submit solved blocks. |
| `getnetworkinfo` | Peer counts, protocol version, relay fees. |
| `gettimesyncinfo` | Status of the built-in NTP client (enabled flag, offset, drift). |

## Wallet Methods (when wallet enabled)

| Method | Notes |
|--------|-------|
| `getnewaddress [label]` | Derive the next deterministic address. |
| `getbalance [account] [min_conf]` | Sum confirmed balance (ignores account parameter). |
| `listaddresses` | Dump address book entries (spendable + watch-only). |
| `listaddressbalances [min_conf]` | Per-address balances. |
| `listunspent [min] [max] [addresses]` | Filtered UTXO view. |
| `sendtoaddress address amount [comment] [comment_to] [options]` | Spend from wallet, optionally recording memo fields and specifying `{ "fromaddresses": [...], "changeaddress": "...", "fee": 0.0005 }`. |
| `gettransaction txid` | Wallet-specific metadata (amount, confirmations, memos). |
| `listtransactions [label] [count] [skip]` | Recent wallet activity. |
| `encryptwallet passphrase` | Permanently encrypt wallet seed; requires restart to unlock. |
| `walletpassphrase pass timeout` | Temporary unlock for `timeout` seconds. |
| `walletlock` | Force-lock encrypted wallet. |
| `dumpwallet path` | Export deterministic seed + address table for backup. |
| `importwallet path [rescan]` | Restore from a dump file. |
| `importaddress address [label] [rescan]` | Add watch-only entries. |
| `importprivkey wif [label] [rescan]` | Import individual keys (e.g., pool payout key). |
| `walletinfo` | Encryption + height status. |

## Error Handling

The handler raises `RPCError(code, message)` for user issues (bad params, invalid tx, locked wallet). Unexpected exceptions bubble up as `-32603 Internal error` with stack traces in `node.log`.

## Security Tips

- Bind RPC to localhost; use SSH tunnels or reverse proxies if remote control is required.
- Rotate credentials regularly and never share them with miners.
- Limit request size with `rpc.max_request_bytes` (defaults to 256 kB) to avoid DoS via giant payloads.
- Monitor logs for repeated authentication failures.

## Tooling

Two helper CLIs wrap RPC:

- `baseline-wallet` (`tools/wallet_cli.py`) – interactive wallet commands.
- `baseline-miner` (`tools/simple_miner.py`) – CPU reference miner.

Both respect `--config` and reuse the RPC credentials defined there.
