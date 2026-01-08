# Scheduled Transactions

Baseline now ships with a *Scheduled Send* primitive that lets you lock in a transaction now and have its outputs release at a later block height or UTC timestamp. The feature works across CLI, RPC, and GUI clients without requiring special smart contracts: the wallet signs the final transaction up front, records the metadata in its wallet state file (`wallet.json`, under the `schedules` map), and broadcasts the signed tx to the mempool (or until you cancel it if `cancelable=true`).

Key behaviors:

- **Reservation guarantee**: Inputs stay reserved because the signed transaction sits in the node's mempool and the wallet rejects competing spends during the `pending` status. When the lock-time arrives the transaction is mined as any other tx.
- **Cancelable schedules**: If you need to reclaim funds, call `cancelscheduledtx`; the wallet drops the original mempool entry, builds a refund (paying the normal `required_fee`), and rebroadcasts it so funds return to the sender before the lock-time.
- **RPC surface**: `createscheduledtx`, `listscheduledtx`, `getschedule`, and `cancelscheduledtx` expose the state plus raw tx data so light clients can verify the pending transfer. Every entry returns `lock_time`, `raw_tx`, cancelable flag, and optionally `scheduled_at` (UTC timestamp derived from the friendly GUI field).
- **Mempool impact**: Scheduled Tx entries pay the standard relay fee (`MIN_RELAY_FEE_RATE`) and obey the existing mempool limits/eviction policies (`baseline/mempool.py`). If a scheduled tx is evicted before it becomes final, it will not be mined automatically; you can rebroadcast the stored `raw_tx` via `sendrawtransaction` (using `getschedule` to retrieve it).
