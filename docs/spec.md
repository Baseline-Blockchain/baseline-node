# Baseline Protocol Specification

This document captures the normative behavior implemented by the current Baseline node. Unless noted otherwise, all values are expressed in consensus (liner) units and hashes are serialized in big-endian hexadecimal.

## 1. Cryptography & Hashing

- Hash function: double SHA-256 (`sha256d`) over serialized block headers and transactions.
- Signatures: ECDSA over secp256k1 (via `baseline/core/crypto.py`).
- Addresses: legacy Base58 P2PKH. Baseline emits version 0x35 by default (addresses generally begin with `N`), though decoding still accepts 0x00 for compatibility.

## 2. Transaction Model

- Inputs reference previous transaction outputs by `txid` (little-endian on wire) and `vout` index.
- Outputs lock liners to scripts. Only Pay-to-PubKey-Hash (25-byte `OP_DUP OP_HASH160 <20 bytes> OP_EQUALVERIFY OP_CHECKSIG`) scripts are considered standard and mineable.
- Coin creation occurs exclusively in the first transaction (coinbase) of each block.
- Lock time: `lock_time` is a uint32. Values < 500,000,000 are interpreted as block heights; values >= 500,000,000 are interpreted as UNIX timestamps.

### Validation Rules

1. No transaction may create money: sum(outputs) ≤ sum(inputs).
2. Coinbase transaction must include the block height as the first push in `scriptSig` (BIP34-style).
3. Coinbase outputs mature after `coinbase_maturity` (=20) blocks. Spending them earlier is invalid.
4. Maximum serialized transaction size enforced by policy is 100 kB; scripts ≥1,650 bytes (`scriptSig`) or ≥10 kB (`scriptPubKey`) are rejected by the mempool.
5. Script execution follows Bitcoin's stack machine with standard P2PKH validation.
6. Non-coinbase and coinbase transactions must satisfy lock-time finality at the block's height/time or the block is invalid.

## 3. Monetary Policy

- Initial subsidy: 50 BLINE (= 5,000,000,000 liners).
- Smooth decay: `subsidy_halving_interval` represents the half-life of the exponential subsidy curve. Default is 4,158,884 blocks (~2.64 years at 20 s/block), so rewards fall continuously instead of cliff halvings.
- Theoretical supply cap: ~300,000,000 BLINE. The exponential series converges to `initial_reward / (1 - 2^(-1 / subsidy_halving_interval))`, so the slower half-life doubles the asymptote while keeping per-block issuance smooth.
- Foundation allocation: 1% of every subsidy must be paid to the canonical Foundation script defined by `mining.foundation_address` (a legacy Base58 P2PKH). Nodes verify the coinbase includes at least that output before accepting the block.
- Transaction fees (input minus output sum) accrue entirely to the miner of the block that includes the transaction. No burning or redistribution occurs.
- Relay/miner fee floor: 5 000 liners per kB (rounded up) as implemented in `baseline/policy.py`.
- Nodes verify that local `MiningConfig` values match these constants at startup. Any deviation causes initialization to fail unless `allow_consensus_overrides=true` is explicitly set for private testnets, in which case the node logs a warning and may diverge from mainnet.

## 4. Blocks

- Header fields: `version (int32)`, `prev_hash (32 B)`, `merkle_root (32 B)`, `timestamp (uint32)`, `bits (uint32 compact)`, `nonce (uint32)`.
- Block weight limit: `4_000_000` (no witness, so weight ≈ serialized size * 4 ⇒ 1 MB max).
- Timestamp constraints:
  - Must be strictly greater than the median of the previous 11 blocks (`median_time_past`).
- Must not exceed `synchronized_time + 3 minutes`.
- Proof-of-work: header hash interpreted as uint256 must be ≤ target encoded by `bits`.
- Difficulty adjusts every `retarget_interval` (=20) blocks relative to the most recent ancestor 19 blocks prior:

```
actual_span = parent.timestamp - ancestor.timestamp
target_span = block_interval_target * retarget_interval = 20 s * 20 = 400 s
If height ≤ 10,000, actual_span is clamped to [target_span/2, target_span*2]; afterwards it clamps to [target_span/4, target_span*4]
new_target = prev_target * actual_span / target_span
new_target ≤ max_target (set by genesis bits 0x207fffff)
```

- Coinbase reward limit: sum of coinbase outputs ≤ subsidy(height) + total fees from non-coinbase txs.
- Duplicate coinbase transactions or missing coinbase transactions invalidate the block.
- Finality: each non-coinbase transaction must be final with respect to block height/time (see lock_time rule above) or the block is rejected.
- Fork choice: the active chain is the highest-work chain; headers on the active chain are stored with `status=0` and side chains with `status=1`. Reorganizations detach old-branch blocks and attach the higher-work branch.

## 5. Proof-of-Work Parameters

- Algorithm: SHA256d, same as Bitcoin.
- Initial target: 0x207fffff (regtest-like).
- Block interval target: 20 seconds (configurable but default consensus).
- Maximum future drift accepted: +3 minutes relative to synchronized node time.

## 6. Network & Messaging

- Default ports: P2P 9333, RPC 8832, Stratum 3333.
- Handshake: custom header-based protocol defined in `baseline/net/protocol.py` with length + checksum framing.
- Peer discovery: manual seeds + DNS seeds filtered to public IP ranges. Address gossip limited to 32 entries per message.
- Connection limits: default 64 peers (8 outbound target). Idle peers dropped after 90 seconds of silence.
- Nodes refuse to connect to peers advertising localhost/private addresses via DNS seeds.
- Header-first synchronization: nodes request headers via `getheaders`, store them with status=1, then request blocks via `getblocks`/`getdata`.
- Transaction message validation: `lock_time` must be a uint32 (0..2^32-1), inputs/outputs must be present, and output values must not exceed total money.

### Mempool Policy (Relay/Mining)

- Mempool weight limit: 5,000,000 weight units (no witness, so weight ~= 4x size).
- Orphan limit: 100 entries; oldest orphans are evicted when the cap is reached.
- Eviction policy: when full, the lowest fee-rate package (tx + descendants) is removed until under the limit.
- Peer rate limiting: per-peer byte budget with refill (default 500,000 bytes/min, RPC half-budget). Exceeding the budget rejects the submission.
- Standardness: only P2PKH outputs are accepted; non-standard scripts are rejected by mempool.

## 7. Stratum & Mining

- Built-in Stratum v1 server with vardiff window 30 and session timeout 120 seconds.
- Workers identified by username; payouts tracked per worker in `payouts/ledger.json`.
- Pool fee: `mining.pool_fee_percent` (default 1%) skimmed from block rewards before distribution.
- Payouts triggered when worker balances exceed `mining.min_payout` (default 1 BLINE) by constructing signed multi-output transactions spending matured coinbase UTXOs.

## 8. Wallet, Address Index & RPC

- Deterministic HD wallet with PBKDF2 + XOR encryption for seeds.
- Built-in address index tracks per-address UTXOs and transaction history so RPC methods such as `getaddressutxos`, `getaddressbalance`, and `getaddresstxids` require no external indexer.
- RPC surface mirrors Bitcoin Core for core methods (`getblockchaininfo`, `sendtoaddress`, etc.) plus Baseline-specific endpoints like `gettimesyncinfo`.
- Scheduled Send (wallet-level): clients can pre-sign a transaction with a future lock_time and track it in wallet state; cancelable schedules can be refunded before the lock_time via a refund transaction.

## 9. Time Synchronization

- Optional NTP client queries up to 3 servers on each sync (from the configured server list) every 300 seconds with a 5-second UDP timeout.
- The node maintains an in-process time offset (it does not adjust the system clock).
- Offsets greater than 60 seconds trigger warnings.
- `synchronized_time_int()` uses the node’s NTP-derived offset and is used for mining template timestamps and the “future block” validation check.
- RPC `gettimesyncinfo` reports time-sync status including offset, last_sync, time_since_sync, drift_rate, servers, system_time, and synchronized_time.

## 10. Upgrades & Version Bits

- Upgrades are defined by a version bit (0-28), a start/timeout window, a signaling threshold, and a signaling period.
- Nodes signal during the STARTED state by setting the upgrade's bit in the block version.
- LOCKED_IN is reached when the previous full period meets the signaling threshold.
- ACTIVE begins after a full LOCKED_IN period; the activation height is recorded in the state database.
- Unknown signaling bits are tolerated (logged at debug) to preserve forward compatibility.

## 11. Deviations from Bitcoin

- Faster block target (20 s) and short retarget window (20 blocks) with a 2× clamp for the first 10,000 blocks, then 4× afterwards.
- Enforced standardness: only P2PKH outputs are acceptable to mempool/miners.
- Append-only block store implemented in `baseline/storage/blockstore.py`.
- No SegWit, no scripts beyond legacy P2PKH, no compact blocks, no BIP37, etc.
- Native address index exposed over JSON-RPC instead of optional addrindex patches.

This spec should be treated as authoritative for clients wishing to interoperate with the current implementation. Future upgrades should update both the implementation and this document.
