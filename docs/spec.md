# Baseline Protocol Specification

This document captures the normative behavior implemented by the current Baseline node. Unless noted otherwise, all values are expressed in consensus (liner) units and hashes are serialized in big-endian hexadecimal.

## 1. Cryptography & Hashing

- Hash function: double SHA-256 (`sha256d`) over serialized block headers and transactions.
- Signatures: ECDSA over secp256k1 (via `baseline/core/crypto.py`).
- Addresses: legacy Base58 P2PKH. Baseline emits version 0x35 by default (addresses generally begin with `N`), though decoding still accepts 0x00 for compatibility.

## 2. Transaction Model

- Inputs reference previous transaction outputs by `txid` (little-endian on wire) and `vout` index.
- Outputs lock liners to scripts. Only Pay-to-PubKey-Hash (25‑byte `OP_DUP OP_HASH160 <20 bytes> OP_EQUALVERIFY OP_CHECKSIG`) scripts are considered standard and mineable.
- Coin creation occurs exclusively in the first transaction (coinbase) of each block.

### Validation Rules

1. No transaction may create money: sum(outputs) ≤ sum(inputs).
2. Coinbase transaction must include the block height as the first push in `scriptSig` (BIP34-style).
3. Coinbase outputs mature after `coinbase_maturity` (=5) blocks. Spending them earlier is invalid.
4. Maximum serialized transaction size enforced by policy is 100 kB; scripts ≥1,650 bytes (`scriptSig`) or ≥10 kB (`scriptPubKey`) are rejected by the mempool.
5. Script execution follows Bitcoin’s stack machine with standard P2PKH validation.

## 3. Monetary Policy

- Initial subsidy: 50 BLINE (= 5,000,000,000 liners).
- Smooth decay: `subsidy_halving_interval` represents the half-life of the exponential subsidy curve. Default is 4,158,884 blocks (~2.64 years at 20 s/block), so rewards fall continuously instead of cliff halvings.
- Theoretical supply cap: ~300,000,000 BLINE. The exponential series converges to `initial_reward / (1 - 2^(-1 / subsidy_halving_interval))`, so the slower half-life doubles the asymptote while keeping per-block issuance smooth.
- Transaction fees (input minus output sum) accrue entirely to the miner of the block that includes the transaction. No burning or redistribution occurs.
- Relay/miner fee floor: 5 000 liners per kB (rounded up) as implemented in `baseline/policy.py`.
- Nodes verify that local `MiningConfig` values match these constants at startup. Any deviation causes initialization to fail unless `allow_consensus_overrides=true` is explicitly set for private testnets, in which case the node logs a warning and may diverge from mainnet.

### Premine

Genesis plus the first ~75,000 blocks can be mined privately to preallocate 2.5% (3,750,000 BLINE) before opening the network. This is a social policy, not a consensus rule.

## 4. Blocks

- Header fields: `version (int32)`, `prev_hash (32 B)`, `merkle_root (32 B)`, `timestamp (uint32)`, `bits (uint32 compact)`, `nonce (uint32)`.
- Block weight limit: `4_000_000` (no witness, so weight ≈ serialized size * 4 ⇒ 1 MB max).
- Timestamp constraints:
  - Must be strictly greater than the median of the previous 11 blocks (`median_time_past`).
- Must not exceed `synchronized_time + 15 minutes`.
- Proof-of-work: header hash interpreted as uint256 must be ≤ target encoded by `bits`.
- Difficulty adjusts every `retarget_interval` (=20) blocks relative to the most recent ancestor 19 blocks prior:

```
actual_span = parent.timestamp - ancestor.timestamp
target_span = block_interval_target * retarget_interval = 20 s * 20 = 400 s
actual_span is clamped to [target_span/4, target_span*4]
new_target = prev_target * actual_span / target_span
new_target ≤ max_target (set by genesis bits 0x207fffff)
```

- Coinbase reward limit: sum of coinbase outputs ≤ subsidy(height) + total fees from non-coinbase txs.
- Duplicate coinbase transactions or missing coinbase transactions invalidate the block.

## 5. Proof-of-Work Parameters

- Algorithm: SHA256d, same as Bitcoin.
- Initial target: 0x207fffff (regtest-like).
- Block interval target: 20 seconds (configurable but default consensus).
- Maximum future drift accepted: +15 minutes relative to synchronized node time.

## 6. Network & Messaging

- Default ports: P2P 9333, RPC 8832, Stratum 3333.
- Handshake: custom header-based protocol defined in `baseline/net/protocol.py` with length + checksum framing.
- Peer discovery: manual seeds + DNS seeds filtered to public IP ranges. Address gossip limited to 32 entries per message.
- Connection limits: default 64 peers (8 outbound target). Idle peers dropped after 90 seconds of silence.
- Nodes refuse to connect to peers advertising localhost/private addresses via DNS seeds.
- Header-first synchronization: nodes request headers via `getheaders`, store them with status=1, then request blocks via `getblocks`/`getdata`.

## 7. Stratum & Mining

- Built-in Stratum v1 server with vardiff window 30 and session timeout 120 seconds.
- Workers identified by username; payouts tracked per worker in `payouts/ledger.json`.
- Pool fee: `mining.pool_fee_percent` (default 1%) skimmed from block rewards before distribution.
- Payouts triggered when worker balances exceed `mining.min_payout` (default 1 BLINE) by constructing signed multi-output transactions spending matured coinbase UTXOs.

## 8. Wallet, Address Index & RPC

- Deterministic HD wallet with PBKDF2 + XOR encryption for seeds.
- Built-in address index tracks per-address UTXOs and transaction history so RPC methods such as `getaddressutxos`, `getaddressbalance`, and `getaddresstxids` require no external indexer.
- RPC surface mirrors Bitcoin Core for core methods (`getblockchaininfo`, `sendtoaddress`, etc.) plus Baseline-specific endpoints like `gettimesyncinfo`.

## 9. Time Synchronization

- Optional NTP client queries up to 3 servers every 300 seconds with 5-second socket timeout.
- Offsets >60 seconds trigger warnings; time manager adjusts `synchronized_time_int()` used for block validation.

## 10. Deviations from Bitcoin

- Faster block target (20 s) and short retarget window (20 blocks) with 4× clamp.
- Enforced standardness: only P2PKH outputs are acceptable to mempool/miners.
- Append-only block store implemented in `baseline/storage/blockstore.py`.
- No SegWit, no scripts beyond legacy P2PKH, no compact blocks, no BIP37, etc.
- Native address index exposed over JSON-RPC instead of optional addrindex patches.

This spec should be treated as authoritative for clients wishing to interoperate with the current implementation. Future upgrades should update both the implementation and this document.
