# Governance & Upgrade Process

Baseline prioritizes stability: the shipped binaries enforce fixed consensus parameters and no upgrades are active by default. When change is necessary, everyone needs a predictable path to evaluate, test, and adopt it. This document formalizes that path.

## Guiding Principles

1. **Predictable monetary policy** – Issuance-related parameters never change without broad consensus and extensive notice.
2. **Safe activation** – Every upgrade is proven on a public testnet before miners signal on mainnet.
3. **Transparent proposals** – Significant changes start life as written “Baseline Improvement Proposals” (BIPs) that live inside the repo (or a public spec repository).
4. **Operator autonomy** – Upgrade windows give node operators time to upgrade or deliberately dissent.

## Baseline Improvement Proposals (BIPs)

Each consensus/behavioral change follows this lifecycle:

1. **Draft** - Author opens a GitHub Discussion (or Markdown document) explaining motivation, specification, security impacts, and activation parameters.
2. **Review** - Gather feedback in that Discussion and other community channels (forums, Discord). Iterate until rough consensus exists.
3. **Test Plan** – Document unit/integration tests plus testnet scenarios and rollback considerations.
4. **Status** – Mark the BIP as Draft, Accepted, Rejected, or Final. Code only lands once the BIP is Accepted.

Template (use as the body of a GitHub Discussion when proposing a change):

````markdown
# BIP-000X: Short Title

- Author(s): ...
- Status: Draft
- Created: YYYY-MM-DD
- License: CC-BY 4.0

## Motivation
...

## Specification
...

## Activation Parameters
...

## Security Considerations
...

## Reference Implementation / Tests
...
````

## Activation Pipeline

Baseline reuses version-bit signaling (see aseline/core/upgrade.py). Upgrades move through phases similar to Bitcoin’s BIP9:

1. **Implementation** – Merge code guarded behind an UpgradeDefinition with 
ame, it, start_time, 	imeout, and 	hreshold fields.
2. **Testnet Deployment** – Launch or reuse a public testnet where miners/pools can exercise the change. Record results in the BIP.
3. **Release Candidate** – Cut a tagged build (e.g., 0.2.0-rc1) containing the upgrade with activation parameters pointing to the future mainnet window.
4. **Miner Signaling** – Pools upgrade and begin setting the assigned version bit. The upgrade manager tracks states (DEFINED → STARTED → LOCKED_IN → ACTIVE). Thresholds default to ≥95% of blocks in a retarget window (20 blocks) unless the BIP specifies otherwise.
5. **Activation** – One retarget interval after LOCKED_IN, the new rules become mandatory. Non-upgraded nodes will reject future blocks.

### Emergency Procedures

- Before LOCKED_IN: publish a hotfix that disables the upgrade (remove the definition) and advise miners to stop signaling.
- After LOCKED_IN: only a coordinated hard fork can revert. Avoid this by demanding extensive testnet soak and code review before STARTED.

## Current Status

- No upgrades are defined. UpgradeManager reports no active bits.
- Consensus parameters (coinbase_maturity, lock_interval_target, 
etarget_interval, initial_bits, subsidy_halving_interval) are locked at startup. Nodes refuse to run if the config deviates unless mining.allow_consensus_overrides=true is explicitly set (testnet/dev-only).

## Responsibilities

| Actor            | Responsibilities |
|------------------|------------------|
| Authors          | Draft BIPs, respond to feedback, write tests. |
| Core maintainers | Review specs + code, ensure test coverage, schedule releases/activations. |
| Miners/pools     | Evaluate BIPs, upgrade software, signal during STARTED phase, monitor locking state. |
| Node operators   | Track upcoming upgrades, upgrade binaries before LOCKED_IN, monitor RPC/logs for activation info. |

## Operator Checklist

1. Monitor docs/bips/ (or announcements) for accepted proposals.
2. Upgrade to the tagged release before the start_time published in the BIP.
3. Use getblockchaininfo (future release will expose upgrade state) or node logs to confirm signaling/activation status.
4. If dissenting, prepare to run alternative software or accept that the node will fall off the main chain once the upgrade activates.

## Future Enhancements

- Expose upgrade status via RPC/metrics (e.g., getblockchaininfo["upgrades"]).
- Automate BIP template generation and activation dashboards.
- Publish a long-lived public testnet dedicated to upcoming BIPs.
