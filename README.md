# Baseline ![Tests](https://github.com/Baseline-Blockchain/baseline-node/actions/workflows/tests.yml/badge.svg)

Baseline is a minimalist, Bitcoin-style payments chain—built for simple transfers, predictable rules, and clean integration. No smart-contract complexity. Just money.

- **Simple payments**: standard transactions only, smaller attack surface
- **Operator-ready**: production-grade RPC / Bitcoin Core compatibility + built-in Stratum pool server
- **Lightweight & efficient**: Python 3.12+, no external dependencies, low resource usage
- **Understandable by design**: compact codebase, clear documentation, and formal spec
- **Compatible with Bitcoin tooling**: Designed for Bitcoin RPC compatibility; most Core-compatible wallets and explorers should work with Baseline with minimal changes.

## Network Parameters

- **Consensus**: SHA256d proof-of-work, 32-byte block hashes, 0x207fffff genesis target (roughly Bitcoin regtest difficulty, the difficulty retarget is immediate and hardened).
- **Timing**: 20-second block target, retarget every 20 blocks (≈6.7 minutes) with a tighter 2× clamp for the first 10,000 blocks and 4× thereafter to stay responsive without oscillations.
- **Difficulty** is the target hash threshold miners must beat; lower targets = harder work. Baseline encodes this exactly like Bitcoin in the header `bits` field.
Every **20 blocks** (~6.7 minutes) the node compares the actual elapsed time with the expected `20 * 20s = 400s`. If blocks arrive too fast, the target tightens; too slow and it loosens.
Adjustments are clamped to 2× faster/slower for the first 10,000 blocks, then 4× after the warm-up so that hash-rate spikes do not cause whiplash while the 20-second cadence stays smooth.
- **Premine**: No premine. 1% block subsidy dev fund (on-chain).
- **Rewards**: 50 BLINE base subsidy decays smoothly every block using an exponential curve with a 4,158,884-block half-life (~2.64 years). The remaining 99% of each subsidy (plus all transaction fees) flows to miners, while a mandatory 1% is automatically sent to the Baseline Foundation address (consensus-locked) to fund protocol stewardship.
- **Coinbase maturity**: 20 blocks before mined funds can be spent.
- **Fees**: Minimum relay fee is 5,000 liners per kB; non-standard scripts are rejected, so typical P2PKH transactions should pay at least ~0.0000125 BLINE for a 250-byte tx.
- **Ports**: P2P `9333`, RPC `8832`, Stratum `3333`.

### Supply Schedule

![Baseline supply schedule](docs/assets/supply_curve.png)

Subsidy decays exponentially each block with a 4,158,884-block half-life (~2.64 years), so rewards glide downward and asymptotically cap supply at ~300 million BLINE without cliff events.

## Quick Start

### 1. Install Python environment

Baseline targets **Python 3.12 or newer** (3.12/3.13 verified). Make sure your `python` shim points to a compatible interpreter before creating the virtualenv.
   ```bash
   python -m venv .venv
   . .venv/Scripts/activate  # or source .venv/bin/activate on UNIX
   pip install --upgrade pip
   ```
   No extra packages are required; the stdlib is enough.

### 2. Install Baseline
   ```bash
   pip install -e .
   ```
   This exposes the `baseline-node`, `baseline-wallet`, `baseline-wallet-gui`, and `baseline-miner` executables in your virtual environment.

### 3. Set config values

  A starter `config.json` lives at the repo root with reasonable defaults. Before launching your node, make these important changes:

- Pick RPC creds: Modify `rpc.username` and `rpc.password` in `config.json` and keep them secret.
- Set peers: In `network.seeds`, add reachable Baseline nodes as a list to help your node find peers or **leave it empty** to start a private testnet. The current starter config.json has a seed node for the public Baseline testnet.

### 4. Launch the node
   ```bash
   baseline-node --config config.json --log-level info
   ```
   The runner initializes the append-only block store + SQLite chainstate, and starts P2P sync, wallet, and the authenticated JSON-RPC server. If a pool private key is configured it also spins up the payout tracker and Stratum listener. Use Ctrl+C (or SIGTERM) for graceful shutdown.
   Point a browser or `curl` at `http://127.0.0.1:8832/` (with your RPC username/password) to view the built-in status panel—it summarizes height, peers, mempool size, and uptime at a glance.

### 5. Initialize the wallet once the node is running
   ```bash
   baseline-wallet --config config.json setup --encrypt
   ```
   The CLI guides you through creating (and optionally encrypting) the deterministic wallet so you can request payout/operational addresses via `baseline-wallet newaddress`. The wallet auto-creates an initial receiving address; view it (and any later ones) with `baseline-wallet --config config.json listaddresses`, and see per-address balances with `baseline-wallet --config config.json balances`.

**TIP: Need to resync from scratch?** Stop the node and run:
```bash
baseline-node --config config.json --reset-chainstate
```
This wipes blocks/chainstate/peers/logs while preserving wallets and payout data, then restarts the node with a clean slate.

## Wallet

The node writes `wallet/wallet.json` under the data dir and exposes it over JSON-RPC. For friendlier use, run the helper CLI:

```bash
# show commands
baseline-wallet --config config.json --help
```

Prefer a point-and-click interface? Launch the Tkinter-based helper:

```bash
baseline-wallet-gui
```

It reuses the same RPC settings you keep in `config.json`.

## Mining

### 1. Create the pool payout key
  This key is separate from the wallet: it controls who receives block rewards from your Stratum pool. Generate one with:
  ```bash
  baseline-wallet generate-key
  ```
  It prints the 32-byte hex key *and* a WIF string. Replace `mining.pool_private_key` (which defaults to `null`) in `config.json` with the hex value. Keep both hex + WIF offline-import the WIF into any wallet (Baseline or external) whenever you need to manually spend pool-held funds.

### 2. Start mining with Stratum
1. Launch the node with your configured payout key. The Stratum listener will come up automatically whenever that key is present.
2. Point miners:
   ```bash
   bfgminer -o stratum+tcp://127.0.0.1:3333 -u YOURRECEIVERADDRESS.worker1 -p x --set-device bitcoin:clock=500
   ```
   Workers are tracked by username for payouts; shares accrue until coinbase rewards mature (20 blocks), then the payout tracker crafts and broadcasts the payment transaction..

### Reference miner (`tools/simple_miner.py`)

This script is a CPU-only proof-of-concept that repeatedly fetches templates over RPC and submits solved blocks via `submitblock`:

```bash
baseline-miner --config config.json --attempts-per-template 500000
```

It is intentionally simple—great for smoke testing, not for real hash-rate. For real deployments, point ASICs at the built-in Stratum endpoint.

## How It Works (High-Level)

- **Consensus**: Full UTXO validation with deterministic difficulty transitions, script execution, and chain selection stored in SQLite. Undo data enables safe reorgs and fast rescan.
- **P2P**: Header-first initial block download with watchdog retries, strict message framing (length + checksum), peer scoring/banning, addr/inv relay, and persisted address books.
- **Persistence**: Append-only block files plus fsyncs; headers, UTXOs, and metadata are transactional via SQLite with periodic sanity checks.
- **Consensus safeguards**: Subsidy, maturity, and retarget parameters are hard-locked; nodes refuse to start if `config.json` deviates (unless explicitly overriding for testnets), preventing accidental forks.
- **Address index**: SQLite maintains per-address UTXOs and history so explorers/wallets can query `getaddress*` RPCs without extra indexers.
- **Wallet security**: PBKDF2-HMAC-SHA256 creates an XOR pad for the seed. Locked wallets never hold plaintext on disk; unlock state stays only in RAM and expires automatically.
- **JSON-RPC & Stratum**: Bitcoin-style error codes, request size limits, and Basic Auth keep RPC friendly for exchanges and explorers. Stratum tracks vardiff, session heartbeats, and bans misbehaving miners to avoid DoS.
- **Upgrades**: `docs/governance.md` outlines the Baseline Improvement Proposal process and version-bit activation flow; no upgrades are active by default.

## Documentation

Additional operational docs live under [`docs/`](docs):

- [`prerequisites.md`](docs/prerequisites.md) – runtime requirements, hardware sizing, networking basics.
- [`configuration.md`](docs/configuration.md) – `config.json` reference, environment overrides, NTP guidance.
- [`networking.md`](docs/networking.md) – peer discovery, seeds, and connection policies.
- [`mining-and-payouts.md`](docs/mining-and-payouts.md) – Stratum usage, share accounting, payout lifecycle.
- [`rpc.md`](docs/rpc.md) – JSON-RPC surface area with example calls.
- [`operations.md`](docs/operations.md) – backups, monitoring, troubleshooting, and upgrade workflows.
- [`spec.md`](docs/spec.md) – formal protocol specification derived from the current implementation.
- [`governance.md`](docs/governance.md) – upgrade governance model and Baseline Improvement Proposal process.

## Code Quality

Ruff enforces a strict lint profile defined in `pyproject.toml`. Install it and run checks before committing:

```bash
pip install ruff
ruff check .
```

The configuration enables pycodestyle/pyflakes, import sorting, naming rules, bugbear, pyupgrade, and security checks so issues surface early.
