# Baseline

Baseline is a minimalist, Bitcoin-style payments chain—built for simple transfers, predictable rules, and clean integration. No smart-contract complexity. Just money.

## Network Parameters

- **Consensus**: SHA256d proof-of-work, 32-byte block hashes, 0x207fffff genesis target (roughly Bitcoin regtest difficulty).
- **Timing**: 20-second block target, retarget every 20 blocks (≈6.7 minutes) with 4× dampening to stay responsive without oscillations.
- **Difficulty** is the target hash threshold miners must beat; lower targets = harder work. Baseline encodes this exactly like Bitcoin in the header `bits` field.
Every **20 blocks** (~6.7 minutes) the node compares the actual elapsed time with the expected `20 * 20s = 400s`. If blocks arrive too fast, the target tightens; too slow and it loosens.
Adjustments are clamped to 4× faster/slower than expected so that hash-rate spikes do not cause whiplash while the 20-second cadence stays smooth.
- **Rewards**: 50 BLINE block subsidy halving every 150,000 blocks (~34.7 days). Total supply caps at 15 million BLINE (geometric series of 50 * 150k).
- **Coinbase maturity**: 5 blocks before mined funds can be spent.
- **Fees**: Minimum relay fee is 1,000 satoshis per kB; non-standard scripts are rejected, so typical P2PKH transactions should pay at least 0.00001 BLINE for a 250-byte tx.
- **Premine**: Reserve 2.5% of total supply (375,000 BLINE) by mining ~7,500 blocks at launch. With 20-second blocks, that’s roughly 42 hours of dedicated premining before opening the network.
- **Ports**: P2P `9333`, RPC `8832`, Stratum `3333`.

## Quick Start

### 1. Install Python environment
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
   This exposes the `baseline-node`, `baseline-wallet`, and `baseline-miner` executables in your virtual environment.

### 3. Generate keys + config

  A starter `config.json` lives at the repo root with reasonable defaults. Before launching your node, make two important changes:

- Pick RPC creds: Modify `rpc.username` and `rpc.password` in `config.json` and keep them secret; all wallet tooling and miners authenticate with them.
- Set peers: In `network.seeds`, add reachable Baseline nodes as a list to help your node find peers.

### 4. Create the pool payout key
  This key is separate from the wallet: it controls who receives block rewards from your Stratum pool. Generate one with:
  ```bash
  baseline-wallet generate-key
  ```
  It prints the 32-byte hex key *and* a WIF string. Replace `"CHANGE-ME"` in `config.json` with the hex value. Keep both hex + WIF offline-import the WIF into any wallet (Baseline or external) whenever you need to manually spend pool-held funds.

### 5. Launch the node
   ```bash
   baseline-node --config config.json --log-level info
   ```
   The runner initializes the append-only block store + SQLite chainstate, starts P2P sync, the Stratum pool, wallet, payout tracker, and the authenticated JSON-RPC server. Use Ctrl+C (or SIGTERM) for graceful shutdown.

### 6. Initialize the wallet once the node is running
   ```bash
   baseline-wallet --config config.json setup --encrypt
   ```
   The CLI guides you through creating (and optionally encrypting) the deterministic wallet so you can request payout/operational addresses via `baseline-wallet newaddress`. The wallet auto-creates an initial receiving address; view it (and any later ones) with `baseline-wallet --config config.json listaddresses`, and see per-address balances with `baseline-wallet --config config.json balances`. Wallet funds are distinct from the pool payout key, but you can sweep rewards into the wallet by importing the pool WIF or by mining directly to a wallet-derived address.

## Wallet CLI (`tools/wallet_cli.py`)

The node writes `wallet/wallet.json` under the data dir and exposes it over JSON-RPC. For friendlier use, run the helper CLI:

```bash
# show commands
baseline-wallet --config config.json --help

# common flows
baseline-wallet --config config.json setup --encrypt   # first run, optionally encrypts wallet
baseline-wallet --config config.json newaddress payout
baseline-wallet --config config.json listaddresses     # list wallet + watch-only addresses
baseline-wallet --config config.json balances          # show balance per address
baseline-wallet --config config.json send bl1example... 1.25 --from-address bl1abc...  # prompts for passphrase if encrypted; omit optional flags for defaults
baseline-wallet --config config.json dump /secure/backups/baseline-wallet.json
baseline-wallet --config config.json importaddress bl1watch... --label "monitor" --rescan
baseline-wallet --config config.json importprivkey <WIF> --label "hot key" --rescan  # imports a single private key - useful for pool payout key
```

The CLI automatically prompts for passphrases when sending or during the `setup --encrypt` flow, so you rarely need to call `encryptwallet`/`walletpassphrase` manually. All commands ultimately talk to JSON-RPC via Basic Auth, so they work locally or against remote nodes.

## Mining

Every node ships with a Stratum v1 pool so you can mine right away-ASICs connect via `stratum+tcp://<host>:3333` and authenticate with any `worker:password` pair. Steps:

1. Launch the node with your configured payout key (see Quick Start). The Stratum listener comes up automatically.
2. Point miners:
   ```bash
   bfgminer -o stratum+tcp://127.0.0.1:3333 -u worker1 -p x --set-device bitcoin:clock=500
   ```
   Workers are tracked by username for payouts; shares accrue until coinbase rewards mature (5 blocks), then the payout tracker crafts and broadcasts the payment transaction..

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
- **Wallet security**: PBKDF2-HMAC-SHA256 creates an XOR pad for the seed. Locked wallets never hold plaintext on disk; unlock state stays only in RAM and expires automatically.
- **JSON-RPC & Stratum**: Bitcoin-style error codes, request size limits, and Basic Auth keep RPC friendly for exchanges and explorers. Stratum tracks vardiff, session heartbeats, and bans misbehaving miners to avoid DoS.

## Code Quality

Ruff enforces a strict lint profile defined in `pyproject.toml`. Install it and run checks before committing:

```bash
pip install ruff
ruff check .
```

The configuration enables pycodestyle/pyflakes, import sorting, naming rules, bugbear, pyupgrade, and security checks so issues surface early.
