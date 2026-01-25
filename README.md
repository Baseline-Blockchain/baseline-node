# Baseline Cash

Baseline Cash is a minimalist Bitcoin-style payments chain — but it ships with the stuff operators actually need built-in: fast blocks, Core-ish RPC, a native address index, and a turnkey pool server. No smart contracts, no token zoo, no bloat.

## Quick Start

### 1. Install Python environment

Baseline Cash targets **Python 3.12 or newer** (3.12/3.13 verified). Confirm Python is available before continuing:

```bash
python --version || py -3 --version
```

If the first command fails on Windows, try `py -3`. Once you have a compatible interpreter, create the virtualenv:

```bash
python -m venv .venv
. .venv/Scripts/activate  # or source .venv/bin/activate on UNIX
pip install --upgrade pip
```

No extra pip packages are required; the stdlib is enough. The GUI uses Tkinter (bundled with official Windows/macOS installers); on Linux install your distro's Tk bindings if missing (e.g., `sudo apt-get install python3-tk` or `sudo dnf install python3-tkinter`) before running `baseline-wallet-gui`.

### 2. Install Baseline Cash
   ```bash
   pip install -e .
   ```
   This exposes the `baseline-node`, `baseline-wallet`, and `baseline-wallet-gui` executables in your virtual environment.

### 3. Set config values

  A starter `config.json` lives at the repo root with reasonable defaults. Before launching your node, make these important changes:

- Pick RPC creds: Modify `rpc.username` and `rpc.password` in `config.json` and keep them secret.
- Set peers: In `network.seeds`, add reachable Baseline Cash nodes as a list to help your node find peers or **leave it empty** to start a private testnet. The current starter config.json has a seed node for the public Baseline Cash mainnet.

### 4. Launch the node
   ```bash
   baseline-node --config config.json --log-level info
   ```
   The runner initializes the append-only block store + SQLite chainstate, and starts P2P sync, wallet, and the authenticated JSON-RPC server. If a pool private key is configured it also spins up the payout tracker and Stratum listener. Use Ctrl+C (or SIGTERM) for graceful shutdown.
   Point a browser or `curl` at `http://127.0.0.1:8832/` to view the built-in status panel (unauthenticated but rate-limited)-it summarizes height, peers, mempool size, and uptime at a glance.

**TIP: Need to resync from scratch?** Stop the node and run:
```bash
baseline-node --config config.json --reset-chainstate
```
This wipes blocks/chainstate/peers/logs while preserving wallets and payout data, then restarts the node with a clean slate.

## Wallet

The node writes `wallet/wallet.json` under the data dir and exposes it over JSON-RPC. For friendlier use, run the CLI tool.:

```bash
# show commands
baseline-wallet --config config.json --help
```

Optional wallet notify hook: set `walletnotify` in `config.json` to run a command whenever the wallet sees a transaction or its confirmation status changes. Use `%s` for the txid.

```json
"walletnotify": "curl -X POST https://exchange.example/walletnotify?txid=%s"
```

## Mining

### 1. Create the pool payout key
  This key is separate from the wallet: it controls who receives block rewards from your Stratum pool. Generate one with:
  ```bash
  baseline-wallet generate-key
  ```
  It prints the 32-byte hex key *and* a WIF string. Replace `mining.pool_private_key` (which defaults to `null`) in `config.json` with the hex value. Keep both hex + WIF offline.
  
  Import the WIF into any wallet (Baseline Cash Wallet or external) whenever you need to manually spend pool-held funds.

### 2. Start mining with Stratum
1. Launch the node with your configured payout key. The Stratum listener will come up automatically whenever that key is present.
2. Point miners:
   ```bash
   # Install baseline-miner (CPU Stratum miner)
   python -m pip install git+https://github.com/Baseline-Blockchain/baseline-miner.git

   # Local Solo Miner (connect to your own node's Stratum server)
   baseline-miner --host 127.0.0.1 --port 3333 --address YOURRECEIVERADDRESS --worker worker1

   # Public pool (replace pool.example.org, open TCP/3333, ensure config.json has stratum.host = 0.0.0.0)
   baseline-miner --host pool.example.org --port 3333 --address YOURRECEIVERADDRESS --worker worker1
   ```
   Workers are tracked by username for payouts; shares accrue until coinbase rewards mature (20 blocks), then the payout tracker crafts and broadcasts the payment transaction..

- Yes, you can already have community mining pools! Just point miners with proper addresses at a Baseline Cash node with Stratum enabled and the payouts will flow automatically.

**Pool operator knobs (config.json):**
- `mining.pool_fee_percent`: percent taken from mined subsidy before distributing to workers.
- `mining.min_payout`: minimum worker balance (in liners) before including them in a payout tx.
- `stratum.*`: vardiff/session tuning; see `docs/mining-and-payouts.md` and monitor `data_dir/payouts/ledger.json`.
