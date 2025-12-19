#!/usr/bin/env python3
"""
TPS (Transactions Per Second) Benchmark for Baseline Blockchain

This script measures the transaction processing performance of the Baseline blockchain
by generating and submitting transactions, then measuring throughput and confirmation times.

Usage:
    python tools/tps_benchmark.py --config config.json [options]
"""

import argparse
import asyncio
import json
import logging
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import base64
import hashlib
import hmac
import http.client
import urllib.parse


@dataclass
class BenchmarkResult:
    """Results from a TPS benchmark run."""
    total_transactions: int
    submission_time: float
    submission_tps: float
    confirmed_transactions: int
    confirmation_time: float
    confirmation_tps: float
    failed_submissions: int
    average_confirmation_delay: float
    min_confirmation_delay: float
    max_confirmation_delay: float
    mempool_peak_size: int
    errors: List[str]


class RPCClient:
    """Simple JSON-RPC client for Baseline node."""
    
    def __init__(self, host: str, port: int, username: str, password: str):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.request_id = 0
    
    def call(self, method: str, params: List[Any] = None) -> Any:
        """Make an RPC call to the node."""
        if params is None:
            params = []
        
        self.request_id += 1
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": self.request_id
        }
        
        # Create HTTP Basic Auth header
        credentials = f"{self.username}:{self.password}"
        auth_string = base64.b64encode(credentials.encode()).decode()
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Basic {auth_string}"
        }
        
        try:
            conn = http.client.HTTPConnection(self.host, self.port, timeout=30)
            conn.request("POST", "/", json.dumps(payload), headers)
            response = conn.getresponse()
            
            if response.status != 200:
                raise Exception(f"HTTP {response.status}: {response.reason}")
            
            data = json.loads(response.read().decode())
            conn.close()
            
            if "error" in data and data["error"]:
                raise Exception(f"RPC Error: {data['error']}")
            
            return data.get("result")
            
        except Exception as e:
            raise Exception(f"RPC call failed: {e}")


class TransactionGenerator:
    """Generates test transactions for benchmarking."""
    
    def __init__(self, rpc_client: RPCClient):
        self.rpc = rpc_client
        self.test_addresses = []
        self.utxos = []
    
    def setup_test_environment(self, num_addresses: int = 10) -> None:
        """Set up test addresses and initial UTXOs for benchmarking."""
        logging.info(f"Setting up test environment with {num_addresses} addresses...")
        
        try:
            # Get some initial addresses from the wallet
            for i in range(num_addresses):
                try:
                    address = self.rpc.call("getnewaddress", [f"benchmark_test_{i}"])
                    self.test_addresses.append(address)
                except Exception as e:
                    logging.warning(f"Could not create test address {i}: {e}")
            
            if not self.test_addresses:
                raise Exception("No test addresses available. Make sure wallet is set up.")
            
            # Get available UTXOs
            try:
                unspent = self.rpc.call("listunspent")
                self.utxos = [utxo for utxo in unspent if utxo.get("amount", 0) > 0.001]
                logging.info(f"Found {len(self.utxos)} available UTXOs")
            except Exception as e:
                logging.warning(f"Could not list UTXOs: {e}")
                
        except Exception as e:
            logging.error(f"Failed to set up test environment: {e}")
            raise
    
    def generate_simple_transaction(self, amount: float = 0.0001) -> Optional[str]:
        """Generate a simple transaction for testing."""
        if len(self.test_addresses) < 2:
            return None
        
        try:
            # Simple send between test addresses
            from_addr = self.test_addresses[0]
            to_addr = self.test_addresses[1]
            
            # Try to send a small amount
            tx_hash = self.rpc.call("sendtoaddress", [to_addr, amount])
            return tx_hash
            
        except Exception as e:
            logging.debug(f"Failed to generate transaction: {e}")
            return None


class TPSBenchmark:
    """Main TPS benchmark runner."""
    
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.rpc = RPCClient(
            self.config["rpc"]["host"],
            self.config["rpc"]["port"],
            self.config["rpc"]["username"],
            self.config["rpc"]["password"]
        )
        self.tx_generator = TransactionGenerator(self.rpc)
        self.logger = logging.getLogger("tps_benchmark")
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from JSON file."""
        with open(config_path, 'r') as f:
            return json.load(f)
    
    def check_node_status(self) -> bool:
        """Check if the node is running and responsive."""
        try:
            info = self.rpc.call("getblockchaininfo")
            self.logger.info(f"Node is running. Current height: {info.get('blocks', 'unknown')}")
            return True
        except Exception as e:
            self.logger.error(f"Node is not responsive: {e}")
            return False
    
    def get_mempool_size(self) -> int:
        """Get current mempool size."""
        try:
            # Try to get mempool info if available
            result = self.rpc.call("getmempoolinfo")
            return result.get("size", 0)
        except:
            # Fallback: count transactions in mempool
            try:
                mempool = self.rpc.call("getrawmempool")
                return len(mempool) if mempool else 0
            except:
                return 0
    
    def wait_for_confirmations(self, tx_hashes: List[str], timeout: float = 300) -> Dict[str, float]:
        """Wait for transactions to be confirmed and measure times."""
        confirmed = {}
        start_time = time.time()
        
        while len(confirmed) < len(tx_hashes) and (time.time() - start_time) < timeout:
            for tx_hash in tx_hashes:
                if tx_hash in confirmed:
                    continue
                
                try:
                    # Check if transaction is confirmed
                    tx_info = self.rpc.call("gettransaction", [tx_hash])
                    if tx_info.get("confirmations", 0) > 0:
                        confirmed[tx_hash] = time.time() - start_time
                except:
                    # Transaction might not be found yet
                    pass
            
            time.sleep(0.5)  # Check every 500ms
        
        return confirmed
    
    def run_submission_benchmark(self, num_transactions: int, max_workers: int = 10) -> BenchmarkResult:
        """Run the main TPS benchmark."""
        self.logger.info(f"Starting TPS benchmark with {num_transactions} transactions...")
        
        # Setup test environment
        try:
            self.tx_generator.setup_test_environment()
        except Exception as e:
            self.logger.error(f"Failed to setup test environment: {e}")
            return BenchmarkResult(0, 0, 0, 0, 0, 0, num_transactions, 0, 0, 0, 0, [str(e)])
        
        submitted_txs = []
        failed_submissions = 0
        errors = []
        
        # Record initial mempool size
        initial_mempool_size = self.get_mempool_size()
        peak_mempool_size = initial_mempool_size
        
        # Submission phase
        submission_start = time.time()
        
        def submit_transaction():
            try:
                tx_hash = self.tx_generator.generate_simple_transaction()
                if tx_hash:
                    return tx_hash, time.time()
                else:
                    return None, time.time()
            except Exception as e:
                return None, time.time()
        
        # Submit transactions with threading for better throughput
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(submit_transaction) for _ in range(num_transactions)]
            
            for future in futures:
                try:
                    tx_hash, submit_time = future.result(timeout=30)
                    if tx_hash:
                        submitted_txs.append((tx_hash, submit_time))
                    else:
                        failed_submissions += 1
                except Exception as e:
                    failed_submissions += 1
                    errors.append(str(e))
                
                # Monitor mempool size
                current_mempool = self.get_mempool_size()
                peak_mempool_size = max(peak_mempool_size, current_mempool)
        
        submission_end = time.time()
        submission_time = submission_end - submission_start
        submission_tps = len(submitted_txs) / submission_time if submission_time > 0 else 0
        
        self.logger.info(f"Submitted {len(submitted_txs)} transactions in {submission_time:.2f}s")
        self.logger.info(f"Submission TPS: {submission_tps:.2f}")
        self.logger.info(f"Failed submissions: {failed_submissions}")
        
        if not submitted_txs:
            return BenchmarkResult(
                total_transactions=num_transactions,
                submission_time=submission_time,
                submission_tps=submission_tps,
                confirmed_transactions=0,
                confirmation_time=0,
                confirmation_tps=0,
                failed_submissions=failed_submissions,
                average_confirmation_delay=0,
                min_confirmation_delay=0,
                max_confirmation_delay=0,
                mempool_peak_size=peak_mempool_size,
                errors=errors
            )
        
        # Confirmation phase
        self.logger.info("Waiting for confirmations...")
        tx_hashes = [tx[0] for tx in submitted_txs]
        
        confirmation_start = time.time()
        confirmed_times = self.wait_for_confirmations(tx_hashes, timeout=600)
        confirmation_end = time.time()
        
        confirmation_time = confirmation_end - confirmation_start
        confirmed_count = len(confirmed_times)
        confirmation_tps = confirmed_count / confirmation_time if confirmation_time > 0 else 0
        
        # Calculate confirmation delay statistics
        delays = list(confirmed_times.values()) if confirmed_times else [0]
        avg_delay = statistics.mean(delays)
        min_delay = min(delays)
        max_delay = max(delays)
        
        self.logger.info(f"Confirmed {confirmed_count} transactions in {confirmation_time:.2f}s")
        self.logger.info(f"Confirmation TPS: {confirmation_tps:.2f}")
        self.logger.info(f"Average confirmation delay: {avg_delay:.2f}s")
        
        return BenchmarkResult(
            total_transactions=num_transactions,
            submission_time=submission_time,
            submission_tps=submission_tps,
            confirmed_transactions=confirmed_count,
            confirmation_time=confirmation_time,
            confirmation_tps=confirmation_tps,
            failed_submissions=failed_submissions,
            average_confirmation_delay=avg_delay,
            min_confirmation_delay=min_delay,
            max_confirmation_delay=max_delay,
            mempool_peak_size=peak_mempool_size,
            errors=errors
        )
    
    def print_results(self, result: BenchmarkResult) -> None:
        """Print benchmark results in a formatted way."""
        print("\n" + "="*60)
        print("BASELINE BLOCKCHAIN TPS BENCHMARK RESULTS")
        print("="*60)
        print(f"Total Transactions Attempted: {result.total_transactions}")
        print(f"Successfully Submitted: {result.total_transactions - result.failed_submissions}")
        print(f"Failed Submissions: {result.failed_submissions}")
        print()
        print("SUBMISSION PERFORMANCE:")
        print(f"  Time: {result.submission_time:.2f} seconds")
        print(f"  TPS:  {result.submission_tps:.2f} transactions/second")
        print()
        print("CONFIRMATION PERFORMANCE:")
        print(f"  Confirmed Transactions: {result.confirmed_transactions}")
        print(f"  Confirmation Time: {result.confirmation_time:.2f} seconds")
        print(f"  Confirmation TPS: {result.confirmation_tps:.2f} transactions/second")
        print()
        print("TIMING STATISTICS:")
        print(f"  Average Confirmation Delay: {result.average_confirmation_delay:.2f}s")
        print(f"  Min Confirmation Delay: {result.min_confirmation_delay:.2f}s")
        print(f"  Max Confirmation Delay: {result.max_confirmation_delay:.2f}s")
        print()
        print("SYSTEM METRICS:")
        print(f"  Peak Mempool Size: {result.mempool_peak_size} transactions")
        print()
        if result.errors:
            print("ERRORS ENCOUNTERED:")
            for error in result.errors[:5]:  # Show first 5 errors
                print(f"  - {error}")
            if len(result.errors) > 5:
                print(f"  ... and {len(result.errors) - 5} more errors")
        print("="*60)


def main():
    parser = argparse.ArgumentParser(description="TPS Benchmark for Baseline Blockchain")
    parser.add_argument("--config", required=True, help="Path to config.json file")
    parser.add_argument("--transactions", type=int, default=100, 
                       help="Number of transactions to generate (default: 100)")
    parser.add_argument("--workers", type=int, default=10,
                       help="Number of worker threads for submission (default: 10)")
    parser.add_argument("--log-level", default="INFO",
                       choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       help="Logging level (default: INFO)")
    parser.add_argument("--output", help="Save results to JSON file")
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Run benchmark
    benchmark = TPSBenchmark(args.config)
    
    # Check node status first
    if not benchmark.check_node_status():
        print("ERROR: Node is not running or not responsive!")
        print("Please start the node with: baseline-node --config config.json")
        return 1
    
    # Run the benchmark
    result = benchmark.run_submission_benchmark(args.transactions, args.workers)
    
    # Print results
    benchmark.print_results(result)
    
    # Save to file if requested
    if args.output:
        output_data = {
            "timestamp": time.time(),
            "config": {
                "transactions": args.transactions,
                "workers": args.workers
            },
            "results": {
                "total_transactions": result.total_transactions,
                "submission_time": result.submission_time,
                "submission_tps": result.submission_tps,
                "confirmed_transactions": result.confirmed_transactions,
                "confirmation_time": result.confirmation_time,
                "confirmation_tps": result.confirmation_tps,
                "failed_submissions": result.failed_submissions,
                "average_confirmation_delay": result.average_confirmation_delay,
                "min_confirmation_delay": result.min_confirmation_delay,
                "max_confirmation_delay": result.max_confirmation_delay,
                "mempool_peak_size": result.mempool_peak_size,
                "errors": result.errors
            }
        }
        
        with open(args.output, 'w') as f:
            json.dump(output_data, f, indent=2)
        print(f"\nResults saved to {args.output}")
    
    return 0


if __name__ == "__main__":
    exit(main())