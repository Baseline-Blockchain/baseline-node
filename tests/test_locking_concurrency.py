
import threading
import time
import json
import tempfile
import pathlib
from baseline.mining.payout import PayoutTracker

import threading
import time
import json
import tempfile
import pathlib
from unittest.mock import patch
from baseline.mining.payout import PayoutTracker

def test_payout_tracker_locking_contention():
    """
    Simulates the locking contention in PayoutTracker.
    
    Scenario:
    - Main thread (Stratum) needs to acquire lock frequently for fast operations (record_share).
    - Background thread (Payout Task) periodically saves state, which does I/O while holding lock.
    
    We measure the max latency observed by the main thread when trying to acquire the lock.
    """
    
    # Mock script_from_address to avoid invalid address errors
    with patch("baseline.mining.payout.script_from_address", return_value=b"\x00"*25):
        _run_test()

def _run_test():
    # Setup
    tmp_dir = tempfile.TemporaryDirectory()
    data_path = pathlib.Path(tmp_dir.name) / "payouts.json"
    
    tracker = PayoutTracker(
        data_path=data_path,
        pool_privkey=1,
        pool_pubkey=b'\x00'*33,
        pool_script=b'\x00'*25,
        maturity=10,
        min_payout=1000,
        pool_fee_percent=1.0
    )
    
    # Seed with some data to make save take a non-zero amount of time
    # Increased to 10000 to better simulate load
    for i in range(10000):
        tracker.workers[f"worker_{i}"] = type("WorkerState", (), {"address": "addr", "script": b"", "balance": 100})()
        
    running = True
    latencies = []
    
    def background_saver():
        while running:
            # Simulate the flush that happens in _payout_task
            # We force it to be dirty first
            with tracker.lock:
                tracker._dirty = True
            
            # This calls _save() which does I/O under lock
            tracker.flush() 
            time.sleep(0.1)

    saver_thread = threading.Thread(target=background_saver)
    saver_thread.start()
    
    start_time = time.time()
    try:
        # Simulate Stratum Server processing shares
        while time.time() - start_time < 2.0:
            t0 = time.time()
            # record_share acquires lock
            tracker.record_share("worker_1", "addr", 1.0)
            t1 = time.time()
            latencies.append(t1 - t0)
            time.sleep(0.001) # fast share rate
    finally:
        running = False
        saver_thread.join()
        tracker.stop()
        tmp_dir.cleanup()
        
    max_latency = max(latencies)
    avg_latency = sum(latencies) / len(latencies)
    
    print(f"Max acquisition latency: {max_latency*1000:.2f}ms")
    print(f"Avg acquisition latency: {avg_latency*1000:.2f}ms")
    
    # If I/O is blocking, we expect some spikes > 10ms (depending on disk speed, but with 1000 workers it writes some JSON)
    # On a fast SSD this might be small, but the principle holds. 
    # We want to see this go down significantly after optimization.

if __name__ == "__main__":
    test_payout_tracker_locking_contention()
