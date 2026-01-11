import base64
import os
import time
from typing import Any, Dict, List
import contextlib

from ..core.tx import COIN

class DashboardRenderer:
    def __init__(self):
        self.template_path = os.path.join(os.path.dirname(__file__), "templates", "pool.html")
        self.style_path = os.path.join(os.path.dirname(__file__), "static", "style.css")
        self.logo_path = os.path.join(os.path.dirname(__file__), "static", "logo.png")
        self._template_cache = None
        self._css_cache = None
        self._logo_b64_cache = None

    def _load_assets(self):
        if self._template_cache is None:
            with open(self.template_path, "r", encoding="utf-8") as f:
                self._template_cache = f.read()
        
        if self._css_cache is None:
            with open(self.style_path, "r", encoding="utf-8") as f:
                self._css_cache = f.read()
                
        if self._logo_b64_cache is None:
            with open(self.logo_path, "rb") as f:
                self._logo_b64_cache = base64.b64encode(f.read()).decode("utf-8")

    def render(self, handlers: Any, request_path: Any, request_host: str | None = None) -> bytes:
        self._load_assets()
        
        # --- Data Collection Logic (extracted from server.py) ---
        if not getattr(handlers, "stratum", None):
            return b"<html><body><h1>Stratum mining is disabled on this node.</h1></body></html>"

        tracker = getattr(handlers, "payout_tracker", None)
        if tracker:
            with contextlib.suppress(Exception):
                handlers.state_db and tracker.prune_stale_entries(handlers.state_db)

        try:
            stats = handlers.getpoolstats()
            pending = handlers.getpoolpendingblocks()
            matured = handlers.getpoolmatured()
        except Exception as exc:
            return f"<html><body><h1>Error rendering pool dashboard: {exc}</h1></body></html>".encode("utf-8")

        # Data Collection
        entries = []
        if getattr(handlers, "payout_tracker", None):
             tracker = handlers.payout_tracker
             with tracker.lock:
                 for worker_id, state in tracker.workers.items():
                     entries.append({
                         "worker_id": worker_id,
                         "address": state.address,
                         "balance_liners": state.balance,
                         # "round_shares" retrieval would require more access, but we removed shares column
                         # If we needed shares, we'd access tracker.round_shares
                     })

        
        # Calculate stats
        pool_fee = stats.get('pool_fee_percent', 0)
        min_payout_liners = int(stats.get("min_payout", 0) or 0)
        min_payout = min_payout_liners / COIN
        
        owed_total = sum(w.get("balance_liners", 0) for w in entries) / COIN
        
        pool_hash = float(stats.get("stratum", {}).get("pool_hashrate", 0.0) or 0.0)
        pool_hash_fmt = self._format_hashrate(pool_hash)
        
        active_miners = 0
        active_worker_ids = set()
        if getattr(handlers, "stratum", None):
             sessions = handlers.stratum.sessions
             active_miners = len(sessions)
             for session in sessions.values():
                 if session.authorized and session.worker_id:
                     active_worker_ids.add(session.worker_id)
        
        # Connection Info
        stratum = stats.get("stratum", {})
        stratum_host = str(stratum.get("host") or "")
        stratum_port = int(stratum.get("port") or 0)
        connect_host = ""
        if stratum_host and stratum_host not in {"0.0.0.0", "::", "127.0.0.1", "localhost"}:
            connect_host = stratum_host
        elif request_host:
            host = request_host.strip()
            if host.startswith("[") and "]" in host:
                connect_host = host[1 : host.index("]")]
            else:
                connect_host = host.split(":", 1)[0]
        if not connect_host:
            connect_host = "POOL_HOST"
        stratum_url = f"{connect_host}:{stratum_port or 3333}"

        # Sync State / Height
        best_height = 0
        sync_state = "Syncing..."
        if getattr(handlers, "state_db", None):
            best = handlers.state_db.get_best_tip()
            if best:
                best_height = int(best[1])
        
        network = getattr(handlers, "network", None)
        if network:
             if not network.sync_active and not network.header_sync_active:
                 sync_state = "Synced"
             else:
                 sync_state = f"Syncing ({network.sync_remote_height})"

        # Last Payout
        total_paid = 0.0 
        last_payout_time = "Never"
        if tracker and getattr(tracker, "payout_history", None):
            last = tracker.payout_history[-1]
            ts = float(last.get("time", 0.0) or 0.0)
            if ts:
                last_payout_time = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(ts))
            
            total_paid = sum(p.get('total_paid', 0) for p in tracker.payout_history) / COIN


        pending_rows = ""
        if not pending:
            pending_rows = "<tr><td colspan='4' class='empty-state'>No pending blocks</td></tr>"
        if pending:
             for blk in pending:
                 h = blk.get('height')
                 txid = blk.get('txid', '')
                 shorth = f"{txid[:8]}...{txid[-8:]}" if txid else "unknown"
                 reward = (blk.get('distributable', 0) or 0) / COIN
                 # Maturity check
                 maturity_blocks = int(stats.get("coinbase_maturity", 0) or 0)
                 matures_in = "-"
                 if best_height and maturity_blocks:
                     confirms = max(0, best_height - int(h))
                     left = max(0, maturity_blocks - confirms)
                     matures_in = f"{left} blocks" if left > 0 else "Mature"
                 
                 pending_rows += f"<tr><td>{h}</td><td><span class='hash'>{shorth}</span></td><td>{reward:.4f}</td><td>{matures_in}</td></tr>"

        # Calculate Immature Balances
        worker_immature = {}
        if pending:
            for blk in pending:
                 dist = float(blk.get('distributable', 0) or 0)
                 shares = blk.get('shares', {})
                 total_s = sum(shares.values())
                 if total_s > 0:
                     for wid, s in shares.items():
                         portion = (dist * (s / total_s))
                         worker_immature[wid] = worker_immature.get(wid, 0.0) + portion

        # Workers Table
        workers_rows = ""
        # Filter entries to only show active workers
        active_entries = [w for w in entries if w.get('worker_id') in active_worker_ids]
        
        if not active_entries:
            workers_rows = "<tr><td colspan='4' class='empty-state'>No active workers</td></tr>"
        else:
            # Sort by total balance (mature + immature) descending
            for w in active_entries:
                w['total_sort'] = w.get('balance_liners', 0) + worker_immature.get(w.get('worker_id'), 0)
            
            sorted_entries = sorted(active_entries, key=lambda x: x.get('total_sort', 0), reverse=True)
            
            for w in sorted_entries:
                name = w.get('worker_id', 'unknown')
                address = w.get('address', 'unknown')
                short_addr = f"{address[:8]}...{address[-8:]}" if len(address) > 16 else address
                mature = w.get('balance_liners', 0) / COIN
                immature = worker_immature.get(name, 0) / COIN
                
                workers_rows += f"<tr><td>{name}</td><td><span class='hash'>{short_addr}</span></td><td>{immature:.8f}</td><td>{mature:.8f}</td></tr>"

        # --- Replacement ---
        html = self._template_cache
        html = html.replace("/* CSS_INJECTION_POINT */", self._css_cache)
        html = html.replace("{{ logo_b64 }}", self._logo_b64_cache)
        
        # Metrics
        html = html.replace("{{ pool_hashrate }}", pool_hash_fmt)
        html = html.replace("{{ active_miners }}", str(active_miners))
        html = html.replace("{{ balance_total }}", f"{owed_total:.4f}")
        html = html.replace("{{ total_paid }}", f"{total_paid:.4f}") 
        html = html.replace("{{ last_payout_time }}", last_payout_time)
        html = html.replace("{{ block_height }}", str(best_height))
        html = html.replace("{{ stratum_url }}", stratum_url)
        html = html.replace("{{ stratum_host }}", connect_host)
        html = html.replace("{{ stratum_port }}", str(stratum_port))
        html = html.replace("{{ sync_state }}", sync_state)
        html = html.replace("{{ node_version }}", "0.1.0") # Hardcoded or fetch from config
        
        # Tables
        html = html.replace("{{ workers_rows }}", workers_rows)
        html = html.replace("{{ pending_rows }}", pending_rows)
        
        # Meta
        html = html.replace("{{ pool_fee }}", str(pool_fee))
        html = html.replace("{{ min_payout }}", str(min_payout))

        return html.encode("utf-8")

    def _format_hashrate(self, hashes: float) -> str:
        if hashes >= 1e18: return f"{hashes / 1e18:.2f} EH/s"
        if hashes >= 1e15: return f"{hashes / 1e15:.2f} PH/s"
        if hashes >= 1e12: return f"{hashes / 1e12:.2f} TH/s"
        if hashes >= 1e9:  return f"{hashes / 1e9:.2f} GH/s"
        if hashes >= 1e6:  return f"{hashes / 1e6:.2f} MH/s"
        if hashes >= 1e3:  return f"{hashes / 1e3:.2f} kH/s"
        return f"{hashes:.2f} H/s"
