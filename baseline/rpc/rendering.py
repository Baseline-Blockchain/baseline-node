import base64
import os
from datetime import datetime
from typing import Any

from ..core.tx import COIN

class DashboardRenderer:
    def __init__(self):
        self.template_path = os.path.join(os.path.dirname(__file__), "templates", "pool.html")
        self.style_path = os.path.join(os.path.dirname(__file__), "static", "style.css")
        self.logo_path = os.path.join(os.path.dirname(__file__), "static", "logo.png")
        self._template_cache = None
        self._css_cache = None
        self._logo_b64_cache = None
        self._base_html_cache = None

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
        
        if self._base_html_cache is None and self._template_cache is not None and self._css_cache is not None:
            html = self._template_cache
            html = html.replace("/* CSS_INJECTION_POINT */", self._css_cache)
            html = html.replace("{{ logo_b64 }}", self._logo_b64_cache) # Replaces all occurrences
            self._base_html_cache = html

    def render(self, handlers: Any, request_path: Any, request_host: str | None = None) -> bytes:
        self._load_assets()
        
        # --- Data Collection Logic (extracted from server.py) ---
        if not getattr(handlers, "stratum", None):
            return b"<html><body><h1>Stratum mining is disabled on this node.</h1></body></html>"

        tracker = getattr(handlers, "payout_tracker", None)
        if not tracker:
            return b"<html><body><h1>Pool payout tracker is not configured.</h1></body></html>"

        # Single lock acquisition to get all dashboard data
        try:
            snapshot = tracker.get_dashboard_snapshot()
        except Exception as exc:
            return f"<html><body><h1>Error rendering pool dashboard: {exc}</h1></body></html>".encode("utf-8")

        # Extract data from snapshot
        pending = snapshot["pending_blocks"]
        matured = snapshot["matured_utxos"]
        payout_history = snapshot.get("payout_history", [])

        # Build entries from workers snapshot
        entries = []
        for worker_id, worker_data in snapshot["workers"].items():
            entries.append({
                "worker_id": worker_id,
                "address": worker_data["address"],
                "balance_liners": worker_data["balance"],
            })

        # Build stats dict (mimicking getpoolstats output structure)
        stats = {
            "pool_fee_percent": snapshot["pool_fee_percent"],
            "min_payout": snapshot["min_payout"],
            "coinbase_maturity": snapshot["maturity"],
            "stratum": {},
        }
        if handlers.stratum:
            try:
                stats["stratum"] = handlers.stratum.snapshot_stats()
            except Exception:
                stats["stratum"] = {}

        
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
             try:
                 sessions = handlers.stratum.snapshot_sessions()
             except Exception:
                 sessions = []
             active_miners = len(sessions)
             for session in sessions:
                 if session.get("authorized") and session.get("worker_id"):
                     active_worker_ids.add(session["worker_id"])
        
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

        payout_rows = ""
        if not payout_history:
            payout_rows = "<tr><td colspan='4' class='empty-state'>No payouts yet</td></tr>"
        else:
            sorted_payments = sorted(
                payout_history, key=lambda entry: float(entry.get("time") or 0.0), reverse=True
            )
            for entry in sorted_payments:
                txid = entry.get("txid", "")
                short_tx = f"{txid[:8]}...{txid[-8:]}" if txid else "unknown"
                total_paid = (int(entry.get("total_paid", 0) or 0)) / COIN
                payees = entry.get("payees", [])
                payees_count = len(payees)
                timestamp = entry.get("time")
                if timestamp:
                    time_label = datetime.fromtimestamp(float(timestamp)).strftime("%Y-%m-%d %H:%M:%S")
                else:
                    time_label = "unknown"

                payout_rows += (
                    f"<tr>"
                    f"<td><span class='hash'>{short_tx}</span></td>"
                    f"<td>{total_paid:.4f}</td>"
                    f"<td>{payees_count}</td>"
                    f"<td>{time_label}</td>"
                    f"</tr>"
                )

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
        html = self._base_html_cache
        
        # Metrics
        html = html.replace("{{ pool_hashrate }}", pool_hash_fmt)
        html = html.replace("{{ active_miners }}", str(active_miners))
        html = html.replace("{{ balance_total }}", f"{owed_total:.4f}")
        html = html.replace("{{ block_height }}", str(best_height))
        html = html.replace("{{ stratum_url }}", stratum_url)
        html = html.replace("{{ stratum_host }}", connect_host)
        html = html.replace("{{ stratum_port }}", str(stratum_port))
        html = html.replace("{{ sync_state }}", sync_state)
        html = html.replace("{{ node_version }}", "0.1.0") # Hardcoded or fetch from config
        
        # Tables
        html = html.replace("{{ workers_rows }}", workers_rows)
        html = html.replace("{{ payouts_rows }}", payout_rows)
        
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
