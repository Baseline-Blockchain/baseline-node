"""RPC helpers shared by the wallet GUI."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from ..client import RPCClient
from ..helpers import fetch_wallet_info
from .styles import human_bytes, shorten

log = logging.getLogger(__name__)


class RPCMixin:
    """Mixin with RPC helpers and refresh routines."""

    def _build_client(self) -> RPCClient:
        settings = self.rpc_settings
        return RPCClient(
            settings["host"],
            settings["port"],
            settings["username"],
            settings["password"],
            settings.get("timeout", 15.0),
        )

    def refresh_all(self) -> None:
        self._refresh_status()
        self._update_menu_state()
        self._refresh_addresses()
        self._refresh_transactions()
        self._refresh_mempool()
        self._refresh_schedules()
        self._update_fee_estimate()

    def _refresh_from_combo(self) -> None:
        spendable = [record["address"] for record in self.address_records if record.get("spendable", True)]
        if self.from_combo:
            current = self.send_from_var.get()
            self.from_combo["values"] = spendable
            if spendable:
                if current not in spendable:
                    self.send_from_var.set(spendable[0])
            else:
                self.send_from_var.set("")
            self._update_from_balance()

        schedule_combo = getattr(self, "schedule_from_combo", None)
        if schedule_combo:
            current = self.schedule_from_var.get()
            schedule_combo["values"] = spendable
            if spendable:
                if current not in spendable:
                    self.schedule_from_var.set(spendable[0])
            else:
                self.schedule_from_var.set("")
            self._update_schedule_from_balance()

    def _refresh_status(self) -> None:
        try:
            client = self._build_client()
            info = fetch_wallet_info(client)
            wallet_balance = client.call("getbalance", [])
            try:
                chain_info = client.call("getblockchaininfo", [])
            except Exception:
                chain_info = None
            try:
                network_info = client.call("getnetworkinfo", [])
            except Exception:
                network_info = None
        except Exception as exc:
            print(f"[wallet-gui] RPC status refresh failed: {exc}")
            self.rpc_online = False
            self.wallet_info = {}
            self.status_var.set("RPC status: offline - unable to connect")
            self.balance_var.set("0.0")
            self.height_var.set("0")
            self.mempool_var.set("unavailable")
            self._update_wallet_tip("RPC offline. Ensure the node is running and reachable.")
            self.chain_progress_var.set("n/a")
            self.chain_difficulty_var.set("n/a")
            self.chain_hash_var.set("n/a")
            self.chain_time_var.set("n/a")
            self.chain_genesis_var.set("n/a")
            self.wallet_height_var.set("n/a")
            self.peer_count_var.set("0 peers")
            self.network_name_var.set("n/a")
            self.network_version_var.set("n/a")
            self.network_fee_var.set("n/a")
            self.mempool_usage_var.set("0 / 0 B")
            self.mempool_minfee_var.set("n/a")
            return

        self.rpc_online = True
        self.wallet_info = info
        self.status_var.set("RPC status: connected")
        self.balance_var.set(f"{wallet_balance:,.8f}")
        processed_height = info.get("processed_height", info.get("height", "0"))
        if isinstance(processed_height, int):
            self.wallet_height_var.set(f"{processed_height:,}")
            self.height_var.set(f"{processed_height:,}")
        else:
            self.wallet_height_var.set(str(processed_height))
            self.height_var.set(str(processed_height))
        wallet_height_int = int(processed_height) if isinstance(processed_height, int) else 0

        self._update_wallet_tip(None)

        mempool_info = self._get_mempool_info(client)
        if mempool_info:
            size = mempool_info.get("size") or mempool_info.get("tx", 0)
            self.mempool_var.set(f"{size} transactions")
            usage = mempool_info.get("bytes", 0)
            max_pool = mempool_info.get("maxmempool", 0)
            self.mempool_usage_var.set(f"{human_bytes(usage)} / {human_bytes(max_pool)}")
            min_fee = mempool_info.get("mempoolminfee", mempool_info.get("minrelaytxfee"))
            if isinstance(min_fee, (int, float)):
                self.mempool_minfee_var.set(f"{min_fee:.8f} BLINE/KB")
            else:
                self.mempool_minfee_var.set("n/a")
        else:
            self.mempool_var.set("n/a")
            self.mempool_usage_var.set("n/a")
            self.mempool_minfee_var.set("n/a")

        chain_height_int = 0
        if chain_info:
            blocks = chain_info.get("blocks")
            if isinstance(blocks, int):
                self.height_var.set(f"{blocks:,}")
                chain_height_int = blocks
            progress = chain_info.get("verificationprogress")
            if isinstance(progress, (int, float)):
                self.chain_progress_var.set(f"{progress * 100:.2f}% synced")
            else:
                self.chain_progress_var.set("n/a")
            difficulty = chain_info.get("difficulty")
            if isinstance(difficulty, (int, float)):
                self.chain_difficulty_var.set(f"{difficulty:,.3f}")
            else:
                self.chain_difficulty_var.set("n/a")
            timestamp = chain_info.get("time")
            if isinstance(timestamp, (int, float)):
                self.chain_time_var.set(datetime.fromtimestamp(int(timestamp)).strftime("%Y-%m-%d %H:%M:%S"))
            else:
                self.chain_time_var.set("n/a")
            self.chain_hash_var.set(shorten(chain_info.get("bestblockhash")))
            try:
                genesis = client.call("getblockhash", [0])
            except Exception:
                genesis = None
            self.chain_genesis_var.set(shorten(genesis))
            self.network_name_var.set("unknown")
            if isinstance(genesis, str):
                try:
                    from baseline.core.chain import MAINNET_GENESIS_HASH

                    if genesis == MAINNET_GENESIS_HASH:
                        self.network_name_var.set("mainnet")
                except Exception as exc:  # noqa: BLE001
                    log.debug("Failed to compare genesis hash for network name: %s", exc)
        else:
            self.chain_progress_var.set("n/a")
            self.chain_difficulty_var.set("n/a")
            self.chain_time_var.set("n/a")
            self.chain_hash_var.set("n/a")
            self.chain_genesis_var.set("n/a")
            self.network_name_var.set("n/a")

        if network_info:
            peers = network_info.get("connections", 0)
            inbound = network_info.get("connections_in", network_info.get("connectionsin", 0))
            outbound = network_info.get("connections_out", network_info.get("connectionsout", 0))
            self.peer_count_var.set(f"{peers} peers ({inbound} in / {outbound} out)")
            version = network_info.get("subversion") or f"v{network_info.get('version', 'n/a')}"
            self.network_version_var.set(version)
            relay_fee = network_info.get("relayfee")
            if isinstance(relay_fee, (int, float)):
                self.network_fee_var.set(f"{relay_fee:.8f} BLINE/KB")
            else:
                self.network_fee_var.set("n/a")
        else:
            self.peer_count_var.set("0 peers")
            self.network_version_var.set("n/a")
            self.network_fee_var.set("n/a")

        tips: list[str] = []
        if chain_info and bool(chain_info.get("initialblockdownload")):
            headers_height = chain_info.get("headers", 0)
            headers_height_int = headers_height if isinstance(headers_height, int) else 0
            peers_hint = 0
            if network_info:
                peers_raw = network_info.get("connections", 0)
                peers_hint = peers_raw if isinstance(peers_raw, int) else 0
            if chain_height_int == 0 and headers_height_int == 0 and peers_hint == 0:
                tips.append("Node is at genesis and has no peers yet (no headers downloaded).")
            elif peers_hint == 0:
                tips.append("Node has no peers; it cannot sync beyond local blocks.")
            else:
                tips.append("Initial block download in progress: balances may be incomplete.")
        if chain_height_int and wallet_height_int < chain_height_int:
            tips.append(f"Wallet is syncing: {wallet_height_int:,} / {chain_height_int:,}.")
        if info.get("syncing"):
            tips.append("Wallet background sync is running.")
        if not info.get("encrypted"):
            tips.append("Wallet is not encrypted. Use Wallet → Encrypt Wallet to protect it.")
        self._update_wallet_tip(" ".join(tips) if tips else None)

    def _refresh_addresses(self) -> None:
        if not self.address_tree:
            return
        for row in self.address_tree.get_children():
            self.address_tree.delete(row)
        self.address_records = []
        if not self.rpc_online:
            if self.address_tree:
                self.address_tree.insert("", "end", values=("RPC offline", "", "", ""), tags=("offline",))
            self._refresh_from_combo()
            return

        try:
            client = self._build_client()
            entries = client.call("listaddresses", [])
            balance_map = {
                item["address"]: item["balance"]
                for item in client.call("listaddressbalances", [1])
                if isinstance(item, dict)
            }
        except Exception as exc:
            print(f"[wallet-gui] Unable to load addresses: {exc}")
            self.status_var.set("RPC status: error while loading addresses")
            self._refresh_from_combo()
            return
        for idx, entry in enumerate(entries):
            address = entry.get("address", "")
            label = entry.get("label", "")
            spendable_flag = entry.get("spendable", True)
            spendable = "yes" if spendable_flag else "no"
            balance = balance_map.get(address, 0.0)
            self.address_records.append(
                {"address": address, "label": label, "balance": balance, "spendable": spendable_flag}
            )
            tag = "odd" if idx % 2 else "even"
            self.address_tree.insert("", "end", values=(address, label, spendable, f"{balance:.8f}"), tags=(tag,))
        self._refresh_from_combo()

    def _refresh_mempool(self) -> None:
        if not self.mempool_box:
            return
        self.mempool_box.delete("1.0", "end")
        if not self.rpc_online:
            self.mempool_box.insert("end", "RPC offline.\n")
            return
        try:
            client = self._build_client()
            txids = client.call("getrawmempool", [])
        except Exception as exc:
            self.mempool_box.insert("end", f"Unable to fetch mempool: {exc}\n")
            return
        if not txids:
            self.mempool_box.insert("end", "Mempool empty.\n")
            return
        for txid in txids:
            self.mempool_box.insert("end", txid + "\n")

    def _refresh_schedules(self) -> None:
        tree = self.schedule_tree
        if not tree:
            return
        for row in tree.get_children():
            tree.delete(row)
        self.schedule_row_map.clear()
        self.schedule_records = []
        if not self.rpc_online:
            tree.insert("", "end", values=("RPC offline", "", "", "", "", "", ""), tags=("offline",))
            self._update_schedule_cancel_button_state()
            return
        try:
            client = self._build_client()
            entries = client.call("listscheduledtx", [])
        except Exception as exc:
            tree.insert(
                "",
                "end",
                values=("unable to load schedules", str(exc), "", "", "", "", ""),
                tags=("offline",),
            )
            self._update_schedule_cancel_button_state()
            return
        for idx, entry in enumerate(entries):
            schedule_id = entry.get("schedule_id", "")
            iid = schedule_id or f"schedule-{idx}"
            tag = "odd" if idx % 2 else "even"
            self.schedule_row_map[iid] = entry
            tree.insert(
                "",
                "end",
                iid=iid,
                values=(
                    shorten(schedule_id, 14),
                    entry.get("dest_address", ""),
                    f"{entry.get('amount', 0.0):.8f}",
                    f"{entry.get('fee', 0.0):.8f}",
                    self._format_schedule_time(entry.get("lock_time")),
                    entry.get("status", ""),
                    "✓" if entry.get("cancelable") else "✕",
                ),
                tags=(tag,),
            )
        self.schedule_records = entries
        self._update_schedule_cancel_button_state()

    def _refresh_transactions(self) -> None:
        if not self.tx_tree:
            return
        for row in self.tx_tree.get_children():
            self.tx_tree.delete(row)
        self.tx_records = []
        self.tx_row_map.clear()
        if not self.rpc_online:
            self.tx_tree.insert("", "end", values=("RPC offline", "", "", "", "", ""), tags=("offline",))
            return
        try:
            client = self._build_client()
            entries = client.call("listtransactions", ["*", 50, 0, True])
        except Exception as exc:
            print(f"[wallet-gui] Unable to fetch history: {exc}")
            self.tx_tree.insert("", "end", values=("error fetching history", "", "", "", "", ""))
            return
        for entry in entries:
            timestamp = entry.get("time", 0)
            timestr = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S") if timestamp else ""
            amount = float(entry.get("amount", 0.0))
            category = entry.get("category", "")
            txid = entry.get("txid", "")
            conf = entry.get("confirmations", 0)
            iid = txid or f"tx-{len(self.tx_records)}"
            self.tx_tree.insert(
                "",
                "end",
                iid=iid,
                values=(timestr, txid, category, f"{amount:.8f}", conf),
            )
            self.tx_records.append(entry)
            self.tx_row_map[iid] = entry

    def _update_fee_estimate(self) -> None:
        current = self.send_fee_var.get().strip()
        if current and self._auto_fee_value and current != self._auto_fee_value:
            return
        try:
            client = self._build_client()
            result = client.call("estimatesmartfee", [6])
            feerate = 0.0
            if isinstance(result, dict):
                feerate = float(result.get("feerate") or 0.0)
            else:
                feerate = float(result or 0.0)
            if feerate > 0:
                fee_str = f"{feerate:.8f}"
                self.send_fee_var.set(fee_str)
                self._auto_fee_value = fee_str
                schedule_current = self.schedule_fee_var.get().strip()
                if (not schedule_current) or (
                    self._auto_schedule_fee_value and schedule_current == self._auto_schedule_fee_value
                ):
                    self.schedule_fee_var.set(fee_str)
                    self._auto_schedule_fee_value = fee_str
        except Exception as exc:
            print(f"[wallet-gui] Unable to estimate fee: {exc}")

    def _get_mempool_info(self, client: RPCClient) -> dict[str, Any] | None:
        try:
            return client.call("getmempoolinfo", [])
        except Exception:
            return None

    def _format_schedule_time(self, lock_time: Any) -> str:
        if not isinstance(lock_time, int):
            return ""
        if 1_000_000_000 <= lock_time <= 0xFFFFFFFF:
            try:
                dt = datetime.utcfromtimestamp(lock_time)
                return dt.strftime("%Y-%m-%d %H:%M UTC")
            except (OverflowError, OSError, ValueError):
                pass
        return f"Height {lock_time}"
