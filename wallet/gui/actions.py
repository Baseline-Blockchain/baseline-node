"""Action helpers for wallet commands, dialogs, and transactions."""

from __future__ import annotations

import contextlib
import tkinter as tk
from datetime import datetime
from tkinter import filedialog, messagebox, simpledialog, ttk
from typing import Any

from ..client import RPCClient
from ..helpers import fetch_wallet_info


def parse_schedule_target(raw: str) -> int:
    """Convert user-facing schedule text into numeric lock_time."""

    value = raw.strip()
    if not value:
        raise ValueError("Scheduled date or height is required.")
    if value.isdigit():
        return int(value)
    cleaned = value.upper()
    if cleaned.endswith("UTC"):
        value = value[:-3].strip()
        cleaned = value.upper()
    if cleaned.endswith("Z"):
        value = value[:-1].strip()
        cleaned = value.upper()
    dt = None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        pass
    if dt is None:
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(value, fmt)
                break
            except ValueError:
                continue
    if dt is None:
        raise ValueError("Scheduled date must be YYYY-MM-DD[ HH:MM[:SS]] or a block height.")
    return int(dt.replace(microsecond=0).timestamp())


class ActionMixin:
    """Mixin for user actions and dialog flows."""

    def _extract_tx_address(self, entry: dict[str, Any]) -> str:
        address = entry.get("address")
        if address:
            return str(address)
        addresses = entry.get("addresses")
        if isinstance(addresses, list) and addresses:
            return str(addresses[0])
        label = entry.get("label")
        if label:
            return f"[{label}]"
        details = entry.get("details")
        if isinstance(details, list):
            for detail in details:
                addr = detail.get("address")
                if addr:
                    return str(addr)
        return "(internal)"

    def _summarize_details(self, entry: dict[str, Any], limit: int = 2) -> str:
        detail_entries: list[str] = []
        details = entry.get("details")
        if isinstance(details, list) and details:
            for detail in details:
                addr = detail.get("address") or "(internal)"
                amt = detail.get("amount")
                amt_str = f"{amt:.8f}" if isinstance(amt, (int, float)) else str(amt)
                cat = detail.get("category", "")
                detail_entries.append(f"{cat}: {amt_str} -> {addr}")
        else:
            addresses = entry.get("addresses")
            if isinstance(addresses, list) and addresses:
                for addr in addresses:
                    detail_entries.append(str(addr))
        if not detail_entries:
            detail_entries.append("(internal transfer)")
        if len(detail_entries) > limit:
            preview = ", ".join(detail_entries[:limit])
            return f"{preview} (+{len(detail_entries) - limit} more)"
        return ", ".join(detail_entries)

    def _show_transaction_details(self, _event: tk.Event[tk.Misc]) -> None:
        if not self.tx_tree:
            return
        selection = self.tx_tree.selection()
        if not selection:
            return
        iid = selection[0]
        entry = self.tx_row_map.get(iid)
        if not entry:
            return
        window = tk.Toplevel(self)
        window.title("Transaction Details")
        window.geometry("520x380")
        text = tk.Text(window, wrap="word", state="normal")
        text.pack(fill="both", expand=True)
        lines = [
            f"TxID: {entry.get('txid', '')}",
            f"Time: {datetime.fromtimestamp(entry.get('time', 0)).strftime('%Y-%m-%d %H:%M:%S') if entry.get('time') else 'n/a'}",
            f"Category: {entry.get('category', '')}",
            f"Amount: {entry.get('amount', 0.0):.8f} BLINE",
            f"Confirmations: {entry.get('confirmations', 0)}",
            f"Fee: {entry.get('fee', 0.0)}",
            "",
            "Details:",
        ]
        details = entry.get("details")
        if isinstance(details, list) and details:
            for detail in details:
                addr = detail.get("address") or "(internal)"
                amt = detail.get("amount", 0.0)
                cat = detail.get("category", "")
                lines.append(f"  - {cat}: {amt:.8f} BLINE -> {addr}")
        else:
            for addr in entry.get("addresses", []):
                lines.append(f"  - {addr}")
        text.insert("1.0", "\n".join(lines))
        text.config(state="disabled")

    def _handle_wallet_setup(self) -> None:
        try:
            client = self._build_client()
            info = fetch_wallet_info(client)
        except Exception as exc:
            messagebox.showerror("Wallet Setup", f"Failed to query wallet: {exc}")
            return
        self._setup_skipped = False
        self._launch_initial_setup(info)

    def _handle_encrypt_wallet(self) -> None:
        if self._encrypt_wallet_flow():
            self.refresh_all()

    def _launch_initial_setup(self, info: dict[str, Any]) -> None:
        if self._setup_window and self._setup_window.winfo_exists():
            self._update_setup_labels(info)
            self._setup_window.deiconify()
            self._setup_window.lift()
            return
        win = tk.Toplevel(self)
        win.title("Wallet Setup")
        win.geometry("500x340")
        win.transient(self)
        self._setup_window = win
        frame = ttk.Frame(win, padding=14)
        frame.pack(fill="both", expand=True)
        ttk.Label(frame, textvariable=self._setup_message_var, wraplength=460).pack(anchor="w", pady=(0, 10))
        ttk.Label(frame, textvariable=self._setup_info_var, style="CardLabel.TLabel").pack(anchor="w", pady=(0, 6))
        ttk.Button(frame, text="Create Address", style="Primary.TButton", command=self._setup_create_initial_address).pack(
            anchor="w", pady=(10, 4)
        )
        ttk.Label(frame, textvariable=self._setup_address_var, style="CardLabel.TLabel").pack(anchor="w")
        self._setup_encrypt_button = ttk.Button(frame, text="Encrypt Wallet...", style="Primary.TButton", command=self._setup_encrypt_wallet)
        self._setup_encrypt_button.pack(anchor="w", pady=(12, 4))
        ttk.Label(frame, textvariable=self._setup_encrypt_var, style="CardLabel.TLabel").pack(anchor="w")
        ttk.Button(frame, text="Skip for now", command=self._skip_setup).pack(anchor="e", pady=(20, 0))
        self._update_setup_labels(info)

    def _close_setup_window(self) -> None:
        if self._setup_window and self._setup_window.winfo_exists():
            self._setup_window.destroy()
        self._setup_window = None

    def _skip_setup(self) -> None:
        self._setup_skipped = True
        self._close_setup_window()

    def _update_setup_labels(self, info: dict[str, Any]) -> None:
        count = info.get("address_count", 0)
        if count:
            self._setup_message_var.set("Wallet already exists. You can create additional addresses or encrypt/back up here.")
        else:
            self._setup_message_var.set(
                "This node does not have a wallet yet. Create the first address and optionally encrypt it to continue."
            )
        self._setup_info_var.set(f"Address count: {count}")
        encrypted = bool(info.get("encrypted"))
        self._setup_encrypt_var.set("Wallet encrypted" if encrypted else "Wallet not encrypted")
        if self._setup_encrypt_button:
            if encrypted:
                self._setup_encrypt_button.state(["disabled"])
            else:
                self._setup_encrypt_button.state(["!disabled"])

    def _setup_create_initial_address(self) -> None:
        try:
            client = self._build_client()
            unlocked = self._ensure_unlocked(client)
            if unlocked is None:
                return
            try:
                new_addr = client.call("getnewaddress", [])
            finally:
                if unlocked:
                    with contextlib.suppress(Exception):
                        client.call("walletlock", [])
        except Exception as exc:
            messagebox.showerror("Create Address", f"Failed to create address: {exc}")
            return
        self._setup_address_var.set(f"Created address: {new_addr}")
        messagebox.showinfo("Create Address", f"New receiving address:\n{new_addr}")
        self.refresh_all()

    def _setup_encrypt_wallet(self) -> None:
        if self._encrypt_wallet_flow():
            self.refresh_all()

    def _handle_new_address(self) -> None:
        label = simpledialog.askstring("New Address", "Label for the new address (optional):", parent=self) or ""
        try:
            client = self._build_client()
            unlocked = self._ensure_unlocked(client)
            if unlocked is None:
                return
            params: list[Any] = []
            if label:
                params.append(label)
            try:
                new_addr = client.call("getnewaddress", params)
            finally:
                if unlocked:
                    with contextlib.suppress(Exception):
                        client.call("walletlock", [])
        except Exception as exc:
            messagebox.showerror("New Address", f"Failed to create address: {exc}")
            return
        messagebox.showinfo("New Address", f"Created receiving address:\n{new_addr}")
        self.refresh_all()

    def _encrypt_wallet_flow(self) -> bool:
        pass1 = simpledialog.askstring("Encrypt Wallet", "Enter passphrase:", show="*", parent=self)
        if not pass1:
            return False
        pass2 = simpledialog.askstring("Encrypt Wallet", "Confirm passphrase:", show="*", parent=self)
        if pass1 != pass2:
            messagebox.showwarning("Encrypt Wallet", "Passphrases did not match.")
            return False
        try:
            client = self._build_client()
            client.call("encryptwallet", [pass1])
        except Exception as exc:
            messagebox.showerror("Encrypt Wallet", f"Failed to encrypt wallet: {exc}")
            return False
        messagebox.showinfo("Encrypt Wallet", "Wallet encrypted. Restart the node to continue.")
        return True

    def _handle_dump_wallet(self) -> None:
        path = filedialog.asksaveasfilename(
            title="Dump Wallet",
            defaultextension=".json",
            filetypes=[("JSON", "*.json"), ("All files", "*.*")],
            parent=self,
        )
        if not path:
            return
        try:
            client = self._build_client()
            unlocked = self._ensure_unlocked(client)
            if unlocked is None:
                return
            try:
                client.call("dumpwallet", [path])
            finally:
                if unlocked:
                    with contextlib.suppress(Exception):
                        client.call("walletlock", [])
        except Exception as exc:
            messagebox.showerror("Dump Wallet", f"Failed to dump wallet: {exc}")
            return
        messagebox.showinfo("Dump Wallet", f"Wallet backup saved to:\n{path}")

    def _handle_import_wallet(self) -> None:
        path = filedialog.askopenfilename(
            title="Import Wallet",
            filetypes=[("JSON", "*.json"), ("All files", "*.*")],
            parent=self,
        )
        if not path:
            return
        rescan = messagebox.askyesno("Import Wallet", "Rescan the blockchain after import?")
        try:
            client = self._build_client()
            unlocked = self._ensure_unlocked(client)
            if unlocked is None:
                return
            try:
                client.call("importwallet", [path, rescan])
            finally:
                if unlocked:
                    with contextlib.suppress(Exception):
                        client.call("walletlock", [])
        except Exception as exc:
            messagebox.showerror("Import Wallet", f"Failed to import wallet: {exc}")
            return
        messagebox.showinfo("Import Wallet", "Wallet imported successfully.")
        self.refresh_all()

    def _handle_import_privkey(self) -> None:
        key = simpledialog.askstring("Import Private Key", "Enter WIF private key:", parent=self)
        if not key:
            return
        label = simpledialog.askstring("Import Private Key", "Enter label (optional):", parent=self) or ""
        rescan = messagebox.askyesno("Import Private Key", "Rescan the blockchain for this key?", parent=self)
        try:
            client = self._build_client()
            unlocked = self._ensure_unlocked(client)
            if unlocked is None:
                return
            try:
                client.call("importprivkey", [key, label, rescan])
            finally:
                if unlocked:
                    with contextlib.suppress(Exception):
                        client.call("walletlock", [])
        except Exception as exc:
            messagebox.showerror("Import Private Key", f"Failed to import key: {exc}")
            return
        messagebox.showinfo("Import Private Key", "Private key imported.")
        self.refresh_all()

    def _handle_rescan_wallet(self) -> None:
        if not messagebox.askyesno(
            "Rescan Wallet",
            "Rescan the entire blockchain for wallet transactions? This may take a while.",
        ):
            return
        try:
            client = self._build_client()
            client.call("rescanwallet", [])
        except Exception as exc:
            messagebox.showerror("Rescan Wallet", f"Failed to start rescan: {exc}")
            return
        messagebox.showinfo("Rescan Wallet", "Wallet rescan started. Balances will update once it completes.")

    def _send_payment(self) -> None:
        address = self.send_address_var.get().strip()
        amount_raw = self.send_amount_var.get().strip()
        fee_raw = self.send_fee_var.get().strip()
        from_addr = self.send_from_var.get().strip()
        memo = self.send_memo_var.get().strip()
        memo_to = self.send_memo_to_var.get().strip()
        if not address or not amount_raw:
            messagebox.showwarning("Missing Fields", "Destination address and amount are required.")
            return
        try:
            amount = float(amount_raw)
        except ValueError:
            messagebox.showerror("Amount Error", "Amount must be a decimal number.")
            return
        try:
            fee = float(fee_raw) if fee_raw else None
        except ValueError:
            messagebox.showerror("Fee Error", "Fee must be a decimal number.")
            return

        if from_addr:
            available = self._lookup_balance(from_addr)
            if amount > available:
                messagebox.showerror(
                    "Insufficient Funds",
                    f"Selected address only has {available:.8f} BLINE available.",
                )
                return

        client = self._build_client()
        unlocked = self._ensure_unlocked(client)
        if unlocked is None:
            return
        params: list[Any] = [address, amount]
        options: dict[str, Any] = {}
        if fee is not None:
            options["fee"] = fee
        if from_addr:
            options["fromaddresses"] = [from_addr]
        if memo or memo_to or options:
            params.append(memo)
            params.append(memo_to)
            if options:
                params.append(options)

        try:
            txid = client.call("sendtoaddress", params)
        except SystemExit as exc:
            messagebox.showerror("Send Failed", str(exc))
            return
        except Exception as exc:
            messagebox.showerror("Send Failed", str(exc))
            return
        finally:
            if unlocked:
                try:
                    client.call("walletlock", [])
                except Exception as exc:
                    self.status_var.set(f"RPC status: warning (walletlock failed: {exc})")
        messagebox.showinfo("Transaction Sent", f"Broadcasted transaction:\n{txid}")
        self.refresh_all()
        self.send_address_var.set("")
        self.send_amount_var.set("")
        self.send_memo_var.set("")
        self.send_memo_to_var.set("")

    def _ensure_unlocked(self, client: RPCClient) -> bool | None:
        try:
            info = fetch_wallet_info(client)
        except Exception as exc:
            messagebox.showerror("RPC Error", f"Unable to check wallet status: {exc}")
            return None
        self.wallet_info = info
        if not info.get("encrypted"):
            return False
        if not info.get("locked"):
            return True
        passphrase = simpledialog.askstring("Unlock Wallet", "Wallet passphrase:", show="*", parent=self)
        if not passphrase:
            return None
        timeout = simpledialog.askinteger("Unlock Duration", "Seconds to keep wallet unlocked:", parent=self, initialvalue=120, minvalue=30)
        if timeout is None:
            return None
        try:
            client.call("walletpassphrase", [passphrase, timeout])
        except SystemExit as exc:
            messagebox.showerror("Unlock Failed", str(exc))
            return None
        return True

    def _parse_schedule_target(self, raw: str) -> int:
        return parse_schedule_target(raw)

    def _create_scheduled_transaction(self) -> None:
        dest = self.schedule_dest_var.get().strip()
        amount_raw = self.schedule_amount_var.get().strip()
        lock_raw = self.schedule_lock_var.get().strip()
        if not dest or not amount_raw or not lock_raw:
            messagebox.showwarning("Missing Fields", "Destination, amount, and scheduled date are required.")
            return
        try:
            amount = float(amount_raw)
        except ValueError:
            messagebox.showerror("Amount Error", "Amount must be a decimal number.")
            return
        try:
            lock_time = self._parse_schedule_target(lock_raw)
        except ValueError as exc:
            messagebox.showerror("Scheduled Date Error", str(exc))
            return
        cancelable = bool(self.schedule_cancelable_var.get())

        client = self._build_client()
        unlocked = self._ensure_unlocked(client)
        if unlocked is None:
            return
        try:
            result = client.call("createscheduledtx", [dest, amount, lock_time, cancelable])
        except Exception as exc:
            messagebox.showerror("Schedule Failed", str(exc))
            return
        finally:
            if unlocked:
                with contextlib.suppress(Exception):
                    client.call("walletlock", [])
        schedule_id = result.get("schedule_id", "n/a")
        messagebox.showinfo(
            "Scheduled Transaction Created",
            f"Schedule {schedule_id} created for {amount:.8f} BLINE locking at {lock_time}.",
        )
        self.schedule_dest_var.set("")
        self.schedule_amount_var.set("")
        default_time = self._default_schedule_time() if hasattr(self, "_default_schedule_time") else ""
        self.schedule_lock_var.set(default_time)
        self.schedule_cancelable_var.set(True)
        self.refresh_all()

    def _cancel_selected_schedule(self) -> None:
        tree = self.schedule_tree
        if not tree:
            return
        selection = tree.selection()
        if not selection:
            messagebox.showwarning("No Selection", "Select a schedule to cancel.")
            return
        entry = self.schedule_row_map.get(selection[0])
        if not entry:
            return
        schedule_id = entry.get("schedule_id")
        if not entry.get("cancelable"):
            messagebox.showwarning("Not Cancelable", "This schedule cannot be canceled.")
            return
        if entry.get("status") != "pending":
            messagebox.showwarning("Schedule Locked", "This schedule is not pending anymore.")
            return

        client = self._build_client()
        unlocked = self._ensure_unlocked(client)
        if unlocked is None:
            return
        try:
            result = client.call("cancelscheduledtx", [schedule_id])
        except Exception as exc:
            messagebox.showerror("Cancel Failed", str(exc))
            return
        finally:
            if unlocked:
                with contextlib.suppress(Exception):
                    client.call("walletlock", [])
        cancel_txid = result.get("cancel_txid", "n/a")
        messagebox.showinfo(
            "Schedule Canceled",
            f"Schedule {schedule_id} canceled. Refund tx: {cancel_txid}",
        )
        self.refresh_all()

    def _on_schedule_selection(self, _event: tk.Event[tk.Misc] | None) -> None:
        self._update_schedule_cancel_button_state()

    def _update_schedule_cancel_button_state(self) -> None:
        button = self._schedule_cancel_button
        tree = self.schedule_tree
        if not button or not tree:
            return
        selection = tree.selection()
        disable = True
        if selection:
            entry = self.schedule_row_map.get(selection[0])
            disable = not entry or not entry.get("cancelable") or entry.get("status") != "pending"
        if disable:
            button.state(["disabled"])
        else:
            button.state(["!disabled"])
