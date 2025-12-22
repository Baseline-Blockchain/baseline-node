#!/usr/bin/env python3
"""Tkinter launcher for Baseline wallet RPC workflows."""

from __future__ import annotations

import contextlib
import tkinter as tk
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox, simpledialog, ttk
from typing import Any

from .client import RPCClient
from .config import load_config_overrides
from .helpers import fetch_wallet_info

PALETTE = {
    "bg": "#f5f7ff",
    "panel": "#e5edff",
    "accent": "#d0ddff",
    "tab_active": "#c2d3ff",
    "status": "#d8e2ff",
    "highlight": "#2563eb",
    "text": "#111827",
    "muted": "#6b7280",
    "button": "#2563eb",
}


def lighten(hex_color: str, factor: float = 0.2) -> str:
    """Return a lighter variant of the hex color."""

    value = int(hex_color.lstrip("#"), 16)
    r = (value >> 16) & 0xFF
    g = (value >> 8) & 0xFF
    b = value & 0xFF
    r = min(255, int(r + (255 - r) * factor))
    g = min(255, int(g + (255 - g) * factor))
    b = min(255, int(b + (255 - b) * factor))
    return f"#{r:02x}{g:02x}{b:02x}"


def human_bytes(value: float) -> str:
    """Return a human friendly byte string."""

    try:
        num = float(value)
    except (TypeError, ValueError):
        return "n/a"
    units = ["B", "KB", "MB", "GB", "TB"]
    for unit in units:
        if abs(num) < 1024.0 or unit == units[-1]:
            return f"{num:.1f} {unit}"
        num /= 1024.0
    return f"{num:.1f} TB"


def shorten(text: str | None, length: int = 20) -> str:
    """Return a shortened hash-style string."""

    if not text:
        return "n/a"
    if len(text) <= length:
        return text
    return f"{text[:length]}…"


ASSET_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG = Path.cwd() / "config.json"


class WalletLauncher(tk.Tk):
    """Desktop UI wrapping the wallet RPC surface."""

    def __init__(self) -> None:
        super().__init__()
        self.title("Baseline Wallet")
        self.geometry("880x660")
        self.resizable(False, False)
        self.configure(bg=PALETTE["bg"])
        self._icon_image: tk.PhotoImage | None = None
        self._load_icon()
        self._apply_theme()
        self._build_menu()

        self.config_path: Path | None = None
        self.rpc_settings: dict[str, Any] = {
            "host": "127.0.0.1",
            "port": 8832,
            "username": "rpcuser",
            "password": "rpcpass",
            "timeout": 15.0,
        }
        self.wallet_info: dict[str, Any] = {}
        self.rpc_online = False

        self.status_var = tk.StringVar(value="RPC status: unknown")
        self.balance_var = tk.StringVar(value="0.0")
        self.height_var = tk.StringVar(value="0")
        self.mempool_var = tk.StringVar(value="0 transactions")
        self.wallet_tip_var = tk.StringVar(value="")
        self.wallet_tip_label: ttk.Label | None = None
        self.chain_progress_var = tk.StringVar(value="n/a")
        self.chain_difficulty_var = tk.StringVar(value="n/a")
        self.chain_hash_var = tk.StringVar(value="n/a")
        self.chain_time_var = tk.StringVar(value="n/a")
        self.peer_count_var = tk.StringVar(value="0 peers")
        self.network_version_var = tk.StringVar(value="n/a")
        self.network_fee_var = tk.StringVar(value="n/a")
        self.mempool_usage_var = tk.StringVar(value="0 / 0 B")
        self.mempool_minfee_var = tk.StringVar(value="n/a")

        self.address_tree: ttk.Treeview | None = None
        self.tx_tree: ttk.Treeview | None = None
        self.mempool_box: tk.Text | None = None
        self.address_records: list[dict[str, Any]] = []
        self.tx_records: list[dict[str, Any]] = []
        self.tx_row_map: dict[str, dict[str, Any]] = {}
        self.from_combo: ttk.Combobox | None = None
        self.wallet_menu: tk.Menu | None = None
        self._encrypt_menu_present = False
        self._setup_window: tk.Toplevel | None = None
        self._setup_message_var = tk.StringVar(value="")
        self._setup_info_var = tk.StringVar(value="")
        self._setup_address_var = tk.StringVar(value="No address created yet.")
        self._setup_encrypt_var = tk.StringVar(value="")
        self._setup_encrypt_button: ttk.Button | None = None
        self._setup_skipped = False

        self.send_address_var = tk.StringVar()
        self.send_amount_var = tk.StringVar()
        self.send_fee_var = tk.StringVar()
        self._auto_fee_value: str | None = None
        self.send_from_var = tk.StringVar()
        self.send_from_balance_var = tk.StringVar(value="Balance: 0.0 BLINE")

        self._build_layout()
        self._load_startup_config()
        self.refresh_all()

    def _build_menu(self) -> None:
        """Create the top-level menu bar with common actions."""

        menubar = tk.Menu(self)
        actions = tk.Menu(menubar, tearoff=False)
        actions.add_command(label="Refresh", command=self.refresh_all)
        actions.add_command(label="Reload Config", command=self._load_config_dialog)
        menubar.add_cascade(label="Actions", menu=actions)

        wallet_menu = tk.Menu(menubar, tearoff=False)
        wallet_menu.add_command(label="Dump Wallet...", command=self._handle_dump_wallet)
        wallet_menu.add_command(label="Import Wallet...", command=self._handle_import_wallet)
        wallet_menu.add_separator()
        wallet_menu.add_command(label="Import Private Key...", command=self._handle_import_privkey)
        wallet_menu.add_separator()
        wallet_menu.add_command(label="Rescan Wallet", command=self._handle_rescan_wallet)
        menubar.add_cascade(label="Wallet", menu=wallet_menu)
        self.wallet_menu = wallet_menu
        self.config(menu=menubar)

    def _update_from_balance(self, *_: object) -> None:
        """Update the displayed balance for the selected from-address."""

        addr = self.send_from_var.get().strip()
        balance = self._lookup_balance(addr)
        self.send_from_balance_var.set(f"Balance: {balance:.8f} BLINE")

    def _update_wallet_menu_state(self, info: dict[str, Any]) -> None:
        if not self.wallet_menu:
            return
        should_show = not info.get("encrypted")
        if should_show and not self._encrypt_menu_present:
            self.wallet_menu.insert(
                0,
                "command",
                label="Encrypt Wallet...",
                command=self._handle_encrypt_wallet,
            )
            self._encrypt_menu_present = True
        elif not should_show and self._encrypt_menu_present:
            idx = self._find_menu_entry(self.wallet_menu, "Encrypt Wallet...")
            if idx is not None:
                self.wallet_menu.delete(idx)
            self._encrypt_menu_present = False

    def _lookup_balance(self, address: str) -> float:
        for record in self.address_records:
            if record["address"] == address:
                return float(record.get("balance", 0.0))
        return 0.0

    def _refresh_from_combo(self) -> None:
        """Refresh the from-address combobox when address data changes."""

        if not self.from_combo:
            return
        spendable = [record["address"] for record in self.address_records if record.get("spendable", True)]
        current = self.send_from_var.get()
        self.from_combo["values"] = spendable
        if spendable:
            if current not in spendable:
                self.send_from_var.set(spendable[0])
        else:
            self.send_from_var.set("")
        self._update_from_balance()

    def _load_icon(self) -> None:
        """Load the packaged logo and apply it as the window icon."""

        png_path = ASSET_DIR / "logo.png"
        if png_path.exists():
            try:
                self._icon_image = tk.PhotoImage(file=str(png_path))
                self.tk.call("wm", "iconphoto", self._w, self._icon_image)
            except Exception as exc:
                print(f"[wallet-gui] Unable to load logo.png: {exc}")
        else:
            print("[wallet-gui] logo.png not found; using default Tk icon")

    def _apply_theme(self) -> None:
        """Configure ttk styles for the light palette."""

        style = ttk.Style()
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass

        self.option_add("*Foreground", PALETTE["text"])
        self.option_add("*Background", PALETTE["bg"])
        style.configure("TFrame", background=PALETTE["bg"])
        style.configure("Card.TFrame", background=PALETTE["panel"])
        style.configure("TLabel", background=PALETTE["bg"], foreground=PALETTE["text"])
        style.configure("Status.TLabel", background=PALETTE["status"], foreground=PALETTE["muted"])
        style.configure("StatusBar.TFrame", background=PALETTE["status"])
        style.configure("CardLabel.TLabel", background=PALETTE["panel"], foreground=PALETTE["muted"])
        style.configure(
            "MetricLabel.TLabel",
            background=PALETTE["panel"],
            foreground=PALETTE["text"],
            font=("Segoe UI", 12),
        )
        style.configure(
            "MetricLarge.TLabel",
            background=PALETTE["panel"],
            foreground=PALETTE["text"],
            font=("Segoe UI", 15),
        )
        style.configure(
            "MonoLabel.TLabel",
            background=PALETTE["panel"],
            foreground=PALETTE["text"],
            font=("Consolas", 10),
        )
        style.configure(
            "SectionHeading.TLabel",
            background=PALETTE["panel"],
            foreground=PALETTE["muted"],
            font=("Segoe UI", 9),
        )
        style.configure(
            "Primary.TButton",
            background=PALETTE["button"],
            foreground="#ffffff",
            borderwidth=0,
            focusthickness=1,
            focuscolor=PALETTE["highlight"],
            padding=(12, 6),
        )
        style.map(
            "Primary.TButton",
            background=[("active", lighten(PALETTE["button"]))],
            foreground=[("disabled", PALETTE["muted"])],
        )
        style.configure(
            "TEntry",
            fieldbackground=PALETTE["panel"],
            background=PALETTE["panel"],
            foreground=PALETTE["text"],
            bordercolor=PALETTE["accent"],
            insertcolor=PALETTE["text"],
            padding=4,
        )
        style.configure(
            "TNotebook",
            background=PALETTE["bg"],
            borderwidth=0,
            tabmargins=(4, 4, 4, 0),
        )
        style.configure(
            "TNotebook.Tab",
            background=PALETTE["panel"],
            foreground=PALETTE["muted"],
            padding=(14, 6),
            borderwidth=0,
        )
        style.map(
            "TNotebook.Tab",
            background=[("selected", PALETTE["tab_active"])],
            foreground=[("selected", PALETTE["text"])],
        )
        style.configure(
            "Card.Treeview",
            background=PALETTE["panel"],
            fieldbackground=PALETTE["panel"],
            foreground=PALETTE["text"],
            rowheight=24,
            bordercolor=PALETTE["accent"],
        )
        style.configure(
            "Treeview.Heading",
            background=PALETTE["accent"],
            foreground=PALETTE["text"],
            padding=(6, 4),
        )
        style.map(
            "Card.Treeview",
            background=[("selected", PALETTE["highlight"])],
            foreground=[("selected", PALETTE["text"])],
        )
        style.map(
            "Treeview",
            background=[("selected", PALETTE["highlight"])],
            foreground=[("selected", "#ffffff")],
        )

    # ------------------------------------------------------------------ UI
    def _build_layout(self) -> None:
        container = ttk.Frame(self, padding=(10, 10, 10, 10))
        container.pack(fill="both", expand=True)

        notebook = ttk.Notebook(container)
        notebook.pack(fill="both", expand=True)

        # Overview tab
        overview = ttk.Frame(notebook, padding=10)
        notebook.add(overview, text="Overview")
        self._build_overview_tab(overview)

        # Addresses tab
        addresses = ttk.Frame(notebook, padding=10)
        notebook.add(addresses, text="Addresses")
        self._build_addresses_tab(addresses)

        # History tab
        history = ttk.Frame(notebook, padding=10)
        notebook.add(history, text="History")
        self._build_history_tab(history)

        # Send tab
        send = ttk.Frame(notebook, padding=10)
        notebook.add(send, text="Send")
        self._build_send_tab(send)

        # Mempool tab
        mempool = ttk.Frame(notebook, padding=10)
        notebook.add(mempool, text="Mempool")
        self._build_mempool_tab(mempool)

        status_bar = ttk.Frame(container, style="StatusBar.TFrame", padding=(10, 8))
        status_bar.pack(fill="x", pady=(8, 0))
        ttk.Label(status_bar, textvariable=self.status_var, style="Status.TLabel").pack(anchor="w")

    def _build_overview_tab(self, frame: ttk.Frame) -> None:
        stats = ttk.Frame(frame)
        stats.pack(fill="x")
        stat_cards = (
            ("BALANCE (BLINE)", self.balance_var),
            ("CHAIN HEIGHT", self.height_var),
            ("PEERS", self.peer_count_var),
            ("MEMPOOL", self.mempool_var),
        )
        for idx, (title, var) in enumerate(stat_cards):
            card = ttk.Frame(stats, style="Card.TFrame", padding=16)
            card.grid(row=0, column=idx, padx=(0 if idx == 0 else 10, 0), sticky="nsew")
            ttk.Label(card, text=title, style="SectionHeading.TLabel").pack(anchor="w")
            ttk.Label(card, textvariable=var, style="MetricLarge.TLabel").pack(anchor="w", pady=(4, 0))
            stats.columnconfigure(idx, weight=1)

        self.wallet_tip_label = ttk.Label(frame, textvariable=self.wallet_tip_var, style="CardLabel.TLabel", padding=(12, 4))
        self._update_wallet_tip(None)

        detail = ttk.Frame(frame, padding=(0, 12, 0, 0))
        detail.pack(fill="both", expand=True)
        detail.columnconfigure(0, weight=1)
        detail.columnconfigure(1, weight=1)
        detail.columnconfigure(2, weight=1)

        chain_card = ttk.Frame(detail, style="Card.TFrame", padding=16)
        chain_card.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        ttk.Label(chain_card, text="CHAIN STATUS", style="SectionHeading.TLabel").pack(anchor="w")
        self._add_overview_row(chain_card, "Sync Progress", self.chain_progress_var)
        self._add_overview_row(chain_card, "Difficulty", self.chain_difficulty_var)
        self._add_overview_row(chain_card, "Last Block Time", self.chain_time_var)
        self._add_overview_row(chain_card, "Best Block", self.chain_hash_var, style="MonoLabel.TLabel", wrap=280)

        network_card = ttk.Frame(detail, style="Card.TFrame", padding=16)
        network_card.grid(row=0, column=1, sticky="nsew", padx=8)
        ttk.Label(network_card, text="NETWORK", style="SectionHeading.TLabel").pack(anchor="w")
        self._add_overview_row(network_card, "Connections", self.peer_count_var)
        self._add_overview_row(network_card, "Client Version", self.network_version_var)
        self._add_overview_row(network_card, "Relay Fee", self.network_fee_var)

        mempool_card = ttk.Frame(detail, style="Card.TFrame", padding=16)
        mempool_card.grid(row=0, column=2, sticky="nsew", padx=(8, 0))
        ttk.Label(mempool_card, text="MEMPOOL", style="SectionHeading.TLabel").pack(anchor="w")
        self._add_overview_row(mempool_card, "Transactions", self.mempool_var)
        self._add_overview_row(mempool_card, "Usage", self.mempool_usage_var)
        self._add_overview_row(mempool_card, "Min Fee", self.mempool_minfee_var)

    def _add_overview_row(
        self,
        container: ttk.Frame,
        label: str,
        var: tk.StringVar,
        *,
        style: str = "MetricLabel.TLabel",
        wrap: int | None = None,
    ) -> None:
        wrapper = ttk.Frame(container, style="Card.TFrame")
        wrapper.pack(fill="x", pady=(8, 0))
        ttk.Label(wrapper, text=label, style="SectionHeading.TLabel").pack(anchor="w")
        lbl = ttk.Label(wrapper, textvariable=var, style=style)
        if wrap:
            lbl.configure(wraplength=wrap, justify="left")
        lbl.pack(anchor="w")

    def _find_menu_entry(self, menu: tk.Menu, label: str) -> int | None:
        end_index = menu.index("end")
        if end_index is None:
            return None
        for idx in range(end_index + 1):
            if menu.type(idx) == "separator":
                continue
            if menu.entrycget(idx, "label") == label:
                return idx
        return None

    def _update_wallet_tip(self, message: str | None) -> None:
        if not self.wallet_tip_label:
            return
        if message:
            self.wallet_tip_var.set(message)
            if not self.wallet_tip_label.winfo_ismapped():
                self.wallet_tip_label.pack(fill="x", pady=(12, 0))
        else:
            self.wallet_tip_var.set("")
            if self.wallet_tip_label.winfo_ismapped():
                self.wallet_tip_label.pack_forget()

    def _build_addresses_tab(self, frame: ttk.Frame) -> None:
        columns = ("address", "label", "spendable", "balance")
        tree = ttk.Treeview(frame, columns=columns, show="headings", height=18, style="Card.Treeview")
        self.address_tree = tree
        tree.heading("address", text="Address")
        tree.heading("label", text="Label")
        tree.heading("spendable", text="Spendable")
        tree.heading("balance", text="Balance")
        tree.column("address", width=360)
        tree.column("label", width=160)
        tree.column("spendable", width=100, anchor="center")
        tree.column("balance", width=120, anchor="e")
        odd_color = lighten(PALETTE["accent"], 0.35)
        tree.tag_configure("even", background=PALETTE["panel"], foreground=PALETTE["text"])
        tree.tag_configure("odd", background=odd_color, foreground=PALETTE["text"])
        tree.tag_configure("offline", background=PALETTE["panel"], foreground=PALETTE["muted"])
        tree.pack(fill="both", expand=True)

    def _build_history_tab(self, frame: ttk.Frame) -> None:
        columns = ("time", "txid", "category", "amount", "confirmations")
        tree = ttk.Treeview(frame, columns=columns, show="headings", height=18, style="Card.Treeview")
        self.tx_tree = tree
        tree.heading("time", text="Time")
        tree.heading("txid", text="TxID")
        tree.heading("category", text="Type")
        tree.heading("amount", text="Amount (BLINE)")
        tree.heading("confirmations", text="Conf")
        tree.column("time", width=150)
        tree.column("txid", width=260)
        tree.column("category", width=120, anchor="center")
        tree.column("amount", width=150, anchor="e")
        tree.column("confirmations", width=90, anchor="center")
        tree.bind("<Double-1>", self._show_transaction_details)
        tree.pack(fill="both", expand=True)

    def _build_send_tab(self, frame: ttk.Frame) -> None:
        ttk.Label(frame, text="From Address").grid(row=0, column=0, sticky="w")
        self.from_combo = ttk.Combobox(frame, textvariable=self.send_from_var, state="readonly", width=60)
        self.from_combo.grid(row=1, column=0, columnspan=2, sticky="ew", pady=4)
        self.from_combo.bind("<<ComboboxSelected>>", self._update_from_balance)
        ttk.Label(frame, textvariable=self.send_from_balance_var, style="CardLabel.TLabel").grid(
            row=2, column=0, columnspan=2, sticky="w", pady=(0, 10)
        )

        ttk.Label(frame, text="Destination Address").grid(row=3, column=0, sticky="w")
        ttk.Entry(frame, textvariable=self.send_address_var, width=60).grid(row=4, column=0, columnspan=2, sticky="ew", pady=4)

        ttk.Label(frame, text="Amount (BLINE)").grid(row=5, column=0, sticky="w")
        ttk.Entry(frame, textvariable=self.send_amount_var, width=20).grid(row=6, column=0, sticky="w", pady=4)

        ttk.Label(frame, text="Fee (BLINE)").grid(row=5, column=1, sticky="w")
        ttk.Entry(frame, textvariable=self.send_fee_var, width=20).grid(row=6, column=1, sticky="w", pady=4)

        ttk.Button(frame, text="Send Payment", command=self._send_payment, style="Primary.TButton").grid(
            row=7, column=0, pady=15, sticky="w"
        )

        frame.columnconfigure(0, weight=1)

    def _build_mempool_tab(self, frame: ttk.Frame) -> None:
        self.mempool_box = tk.Text(
            frame,
            height=20,
            wrap="none",
            bg=PALETTE["panel"],
            fg=PALETTE["text"],
            insertbackground=PALETTE["text"],
            highlightbackground=PALETTE["accent"],
            highlightcolor=PALETTE["accent"],
            relief="flat",
        )
        self.mempool_box.pack(fill="both", expand=True)

    # ------------------------------------------------------------------ Config
    def _load_startup_config(self) -> None:
        if DEFAULT_CONFIG.exists():
            self._apply_config(DEFAULT_CONFIG)
        else:
            messagebox.showwarning("Config Missing", "config.json not found at repository root. Select it manually.")
            self._load_config_dialog()

    def _load_config_dialog(self) -> None:
        path = filedialog.askopenfilename(
            title="Select config.json",
            filetypes=[("JSON", "*.json"), ("All files", "*.*")],
        )
        if path:
            self._apply_config(Path(path))

    def _apply_config(self, path: Path) -> None:
        try:
            overrides = load_config_overrides(path)
        except SystemExit as exc:
            messagebox.showerror("Config Error", str(exc))
            return
        except Exception as exc:
            messagebox.showerror("Config Error", str(exc))
            return
        rpc = overrides.get("rpc", {})
        self.rpc_settings["host"] = rpc.get("host", self.rpc_settings["host"])
        self.rpc_settings["port"] = int(rpc.get("port", self.rpc_settings["port"]))
        self.rpc_settings["username"] = rpc.get("username", self.rpc_settings["username"])
        self.rpc_settings["password"] = rpc.get("password", self.rpc_settings["password"])
        self.config_path = path
        self.status_var.set("RPC status: checking…")
        self.refresh_all()

    # ------------------------------------------------------------------ RPC helpers
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
        self._refresh_addresses()
        self._refresh_transactions()
        self._refresh_mempool()
        self._update_fee_estimate()

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
            self.peer_count_var.set("0 peers")
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
            self.height_var.set(f"{processed_height:,}")
        else:
            self.height_var.set(str(processed_height))

        if not info.get("encrypted"):
            self._update_wallet_tip("Wallet is not encrypted. Use Wallet → Encrypt Wallet to protect it.")
        else:
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

        if chain_info:
            blocks = chain_info.get("blocks")
            if isinstance(blocks, int):
                self.height_var.set(f"{blocks:,}")
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
        else:
            self.chain_progress_var.set("n/a")
            self.chain_difficulty_var.set("n/a")
            self.chain_time_var.set("n/a")
            self.chain_hash_var.set("n/a")

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

        self._update_wallet_menu_state(info)
        if info.get("address_count", 0) == 0:
            if not self._setup_skipped:
                self._launch_initial_setup(info)
        else:
            self._setup_skipped = False
            self._close_setup_window()

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

    def _refresh_transactions(self) -> None:
        if not self.tx_tree:
            return
        for row in self.tx_tree.get_children():
            self.tx_tree.delete(row)
        self.tx_records = []
        self.tx_row_map.clear()
        if not self.rpc_online:
            self.tx_tree.insert("", "end", values=("RPC offline", "", "", "", "", ""))
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
                detail_entries.append(f"{cat}: {amt_str} → {addr}")
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
                lines.append(f"  - {cat}: {amt:.8f} BLINE → {addr}")
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
        """Encrypt the wallet from the main menu."""

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
            new_addr = client.call("getnewaddress", [])
        except Exception as exc:
            messagebox.showerror("Create Address", f"Failed to create address: {exc}")
            return
        self._setup_address_var.set(f"Created address: {new_addr}")
        messagebox.showinfo("Create Address", f"New receiving address:\n{new_addr}")
        self.refresh_all()

    def _setup_encrypt_wallet(self) -> None:
        if self._encrypt_wallet_flow():
            self.refresh_all()

    def _encrypt_wallet_flow(self) -> bool:
        pass1 = simpledialog.askstring("Encrypt Wallet", "Enter passphrase:", show="*")
        if not pass1:
            return False
        pass2 = simpledialog.askstring("Encrypt Wallet", "Confirm passphrase:", show="*")
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
        key = simpledialog.askstring("Import Private Key", "Enter WIF private key:")
        if not key:
            return
        label = simpledialog.askstring("Import Private Key", "Enter label (optional):") or ""
        rescan = messagebox.askyesno("Import Private Key", "Rescan the blockchain for this key?")
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

    def _update_fee_estimate(self) -> None:
        """Populate fee field using estimatesmartfee when available."""

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
        except Exception as exc:
            print(f"[wallet-gui] Unable to estimate fee: {exc}")

    def _get_mempool_info(self, client: RPCClient) -> dict[str, Any] | None:
        try:
            return client.call("getmempoolinfo", [])
        except Exception:
            return None

    # ------------------------------------------------------------------ Actions
    def _send_payment(self) -> None:
        address = self.send_address_var.get().strip()
        amount_raw = self.send_amount_var.get().strip()
        fee_raw = self.send_fee_var.get().strip()
        from_addr = self.send_from_var.get().strip()
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
        if unlocked is None:  # user canceled
            return
        params: list[Any] = [address, amount]
        options: dict[str, Any] = {}
        if fee is not None:
            options["fee"] = fee
        if from_addr:
            options["fromaddresses"] = [from_addr]
        if options:
            params.extend(["", "", options])

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
        passphrase = simpledialog.askstring("Unlock Wallet", "Wallet passphrase:", show="*")
        if not passphrase:
            return None
        timeout = simpledialog.askinteger("Unlock Duration", "Seconds to keep wallet unlocked:", initialvalue=120, minvalue=30)
        if timeout is None:
            return None
        try:
            client.call("walletpassphrase", [passphrase, timeout])
        except SystemExit as exc:
            messagebox.showerror("Unlock Failed", str(exc))
            return None
        return True


def main() -> None:
    app = WalletLauncher()
    app.mainloop()


if __name__ == "__main__":
    main()
