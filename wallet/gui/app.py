"""Tkinter-based wallet launcher orchestrating the submodules."""

from __future__ import annotations

from pathlib import Path
import tkinter as tk
from tkinter import ttk
from typing import Any

from .actions import ActionMixin
from .configuration import ConfigMixin
from .layout import LayoutMixin
from .menu import MenuMixin
from .refresh import RefreshMixin
from .rpc import RPCMixin
from .styles import PALETTE


class WalletLauncher(
    tk.Tk,
    LayoutMixin,
    MenuMixin,
    ConfigMixin,
    RPCMixin,
    ActionMixin,
    RefreshMixin,
):
    """Desktop UI wrapping the wallet RPC surface."""

    def __init__(self) -> None:
        super().__init__()
        self.title("Baseline Wallet")
        self.geometry("880x660")
        self.resizable(False, False)
        self.configure(bg=PALETTE["bg"])

        self._icon_image: tk.PhotoImage | None = None

        self.wallet_menu_ready = False
        self._load_icon()
        self._apply_theme()

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
        self.send_memo_var = tk.StringVar()
        self.send_memo_to_var = tk.StringVar()
        self._auto_fee_value: str | None = None
        self.send_from_var = tk.StringVar()
        self.send_from_balance_var = tk.StringVar(value="Balance: 0.0 BLINE")
        self._address_notice_var = tk.StringVar(value="")
        self._address_notice_label: ttk.Label | None = None
        self._address_notice_after: str | None = None
        self._auto_refresh_interval_ms = 5000
        self._auto_refresh_job: str | None = None
        self._last_heavy_refresh = 0.0
        self._rate_limit_backoff_until = 0.0

        self.protocol("WM_DELETE_WINDOW", self._handle_close)
        self._build_layout()
        self._build_menu()
        self._load_startup_config()
        self.refresh_all()
        self._schedule_auto_refresh()
