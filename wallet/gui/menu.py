"""Menu helpers for the wallet GUI."""

from __future__ import annotations

import tkinter as tk


class MenuMixin:
    """Mixin that builds and maintains the menu bar state."""

    def _build_menu(self) -> None:
        """Create the top-level menu bar (only once)."""
        if getattr(self, "_menubar", None) is not None:
            return  # already built

        self._menubar = tk.Menu(self)

        # Actions menu
        self._actions_menu = tk.Menu(self._menubar, tearoff=False)
        self._actions_menu.add_command(label="Refresh", command=self.refresh_all)
        self._actions_menu.add_command(label="Reload Config", command=self._load_config_dialog)
        self._menubar.add_cascade(label="Actions", menu=self._actions_menu)

        self.wallet_menu = tk.Menu(self._menubar, tearoff=False)
        wm = self.wallet_menu

        wm.add_command(label="New Address...", command=self._handle_new_address)
        self._menu_idx_new_addr = wm.index("end")

        wm.add_separator()
        wm.add_command(label="Dump Wallet...", command=self._handle_dump_wallet)
        self._menu_idx_dump = wm.index("end")

        wm.add_command(label="Import Wallet...", command=self._handle_import_wallet)
        self._menu_idx_import_wallet = wm.index("end")

        wm.add_command(label="Encrypt Wallet...", command=self._handle_encrypt_wallet)
        self._menu_idx_encrypt = wm.index("end")

        wm.add_separator()
        wm.add_command(label="Import Private Key...", command=self._handle_import_privkey)
        self._menu_idx_import_privkey = wm.index("end")

        wm.add_separator()
        wm.add_command(label="Rescan Wallet", command=self._handle_rescan_wallet)
        self._menu_idx_rescan = wm.index("end")

        self._menubar.add_cascade(label="Wallet", menu=wm)
        self.config(menu=self._menubar)
        self._update_menu_state()

    def _update_menu_state(self) -> None:
        if not self.wallet_menu:
            return

        online = bool(self.rpc_online)
        encrypted = bool(self.wallet_info.get("encrypted"))

        normal_or_disabled = "normal" if online else "disabled"

        for idx in (
            self._menu_idx_new_addr,
            self._menu_idx_dump,
            self._menu_idx_import_wallet,
            self._menu_idx_import_privkey,
            self._menu_idx_rescan,
        ):
            self.wallet_menu.entryconfig(idx, state=normal_or_disabled)

        encrypt_state = "normal" if (online and not encrypted) else "disabled"
        self.wallet_menu.entryconfig(self._menu_idx_encrypt, state=encrypt_state)

    def _update_from_balance(self, *_: object) -> None:
        """Update the displayed balance for the selected from-address."""

        addr = self.send_from_var.get().strip()
        balance = self._lookup_balance(addr)
        self.send_from_balance_var.set(f"Balance: {balance:.8f} BLINE")

    def _lookup_balance(self, address: str) -> float:
        for record in self.address_records:
            if record["address"] == address:
                return float(record.get("balance", 0.0))
        return 0.0

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
