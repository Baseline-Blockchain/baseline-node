"""Configuration utilities for the GUI wallet."""

from __future__ import annotations

from pathlib import Path
from tkinter import filedialog, messagebox

from ..config import load_config_overrides
from .styles import DEFAULT_CONFIG


class ConfigMixin:
    """Mixin that exposes configuration loading for the GUI."""

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
        self.status_var.set("RPC status: checking.")
        self.refresh_all()
