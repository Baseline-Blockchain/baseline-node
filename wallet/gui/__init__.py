"""Package entry point for the wallet GUI."""

from __future__ import annotations

from .app import WalletLauncher


def main() -> None:
    app = WalletLauncher()
    app.mainloop()
