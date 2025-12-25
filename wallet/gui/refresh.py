"""Refresh scheduling for the wallet GUI."""

from __future__ import annotations

import contextlib
import time

from ..client import RateLimitError


class RefreshMixin:
    """Mixin managing auto-refresh scheduling and rate-limit handling."""

    _HEAVY_REFRESH_INTERVAL_MS = 30_000
    _RATE_LIMIT_COOLDOWN_MS = 20_000

    def _schedule_auto_refresh(self) -> None:
        if self._auto_refresh_job is not None:
            with contextlib.suppress(Exception):
                self.after_cancel(self._auto_refresh_job)
        self._auto_refresh_job = self.after(self._auto_refresh_interval_ms, self._auto_refresh_tick)

    def _auto_refresh_tick(self) -> None:
        if not self.winfo_exists():
            return
        now = time.monotonic()
        if now < self._rate_limit_backoff_until:
            remaining = int(self._rate_limit_backoff_until - now)
            self.status_var.set(f"RPC status: rate-limited ({remaining}s)")
            self._schedule_auto_refresh()
            return
        try:
            self._refresh_status()
        except RateLimitError as exc:
            self._handle_rate_limit(exc)
            self._schedule_auto_refresh()
            return
        except Exception:
            self._update_menu_state()
            self._schedule_auto_refresh()
            return
        self._update_menu_state()
        heavy_due = (now - self._last_heavy_refresh) * 1000 >= self._HEAVY_REFRESH_INTERVAL_MS
        if heavy_due:
            try:
                self._refresh_addresses()
                self._refresh_transactions()
                self._refresh_mempool()
                self._update_fee_estimate()
            except RateLimitError as exc:
                self._handle_rate_limit(exc)
                self._schedule_auto_refresh()
                return
            self._last_heavy_refresh = now
        self._schedule_auto_refresh()

    def _handle_close(self) -> None:
        if self._auto_refresh_job is not None:
            with contextlib.suppress(Exception):
                self.after_cancel(self._auto_refresh_job)
            self._auto_refresh_job = None
        self.destroy()

    def _handle_rate_limit(self, exc: RateLimitError) -> None:
        self._rate_limit_backoff_until = time.monotonic() + self._RATE_LIMIT_COOLDOWN_MS / 1000
        self.status_var.set(f"RPC status: rate-limited ({exc})")
