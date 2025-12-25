"""Theme and utility helpers shared by the GUI launcher."""

from __future__ import annotations

from pathlib import Path
from typing import Any

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

ASSET_DIR = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = Path.cwd() / "config.json"


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
    return f"{text[:length]}."
