"""
Logging helpers for the Baseline node.
"""

from __future__ import annotations

import logging
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path


class UTCFormatter(logging.Formatter):
    """Formatter that renders timestamps in UTC ISO-8601."""

    converter = time.gmtime

    def __init__(self) -> None:
        super().__init__(
            fmt="%(asctime)sZ %(levelname)s %(name)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )


def setup_logging(log_file: Path, *, level: int = logging.INFO) -> logging.Logger:
    """Configure root logger with stdout + rotating file sinks."""

    log_file.parent.mkdir(parents=True, exist_ok=True)
    formatter = UTCFormatter()
    root = logging.getLogger("simplechain")
    root.setLevel(level)
    root.handlers.clear()

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)

    file_handler = RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    root.debug("Logging configured at level %s", logging.getLevelName(level))
    return root
