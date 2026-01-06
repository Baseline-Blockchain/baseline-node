"""
Logging helpers for the Baseline node.
"""

from __future__ import annotations

import logging
import sys
import time
import queue
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
from pathlib import Path


_LOG_LISTENER: QueueListener | None = None


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

    global _LOG_LISTENER
    if _LOG_LISTENER:
        _LOG_LISTENER.stop()
        _LOG_LISTENER = None

    log_file.parent.mkdir(parents=True, exist_ok=True)
    formatter = UTCFormatter()
    root = logging.getLogger("baseline")
    root.setLevel(level)
    root.handlers.clear()

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(level)

    file_handler = RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level)

    log_queue: queue.SimpleQueue[logging.LogRecord] = queue.SimpleQueue()
    queue_handler = QueueHandler(log_queue)
    queue_handler.setLevel(level)
    root.addHandler(queue_handler)

    listener = QueueListener(log_queue, stream_handler, file_handler, respect_handler_level=True)
    listener.start()
    _LOG_LISTENER = listener

    root.debug("Logging configured at level %s", logging.getLevelName(level))
    return root


def shutdown_logging() -> None:
    """Flush and stop the background logging thread."""
    global _LOG_LISTENER
    if _LOG_LISTENER:
        _LOG_LISTENER.stop()
        _LOG_LISTENER = None
