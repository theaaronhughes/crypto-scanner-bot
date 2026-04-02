"""
Logging setup: console + rotating-friendly single file under logs/.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path


_CONFIGURED = False


def setup_logging(log_dir: str | Path | None = None, level: int = logging.INFO) -> logging.Logger:
    """
    Configure root logging once. Log file: logs/scanner_YYYY-MM-DD.log
    """
    global _CONFIGURED
    root = logging.getLogger()
    if _CONFIGURED:
        return logging.getLogger("scanner")

    _CONFIGURED = True
    root.setLevel(level)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    log_path = Path(log_dir or Path(__file__).resolve().parent.parent / "logs")
    log_path.mkdir(parents=True, exist_ok=True)
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    fh = logging.FileHandler(log_path / f"scanner_{day}.log", encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(fmt)

    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(fmt)

    root.handlers.clear()
    root.addHandler(fh)
    root.addHandler(ch)

    log = logging.getLogger("scanner")
    log.debug("Logging initialized (dir=%s)", os.fspath(log_path))
    return log


def get_logger(name: str = "scanner") -> logging.Logger:
    return logging.getLogger(name)
