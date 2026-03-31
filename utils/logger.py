"""
Brotochondria — Dual Logging (file + console)
"""
import logging
from pathlib import Path


def setup_logger(log_path: Path = None):
    """Initialize the root brotochondria logger with file + console handlers."""
    if log_path is None:
        from config import LOG_PATH
        log_path = LOG_PATH

    logger = logging.getLogger('brotochondria')
    if logger.handlers:
        return logger  # Already initialized

    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)-25s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # File handler — full debug output
    log_path.parent.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(log_path, encoding='utf-8')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Console handler — info and above
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger


def get_logger(name: str):
    """Get a child logger under the brotochondria namespace."""
    return logging.getLogger(f'brotochondria.{name}')
