"""Centralized logging configuration for TickVault."""

import logging
import sys
from pathlib import Path

from .config import CONFIG


class LazyFileHandler(logging.FileHandler):
    """FileHandler that defers file opening and parent directory creation."""

    def __init__(
        self,
        filename: str | Path,
        mode: str = "a",
        encoding: str | None = None,
        delay: bool = True,
    ) -> None:
        super().__init__(filename, mode, encoding, delay=True)

    def _open(self):
        Path(self.baseFilename).parent.mkdir(parents=True, exist_ok=True)
        return super()._open()


def setup_logger() -> logging.Logger:
    """
    Configure and return a logger instance.

    Returns:
        logging.Logger: Configured logger instance
    """
    logger = logging.getLogger("tick_vault")

    # Only configure if not already configured
    if logger.handlers:
        return logger

    logger.setLevel(CONFIG.base_log_level)

    # Console handler with colored output
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(CONFIG.console_log_level)

    # File handler (using LazyFileHandler to defer creation of directory and file)
    log_file = CONFIG.log_file_path
    file_handler = LazyFileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)  # File gets everything

    # Formatter
    console_format = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%H:%M:%S"
    )
    file_format = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - "
        "%(funcName)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler.setFormatter(console_format)
    file_handler.setFormatter(file_format)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


def update_log_file() -> None:
    """Update the file handler to point to the current CONFIG.log_file_path."""
    logger = logging.getLogger("tick_vault")

    # Close and remove existing file handlers
    for handler in list(logger.handlers):
        if isinstance(handler, logging.FileHandler):
            handler.close()
            logger.removeHandler(handler)

    # Add new LazyFileHandler with updated path
    log_file = CONFIG.log_file_path
    file_handler = LazyFileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - "
        "%(funcName)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)


# Root logger for the package
logger = setup_logger()
