"""
Centralized logging configuration for the ETL pipeline.

The pipeline writes logs to both:
1. Console - useful during development and execution.
2. File - useful for auditing and troubleshooting after execution.
"""

import logging
from pathlib import Path


LOG_FILE = Path("logs/pipeline.log")


def setup_logger():
    """
    Configure and return the application logger.

    A single logger configuration is used throughout the pipeline
    so every module follows the same logging format.
    """

    LOG_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    logger = logging.getLogger("pokemon_pipeline")

    # Prevent duplicate handlers if setup_logger() is called again.
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # File handler
    file_handler = logging.FileHandler(
        LOG_FILE,
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger