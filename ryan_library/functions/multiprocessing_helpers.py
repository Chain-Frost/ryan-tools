# ryan_library/functions/multiprocessing_helpers.py
"""Multiprocessing helpers for ryan_library."""

import multiprocessing
from loguru import logger


def calculate_pool_size(num_files: int) -> int:
    """Calculate the optimal pool size based on the number of files and CPU cores.
    Args:
        num_files (int): Number of files to process.
    Returns:
        int: Number of threads to use."""
    splits: int = max(num_files // 3, 1)
    available_cores: int = min(multiprocessing.cpu_count(), 20)
    calc_threads: int = min(available_cores - 1, splits) if available_cores > 1 else 1
    logger.info("Processing threads: {}", calc_threads)
    return calc_threads
