# ryan_library/functions/misc_functions.py

from __future__ import annotations

import multiprocessing
from importlib import metadata
from typing import Any
from loguru import logger


def get_tools_version(package: str = "ryan_functions") -> str:
    """Return the installed version of ``package`` if available."""
    try:
        return metadata.version(distribution_name=package)
    except metadata.PackageNotFoundError:
        return "unknown"


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


def split_strings(input_str: str | list[str]) -> list[str]:
    """Split input string(s) by whitespace into a flat list of strings.
    Args:
        input_str (str | list[str]): A string or list of strings to split.
    Returns:
        list[str]: A flat list of split strings."""
    if isinstance(input_str, str):
        input_list: list[str] = [input_str]
    else:  # input is already a list
        input_list = input_str

    # Split each string by whitespace and flatten the list
    split_list: list[str] = []
    for item in input_list:
        split_list.extend(item.split())

    return split_list


def split_strings_in_dict(params_dict: dict[str, list[str]]) -> dict[str, list[str]]:
    """Apply split_strings to each list of strings in the dictionary.
    Args:
        params_dict (dict[str, list[str]]): Dictionary with string lists to split.
    Returns:
        dict[str, list[str]]: Dictionary with split string lists."""
    for key, value in params_dict.items():
        # Use split_strings to handle both string and list of strings cases
        params_dict[key] = split_strings(input_str=value)
    return params_dict


def __getattr__(name: str) -> Any:
    """Lazily load excel export functionalities to avoid eager loading pandas."""
    if name in {
        "ExportContent",
        "build_data_dictionary",
        "ExcelExporter",
        "export_dataframes",
        "save_to_excel",
        "ParquetCompression",
        "DATA_DICTIONARY_SHEET_NAME",
    }:
        import ryan_library.functions.excel_export as excel_export

        return getattr(excel_export, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
