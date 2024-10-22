import multiprocessing
from datetime import datetime
import pandas as pd
import logging
import sys
from typing import Optional
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

# Attempt to import colorlog; handle gracefully if not installed
try:
    import colorlog

    COLORLOG_AVAILABLE = True
except ImportError:
    COLORLOG_AVAILABLE = False


def setup_logging(
    log_level: int = logging.INFO,
    log_file: Optional[str] = None,
    max_bytes: int = 10**6,  # 1 MB
    backup_count: int = 5,
    use_rotating_file: bool = False,
    enable_color: bool = True,
) -> None:
    """
    Setup logging with colored console output and optional file logging.
    Tailored to be robust for Windows Store Python environments.

    Args:
        log_level (int): Logging level (e.g., logging.DEBUG, logging.INFO).
        log_file (Optional[str]): Name of the log file. If None, file logging is disabled.
        max_bytes (int): Maximum size in bytes before rotating the log file.
        backup_count (int): Number of backup log files to keep.
        use_rotating_file (bool): Whether to use a rotating file handler.
        enable_color (bool): Whether to enable colored console logs.
    """
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # Avoid adding multiple handlers if already configured
    if logger.hasHandlers():
        logger.handlers.clear()

    # === Console Handler with Colored Formatting ===
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)

    if enable_color and COLORLOG_AVAILABLE:
        # Define color formatter
        color_formatter = colorlog.ColoredFormatter(  # type: ignore
            fmt=(
                "%(log_color)s%(asctime)s - %(name)s - %(levelname)s - "
                "%(filename)s:%(lineno)d - %(funcName)s() - %(message)s%(reset)s"
            ),
            datefmt="%Y-%m-%d %H:%M:%S",
            log_colors={
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "bold_red",
            },
            secondary_log_colors={},
            style="%",
        )
        console_handler.setFormatter(color_formatter)
    else:
        # Fallback to standard formatter if colorlog is unavailable or color is disabled
        standard_formatter = logging.Formatter(
            fmt=(
                "%(asctime)s - %(name)s - %(levelname)s - "
                "%(filename)s:%(lineno)d - %(funcName)s() - %(message)s"
            ),
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        console_handler.setFormatter(standard_formatter)

    logger.addHandler(console_handler)

    # === File Handler with Standard Formatting ===
    if log_file:
        try:
            # Define log directory within user's Documents
            log_dir = Path.home() / "Documents" / "MyAppLogs"
            log_dir.mkdir(parents=True, exist_ok=True)
            log_path = log_dir / log_file

            if use_rotating_file:
                file_handler = RotatingFileHandler(
                    log_path,
                    maxBytes=max_bytes,
                    backupCount=backup_count,
                    encoding="utf-8",
                )
            else:
                file_handler = logging.FileHandler(log_path, encoding="utf-8")

            file_handler.setLevel(log_level)

            # Define standard formatter for file logs
            file_formatter = logging.Formatter(
                fmt=(
                    "%(asctime)s - %(name)s - %(levelname)s - "
                    "%(filename)s:%(lineno)d - %(funcName)s() - %(message)s"
                ),
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
        except Exception as e:
            logger.error(f"Failed to set up file logging: {e}")

    # === Null Handler to Prevent "No Handlers" Warning ===
    # Useful for modules that might log without configuring logging
    logging.getLogger(__name__).addHandler(logging.NullHandler())

    logger.info("Logging is configured successfully.")


# def setup_logging() -> None:
#     if not logging.getLogger().hasHandlers():
#         # Setup logging configuration
#         logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# def setup_logging() -> None:
#     """Setup logging configuration if not already configured."""
#     if not logging.getLogger().hasHandlers():
#         logging.basicConfig(
#             level=logging.INFO,
#             format="%(asctime)s - %(levelname)s - %(message)s",
#             handlers=[logging.StreamHandler()],
#         )
#         logging.info("Logging is set up.")


def calculate_pool_size(num_files: int) -> int:
    """
    Calculate the optimal pool size based on the number of files and CPU cores.

    Args:
        num_files (int): Number of files to process.

    Returns:
        int: Number of threads to use.
    """
    splits = max(num_files // 3, 1)
    available_cores = min(multiprocessing.cpu_count(), 20)
    calc_threads = min(available_cores - 1, splits) if available_cores > 1 else 1
    logging.info(f"Processing threads: {calc_threads}")
    return calc_threads


def split_strings(input: str | list[str]) -> list[str]:
    # Normalize the input to a list
    if isinstance(input, str):
        input_list: list[str] = [input]
    else:  # input is already a list
        input_list = input

    # Split each string by whitespace and flatten the list
    split_list: list[str] = []
    for item in input_list:
        split_list.extend(item.split())

    return split_list


def split_strings_in_dict(params_dict: dict[str, list[str]]) -> dict[str, list[str]]:
    for key, value in params_dict.items():
        # Use split_strings to handle both string and list of strings cases
        params_dict[key] = split_strings(input=value)
    return params_dict


def save_to_excel(
    data_frame: pd.DataFrame,
    file_name_prefix: str = "Export",
    sheet_name: str = "Export",
) -> None:
    """Save the DataFrame to an Excel file."""
    datetime_string: str = datetime.now().strftime(format="%Y%m%d-%H%M")
    file_name: str = f"{file_name_prefix}_{datetime_string}.xlsx"
    data_frame.to_excel(
        excel_writer=file_name, merge_cells=False, sheet_name=sheet_name, index=False
    )

    setup_logging()  # Call the function to ensure logging is set up
    logging.info(msg=f"Data saved to {file_name}")


# def export_dataframes(
#     dataframes: List[pd.DataFrame], file_names: List[str], sheet_names: list[list[str]]
# ) -> None:
#     DateTimeString: str = datetime.now().strftime(format="%Y%m%d-%H%M")
#     for dataframe, file_name, sheet_name in zip(dataframes, file_names, sheet_names):
#         export_name: str = f"{DateTimeString}_{file_name}.xlsx"
#         print(f"Exporting {export_name}")
#         with pd.ExcelWriter(path=export_name) as writer:
#             for df, sheet in zip(dataframe, sheet_name):
#                 df.to_excel(excel_writer=writer, sheet_name=sheet, merge_cells=False)
#         print(dataframe)
#         for actual_df in dataframe:
#             print(actual_df.columns)


def export_dataframes(export_dict: dict[str, dict[str, list]]) -> None:
    DateTimeString: str = datetime.now().strftime(format="%Y%m%d-%H%M")

    for file_name, content in export_dict.items():
        dataframes: list[pd.DataFrame] = content.get("dataframes", [])
        sheets: list[str] = content.get("sheets", [])

        if len(dataframes) != len(sheets):
            raise ValueError(
                f"For file '{file_name}', the number of dataframes and sheets must match."
            )

        export_name: str = f"{DateTimeString}_{file_name}.xlsx"
        print(f"Exporting {export_name}")

        with pd.ExcelWriter(path=export_name) as writer:
            for df, sheet in zip(dataframes, sheets):
                df.to_excel(excel_writer=writer, sheet_name=sheet, merge_cells=False)

        print(f"Finished exporting {file_name}")
        for actual_df in dataframes:
            print(actual_df.columns)


# Example usage
# df, meddf, all_bins_df = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
# export_dict = {
#     "PO_max": {
#         "dataframes": [df],
#         "sheets": ["PO_max"]
#     },
#     "PO_max_median": {
#         "dataframes": [meddf, all_bins_df],
#         "sheets": ["PO_medians", "PO_bins"]
#     }
# }
# export_dataframes(export_dict)
