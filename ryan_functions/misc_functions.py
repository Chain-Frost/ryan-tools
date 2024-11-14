# ryan_functions.misc_functions.py
import multiprocessing
from datetime import datetime
import pandas as pd
import logging
import sys
from typing import Optional
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logging(
    log_level: int = logging.INFO,
    log_file: Optional[str] = None,
    max_bytes: int = 10**6,  # 1 MB
    backup_count: int = 5,
    use_rotating_file: bool = False,
    enable_color: bool = True,
) -> None:
    print(
        "Script needs to have the setup_logging() function call updated to logging_helpers.setup_logging"
    )
    import logging_helpers

    logging_helpers.setup_logging(
        log_level, log_file, max_bytes, backup_count, use_rotating_file, enable_color
    )


def calculate_pool_size(num_files: int) -> int:
    """
    Calculate the optimal pool size based on the number of files and CPU cores.

    Args:
        num_files (int): Number of files to process.

    Returns:
        int: Number of threads to use.
    """
    import multiprocessing

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


def export_dataframes(export_dict: dict[str, dict[str, list]]) -> None:
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
