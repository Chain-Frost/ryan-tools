# ryan_library.functions.misc_functions.py
from datetime import datetime
import pandas as pd
import logging
from typing import Optional
from pathlib import Path
from ryan_library.functions.logging_helpers import setup_logging as new_setup_logging


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
    import ryan_library.functions.logging_helpers

    new_setup_logging(
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


class ExcelExporter:
    """
    A simple utility class for exporting pandas DataFrames to Excel files.

    Methods:
        export_dataframes(export_dict):
            Export multiple DataFrames to one or more Excel files, each potentially
            containing multiple sheets.

        save_to_excel(data_frame, file_name_prefix, sheet_name):
            Export a single DataFrame to a single-sheet Excel file.

    The class is stateless and can be instantiated as needed.
    """

    def export_dataframes(
        self, export_dict: dict[str, dict[str, list[pd.DataFrame]]]
    ) -> None:
        """
        Export multiple DataFrames to Excel files.

        Args:
            export_dict (dict[str, dict[str, list[pd.DataFrame]]]):
                A dictionary where each key is a base file name and each value
                has two keys:
                - "dataframes": list of DataFrames
                - "sheets": list of corresponding sheet names

        Raises:
            ValueError: If the number of DataFrames doesn't match the number of sheets.

        Example:
            export_dict = {
                "Report": {
                    "dataframes": [df1, df2],
                    "sheets": ["Summary", "Details"]
                },
                "AnotherReport": {
                    "dataframes": [df3],
                    "sheets": ["Data"]
                }
            }
            ExcelExporter().export_dataframes(export_dict)
        """
        datetime_string = datetime.now().strftime("%Y%m%d-%H%M")

        for file_name, content in export_dict.items():
            dataframes = content.get("dataframes", [])
            sheets = content.get("sheets", [])

            if len(dataframes) != len(sheets):
                raise ValueError(
                    f"For file '{file_name}', the number of dataframes and sheets must match."
                )

            export_name = f"{datetime_string}_{file_name}.xlsx"
            export_path = Path(export_name)

            logging.info(f"Exporting {export_path}")

            with pd.ExcelWriter(export_path) as writer:
                for df, sheet in zip(dataframes, sheets):
                    df.to_excel(
                        writer, sheet_name=sheet, merge_cells=False, index=False
                    )

            logging.info(f"Finished exporting {file_name} to {export_path}")

    def save_to_excel(
        self,
        data_frame: pd.DataFrame,
        file_name_prefix: str = "Export",
        sheet_name: str = "Export",
    ) -> None:
        """
        Export a single DataFrame to an Excel file with a single sheet.

        Args:
            data_frame (pd.DataFrame): The DataFrame to export.
            file_name_prefix (str): Prefix for the resulting Excel filename.
            sheet_name (str): Name of the sheet in the Excel file.

        Example:
            ExcelExporter().save_to_excel(df, file_name_prefix="MyData", sheet_name="Sheet1")
        """
        export_dict = {
            file_name_prefix: {"dataframes": [data_frame], "sheets": [sheet_name]}
        }
        self.export_dataframes(export_dict)


# Backwards compatibility functions:
def export_dataframes(export_dict: dict[str, dict[str, list[pd.DataFrame]]]) -> None:
    """
    Backwards-compatible function that delegates to ExcelExporter.
    """
    ExcelExporter().export_dataframes(export_dict)


def save_to_excel(
    data_frame: pd.DataFrame,
    file_name_prefix: str = "Export",
    sheet_name: str = "Export",
) -> None:
    """
    Backwards-compatible function that delegates to ExcelExporter.
    """
    ExcelExporter().save_to_excel(data_frame, file_name_prefix, sheet_name)
