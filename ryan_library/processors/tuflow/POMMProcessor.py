# ryan_library/processors/tuflow/POMMProcessor.py

import logging
from pathlib import Path
import pandas as pd
from loguru import logger

from ryan_library.processors.tuflow.base_processor import BaseProcessor, ProcessorError
from ryan_library.classes.tuflow_string_classes import TuflowStringParser


class POMMProcessor(BaseProcessor):
    """Processor for POMM output CSVs (post‐transpose, extract run‐code parts, derive AbsMax/SignedAbsMax)."""

    def __post_init__(self) -> None:
        # Let the BaseProcessor __post_init__ do its thing (sets self.name_parser, data_type, etc).
        super().__post_init__()

    def process(self) -> None:
        """Read the raw POMM CSV, transpose + promote row 1 to header,
        rename the key columns, calculate AbsMax and SignedAbsMax,
        extract TP, Duration, AEP, and finally add all 'common' columns."""

        try:
            # 1) Load the CSV without headers (header=None)
            raw_df: pd.DataFrame = pd.read_csv(
                filepath_or_buffer=self.file_path, header=None
            )

            # # 2) Extract run_code from top‐left cell
            # raw_run_code = raw_df.iat[0, 0]
            # # Overwrite the parser's raw_run_code in case
            # # the file name didn’t exactly match. But typically:
            # self.name_parser.raw_run_code = raw_run_code

            # 3) Drop the first column and transpose
            transposed: pd.DataFrame = raw_df.drop(columns=0).T

            # 4) Promote row‐0 (of transposed) to headers
            transposed.columns = pd.Index(transposed.iloc[0], dtype=str)
            transposed = transposed.drop(index=transposed.index[0])

            # 5) Rename the core columns to consistent names
            #    We expect header names like:
            #        ['Location', 'Time', 'Maximum (Extracted from Time Series)', 'Time of Maximum',
            #         'Minimum (Extracted From Time Series)', 'Time of Minimum']
            #    They don't 100% match up to data for the Time/Location headers.
            #    We will rename them to:
            #        Location → 'Type'
            #        Time → 'Location'
            #        Maximum (Extracted from Time Series) → 'Max'
            #        Time of Maximum → 'Tmax'
            #        Minimum (Extracted From Time Series) → 'Min'
            #        Time of Minimum → 'Tmin'
            #        Velocity → 'Velocity'
            # Define new column names and their data types
            column_mappings = {
                "Type": ("Location", "string"),
                "Location": ("Time", "string"),
                "Max": ("Maximum (Extracted from Time Series)", "float"),
                "Tmax": ("Time of Maximum", "float"),
                "Min": ("Minimum (Extracted From Time Series)", "float"),
                "Tmin": ("Time of Minimum", "float"),
            }

            # Rename columns and cast to appropriate data types
            for new_col, (old_col, dtype) in column_mappings.items():
                if old_col in transposed.columns:
                    transposed.rename(columns={old_col: new_col}, inplace=True)
                    transposed[new_col] = transposed[new_col].astype(dtype)

            # 8) Preserve this as our “core” DataFrame
            self.df = transposed.copy()

            # 10) Finally, apply the dtype mapping from output_columns (so that everything
            #     matches your JSON’s "output_columns" keys & dtypes).  This is the call that
            #     looks at Config.get_instance().data_types["POMM"].output_columns and does .astype(...)

            # Calculate AbsMax column as the maximum absolute value between 'Max' and 'Min'
            self.df["AbsMax"] = self.df[["Max", "Min"]].abs().max(axis=1)

            # Calculate SignedAbsMax with the sign of the source data
            self.df["SignedAbsMax"] = self.df.apply(
                lambda row: (
                    row["Max"] if abs(row["Max"]) >= abs(row["Min"]) else row["Min"]
                ),
                axis=1,
            )

            # 7) Derive AbsMax and SignedAbsMax
            #    AbsMax = max(|Max|, |Min|), SignedAbsMax = whichever of Max or Min has the larger abs()
            if {"Max", "Min"}.issubset(self.df.columns):
                self.df["AbsMax"] = self.df[["Max", "Min"]].abs().max(axis=1)
                self.df["SignedAbsMax"] = self.df.apply(
                    lambda row: (
                        row["Max"] if abs(row["Max"]) >= abs(row["Min"]) else row["Min"]
                    ),
                    axis=1,
                )
            self.add_common_columns()
            self.apply_output_transformations()
            # Mark success
            self.processed = True
            logger.info(f"{self.file_name}: POMM processed successfully.")

        except Exception as e:
            logger.error(
                f"{self.file_name}: Failed in POMMProcessor.process(): {e}",
                exc_info=True,
            )
            self.df = pd.DataFrame()
            return
