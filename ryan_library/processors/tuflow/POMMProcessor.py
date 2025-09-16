# ryan_library/processors/tuflow/POMMProcessor.py

import pandas as pd
from loguru import logger

from ryan_library.processors.tuflow.base_processor import BaseProcessor, DataValidationError


POMM_RENAME_COLUMNS: dict[str, str] = {
    "Location": "Type",
    "Time": "Location",
    "Maximum (Extracted from Time Series)": "Max",
    "Time of Maximum": "Tmax",
    "Minimum (Extracted From Time Series)": "Min",
    "Time of Minimum": "Tmin",
}


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
            raw_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=self.file_path, header=None)
            self.raw_df = raw_df

            transposed: pd.DataFrame = raw_df.drop(columns=0).T
            transposed.columns = pd.Index(transposed.iloc[0], dtype=str)
            transposed = transposed.drop(index=transposed.index[0])

            headers = transposed.columns.tolist()
            if self.expected_in_header:
                if not self.check_headers_match(headers):
                    raise DataValidationError(f"{self.file_name}: Header mismatch for POMM data. Got {headers}")

            rename_map: dict[str, str] = POMM_RENAME_COLUMNS

            missing_sources = [col for col in rename_map if col not in headers]
            if missing_sources:
                raise DataValidationError(
                    f"{self.file_name}: Missing expected columns {missing_sources} after transpose."
                )

            transposed.rename(columns=rename_map, inplace=True)
            ordered_columns = list(rename_map.values())
            transposed = transposed.loc[:, ordered_columns]

            self.df = transposed

            base_dtype_map: dict[str, str] = {
                column: self.output_columns[column] for column in ordered_columns if column in self.output_columns
            }
            if base_dtype_map:
                self.apply_dtype_mapping(dtype_mapping=base_dtype_map, context="pomm_base_columns")

            if not {"Max", "Min"}.issubset(self.df.columns):
                raise DataValidationError(
                    f"{self.file_name}: Required columns 'Max' and 'Min' not available after renaming."
                )

            self.df["AbsMax"] = self.df[["Max", "Min"]].abs().max(axis=1)
            self.df["SignedAbsMax"] = self.df.apply(
                lambda row: row["Max"] if abs(row["Max"]) >= abs(row["Min"]) else row["Min"],
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
