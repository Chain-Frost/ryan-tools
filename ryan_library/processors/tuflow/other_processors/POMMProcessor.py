# ryan_library/processors/tuflow/POMMProcessor.py

import pandas as pd
from loguru import logger

from ..base_processor import BaseProcessor, DataValidationError


POMM_RENAME_COLUMNS: dict[str, str] = {
    "Location": "Type",
    "Time": "Location",
    "Maximum (Extracted from Time Series)": "Max",
    "Time of Maximum": "Tmax",
    "Minimum (Extracted From Time Series)": "Min",
    "Time of Minimum": "Tmin",
}


class POMMProcessor(BaseProcessor):
    """Processor for POMM output CSVs (post-transpose, extract run-code parts, derive AbsMax/SignedAbsMax)."""

    def __post_init__(self) -> None:
        # Let the BaseProcessor __post_init__ do its thing (sets self.name_parser, data_type, etc).
        super().__post_init__()

    def _derive_abs_metrics(self) -> None:
        """Populate AbsMax and SignedAbsMax from the Max/Min columns."""

        if not {"Max", "Min"}.issubset(self.df.columns):
            return

        absolute_values = self.df[["Max", "Min"]].abs()
        self.df["AbsMax"] = absolute_values.max(axis=1)

        use_max = absolute_values["Max"] >= absolute_values["Min"]
        self.df["SignedAbsMax"] = self.df["Max"].where(use_max, self.df["Min"])

    def process(self) -> None:
        """Read the raw POMM CSV, transpose + promote row 1 to header,
        rename the key columns, calculate AbsMax and SignedAbsMax,
        extract TP, Duration, AEP, and finally add all 'common' columns."""

        try:
            # 1) Load the CSV without headers (header=None)
            raw_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=self.file_path, header=None)  # type: ignore
            self.raw_df = raw_df

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
            headers: list[str] = transposed.columns.tolist()
            if self.expected_in_header:
                if not self.check_headers_match(headers):
                    raise DataValidationError(f"{self.log_path}: Header mismatch for POMM data. Got {headers}")

            rename_map: dict[str, str] = POMM_RENAME_COLUMNS

            missing_sources: list[str] = [col for col in rename_map if col not in headers]
            if missing_sources:
                raise DataValidationError(
                    f"{self.log_path}: Missing expected columns {missing_sources} after transpose."
                )

            transposed.rename(columns=rename_map, inplace=True)
            ordered_columns = list(rename_map.values())
            transposed = transposed.loc[:, ordered_columns]

            self.df = transposed

            # Rename columns and cast to appropriate data types
            base_dtype_map: dict[str, str] = {
                column: self.output_columns[column] for column in ordered_columns if column in self.output_columns
            }
            if base_dtype_map:
                self.apply_dtype_mapping(dtype_mapping=base_dtype_map, context="pomm_base_columns")

            if not {"Max", "Min"}.issubset(self.df.columns):
                raise DataValidationError(
                    f"{self.log_path}: Required columns 'Max' and 'Min' not available after renaming."
                )

            # 10) Finally, apply the dtype mapping from output_columns (so that everything
            #     matches your JSON’s "output_columns" keys & dtypes).  This is the call that
            #     looks at Config.get_instance().data_types["POMM"].output_columns and does .astype(...)

            # 7) Derive AbsMax and SignedAbsMax once the Max/Min columns exist.
            #    AbsMax = max(|Max|, |Min|); SignedAbsMax keeps the sign of the larger magnitude value.
            self._derive_abs_metrics()
            self.apply_entity_filter()
            self.add_common_columns()
            self.apply_output_transformations()
            # Mark success
            self.processed = True
            logger.info(f"{self.log_path}: POMM processed successfully.")

        except Exception as e:
            logger.error(
                f"{self.log_path}: Failed in POMMProcessor.process(): {e}",
                exc_info=True,
            )
            self.df = pd.DataFrame()
            return
