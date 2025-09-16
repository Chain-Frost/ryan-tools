# ryan_library/processors/tuflow/timeseries_processor.py

from typing import Any

import pandas as pd
from loguru import logger

from .base_processor import BaseProcessor, DataValidationError, ProcessorError


class TimeSeriesProcessor(BaseProcessor):
    """Intermediate base class for processing timeseries data types.

    Subclasses call :meth:`read_and_process_timeseries_csv` to ingest source
    CSV files before applying dataset specific rules. The default
    :meth:`process_timeseries_raw_dataframe` implementation is a no-op that can
    be overridden to perform additional validation or reshaping of the melted
    DataFrame stored on :attr:`df`.
    """

    def read_and_process_timeseries_csv(self, data_type: str | None = None) -> int:
        """Read, clean and reshape a timeseries CSV file into long format.

        Args:
            data_type (str | None): The data type identifier (e.g. ``"H"`` or
                ``"Q"``). Defaults to :attr:`self.data_type`.

        Returns:
            int: Status code where ``0`` is success.
        """

        resolved_data_type: str | None = data_type or self.data_type
        if not resolved_data_type:
            logger.error(f"{self.file_name}: Unable to determine timeseries data type.")
            return 3

        try:
            df_full: pd.DataFrame = self._read_csv(file_path=self.file_path)
            if df_full.empty:
                logger.error(f"{self.file_name}: No data found in file: {self.file_path}")
                return 1

            self.raw_df = df_full.copy(deep=False)

            df: pd.DataFrame = self._clean_headers(df=df_full, data_type=resolved_data_type)
            if df.empty:
                logger.error(f"{self.file_name}: DataFrame is empty after cleaning headers.")
                return 1

            df_melted: pd.DataFrame = self._reshape_timeseries_df(
                df=df, data_type=resolved_data_type
            )
            if df_melted.empty:
                logger.error(f"{self.file_name}: No data found after reshaping.")
                return 1

            self.df = df_melted
            self._apply_final_transformations(data_type=resolved_data_type)
            self.processed = True
            logger.info(f"{self.file_name}: Timeseries CSV processed successfully.")
            return 0
        except (ProcessorError, DataValidationError) as exc:
            logger.error(f"{self.file_name}: Processing error: {exc}")
            return 3
        except Exception as exc:
            logger.exception(f"{self.file_name}: Unexpected error: {exc}")
            return 3

    def process_timeseries_raw_dataframe(self, *args: Any, **kwargs: Any) -> int:
        """Hook for subclasses to continue processing ``self.df``.

        Returns:
            int: ``0`` when no additional work is required.
        """

        return 0

    def _reshape_timeseries_df(self, df: pd.DataFrame, data_type: str) -> pd.DataFrame:
        """Reshape the timeseries DataFrame based on the data type."""
        is_1d: bool = "_1d_" in self.file_name.lower()
        category_type: str = "Chan ID" if is_1d else "Location"
        logger.debug(
            f"{self.file_name}: {'1D' if is_1d else '2D'} filename detected; using '{category_type}' as category type."
        )

        try:
            if data_type == "H":
                df_melted: pd.DataFrame = self._reshape_h_data(df=df, category_type=category_type)
            else:
                df_melted = df.melt(id_vars=["Time"], var_name=category_type, value_name=data_type)
                logger.debug(f"Reshaped DataFrame to long format with {len(df_melted)} rows.")
        except Exception as e:
            logger.exception(f"{self.file_name}: Failed to reshape DataFrame: {e}")
            raise ProcessorError(f"Failed to reshape DataFrame: {e}")

        if df_melted.empty:
            logger.error(f"{self.file_name}: No data found after reshaping.")
            raise DataValidationError("No data found after reshaping.")

        expected_headers: list[str] = (
            ["Time", category_type, "H_US", "H_DS"] if data_type == "H" else ["Time", category_type, data_type]
        )
        self.expected_in_header = expected_headers
        if not self.check_headers_match(test_headers=df_melted.columns.tolist()):
            logger.error(f"{self.file_name}: Header mismatch after reshaping.")
            raise DataValidationError("Header mismatch after reshaping.")

        return df_melted

    def _apply_final_transformations(self, data_type: str) -> None:
        """Apply final dtype coercions to the reshaped DataFrame."""
        col_types: dict[str, str] = {
            "Time": "float64",
            data_type: "float64",
        }

        if data_type == "H":
            col_types.update({"H_US": "float64", "H_DS": "float64"})

        self.apply_dtype_mapping(dtype_mapping=col_types, context="final_transformations")
