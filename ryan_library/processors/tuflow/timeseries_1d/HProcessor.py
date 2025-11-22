from __future__ import annotations

"""Processor for TUFLOW ``_H`` timeseries outputs."""

# not tested, don't know if it works even slightly

import pandas as pd
from loguru import logger

from ..base_processor import ProcessorStatus
from ..timeseries_processor import TimeSeriesProcessor


class HProcessor(TimeSeriesProcessor):
    """Handle water level (``H``) timeseries files with upstream/downstream values."""

    def process(self) -> None:  # type: ignore[override]
        """Process a ``_H`` CSV using the shared timeseries pipeline."""

        self._process_timeseries_pipeline(data_type="H")

    def process_timeseries_raw_dataframe(self) -> ProcessorStatus:
        """Normalise the dual-value timeseries DataFrame produced by the shared pipeline."""

        try:
            logger.debug(f"{self.file_name}: Normalising reshaped 'H' DataFrame.")

            required_columns: set[str] = {"Time", "H_US", "H_DS"}
            missing_columns: set[str] = required_columns - set(self.df.columns)
            if missing_columns:
                logger.error(f"{self.file_name}: Missing required columns after melt: {sorted(missing_columns)}.")
                return ProcessorStatus.FAILURE

            identifier_columns: list[str] = [col for col in self.df.columns if col not in required_columns]
            if not identifier_columns:
                logger.error(f"{self.file_name}: No identifier column found alongside 'H_US'/'H_DS'.")
                return ProcessorStatus.FAILURE
            if len(identifier_columns) > 1:
                identifier_error: str = (
                    f"{self.file_name}: Expected a single identifier column alongside 'Time', "
                    f"'H_US' and 'H_DS', got {identifier_columns}."
                )
                logger.error(identifier_error)
                return ProcessorStatus.FAILURE

            identifier_column: str = identifier_columns[0]
            logger.debug(f"{self.file_name}: Using '{identifier_column}' as the identifier column for 'H' values.")

            initial_row_count: int = len(self.df)
            self.df.dropna(subset=["H_US", "H_DS"], how="all", inplace=True)
            dropped_rows: int = initial_row_count - len(self.df)
            if dropped_rows:
                logger.debug(f"{self.file_name}: Dropped {dropped_rows} rows with missing 'H' values.")

            if self.df.empty:
                logger.error(f"{self.file_name}: DataFrame is empty after removing rows with missing 'H' values.")
                return ProcessorStatus.EMPTY_DATAFRAME

            expected_order: list[str] = ["Time", identifier_column, "H_US", "H_DS"]
            self.df = self.df[expected_order]

            if not self.check_headers_match(test_headers=self.df.columns.tolist()):
                logger.error(f"{self.file_name}: Header mismatch after normalising 'H' DataFrame.")
                return ProcessorStatus.HEADER_MISMATCH

            logger.info(f"{self.file_name}: Successfully normalised 'H' DataFrame for downstream processing.")
            return ProcessorStatus.SUCCESS
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.exception(f"{self.file_name}: Unexpected error while normalising 'H' DataFrame: {exc}")
            return ProcessorStatus.FAILURE
