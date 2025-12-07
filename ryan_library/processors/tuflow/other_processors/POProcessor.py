"""Processor for TUFLOW point output (``*_PO.csv``) timeseries files."""

from __future__ import annotations

import pandas as pd
from loguru import logger
from pandas import DataFrame, Series

from ..base_processor import BaseProcessor


class POProcessor(BaseProcessor):
    """Load PO timeseries CSV files into a tidy DataFrame."""

    VALUE_COLUMNS: list[str] = ["Time", "Location", "Type", "Value"]

    def process(self) -> None:
        """Parse the CSV, reshape it to long format, and add common columns."""
        logger.info(f"Starting processing of PO file: {self.log_path}")

        try:
            raw_df: DataFrame = pd.read_csv(self.file_path, header=None, dtype=str)  # type: ignore
            self.raw_df = raw_df.copy()
        except Exception as exc:  # pragma: no cover - IO errors handled here
            logger.exception(f"{self.file_name}: Failed to read CSV file: {exc}")
            self.df = pd.DataFrame()
            return

        tidy_df: DataFrame = self._parse_point_output(raw_df=raw_df)
        if tidy_df.empty:
            logger.warning(f"{self.file_name}: No point output values found after processing.")
            self.df = tidy_df
            return

        self.df = tidy_df

        self.apply_entity_filter()
        self.add_common_columns()
        self.apply_output_transformations()

        if not self.validate_data():
            logger.error(f"{self.file_name}: Data validation failed.")
            return

        self.processed = True
        logger.info(f"Completed processing of PO file: {self.log_path}")

    def _parse_point_output(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """Convert the raw PO CSV structure into a long-form DataFrame."""
        if raw_df.empty:
            logger.error(f"{self.file_name}: CSV file is empty.")
            return pd.DataFrame(columns=self.VALUE_COLUMNS)

        if raw_df.shape[1] < 2 or raw_df.shape[0] < 3:
            logger.error(f"{self.file_name}: CSV file does not contain the expected header or data rows.")
            return pd.DataFrame(columns=self.VALUE_COLUMNS)

        trimmed: DataFrame = raw_df.drop(columns=0)
        measurement_row = trimmed.iloc[0].fillna("")  # type: ignore
        location_row = trimmed.iloc[1].fillna("")  # type: ignore
        data_rows: DataFrame = trimmed.iloc[2:]

        if measurement_row.empty or location_row.empty:
            logger.error(f"{self.file_name}: Missing measurement or location headers.")
            return pd.DataFrame(columns=self.VALUE_COLUMNS)

        numeric_data: DataFrame = data_rows.apply(pd.to_numeric, errors="coerce")  # type: ignore
        numeric_data.reset_index(drop=True, inplace=True)

        time_idx: int | None = self._locate_time_column(measurement_row=measurement_row, location_row=location_row)
        if time_idx is None:
            logger.error(f"{self.file_name}: Unable to locate a time column in the CSV header.")
            return pd.DataFrame(columns=self.VALUE_COLUMNS)

        time_values = numeric_data.iloc[:, time_idx]
        valid_mask: Series[bool] = ~time_values.isna()
        if not valid_mask.any():
            logger.error(f"{self.file_name}: Time column does not contain any numeric values.")
            return pd.DataFrame(columns=self.VALUE_COLUMNS)

        numeric_data = numeric_data.loc[valid_mask].reset_index(drop=True)
        time_values = numeric_data.iloc[:, time_idx].astype("float64")

        tidy_frames: list[pd.DataFrame] = []
        for idx, (measurement, location) in enumerate(zip(measurement_row, location_row)):
            measurement: str = str(measurement).strip()
            location: str = str(location).strip()

            if idx == time_idx:
                continue
            if not measurement or not location:
                continue

            values: Series[float] = numeric_data.iloc[:, idx].astype("float64")
            if values.isna().all():
                logger.debug(
                    "Skipping column '%s'/'%s' because it contains only NaN values.",
                    measurement,
                    location,
                )
                continue

            frame: DataFrame = pd.DataFrame(
                {
                    "Time": time_values,
                    "Location": location,
                    "Type": measurement,
                    "Value": values,
                }
            ).dropna(  # type: ignore
                subset=["Value"]
            )  # type: ignore

            if frame.empty:
                logger.debug(
                    "Skipping column '%s'/'%s' because it produced no valid rows after cleaning.",
                    measurement,
                    location,
                )
                continue

            tidy_frames.append(frame)

        if not tidy_frames:
            logger.warning(f"{self.file_name}: No measurement columns were parsed from the CSV.")
            return pd.DataFrame(columns=self.VALUE_COLUMNS)

        combined: DataFrame = pd.concat(tidy_frames, ignore_index=True)
        combined = combined[self.VALUE_COLUMNS]
        combined.sort_values(by=["Location", "Type", "Time"], inplace=True)
        combined.reset_index(drop=True, inplace=True)
        return combined

    @staticmethod
    def _locate_time_column(measurement_row: pd.Series, location_row: pd.Series) -> int | None:
        """Locate the index of the time column using header metadata."""
        for idx, label in enumerate(location_row):
            if str(label).strip().lower() == "time":
                return idx

        for idx, label in enumerate(measurement_row):
            if str(label).strip().lower() == "time":
                return idx

        return None
