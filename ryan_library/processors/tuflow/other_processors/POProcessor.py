# ryan_library/processors/tuflow/other_processors/POProcessor.py
"""Processor for TUFLOW point output (``*_PO.csv``) timeseries files."""

from __future__ import annotations

__lazy_modules__: list[str] = ["numpy", "pandas"]

from collections.abc import Collection
from typing import ClassVar

import numpy as np
import pandas as pd
from loguru import logger
from pandas import DataFrame, Series

from ....functions.tuflow.sample_validity import clean_hydraulic_samples, is_water_level_measurement
from ..base_processor import BaseProcessor


class POProcessor(BaseProcessor):
    """Load PO timeseries CSV files into a tidy DataFrame."""

    # A valid parsed file can be excluded entirely by a location filter.
    # Keep this separate from processing failures and from exported processors.
    filtered_empty: bool = False
    # Source header IDs, including locations whose samples are all rejected.
    source_locations: frozenset[str] = frozenset()
    # Source locations with at least one usable derived hydraulic value.
    usable_source_locations: frozenset[str] = frozenset()

    VALUE_COLUMNS: ClassVar[list[str]] = ["Time", "Location", "Type", "Value"]

    def filter_locations(self, locations: Collection[str] | None) -> frozenset[str]:
        """Preserve known, wholly rejected locations through public selection."""
        selected = super().filter_locations(locations)
        if not selected:
            return selected
        if self.df.empty:
            self.applied_location_filter = selected
            if selected & (self.source_locations - self.usable_source_locations):
                # Invalid samples were dropped during parsing, but the source
                # contained the location: INVALID/FAIL, not absent/SKIP.
                self.selected_unusable = True
                self.filtered_empty = False
                self.input_health = "INVALID"
            else:
                self.selected_unusable = False
                self.filtered_empty = True
            return selected

        self.selected_unusable = False
        if (
            self.applied_location_filter == selected
            and "AbsValue" in self.df.columns
            and not self.df["AbsValue"].notna().any()
        ):
            self.input_health = "INVALID"
        return selected

    def process(self) -> None:
        """Parse the CSV, reshape it to long format, and add common columns."""
        logger.info(f"Starting processing of PO file: {self.log_path}")
        self.reset_processing_state()
        self._selected_source_present = False
        self.source_locations = frozenset()
        self.usable_source_locations = frozenset()

        try:
            raw_df: DataFrame = pd.read_csv(self.file_path, header=None, dtype=str)
            self.raw_df = raw_df.copy()
        except Exception as exc:  # pragma: no cover - IO errors handled here
            logger.exception(f"{self.file_name}: Failed to read CSV file: {exc}")
            self.input_health = "IO_FAILURE"
            self.df = pd.DataFrame()
            return

        tidy_df: DataFrame = self._parse_point_output(raw_df=raw_df)
        if tidy_df.empty:
            self.df = tidy_df
            # An all-rejected but structurally readable file can still have
            # a genuinely absent selection. Malformed inputs have no source IDs.
            if self.entity_filter and self.source_locations and self.entity_filter.isdisjoint(self.source_locations):
                self.applied_entity_filter = self.entity_filter
                self.filtered_empty = True
                logger.info("{}: No requested PO locations found; skipping file.", self.file_name)
                return
            logger.warning(f"{self.file_name}: No point output values found after processing.")
            self.input_health = "INVALID"
            return

        self.df = tidy_df
        self.df["AbsValue"] = self.df["Value"].abs()
        # Preserve raw TUFLOW level values and row counts for source/split parity,
        # but prevent the -99999 dry marker from contributing to derived extrema.
        levels = self.df["Type"].astype("string").map(lambda name: is_water_level_measurement(str(name)))
        dry = levels & self.df["Value"].eq(-99999).fillna(False)
        self.df.loc[dry, "AbsValue"] = float("nan")

        # Determine usability before direct/collection selection. A valid
        # location removed by an earlier filter is not rejected source data.
        usable_labels: list[object] = self.df.loc[self.df["AbsValue"].notna(), "Location"].tolist()
        self.usable_source_locations = frozenset(str(label).strip() for label in usable_labels if str(label).strip())

        self.apply_entity_filter()
        if self.applied_entity_filter and self.df.empty:
            if self._selected_source_present:
                # Present but wholly rejected is a file failure, not no-match.
                self.input_health = "INVALID"
                logger.warning("{}: Selected PO locations contain no usable samples.", self.file_name)
            else:
                self.filtered_empty = True
                logger.info("{}: No requested PO locations found; skipping file.", self.file_name)
            return
        if not self.df["AbsValue"].notna().any():
            self.input_health = "INVALID"
        self.add_common_columns()
        self.apply_output_transformations()

        if not self.validate_data():
            logger.error(f"{self.file_name}: Data validation failed.")
            self.input_health = "INVALID"
            self.df = pd.DataFrame()
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
        measurement_row: Series = trimmed.iloc[0].fillna("")
        location_row: Series = trimmed.iloc[1].fillna("")
        data_rows: DataFrame = trimmed.iloc[2:]

        if measurement_row.empty or location_row.empty:
            logger.error(f"{self.file_name}: Missing measurement or location headers.")
            return pd.DataFrame(columns=self.VALUE_COLUMNS)

        data_rows = data_rows.reset_index(drop=True)

        time_idx: int | None = self._locate_time_column(measurement_row=measurement_row, location_row=location_row)
        if time_idx is None:
            logger.error(f"{self.file_name}: Unable to locate a time column in the CSV header.")
            return pd.DataFrame(columns=self.VALUE_COLUMNS)

        time_values = pd.to_numeric(data_rows.iloc[:, time_idx], errors="coerce")
        if not np.isfinite(time_values.to_numpy(dtype="float64", na_value=np.nan)).all():
            logger.error("{}: PO timestamps contain invalid or nonfinite values.", self.file_name)
            return pd.DataFrame(columns=self.VALUE_COLUMNS)
        if (time_values.diff().iloc[1:] <= 0).any():
            logger.error("{}: PO timestamps must increase strictly without duplicates.", self.file_name)
            return pd.DataFrame(columns=self.VALUE_COLUMNS)
        time_values = time_values.astype("float64")

        tidy_frames: list[pd.DataFrame] = []
        source_locations: set[str] = set()
        for idx, (measurement, location) in enumerate(zip(measurement_row, location_row, strict=True)):
            measurement_text: str = str(measurement).strip()
            location_text: str = str(location).strip()

            if idx == time_idx:
                continue
            if not measurement_text or not location_text:
                continue

            source_locations.add(location_text)
            if self.entity_filter and location_text in self.entity_filter:
                self._selected_source_present = True

            values, rejected = clean_hydraulic_samples(data_rows.iloc[:, idx], measurement=measurement_text)
            if is_water_level_measurement(measurement_text):
                source_values = pd.to_numeric(data_rows.iloc[:, idx], errors="coerce")
                # A dry-level sentinel remains in the raw Value column for
                # exact source provenance; derived AbsValue is masked later.
                values = values.mask(source_values.eq(-99999).fillna(False), -99999)
            self.record_rejections(rejected)
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
                    "Location": location_text,
                    "Type": measurement_text,
                    "Value": values,
                }
            ).dropna(subset=["Value"])

            if frame.empty:
                logger.debug(
                    "Skipping column '%s'/'%s' because it produced no valid rows after cleaning.",
                    measurement_text,
                    location_text,
                )
                continue

            tidy_frames.append(frame)

        self.source_locations = frozenset(source_locations)
        if not tidy_frames:
            logger.warning(f"{self.file_name}: No measurement columns were parsed from the CSV.")
            return pd.DataFrame(columns=self.VALUE_COLUMNS)

        combined: DataFrame = pd.concat(tidy_frames, ignore_index=True)
        combined = combined[self.VALUE_COLUMNS]
        combined = combined.sort_values(by=["Location", "Type", "Time"])
        combined = combined.reset_index(drop=True)
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
