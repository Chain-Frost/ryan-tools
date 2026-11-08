"""Selected-column wide CSV reader for reporting locations and staged result histories."""

from __future__ import annotations

__lazy_modules__: list[str] = ["pandas"]

import csv
import re
from typing import ClassVar

import pandas as pd
from loguru import logger

from .base_processor import BaseProcessor
from .reporting_location_helpers import parse_reporting_numbers


class ReportingLocationTimeSeriesProcessor(BaseProcessor):
    """Read explicitly selected reporting-location series into the existing PO-shaped schema."""

    filtered_empty: bool = False
    _HEADER: ClassVar[re.Pattern[str]] = re.compile(r"(?!)")
    _MEASUREMENT_TYPE: ClassVar[str] = ""
    _LOCATION_KIND: ClassVar[str] = "reporting location"

    def process(self) -> None:
        """Project requested location columns before reading and expanding samples."""
        self.processed = False
        self.filtered_empty = False
        self.df = pd.DataFrame()
        self.raw_df = pd.DataFrame()
        self.applied_entity_filter = None

        try:
            time_column, locations = self._source_columns()
            selected = [
                (source_column, location)
                for source_column, location in locations
                if not self.entity_filter or location in self.entity_filter
            ]
            if not selected:
                self.filtered_empty = True
                logger.info("{}: No requested {} names; skipping source.", self.file_name, self._LOCATION_KIND)
                return

            # usecols projects locations before converting or materialising long-form rows.
            columns = [time_column, *(source for source, _ in selected)]
            raw = pd.read_csv(
                self.file_path,
                header=0,
                usecols=columns,
                dtype=str,
                keep_default_na=False,
                skipinitialspace=True,
                encoding="utf-8-sig",
            )
            if raw.empty:
                msg = f"{self.file_name}: {self._LOCATION_KIND} history has no data rows."
                raise ValueError(msg)

            time = parse_reporting_numbers(raw[time_column], column="Time (h)", allow_missing=False)
            if not time.is_monotonic_increasing or time.duplicated().any():
                msg = f"{self.file_name}: {self._LOCATION_KIND} timestamps must increase strictly."
                raise ValueError(msg)

            frames: list[pd.DataFrame] = []
            for source_column, location in selected:
                values = parse_reporting_numbers(raw[source_column], column=source_column)
                frames.append(
                    pd.DataFrame(
                        {
                            "Time": time,
                            "Location": location,
                            "Type": self._measurement_type_for_column(source_column),
                            "Value": values,
                        }
                    )
                )
            self.raw_df = raw
            self.df = pd.concat(frames, ignore_index=True)
            if not self.df["Value"].notna().any():
                msg = f"{self.file_name}: Every selected {self._LOCATION_KIND} measurement is missing."
                raise ValueError(msg)

            # Preserve time-series nulls; absolute values must never convert -99999 to 99999.
            self.df["AbsValue"] = self.df["Value"].abs()
            self.apply_entity_filter()
            self.add_common_columns()
            self.apply_output_transformations()
            if not self.validate_data():
                msg = f"{self.file_name}: {self._LOCATION_KIND} history failed validation."
                raise ValueError(msg)
            self.processed = True
        except (OSError, StopIteration, TypeError, ValueError, pd.errors.ParserError) as exc:
            logger.error("{}: Failed to read {} time series: {}", self.file_name, self._LOCATION_KIND, exc)
            self.df = pd.DataFrame()

    def _source_columns(self) -> tuple[str, list[tuple[str, str]]]:
        """Validate input headers and retain the original locations and column labels."""
        if not self._MEASUREMENT_TYPE:
            msg = "A reporting-location measurement type is required."
            raise ValueError(msg)
        with self.file_path.open(encoding="utf-8-sig", newline="") as handle:
            header = next(csv.reader(handle))

        if len(header) < 3 or header[1].strip() not in {"Time (h)", "Time(h)"}:
            msg = f"{self.file_name}: Expected descriptor, Time (h) and {self._LOCATION_KIND} columns."
            raise ValueError(msg)
        if not header[0].strip().startswith(f"{self.name_parser.raw_run_code} ["):
            msg = f"{self.file_name}: Source descriptor disagrees with the filename run code."
            raise ValueError(msg)

        locations: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for source_column in header[2:]:
            match = self._HEADER.fullmatch(source_column.strip())
            if match is None or match.group("run") != self.name_parser.raw_run_code:
                msg = f"{self.file_name}: Unsupported {self._LOCATION_KIND} column {source_column!r}."
                raise ValueError(msg)
            location = match.group("location").strip()
            distinct_field = (location, match.groupdict().get("component", ""))
            if not location or distinct_field in seen:
                msg = f"{self.file_name}: Empty or duplicate {self._LOCATION_KIND} identifier {location!r}."
                raise ValueError(msg)
            locations.append((source_column, location))
            seen.add(distinct_field)
        return header[1], locations

    def _measurement_type_for_column(self, source_column: str) -> str:
        """Default type is common to all columns; component readers may override."""
        _ = source_column
        return self._MEASUREMENT_TYPE
