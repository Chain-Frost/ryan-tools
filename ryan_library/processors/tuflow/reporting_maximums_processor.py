"""Shared source-supplied maximum reader for point and region reporting locations."""

from __future__ import annotations

__lazy_modules__: list[str] = ["pandas"]

import csv
from typing import ClassVar

import pandas as pd
from loguru import logger

from .base_processor import BaseProcessor
from .reporting_location_helpers import parse_reporting_numbers


class ReportingLocationMaximumsProcessor(BaseProcessor):
    """Retain each source maximum and time association without recomputing hydraulic states."""

    filtered_empty: bool = False
    _SOURCE_FIELDS: ClassVar[tuple[str, ...]] = ()
    _PRIMARY_VALUE: ClassVar[str] = ""
    _PRIMARY_TIME: ClassVar[str] = ""
    _MEASUREMENT_TYPE: ClassVar[str] = ""
    _LOCATION_KIND: ClassVar[str] = "reporting location"

    def process(self) -> None:
        """Load source maximum records without reconstructing coincident states."""
        self.processed = False
        self.filtered_empty = False
        self.df = pd.DataFrame()
        self.raw_df = pd.DataFrame()
        self.applied_entity_filter = None

        try:
            raw = self._source_maximum_table()
            required: tuple[str, ...] = self._SOURCE_FIELDS

            locations = raw["ID"].astype("string").str.strip()
            if locations.isna().any() or locations.eq("").any() or locations.duplicated().any():
                msg = f"{self.file_name}: Missing or duplicate {self._LOCATION_KIND} identifier."
                raise ValueError(msg)

            if self.entity_filter:
                raw = raw.loc[locations.isin(self.entity_filter)].copy()
                locations = locations.loc[raw.index]
                if raw.empty:
                    self.filtered_empty = True
                    logger.info("{}: No requested {} maxima; skipping source.", self.file_name, self._LOCATION_KIND)
                    return

            # Retain source maximum/time and independent change/context associations.
            # Never infer a coincident hydraulic state from unrelated source fields.
            numeric = {name: parse_reporting_numbers(raw[name], column=name) for name in required[1:]}
            if not numeric[self._PRIMARY_VALUE].notna().any():
                msg = f"{self.file_name}: Every selected {self._PRIMARY_VALUE} observation is missing."
                raise ValueError(msg)
            if (numeric[self._PRIMARY_VALUE].notna() & numeric[self._PRIMARY_TIME].isna()).any():
                msg = f"{self.file_name}: Some {self._PRIMARY_VALUE} values have no associated {self._PRIMARY_TIME}."
                raise ValueError(msg)

            self.raw_df = raw
            self.df = pd.DataFrame({"Location": locations, "Type": self._MEASUREMENT_TYPE, **numeric}).reset_index(
                drop=True
            )
            self.apply_entity_filter()
            self.add_common_columns()
            self.apply_output_transformations()
            if not self.validate_data():
                msg = f"{self.file_name}: {self._LOCATION_KIND} maximums failed validation."
                raise ValueError(msg)
            self.processed = True
        except (OSError, StopIteration, TypeError, ValueError, pd.errors.ParserError) as exc:
            logger.error("{}: Failed to read {} maximums: {}", self.file_name, self._LOCATION_KIND, exc)
            self.df = pd.DataFrame()

    def _source_maximum_table(self) -> pd.DataFrame:
        """Read the known source schema without treating associations as extra maxima."""
        with self.file_path.open(encoding="utf-8-sig", newline="") as handle:
            header = next(csv.reader(handle))

        required = self._SOURCE_FIELDS
        if not self._PRIMARY_VALUE or not self._PRIMARY_TIME or not self._MEASUREMENT_TYPE:
            msg = "A reporting-location maximum measurement contract is required."
            raise ValueError(msg)
        if len(header) != len(required) + 1 or header[1:] != required:
            msg = f"{self.file_name}: Unsupported {self._LOCATION_KIND} maximum header: {header[1:]!r}."
            raise ValueError(msg)
        if not header[0].strip().startswith(f"{self.name_parser.raw_run_code} ["):
            msg = f"{self.file_name}: Source descriptor disagrees with the filename run code."
            raise ValueError(msg)

        raw = pd.read_csv(
            self.file_path,
            header=0,
            usecols=required,
            dtype=str,
            keep_default_na=False,
            skipinitialspace=True,
            encoding="utf-8-sig",
        )
        if raw.empty:
            msg = f"{self.file_name}: {self._LOCATION_KIND} maximum file has no data rows."
            raise ValueError(msg)
        return raw
