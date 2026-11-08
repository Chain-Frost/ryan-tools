"""Preserve TUFLOW's supplied state at maximum upstream structure water level."""

from __future__ import annotations

__lazy_modules__: list[str] = ["pandas"]

import csv
from typing import ClassVar

import pandas as pd
from loguru import logger

from ..base_processor import BaseProcessor
from ..reporting_location_helpers import parse_reporting_numbers


class SHmxProcessor(BaseProcessor):
    """A source SHmx row is not an independent maximum discharge or velocity."""

    filtered_empty: bool = False
    SOURCE_FIELDS: ClassVar[tuple[str, ...]] = (
        "ID (Lowest)", "Q Below", "Q Above", "Q Total", "A Below", "A Above",
        "A Total", "V Below", "V Above", "V Combined", "h U/S", "h D/S", "dh", "Time",
    )
    MEASUREMENT_TYPE: ClassVar[str] = "Structure State at Maximum Upstream Water Level"

    def process(self) -> None:
        """Validate the actual source schema and retain selected row components verbatim."""
        self.processed = False
        self.filtered_empty = False
        self.df = pd.DataFrame()
        self.raw_df = pd.DataFrame()
        self.applied_entity_filter = None
        try:
            with self.file_path.open(encoding="utf-8-sig", newline="") as handle:
                header = next(csv.reader(handle))
            if len(header) != len(self.SOURCE_FIELDS) + 1 or tuple(header[1:]) != self.SOURCE_FIELDS:
                raise ValueError(f"Unexpected SHmx source header: {header[1:]!r}")
            if not header[0].strip().startswith(f"{self.name_parser.raw_run_code} ["):
                raise ValueError("SHmx descriptor disagrees with its filename run code.")

            raw = pd.read_csv(
                self.file_path,
                header=0,
                usecols=list(self.SOURCE_FIELDS),
                dtype=str,
                keep_default_na=False,
                skipinitialspace=True,
                encoding="utf-8-sig",
            )
            if raw.empty:
                raise ValueError("SHmx has no structure rows.")
            locations = raw["ID (Lowest)"].astype("string").str.strip()
            if locations.isna().any() or locations.eq("").any() or locations.duplicated().any():
                raise ValueError("Missing or duplicate SHmx structure identifiers.")

            if self.entity_filter:
                raw = raw.loc[locations.isin(self.entity_filter)].copy()
                locations = locations.loc[raw.index]
                if raw.empty:
                    self.filtered_empty = True
                    logger.info("{}: No requested structure state rows; skipping.", self.file_name)
                    return

            values = {field: parse_reporting_numbers(raw[field], column=field) for field in self.SOURCE_FIELDS[1:]}
            if not any(series.notna().any() for series in values.values()):
                raise ValueError("Every selected SHmx hydraulic value is missing.")
            if (values["h U/S"].notna() & values["Time"].isna()).any():
                raise ValueError("Upstream water level has no source associated time.")

            self.raw_df = raw
            self.df = pd.DataFrame({"Location": locations, "Type": self.MEASUREMENT_TYPE, **values}).reset_index(
                drop=True
            )
            self.apply_entity_filter()
            self.add_common_columns()
            self.apply_output_transformations()
            if not self.validate_data():
                raise ValueError("SHmx state failed validation.")
            self.processed = True
        except (OSError, StopIteration, TypeError, ValueError, pd.errors.ParserError) as exc:
            logger.error("{}: SHmx processing failed: {}", self.file_name, exc)
            self.df = pd.DataFrame()
