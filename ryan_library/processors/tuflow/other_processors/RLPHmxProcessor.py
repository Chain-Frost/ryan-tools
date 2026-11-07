"""Processor for source-supplied TUFLOW reporting-point maxima ("*_RLP_Hmx.csv")."""

from __future__ import annotations

__lazy_modules__: list[str] = ["pandas"]

import csv

import pandas as pd
from loguru import logger

from ..base_processor import BaseProcessor
from ..reporting_location_helpers import parse_reporting_numbers


class RLPHmxProcessor(BaseProcessor):
    """Retain source Hmax/time and the separate dHmax/time/Q associations."""

    filtered_empty: bool = False

    def process(self) -> None:
        """Load the supplied maximum record without reconstructing coincident states."""
        self.processed = False
        self.filtered_empty = False
        self.df = pd.DataFrame()
        self.raw_df = pd.DataFrame()
        self.applied_entity_filter = None

        try:
            raw = self._source_maximum_table()
            required: list[str] = ["ID", "Hmax", "Time Hmax", "dHmax", "Time dHmax", "Q"]

            locations = raw["ID"].astype("string").str.strip()
            if locations.isna().any() or locations.eq("").any() or locations.duplicated().any():
                msg = f"{self.file_name}: Missing or duplicate reporting-point identifier."
                raise ValueError(msg)

            if self.entity_filter:
                raw = raw.loc[locations.isin(self.entity_filter)].copy()
                locations = locations.loc[raw.index]
                if raw.empty:
                    self.filtered_empty = True
                    logger.info("{}: No requested reporting-point maxima; skipping source.", self.file_name)
                    return

            # Hmax/time describes a source-supplied pair. dHmax/time and Q
            # remain their own source fields; they are not renamed as Hmax.
            numeric = {name: parse_reporting_numbers(raw[name], column=name) for name in required[1:]}
            if not numeric["Hmax"].notna().any():
                msg = f"{self.file_name}: Every selected Hmax observation is missing."
                raise ValueError(msg)
            if (numeric["Hmax"].notna() & numeric["Time Hmax"].isna()).any():
                msg = f"{self.file_name}: Some Hmax values have no associated Time Hmax."
                raise ValueError(msg)

            self.raw_df = raw
            self.df = pd.DataFrame(
                {"Location": locations, "Type": "Reporting Point Water Level", **numeric}
            ).reset_index(drop=True)
            self.apply_entity_filter()
            self.add_common_columns()
            self.apply_output_transformations()
            if not self.validate_data():
                msg = f"{self.file_name}: Reporting-point maximums failed validation."
                raise ValueError(msg)
            self.processed = True
        except (OSError, StopIteration, TypeError, ValueError, pd.errors.ParserError) as exc:
            logger.error("{}: Failed to read reporting-point maximums: {}", self.file_name, exc)
            self.df = pd.DataFrame()

    def _source_maximum_table(self) -> pd.DataFrame:
        """Read the known source schema without treating associations as extra maxima."""
        with self.file_path.open(encoding="utf-8-sig", newline="") as handle:
            header = next(csv.reader(handle))

        required = ["ID", "Hmax", "Time Hmax", "dHmax", "Time dHmax", "Q"]
        if len(header) != len(required) + 1 or header[1:] != required:
            msg = f"{self.file_name}: Unsupported reporting-point maximum header: {header[1:]!r}."
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
            msg = f"{self.file_name}: Reporting-point maximum file has no data rows."
            raise ValueError(msg)
        return raw
