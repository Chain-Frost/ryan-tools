"""Processor for TUFLOW reporting-point water-level history ("*_RLP_H.csv")."""

from __future__ import annotations

__lazy_modules__: list[str] = ["pandas"]

import csv
import re
from typing import ClassVar

import pandas as pd
from loguru import logger

from ..base_processor import BaseProcessor
from ..reporting_location_helpers import parse_reporting_numbers


class RLPHProcessor(BaseProcessor):
    """Read reporting-point levels in simulation hours using the existing PO row schema."""

    filtered_empty: bool = False
    _HEADER: ClassVar[re.Pattern[str]] = re.compile(r"^H (?P<location>.+?) \[(?P<run>[^\]]+)\]$")

    def process(self) -> None:
        """Project requested point columns before reading and expanding samples."""
        self.processed = False
        self.filtered_empty = False
        self.df = pd.DataFrame()
        self.raw_df = pd.DataFrame()
        self.applied_entity_filter = None

        try:
            with self.file_path.open(encoding="utf-8-sig", newline="") as handle:
                header = next(csv.reader(handle))

            if len(header) < 3 or header[1].strip() not in {"Time (h)", "Time(h)"}:
                msg = f"{self.file_name}: Expected descriptor, Time (h) and reporting-point H columns."
                raise ValueError(msg)
            if not header[0].strip().startswith(f"{self.name_parser.raw_run_code} ["):
                msg = f"{self.file_name}: Source descriptor disagrees with the filename run code."
                raise ValueError(msg)

            locations: list[tuple[str, str]] = []
            seen: set[str] = set()
            for source_column in header[2:]:
                match = self._HEADER.fullmatch(source_column.strip())
                if match is None or match.group("run") != self.name_parser.raw_run_code:
                    msg = f"{self.file_name}: Unsupported reporting-point column {source_column!r}."
                    raise ValueError(msg)
                location = match.group("location").strip()
                if not location or location in seen:
                    msg = f"{self.file_name}: Empty or duplicate reporting-point identifier {location!r}."
                    raise ValueError(msg)
                locations.append((source_column, location))
                seen.add(location)

            selected = [
                (source_column, location)
                for source_column, location in locations
                if not self.entity_filter or location in self.entity_filter
            ]
            if not selected:
                self.filtered_empty = True
                logger.info("{}: No requested reporting points; skipping source.", self.file_name)
                return

            # usecols projects locations before converting or materialising long-form rows.
            columns = [header[1], *(source for source, _ in selected)]
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
                msg = f"{self.file_name}: Reporting-point history has no data rows."
                raise ValueError(msg)

            time = parse_reporting_numbers(raw[header[1]], column="Time (h)", allow_missing=False)
            if not time.is_monotonic_increasing or time.duplicated().any():
                msg = f"{self.file_name}: Reporting-point timestamps must increase strictly."
                raise ValueError(msg)

            frames: list[pd.DataFrame] = []
            for source_column, location in selected:
                values = parse_reporting_numbers(raw[source_column], column=source_column)
                frames.append(
                    pd.DataFrame(
                        {"Time": time, "Location": location, "Type": "Reporting Point Water Level", "Value": values}
                    )
                )
            self.raw_df = raw
            self.df = pd.concat(frames, ignore_index=True)
            if not self.df["Value"].notna().any():
                msg = f"{self.file_name}: Every selected reporting-point water level is missing."
                raise ValueError(msg)

            # Preserve time-series nulls; absolute values must never convert -99999 to 99999.
            self.df["AbsValue"] = self.df["Value"].abs()
            self.apply_entity_filter()
            self.add_common_columns()
            self.apply_output_transformations()
            if not self.validate_data():
                msg = f"{self.file_name}: Reporting-point history failed validation."
                raise ValueError(msg)
            self.processed = True
        except (OSError, StopIteration, TypeError, ValueError, pd.errors.ParserError) as exc:
            logger.error("{}: Failed to read reporting-point water levels: {}", self.file_name, exc)
            self.df = pd.DataFrame()
