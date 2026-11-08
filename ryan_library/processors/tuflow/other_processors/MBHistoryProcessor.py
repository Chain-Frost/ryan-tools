"""Source TUFLOW model/domain mass-balance histories: MB, MB1D and MB_HPC."""

from __future__ import annotations

__lazy_modules__: list[str] = ["pandas"]

import csv
from typing import ClassVar

import pandas as pd
from loguru import logger

from ..base_processor import BaseProcessor
from ..reporting_location_helpers import parse_reporting_numbers


class MBHistoryProcessor(BaseProcessor):
    """Retain source model/domain metrics with their own names and time basis."""

    filtered_empty: bool = False
    _SCOPES: ClassVar[dict[str, str]] = {
        "MB": "1D and 2D Model",
        "MB1D": "All 1D Domains",
        "MB_HPC": "HPC Model",
    }

    def process(self) -> None:
        """Read source fields without inventing engineering mass-balance thresholds."""
        self.processed = False
        self.filtered_empty = False
        self.df = pd.DataFrame()
        self.raw_df = pd.DataFrame()
        self.applied_entity_filter = None

        try:
            scope = self._SCOPES[self.data_type]
            if self.entity_filter and scope not in self.entity_filter:
                self.filtered_empty = True
                logger.info("{}: Requested model/domain scope not present.", self.file_name)
                return

            names, fields = self._source_fields()
            raw = pd.read_csv(
                self.file_path,
                header=0,
                dtype=str,
                keep_default_na=False,
                skipinitialspace=True,
                encoding="utf-8-sig",
            )
            if raw.empty or len(raw.columns) != len(names):
                message = "Empty or inconsistent model mass-balance table."
                raise ValueError(message)
            raw.columns = names
            times = parse_reporting_numbers(raw[names[0]], column="Time (h)", allow_missing=False)
            if not times.is_monotonic_increasing:
                message = "Model mass-balance timestamps are not monotonic."
                raise ValueError(message)

            frames: list[pd.DataFrame] = []
            for field in fields:
                values = parse_reporting_numbers(raw[field], column=field)
                frames.append(pd.DataFrame({"Time": times, "Location": scope, "Type": field, "Value": values}))
            self.raw_df = raw
            self.df = pd.concat(frames, ignore_index=True)
            if not self.df["Value"].notna().any():
                message = "All model/domain mass-balance measurements are missing."
                raise ValueError(message)

            self.add_common_columns()
            self.apply_output_transformations()
            if not self.validate_data():
                message = "Model mass-balance table failed validation."
                raise ValueError(message)
            self.processed = True
        except (OSError, StopIteration, KeyError, TypeError, ValueError, csv.Error, pd.errors.ParserError) as exc:
            logger.error("{}: Failed to read model/domain mass balance: {}", self.file_name, exc)
            self.df = pd.DataFrame()

    def _source_fields(self) -> tuple[list[str], list[str]]:
        """Identify time, scope descriptor and exact numeric metric column names."""
        with self.file_path.open(encoding="utf-8-sig", newline="") as handle:
            source = csv.reader(handle, strict=True)
            names = [name.strip() for name in next(source)]
            if len(names) < 3 or names[0] != "Time (h)" or len(set(names)) != len(names):
                message = "Unexpected model mass-balance source headers."
                raise ValueError(message)

            # pandas may silently infer an index from extra CSV fields, or pad short
            # records with missing values. Validate the original field counts first.
            for record_number, record in enumerate(source, start=2):
                if record and len(record) != len(names):
                    message = (
                        f"Model mass-balance record {record_number} contains {len(record)} fields; "
                        f"expected {len(names)}."
                    )
                    raise ValueError(message)
        if self.data_type in {"MB", "MB1D"}:
            if not names[1].endswith(":"):
                message = "Missing model/domain descriptor column."
                raise ValueError(message)
            fields = names[2:]
        else:
            fields = names[1:]
        if not fields or any(not field for field in fields):
            message = "Model/domain mass balance has no named numeric metrics."
            raise ValueError(message)
        return names, fields
