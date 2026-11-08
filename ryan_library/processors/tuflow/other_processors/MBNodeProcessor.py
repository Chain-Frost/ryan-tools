"""TUFLOW _1d_MB.csv node cumulative mass-balance summary reader."""

from __future__ import annotations

__lazy_modules__: list[str] = ["pandas"]

import csv
from typing import ClassVar

import pandas as pd
from loguru import logger

from ..base_processor import BaseProcessor
from ..reporting_location_helpers import parse_reporting_numbers


class MBNodeProcessor(BaseProcessor):
    """Read the three source node summaries, not the following time-series rows."""

    filtered_empty: bool = False
    METRICS: ClassVar[tuple[str, ...]] = ("Cum %ME", "Cum VE", "Cum VQ")

    def process(self) -> None:
        """Read only fixed source summary rows, filtering node IDs before expansion."""
        self.processed = False
        self.filtered_empty = False
        self.df = pd.DataFrame()
        self.raw_df = pd.DataFrame()
        self.applied_entity_filter = None
        try:
            nodes, source_values = self._source_summary()
            selected = [
                index for index, node in enumerate(nodes) if not self.entity_filter or node in self.entity_filter
            ]
            if not selected:
                self.filtered_empty = True
                logger.info("{}: No requested mass-balance nodes.", self.file_name)
                return

            selected_nodes = [nodes[index] for index in selected]
            metric_values = {
                metric: parse_reporting_numbers(
                    pd.Series([source_values[metric][index] for index in selected]),
                    column=metric,
                )
                for metric in self.METRICS
            }
            self.df = pd.DataFrame(
                {"Location": selected_nodes, "Type": "1D Node Mass Balance Summary", **metric_values}
            )
            if not self.df[list(self.METRICS)].notna().any(axis=None):
                message = "All selected node mass-balance summary values are missing."
                raise ValueError(message)
            self.raw_df = self.df.copy()
            self.apply_entity_filter()
            self.add_common_columns()
            self.apply_output_transformations()
            if not self.validate_data():
                message = "Node mass-balance summary failed validation."
                raise ValueError(message)
            self.processed = True
        except (OSError, StopIteration, TypeError, ValueError, pd.errors.ParserError) as exc:
            logger.error("{}: Failed to read 1D node mass balance: {}", self.file_name, exc)
            self.df = pd.DataFrame()

    def _source_summary(self) -> tuple[list[str], dict[str, list[str]]]:
        """Verify and read exactly Cum %ME, Cum VE and Cum VQ prelude records."""
        with self.file_path.open(encoding="utf-8-sig", newline="") as handle:
            source = csv.reader(handle)
            header = next(source)
            if len(header) < 3 or header[1].strip() != "Node:":
                message = "Unexpected node mass-balance source header."
                raise ValueError(message)
            nodes = [value.strip() for value in header[2:]]
            if any(not node for node in nodes) or len(set(nodes)) != len(nodes):
                message = "Missing or duplicate node mass-balance identifiers."
                raise ValueError(message)

            values: dict[str, list[str]] = {}
            for field in self.METRICS:
                record = next(source)
                if len(record) != len(header) or record[1].strip() != field:
                    message = f"Unexpected node mass-balance summary row; expected {field}."
                    raise ValueError(message)
                values[field] = record[2:]
        return nodes, values
