"""Processor for TUFLOW velocity (``_V``) timeseries data."""

from __future__ import annotations

import pandas as pd  # type: ignore[import-untyped]

from .base_processor import ProcessorStatus
from .timeseries_processor import TimeSeriesProcessor


class VProcessor(TimeSeriesProcessor):
    """Process ``_V`` CSV exports using the shared timeseries pipeline."""

    def process(self) -> pd.DataFrame:  # type: ignore[override]
        """Process a ``_V`` CSV using the shared timeseries pipeline."""

        return self._process_timeseries_pipeline(data_type="V")

    def process_timeseries_raw_dataframe(self) -> ProcessorStatus:
        """Normalise the melted velocity DataFrame produced by the shared pipeline."""

        return self._normalise_value_dataframe(value_column="V")
