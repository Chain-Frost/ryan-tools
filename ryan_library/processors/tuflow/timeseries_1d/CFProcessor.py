"""Processor for TUFLOW culvert flow (``_CF``) timeseries data."""

from __future__ import annotations

import pandas as pd

from ..base_processor import ProcessorStatus
from ..timeseries_processor import TimeSeriesProcessor


class CFProcessor(TimeSeriesProcessor):
    """Process ``_CF`` CSV exports using the shared timeseries pipeline."""

    def process(self) -> None:  # type: ignore[override]
        """Process a ``_CF`` CSV using the shared timeseries pipeline."""

        self._process_timeseries_pipeline(data_type="CF")

    def process_timeseries_raw_dataframe(self) -> ProcessorStatus:
        """Normalise the melted culvert flow DataFrame produced by the shared pipeline."""

        return self._normalise_value_dataframe(value_column="CF")

    def _clean_column_names(self, columns: pd.Index, data_type: str) -> list[str]:
        """Normalise culvert flow headers which use the ``"F"`` prefix in exports."""

        return super()._clean_column_names(columns=columns, data_type="F")

    def _apply_final_transformations(self, data_type: str) -> None:
        """Coerce culvert flow values to string while keeping time numeric."""

        self.apply_dtype_mapping(
            dtype_mapping={"Time": "float64", "CF": "string"},
            context="final_transformations_cf",
        )
