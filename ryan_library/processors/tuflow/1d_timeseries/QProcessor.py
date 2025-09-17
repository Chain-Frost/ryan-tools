# ryan_library/processors/tuflow/QProcessor.py

from __future__ import annotations

import pandas as pd

from ..base_processor import ProcessorStatus
from ..timeseries_processor import TimeSeriesProcessor


class QProcessor(TimeSeriesProcessor):
    """Processor for ``_Q`` timeseries CSV outputs."""

    def process(self) -> pd.DataFrame:  # type: ignore[override]
        """Process a ``_Q`` CSV using the shared timeseries pipeline."""

        return self._process_timeseries_pipeline(data_type="Q")

    def process_timeseries_raw_dataframe(self) -> ProcessorStatus:
        """Normalise the long-form ``Q`` DataFrame produced by the shared pipeline.

        The shared reader trims the leading ``"Q "`` prefix and any bracketed
        unit descriptors from the raw column headers (for example ``"Q ds1
        [M11_5m_001]"`` becomes ``"ds1"``).  This helper keeps whichever
        identifier column was created during the melt (``"Chan ID"`` for 1D
        exports or ``"Location"`` for 2D), removes empty discharge values and
        re-confirms the headers so downstream validation behaves predictably.

        Returns:
            ProcessorStatus: Status flag following the shared convention used by
            :class:`~ryan_library.processors.tuflow.base_processor.BaseProcessor`.
        """
        return self._normalise_value_dataframe(value_column="Q")
