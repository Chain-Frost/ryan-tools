"""Source channel and pit loss coefficient histories from *_1d_L.csv."""

from __future__ import annotations

__lazy_modules__: list[str] = ["pandas"]

import re
from typing import ClassVar

from ..reporting_timeseries_processor import ReportingLocationTimeSeriesProcessor


class LProcessor(ReportingLocationTimeSeriesProcessor):
    """Retain entry, additional, exit and pit coefficients as dimensionless values."""

    _HEADER: ClassVar[re.Pattern[str]] = re.compile(
        r"^(?P<component>Entry LC|Additional LC|Exit LC|LC) (?P<location>.+?) \[(?P<run>[^\]]+)\]$"
    )
    _MEASUREMENT_TYPE: ClassVar[str] = "Loss Coefficient"
    _LOCATION_KIND: ClassVar[str] = "1D loss-coefficient location"

    def _measurement_type_for_column(self, source_column: str) -> str:
        """Retain the named source component instead of collapsing coefficients."""
        match = self._HEADER.fullmatch(source_column.strip())
        if match is None:
            return self._MEASUREMENT_TYPE
        return match.group("component")
