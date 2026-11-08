"""TUFLOW reporting-point water-level time-series reader."""

from __future__ import annotations

__lazy_modules__: list[str] = ["pandas"]

import re
from typing import ClassVar

from ..reporting_timeseries_processor import ReportingLocationTimeSeriesProcessor


class RLPHProcessor(ReportingLocationTimeSeriesProcessor):
    """Preserve reporting-point water levels and their point geometry identity."""

    _HEADER: ClassVar[re.Pattern[str]] = re.compile(r"^H (?P<location>.+?) \[(?P<run>[^\]]+)\]$")
    _MEASUREMENT_TYPE: ClassVar[str] = "Reporting Point Water Level"
    _LOCATION_KIND: ClassVar[str] = "reporting-point"
