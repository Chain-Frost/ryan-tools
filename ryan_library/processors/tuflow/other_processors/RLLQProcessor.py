"""TUFLOW reporting-line flow history ("*_RLL_Q.csv") processor."""

from __future__ import annotations

__lazy_modules__: list[str] = ["pandas"]

import re
from typing import ClassVar

from ..reporting_timeseries_processor import ReportingLocationTimeSeriesProcessor


class RLLQProcessor(ReportingLocationTimeSeriesProcessor):
    """Preserve TUFLOW's signed combined line-flow series, not network-derived flows."""

    _HEADER: ClassVar[re.Pattern[str]] = re.compile(r"^Q (?P<location>.+?) \[(?P<run>[^\]]+)\]$")
    _MEASUREMENT_TYPE: ClassVar[str] = "Reporting Line Flow"
    _LOCATION_KIND: ClassVar[str] = "reporting-line"
