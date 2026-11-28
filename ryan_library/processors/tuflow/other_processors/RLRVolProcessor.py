"""TUFLOW reporting-region volume history ("*_RLR_Vol.csv") processor."""

from __future__ import annotations

__lazy_modules__: list[str] = ["pandas"]

import re
from typing import ClassVar

from ..reporting_timeseries_processor import ReportingLocationTimeSeriesProcessor


class RLRVolProcessor(ReportingLocationTimeSeriesProcessor):
    """Preserve source region-volume series without confusing them with point levels."""

    _HEADER: ClassVar[re.Pattern[str]] = re.compile(r"^Vol (?P<location>.+?) \[(?P<run>[^\]]+)\]$")
    _MEASUREMENT_TYPE: ClassVar[str] = "Reporting Region Volume"
    _LOCATION_KIND: ClassVar[str] = "reporting-region"
