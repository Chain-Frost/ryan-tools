"""Alternate TUFLOW 2D V CSV, not an additional ensemble observation."""

from __future__ import annotations

__lazy_modules__: list[str] = ["pandas"]

import re
from typing import ClassVar

from ..reporting_timeseries_processor import ReportingLocationTimeSeriesProcessor


class Split2dVProcessor(ReportingLocationTimeSeriesProcessor):
    """Preserve V point output identity in the PO-shaped output contract."""

    _HEADER: ClassVar[re.Pattern[str]] = re.compile(r"^V (?P<location>.+?) \[(?P<run>[^\]]+)\]$")
    _MEASUREMENT_TYPE: ClassVar[str] = "Velocity"
    _LOCATION_KIND: ClassVar[str] = "2D point output"
