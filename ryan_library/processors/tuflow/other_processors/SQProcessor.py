"""TUFLOW source-computed total structure-crossing flow history."""

from __future__ import annotations

__lazy_modules__: list[str] = ["pandas"]

import re
from typing import ClassVar

from ..reporting_timeseries_processor import ReportingLocationTimeSeriesProcessor


class SQProcessor(ReportingLocationTimeSeriesProcessor):
    """Keep SQ combined structure flow separate from 2D PO QS observations."""

    _HEADER: ClassVar[re.Pattern[str]] = re.compile(r"^SQ (?P<location>.+?) \[(?P<run>[^\]]+)\]$")
    _MEASUREMENT_TYPE: ClassVar[str] = "Structure Total Crossing Flow"
    _LOCATION_KIND: ClassVar[str] = "structure"
