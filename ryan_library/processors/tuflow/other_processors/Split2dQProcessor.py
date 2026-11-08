"""Alternate TUFLOW 2D Q CSV, not an additional ensemble observation."""

from __future__ import annotations

__lazy_modules__: list[str] = ["pandas"]

import re
from typing import ClassVar

from ..reporting_timeseries_processor import ReportingLocationTimeSeriesProcessor


class Split2dQProcessor(ReportingLocationTimeSeriesProcessor):
    """Preserve Q point output identity in the PO-shaped output contract."""

    _HEADER: ClassVar[re.Pattern[str]] = re.compile(r"^Q (?P<location>.+?) \[(?P<run>[^\]]+)\]$")
    _MEASUREMENT_TYPE: ClassVar[str] = "Flow"
    _LOCATION_KIND: ClassVar[str] = "2D point output"
