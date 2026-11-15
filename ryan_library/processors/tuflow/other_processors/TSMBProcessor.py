"""TUFLOW 1D node mass-balance time history from *_1d_TSMB.csv."""

from __future__ import annotations

__lazy_modules__: list[str] = ["pandas"]

import re
from typing import ClassVar

from ..reporting_timeseries_processor import ReportingLocationTimeSeriesProcessor


class TSMBProcessor(ReportingLocationTimeSeriesProcessor):
    """Do not conflate node history with model/domain or node-summary balances."""

    _HEADER: ClassVar[re.Pattern[str]] = re.compile(r"^MB (?P<location>.+?) \[(?P<run>[^\]]+)\]$")
    _MEASUREMENT_TYPE: ClassVar[str] = "1D Node Mass Balance History"
    _LOCATION_KIND: ClassVar[str] = "1D mass-balance node"
