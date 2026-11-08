"""Source-supplied TUFLOW reporting-region Volmx volume summaries processor."""

from __future__ import annotations

__lazy_modules__: list[str] = ["pandas"]

from typing import ClassVar

from ..reporting_maximums_processor import ReportingLocationMaximumsProcessor


class RLRVolmxProcessor(ReportingLocationMaximumsProcessor):
    """Preserve supplied volume maximum/time and independent change/time/level fields."""

    _SOURCE_FIELDS: ClassVar[tuple[str, ...]] = (
        "ID",
        "Vol max",
        "Time Vol max",
        "dVolmax",
        "Time dVolmax",
        "H",
    )
    _PRIMARY_VALUE: ClassVar[str] = "Vol max"
    _PRIMARY_TIME: ClassVar[str] = "Time Vol max"
    _MEASUREMENT_TYPE: ClassVar[str] = "Reporting Region Volume"
    _LOCATION_KIND: ClassVar[str] = "reporting-region"
