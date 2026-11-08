"""Source-supplied TUFLOW reporting-point Hmx maxima processor."""

from __future__ import annotations

__lazy_modules__: list[str] = ["pandas"]

from typing import ClassVar

from ..reporting_maximums_processor import ReportingLocationMaximumsProcessor


class RLPHmxProcessor(ReportingLocationMaximumsProcessor):
    """Preserve Hmax/time, dHmax/time and Q from a reporting point's summary."""

    _SOURCE_FIELDS: ClassVar[tuple[str, ...]] = ("ID", "Hmax", "Time Hmax", "dHmax", "Time dHmax", "Q")
    _PRIMARY_VALUE: ClassVar[str] = "Hmax"
    _PRIMARY_TIME: ClassVar[str] = "Time Hmax"
    _MEASUREMENT_TYPE: ClassVar[str] = "Reporting Point Water Level"
    _LOCATION_KIND: ClassVar[str] = "reporting-point"
