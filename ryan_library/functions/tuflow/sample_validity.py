"""Input-health rules shared by TUFLOW processors and PO quality assurance."""

from __future__ import annotations

__lazy_modules__ = ["numpy", "pandas"]

import csv
from pathlib import Path

import numpy as np
import pandas as pd
from pandas import Series


def is_water_level_measurement(name: str) -> bool:
    """Identify levels using the -99999 dry sentinel, not negative discharge."""
    normalized = "".join(char for char in name.casefold() if char.isalnum())
    return "waterlevel" in normalized or normalized in {"h", "hu", "hd", "ush", "dsh"}


def clean_hydraulic_samples(values: Series, *, measurement: str) -> tuple[Series, dict[str, int]]:
    """Return numeric readings and rejected-sample reasons without replacing missing with zero."""
    text = values.astype("string").str.strip()
    missing = values.isna() | text.fillna("").eq("")
    asterisk = text.fillna("").eq("**********")
    numbers = pd.to_numeric(values, errors="coerce")
    finite = np.isfinite(numbers.to_numpy(dtype="float64", na_value=np.nan))
    nonfinite = pd.Series(~finite, index=values.index) & numbers.notna()
    malformed = numbers.isna() & ~missing & ~asterisk
    sentinel = (
        numbers.eq(-99999).fillna(False)
        if is_water_level_measurement(measurement)
        else pd.Series(False, index=values.index)
    )
    no_data = asterisk | sentinel
    numbers = numbers.mask(nonfinite | no_data)
    reasons = {
        "missing": int(missing.sum()),
        "malformed_numeric": int(malformed.sum()),
        "nonfinite": int(nonfinite.sum()),
        "no_data": int(no_data.sum()),
    }
    return numbers, {reason: count for reason, count in reasons.items() if count}


def has_consistent_csv_widths(path: Path) -> bool:
    """Check original CSV row widths before treating an empty selection as absent.

    Pandas pads truncated rows with NaN, making a shortened source record
    indistinguishable from an explicitly blank measurement in a DataFrame.
    Use this only on the rare all-invalid/absent-selection outcome path.
    """
    try:
        with path.open(encoding="utf-8", newline="") as source:
            expected: int | None = None
            for row in csv.reader(source, strict=True):
                if not row:
                    continue
                if expected is None:
                    expected = len(row)
                elif len(row) != expected:
                    return False
            return expected is not None
    except OSError, UnicodeError, csv.Error:
        return False
