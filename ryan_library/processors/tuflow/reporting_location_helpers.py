"""Numeric conversion shared by the selected reporting-point CSV processors."""

from __future__ import annotations

import numpy as np
import pandas as pd


def parse_reporting_numbers(values: pd.Series, *, column: str, allow_missing: bool = True) -> pd.Series:
    """Keep signed float64 measurements and treat documented missing tokens as null.

    The TUFLOW -99999 sentinel is missing, not a legitimate hydraulic value.
    Unexpected text and nonfinite numbers remain invalid rather than silently
    becoming null. Numerical formatting is not rounded to float32.
    """
    raw = values.astype("string").str.strip()
    missing = raw.isna() | raw.isin(["", "**********"])
    parsed = pd.to_numeric(raw.where(~missing), errors="coerce").astype("float64")
    invalid = ~missing & ~np.isfinite(parsed)
    if invalid.any():
        msg = f"Invalid or nonfinite numeric values in reporting-point column {column!r}."
        raise ValueError(msg)

    parsed = parsed.mask(parsed.eq(-99999.0))
    if not allow_missing and parsed.isna().any():
        msg = f"Missing values are not permitted in reporting-point column {column!r}."
        raise ValueError(msg)
    return parsed
