"""Compatibility helpers for deprecated script imports."""

from __future__ import annotations

import warnings

_DEPRECATION_DATE = "31/12/2026"


def warn_deprecated(module_name: str, replacement: str) -> None:
    """Emit a deprecation warning for the legacy scripts namespace."""
    warnings.warn(
        (
            f"{module_name} is deprecated; use {replacement}. "
            f"Backwards compatibility is supported until {_DEPRECATION_DATE}."
            f"Get the latest wrapper from ryan-tools\ryan-scripts\\ "
        ),
        DeprecationWarning,
        stacklevel=2,
    )
