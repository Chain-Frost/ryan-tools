"""Compatibility package for RORB orchestrators."""

from ryan_library.orchestrators.rorb import closure_durations as closure_durations
from ryan_library.scripts._compat import warn_deprecated

warn_deprecated("ryan_library.scripts.rorb", "ryan_library.orchestrators.rorb")

__all__ = ["closure_durations"]
