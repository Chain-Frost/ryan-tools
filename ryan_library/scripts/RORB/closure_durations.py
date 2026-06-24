"""Compatibility wrapper for the relocated RORB closure duration orchestrator."""

from ryan_library.scripts._compat import warn_deprecated

warn_deprecated(__name__, "ryan_library.orchestrators.rorb.closure_durations")

from ryan_library.orchestrators.rorb.closure_durations import *  # noqa: F401,F403

