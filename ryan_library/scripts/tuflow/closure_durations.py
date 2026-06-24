"""Compatibility wrapper for the relocated TUFLOW closure duration orchestrator."""

from ryan_library.scripts._compat import warn_deprecated

warn_deprecated(__name__, "ryan_library.orchestrators.tuflow.closure_durations")

from ryan_library.orchestrators.tuflow.closure_durations import *  # noqa: F401,F403

