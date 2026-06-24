"""Compatibility wrapper for the relocated TUFLOW log summary orchestrator."""

from ryan_library.scripts._compat import warn_deprecated

warn_deprecated(__name__, "ryan_library.orchestrators.tuflow.tuflow_logsummary")

from ryan_library.orchestrators.tuflow.tuflow_logsummary import *  # noqa: F401,F403

