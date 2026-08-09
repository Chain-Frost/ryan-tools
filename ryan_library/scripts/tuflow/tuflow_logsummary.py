# ryan_library/scripts/tuflow/tuflow_logsummary.py
"""Compatibility wrapper for the relocated TUFLOW log summary orchestrator."""

from ryan_library.scripts._compat import warn_deprecated

warn_deprecated(module_name=__name__, replacement="ryan_library.orchestrators.tuflow.tuflow_logsummary")

from ryan_library.orchestrators.tuflow.tuflow_logsummary import *  # noqa: F401,F403
