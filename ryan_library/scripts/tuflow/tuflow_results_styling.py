# ryan_library/scripts/tuflow/tuflow_results_styling.py
"""Compatibility wrapper for the relocated TUFLOW results styling orchestrator."""

from ryan_library.scripts._compat import warn_deprecated

warn_deprecated(module_name=__name__, replacement="ryan_library.orchestrators.tuflow.tuflow_results_styling")

from ryan_library.orchestrators.tuflow.tuflow_results_styling import *  # noqa: F401,F403
