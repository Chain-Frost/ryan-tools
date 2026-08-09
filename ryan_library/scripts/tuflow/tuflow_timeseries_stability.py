# ryan_library/scripts/tuflow/tuflow_timeseries_stability.py
"""Compatibility wrapper for the relocated TUFLOW timeseries stability orchestrator."""

from ryan_library.scripts._compat import warn_deprecated

warn_deprecated(module_name=__name__, replacement="ryan_library.orchestrators.tuflow.tuflow_timeseries_stability")

from ryan_library.orchestrators.tuflow.tuflow_timeseries_stability import *  # noqa: F401,F403
