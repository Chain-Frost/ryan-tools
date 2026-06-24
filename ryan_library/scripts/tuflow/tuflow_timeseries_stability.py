"""Compatibility wrapper for the relocated TUFLOW timeseries stability orchestrator."""

from ryan_library.scripts._compat import warn_deprecated

warn_deprecated(__name__, "ryan_library.orchestrators.tuflow.tuflow_timeseries_stability")

from ryan_library.orchestrators.tuflow.tuflow_timeseries_stability import *  # noqa: F401,F403

