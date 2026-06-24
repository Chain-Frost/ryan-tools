"""Compatibility wrapper for the relocated TUFLOW culvert timeseries orchestrator."""

from ryan_library.scripts._compat import warn_deprecated

warn_deprecated(__name__, "ryan_library.orchestrators.tuflow.tuflow_culverts_timeseries")

from ryan_library.orchestrators.tuflow.tuflow_culverts_timeseries import *  # noqa: F401,F403

