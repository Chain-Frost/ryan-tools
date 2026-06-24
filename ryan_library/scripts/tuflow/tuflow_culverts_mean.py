"""Compatibility wrapper for the relocated TUFLOW culvert mean orchestrator."""

from ryan_library.scripts._compat import warn_deprecated

warn_deprecated(__name__, "ryan_library.orchestrators.tuflow.tuflow_culverts_mean")

from ryan_library.orchestrators.tuflow.tuflow_culverts_mean import *  # noqa: F401,F403

