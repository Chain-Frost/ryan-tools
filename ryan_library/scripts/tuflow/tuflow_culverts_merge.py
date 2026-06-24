"""Compatibility wrapper for the relocated TUFLOW culvert merge orchestrator."""

from ryan_library.scripts._compat import warn_deprecated

warn_deprecated(__name__, "ryan_library.orchestrators.tuflow.tuflow_culverts_merge")

from ryan_library.orchestrators.tuflow.tuflow_culverts_merge import *  # noqa: F401,F403

