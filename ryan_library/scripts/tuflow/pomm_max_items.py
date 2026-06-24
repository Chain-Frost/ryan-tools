"""Compatibility wrapper for the relocated TUFLOW POMM peak report orchestrator."""

from ryan_library.scripts._compat import warn_deprecated

warn_deprecated(__name__, "ryan_library.orchestrators.tuflow.pomm_max_items")

from ryan_library.orchestrators.tuflow.pomm_max_items import *  # noqa: F401,F403

