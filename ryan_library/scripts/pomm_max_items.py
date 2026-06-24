# ryan_library/scripts/pomm_max_items.py
"""Compatibility wrapper that delegates to the TUFLOW POMM orchestrator.

This shim keeps older imports working now that the implementation lives under
``ryan_library.orchestrators.tuflow.pomm_max_items`` with the other TUFLOW orchestrators.
"""

from ryan_library.scripts._compat import warn_deprecated

warn_deprecated(__name__, "ryan_library.orchestrators.tuflow.pomm_max_items")

from ryan_library.orchestrators.tuflow.pomm_max_items import *  # noqa: F401,F403
