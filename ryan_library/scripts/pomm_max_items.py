# ryan_library/scripts/pomm_max_items.py
"""Compatibility wrapper that delegates to the TUFLOW POMM orchestrator.

This shim keeps older imports working now that the implementation lives under
``ryan_library.scripts.tuflow.pomm_max_items`` with the other TUFLOW orchestrators.
"""

from ryan_library.scripts.tuflow.pomm_max_items import *  # noqa: F401,F403
