"""Smoke tests for the deprecated library script namespace."""

import importlib
import sys
from types import ModuleType

import pytest


@pytest.mark.parametrize(
    ("legacy_name", "active_name", "public_name"),
    (
        ("ryan_library.scripts.wrapper_utils", "ryan_library.functions.wrapper_utils", "pause_console"),
        (
            "ryan_library.scripts.pomm_max_items",
            "ryan_library.orchestrators.tuflow.pomm_max_items",
            "export_mean_peak_report",
        ),
        (
            "ryan_library.scripts.tuflow.po_combine",
            "ryan_library.orchestrators.tuflow.po_combine",
            "main_processing",
        ),
        (
            "ryan_library.scripts.RORB.closure_durations",
            "ryan_library.orchestrators.rorb.closure_durations",
            "run_closure_durations",
        ),
    ),
)
def test_compatibility_wrapper_warns_and_reexports(
    legacy_name: str,
    active_name: str,
    public_name: str,
) -> None:
    """Each shim should warn and expose the active implementation unchanged."""
    active_module: ModuleType = importlib.import_module(active_name)
    sys.modules.pop(legacy_name, None)

    with pytest.warns(DeprecationWarning, match=f"{legacy_name} is deprecated"):
        legacy_module: ModuleType = importlib.import_module(legacy_name)

    assert getattr(legacy_module, public_name) is getattr(active_module, public_name)
