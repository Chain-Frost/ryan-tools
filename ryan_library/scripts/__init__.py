# ryan_library/scripts/__init__.py
"""Lazy compatibility imports for script entry points."""

from importlib import import_module
from types import ModuleType
import sys

_TUFLOW_MODULES: frozenset[str] = frozenset(
    {
        "tuflow_culverts_merge",
        "tuflow_culverts_timeseries",
        "tuflow_logsummary",
        "tuflow_results_styling",
        "pomm_combine",
        "closure_durations",
    }
)

__all__: list[str] = []


def __getattr__(name: str) -> ModuleType:
    """Load relocated script modules only when a legacy alias is requested."""
    if name not in _TUFLOW_MODULES:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module: ModuleType = import_module(f"{__name__}.tuflow.{name}")
    sys.modules[f"{__name__}.{name}"] = module
    globals()[name] = module
    return module
