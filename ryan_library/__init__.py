

from importlib import import_module
from types import ModuleType

_SCRIPT_MODULES: frozenset[str] = frozenset(
    {
        "tuflow_culverts_merge",
        "tuflow_culverts_timeseries",
        "tuflow_logsummary",
        "tuflow_results_styling",
        "pomm_combine",
        "closure_durations",
    }
)


def __getattr__(name: str) -> ModuleType:
    """Load deprecated script aliases only when legacy package-level access is requested."""
    if name not in _SCRIPT_MODULES:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module: ModuleType = import_module(f"{__name__}.scripts.tuflow.{name}")
    globals()[name] = module
    return module
