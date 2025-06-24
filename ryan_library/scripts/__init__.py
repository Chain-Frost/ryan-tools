"""Convenience imports for script entry points."""

from importlib import import_module
import sys

# Expose relocated TUFLOW scripts at this package level for backward compatibility
_tuflow_modules = [
    "tuflow_culverts_merge",
    "tuflow_culverts_timeseries",
    "tuflow_logsummary",
    "tuflow_results_styling",
]

for _mod in _tuflow_modules:
    module = import_module(f"{__name__}.tuflow.{_mod}")
    sys.modules[f"{__name__}.{_mod}"] = module
    globals()[_mod] = module

__all__ = list(_tuflow_modules)
