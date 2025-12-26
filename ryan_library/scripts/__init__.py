"""Compatibility imports for legacy script entry points."""

from __future__ import annotations

import sys

from ryan_library.orchestrators import gdal as gdal
from ryan_library.orchestrators import rorb as rorb
from ryan_library.orchestrators.rorb import closure_durations as rorb_closure_durations
from ryan_library.orchestrators import tuflow as tuflow
from ryan_library.orchestrators.tuflow import closure_durations as closure_durations
from ryan_library.orchestrators.tuflow import pomm_combine as pomm_combine
from ryan_library.orchestrators.tuflow import tuflow_culverts_merge as tuflow_culverts_merge
from ryan_library.orchestrators.tuflow import tuflow_culverts_timeseries as tuflow_culverts_timeseries
from ryan_library.orchestrators.tuflow import tuflow_logsummary as tuflow_logsummary
from ryan_library.orchestrators.tuflow import tuflow_results_styling as tuflow_results_styling
from ryan_library.scripts._compat import warn_deprecated

warn_deprecated("ryan_library.scripts", "ryan_library.orchestrators")

RORB = rorb

sys.modules[f"{__name__}.gdal"] = gdal
sys.modules[f"{__name__}.tuflow"] = tuflow
sys.modules[f"{__name__}.rorb"] = rorb
sys.modules[f"{__name__}.RORB"] = rorb
sys.modules[f"{__name__}.RORB.closure_durations"] = rorb_closure_durations

sys.modules[f"{__name__}.tuflow_culverts_merge"] = tuflow_culverts_merge
sys.modules[f"{__name__}.tuflow_culverts_timeseries"] = tuflow_culverts_timeseries
sys.modules[f"{__name__}.tuflow_logsummary"] = tuflow_logsummary
sys.modules[f"{__name__}.tuflow_results_styling"] = tuflow_results_styling
sys.modules[f"{__name__}.pomm_combine"] = pomm_combine
sys.modules[f"{__name__}.closure_durations"] = closure_durations

__all__ = [
    "gdal",
    "tuflow",
    "rorb",
    "RORB",
    "tuflow_culverts_merge",
    "tuflow_culverts_timeseries",
    "tuflow_logsummary",
    "tuflow_results_styling",
    "pomm_combine",
    "closure_durations",
]
