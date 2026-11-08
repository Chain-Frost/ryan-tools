# ryan_library/processors/tuflow/__init__.py
"""TUFLOW processor package."""

from importlib import import_module
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .maximums_1d import CmxProcessor, NmxProcessor, ccAProcessor
    from .other_processors import (
        ChanProcessor,
        EOFProcessor,
        LProcessor,
        MBHistoryProcessor,
        MBNodeProcessor,
        POMMProcessor,
        POProcessor,
        RLLQmxProcessor,
        RLLQProcessor,
        RLPHmxProcessor,
        RLPHProcessor,
        RLRVolmxProcessor,
        RLRVolProcessor,
        SHmxProcessor,
        Split2dHProcessor,
        Split2dQProcessor,
        Split2dVProcessor,
        SQProcessor,
        TSMBProcessor,
    )
    from .timeseries_1d import CFProcessor, HProcessor, QProcessor, VProcessor

_PROCESSORS = {
    "ccAProcessor": ".maximums_1d",
    "CmxProcessor": ".maximums_1d",
    "NmxProcessor": ".maximums_1d",
    "CFProcessor": ".timeseries_1d",
    "HProcessor": ".timeseries_1d",
    "QProcessor": ".timeseries_1d",
    "VProcessor": ".timeseries_1d",
    "ChanProcessor": ".other_processors",
    "EOFProcessor": ".other_processors",
    "POMMProcessor": ".other_processors",
    "POProcessor": ".other_processors",
    "RLLQmxProcessor": ".other_processors",
    "SHmxProcessor": ".other_processors",
    "SQProcessor": ".other_processors",
    "MBHistoryProcessor": ".other_processors",
    "MBNodeProcessor": ".other_processors",
    "TSMBProcessor": ".other_processors",
    "Split2dVProcessor": ".other_processors",
    "Split2dQProcessor": ".other_processors",
    "Split2dHProcessor": ".other_processors",
    "LProcessor": ".other_processors",
    "RLRVolmxProcessor": ".other_processors",
    "RLRVolProcessor": ".other_processors",
    "RLLQProcessor": ".other_processors",
    "RLPHProcessor": ".other_processors",
    "RLPHmxProcessor": ".other_processors",
}

__all__ = [
    "CFProcessor",
    "ChanProcessor",
    "CmxProcessor",
    "EOFProcessor",
    "HProcessor",
    "LProcessor",
    "MBHistoryProcessor",
    "MBNodeProcessor",
    "NmxProcessor",
    "POMMProcessor",
    "POProcessor",
    "QProcessor",
    "RLLQProcessor",
    "RLLQmxProcessor",
    "RLPHProcessor",
    "RLPHmxProcessor",
    "RLRVolProcessor",
    "RLRVolmxProcessor",
    "SHmxProcessor",
    "SQProcessor",
    "Split2dHProcessor",
    "Split2dQProcessor",
    "Split2dVProcessor",
    "TSMBProcessor",
    "VProcessor",
    "ccAProcessor",
]


def __getattr__(name: str) -> object:
    if name in _PROCESSORS:
        module = import_module(_PROCESSORS[name], __name__)
        obj = getattr(module, name)
        globals()[name] = obj
        return obj
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
