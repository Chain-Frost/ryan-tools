"""TUFLOW processor package."""

from importlib import import_module
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .maximums_1d import ccAProcessor, CmxProcessor, NmxProcessor
    from .timeseries_1d import CFProcessor, HProcessor, QProcessor, VProcessor
    from .other_processors import (
        ChanProcessor,
        EOFProcessor,
        POMMProcessor,
        POProcessor,
        RLLQmxProcessor,
    )

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
}

__all__ = [
    "ccAProcessor",
    "CmxProcessor",
    "NmxProcessor",
    "CFProcessor",
    "HProcessor",
    "QProcessor",
    "VProcessor",
    "ChanProcessor",
    "EOFProcessor",
    "POMMProcessor",
    "POProcessor",
    "RLLQmxProcessor",
]


def __getattr__(name: str) -> object:
    if name in _PROCESSORS:
        module = import_module(_PROCESSORS[name], __name__)
        obj = getattr(module, name)
        globals()[name] = obj
        return obj
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
