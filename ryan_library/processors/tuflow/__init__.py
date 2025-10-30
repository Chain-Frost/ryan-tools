"""TUFLOW processor package."""

from importlib import import_module

__all__: list[str] = []


def _expose(module_name: str, *processor_names: str) -> None:
    """Expose processors from ``module_name`` on the package namespace."""

    module = import_module(f".{module_name}", __name__)
    for name in processor_names:
        globals()[name] = getattr(module, name)
        __all__.append(name)


_expose("1d_maximums", "CmxProcessor", "NmxProcessor")
_expose("1d_timeseries", "CFProcessor", "QProcessor", "VProcessor")
