"""Third-party numerical implementations bundled with ryan-tools.

The historical `from vendor import HMA` import remains available lazily:
accessing unrelated vendor modules does not import PyHMA dependencies.
"""

__all__ = ["HMA"]


def __getattr__(name: str) -> object:
    """Lazily supply the historical top-level HMA class."""
    if name == "HMA":
        from .PyHMA import HMA

        return HMA
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
