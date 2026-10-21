"""Focused PyTUFLOW 1.2 typing surface used by the TUFLOW preflight.

PyTUFLOW does not currently publish PEP 561 typing metadata. Keep this local
stub deliberately narrow and aligned with the upstream public methods used
here. These annotations use Python 3.12-compatible syntax.
"""

from os import PathLike
from pathlib import Path

from pandas import DataFrame


class _ResolvedInput:
    @property
    def files(self) -> list[Path]: ...

    @property
    def has_missing_files(self) -> bool: ...


class _BoundaryDatabase:
    @property
    def fpath(self) -> Path: ...

    @property
    def df(self) -> DataFrame: ...


class _TCFRunState:
    def find_input(self, filter_by: str | None = ..., lhs: str | None = ...) -> list[_ResolvedInput]: ...

    def bc_dbase(self) -> _BoundaryDatabase: ...


class TCF:
    def __init__(self, fpath: str | PathLike[str] | None = ..., **kwargs: object) -> None: ...

    def context(self, run_context: str | dict[str, str] = ...) -> _TCFRunState: ...
