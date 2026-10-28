"""Focused PyTUFLOW 1.2 typing surface used by the TUFLOW preflight.

PyTUFLOW does not currently publish PEP 561 typing metadata. Keep this local
stub deliberately narrow and aligned with the upstream public methods used
here. These annotations use Python 3.12-compatible syntax.
"""

from os import PathLike
from pathlib import Path

from pandas import DataFrame

class _DeferredFileReference:
    """Subset of the fork's immutable PyTUFLOW deferred-reference contract."""

    base_directory: Path
    expanded_value: str
    path_pattern: Path
    layer_pattern: str | None
    origin: str
    requires_directory_scan: bool
    requires_layer_scan: bool
    requires_attribute_scan: bool
    requires_database_scan: bool
    unresolved_variables: tuple[str, ...]

class _ResolvedInput:
    @property
    def file_references(self) -> tuple[_DeferredFileReference, ...]: ...
    @property
    def files(self) -> list[Path]: ...
    @property
    def has_missing_files(self) -> bool: ...

class _BoundaryDatabase:
    @property
    def fpath(self) -> Path: ...
    @property
    def df(self) -> DataFrame: ...

class _DeferredDatabaseSource:
    database_path: Path
    database_kind: str
    entry_name: str
    path_pattern: Path

class _TCFRunState:
    def database_sources(self) -> tuple[_DeferredDatabaseSource, ...]: ...
    def deferred_database_sources(self) -> tuple[_DeferredDatabaseSource, ...]: ...
    def find_input(self, filter_by: str | None = ..., lhs: str | None = ...) -> list[_ResolvedInput]: ...
    def bc_dbase(self) -> _BoundaryDatabase: ...

class XMDF:
    """Public PyTUFLOW XMDF surface and timestamp extraction API."""

    def __init__(
        self, fpath: Path | str, twodm: Path | str | None = ..., driver: str = ...
    ) -> None: ...
    def data_types(self, filter_by: str | None = ...) -> list[str]: ...
    def times(self, filter_by: str | None = ...) -> list[float]: ...
    def surface(self, data_type: str, time: float) -> DataFrame: ...


class TCF:
    def __init__(self, fpath: str | PathLike[str] | None = ..., **kwargs: object) -> None: ...
    def context(
        self, run_context: str | dict[str, str] = ..., *, file_resolution: str | None = ...
    ) -> _TCFRunState: ...
