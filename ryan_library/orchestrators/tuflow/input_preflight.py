"""Read-only preflight of TUFLOW input files for exact proposed run combinations.

Uses PyTUFLOW's scenario-aware run state, and validates all resolved input
references plus source files in the active BC database. Never runs TUFLOW.
"""

from __future__ import annotations

import csv
import inspect
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Protocol, cast

import pandas as pd
from loguru import logger

from ...functions.tuflow.dependency_index import check_unique_dependencies, expand_file_patterns
from ...functions.tuflow.gis_attribute_dependencies import (
    inspect_gis_attribute_sources,
    read_bc_inline_constant_names,
    read_pit_inlet_database,
)
from ...functions.tuflow.raster_metadata_dependencies import inspect_raster_metadata

IssueStatus = Literal["missing", "unresolved"]


class _DeferredReference(Protocol):
    """Syntax-only file dependency returned by the PyTUFLOW deferred API."""

    @property
    def path_pattern(self) -> Path: ...

    @property
    def base_directory(self) -> Path: ...

    @property
    def layer_pattern(self) -> str | None: ...

    @property
    def origin(self) -> str: ...

    @property
    def unresolved_variables(self) -> tuple[str, ...]: ...

    @property
    def requires_attribute_scan(self) -> bool: ...

    @property
    def requires_database_scan(self) -> bool: ...

    @property
    def database_kind(self) -> str | None: ...

    @property
    def requires_table_link_scan(self) -> bool: ...

    @property
    def requires_raster_metadata_scan(self) -> bool: ...


class _DeferredDatabaseSource(Protocol):
    """A secondary source resolved by PyTUFLOW's cached database parser."""

    @property
    def database_path(self) -> Path: ...

    @property
    def database_kind(self) -> str: ...

    @property
    def entry_name(self) -> str: ...

    @property
    def path_pattern(self) -> Path: ...

    @property
    def owning_command(self) -> str: ...

    @property
    def owner_control_file(self) -> Path | None: ...

    @property
    def layer_pattern(self) -> str | None: ...

    @property
    def requires_raster_metadata_scan(self) -> bool: ...


class _Input(Protocol):
    """Minimal read-only input surface used from PyTUFLOW."""

    @property
    def files(self) -> list[Path]: ...

    @property
    def has_missing_files(self) -> bool: ...

    @property
    def file_references(self) -> tuple[_DeferredReference, ...]: ...


class _Database(Protocol):
    """Resolved PyTUFLOW BC database exposed through its pandas representation."""

    @property
    def fpath(self) -> Path: ...

    @property
    def df(self) -> pd.DataFrame: ...


class _Context(Protocol):
    """Scenario/event-resolved PyTUFLOW control-file context."""

    def find_input(self, lhs: str | None = None) -> list[_Input]: ...

    def bc_dbase(self) -> _Database | None: ...

    def deferred_database_sources(self) -> tuple[_DeferredDatabaseSource, ...]: ...

    def database_sources(self) -> tuple[_DeferredDatabaseSource, ...]: ...


class _Model(Protocol):
    """TUFLOW project control file with a context resolution method."""

    def context(self, flags: str, *, file_resolution: str | None = None) -> _Context: ...


class _ModelFactory(Protocol):
    """Constructor returned dynamically by the PyTUFLOW package."""

    def __call__(self, path: str) -> _Model: ...


@dataclass(frozen=True, slots=True)
class RunRequest:
    """One proposed TUFLOW run, matching the launcher's event/scenario flags."""

    index: int
    flags: Mapping[str, str]

    @property
    def arguments(self) -> str:
        """Return PyTUFLOW's documented command-line context syntax."""
        return " ".join(f"-{key} {value}" for key, value in self.flags.items())

    @property
    def description(self) -> str:
        """Display all switch values in a compact, stable order."""
        return " ".join(f"-{key} {value}" for key, value in sorted(self.flags.items()))


@dataclass(frozen=True, slots=True)
class PreflightIssue:
    """Missing or unresolved dependency affecting one simulation."""

    run_index: int
    flags: str
    status: IssueStatus
    origin: str
    reference: str
    detail: str


@dataclass(slots=True)
class PreflightReport:
    """Collected input issues and unique path checks for a set of simulations."""

    run_count: int
    issues: list[PreflightIssue] = field(default_factory=list[PreflightIssue])
    checked_paths: set[str] = field(default_factory=set[str])
    directories_scanned: int = 0
    individually_checked: int = 0
    attribute_databases_scanned: int = 0
    attribute_seconds: float = 0.0
    raster_files_scanned: int = 0
    raster_seconds: float = 0.0
    # Run/activation-scoped BC Source rows, populated from PyTUFLOW's cached tables.
    bc_activation_sources: dict[tuple[int, str], dict[str, Path]] = field(default_factory=dict)
    bc_activation_paths: dict[tuple[int, str], Path] = field(default_factory=dict)
    parse_seconds: float = 0.0
    context_seconds: float = 0.0
    inventory_seconds: float = 0.0

    @property
    def passed(self) -> bool:
        """Only succeed when there are no missing or unresolved inputs."""
        return bool(self.run_count) and not self.issues

    @property
    def affected_runs(self) -> int:
        """Count distinct proposed simulations with at least one issue."""
        return len({issue.run_index for issue in self.issues})

    def write_csv(self, destination: Path) -> Path:
        """Export a repeatable, non-destructive issue register."""
        destination = destination.absolute()
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("w", newline="", encoding="utf-8-sig") as output:
            writer = csv.writer(output)
            writer.writerow(("run_index", "flags", "status", "origin", "reference", "detail"))
            for issue in self.issues:
                writer.writerow(
                    (issue.run_index, issue.flags, issue.status, issue.origin, issue.reference, issue.detail)
                )
        return destination


@dataclass(frozen=True, slots=True)
class _FileReference:
    """A required path and the particular simulation/command that needs it."""

    run: RunRequest
    origin: str
    path: Path
    layer: str = ""
    scan_attributes: bool = False
    database_kind: str = ""
    scan_raster_metadata: bool = False
    scan_table_links: bool = False
    bc_activation_uuid: str = ""


# Commands whose resolved values are not source-file dependencies.
_NON_INPUT_FILE_COMMANDS = frozenset(
    {
        # Event-source commands assign event values to variables. PyTUFLOW
        # currently exposes pipe-delimited assignments through ``files`` as a
        # single pseudo-path, but neither side is an input file.
        "bc event source",
        "output folder",
        "log folder",
        "write check files",
        "check files output folder",
        "1d output folder",
        "2d output folder",
        "output folder 1d",
        "output folder 2d",
    }
)


def _command_name(input_command: object) -> str:
    """Prefer PyTUFLOW's LHS, with a conservative fallback for test doubles."""
    lhs = getattr(input_command, "lhs", None)
    if lhs is None:
        lhs = str(input_command).split("==", maxsplit=1)[0]
    return " ".join(str(lhs).casefold().split())


def _is_non_input_command(input_command: object) -> bool:
    """Assignment RHS strings are not inputs even when they resemble files."""
    name = _command_name(input_command)
    return name in _NON_INPUT_FILE_COMMANDS or name.startswith("set variable ")


class _BCActivation(Protocol):
    sequence: int
    input_uuid: str
    database_kind: str
    database_path: Path


def _bc_source_command(name: str) -> bool:
    """Recognised boundary GIS commands whose Name selects an active BC row."""
    return name in {"read gis bc", "read gis sa", "read gis rf"} or name.startswith(
        ("read gis bc ", "read gis sa ", "read gis rf ")
    )


def _selected_bc_uuid(position: int, activations: Sequence[_BCActivation]) -> str:
    """Find the last applicable, scope-filtered declaration in command order."""
    eligible = [a for a in activations if a.database_kind == "bc" and a.sequence < position]
    if eligible:
        return max(eligible, key=lambda a: a.sequence).input_uuid
    message = "Read GIS BC/SA/RF has no unambiguous active BC Database declaration"
    raise ValueError(message)


def _collect_references(  # noqa: C901 - event-aware collection and BC source traversal
    tcf: Path,
    runs: Sequence[RunRequest],
    model: _Model,
    report: PreflightReport,
) -> tuple[list[_FileReference], dict[Path, set[str]]]:
    """Phase 1: resolve all run-specific inputs without existence checking."""
    references: list[_FileReference] = []
    unique: dict[Path, set[str]] = {}
    seen: set[tuple[int, str, Path, str, bool, str, bool, bool, str]] = set()

    def add_issue(run: RunRequest, origin: str, reference: str, detail: str) -> None:
        report.issues.append(PreflightIssue(run.index, run.description, "unresolved", origin, reference, detail))

    def add_file(
        run: RunRequest,
        source: object,
        origin: str,
        base: Path,
        *,
        require_layer: bool = True,
        layer_override: str | None = None,
        allow_pattern: bool = False,
        scan_attributes: bool = False,
        database_kind: str = "",
        scan_raster_metadata: bool = False,
        scan_table_links: bool = False,
        bc_activation_uuid: str = "",
    ) -> None:
        raw = str(getattr(source, "dbpath", source)).strip()
        if (
            not raw
            or raw == "."
            or "<<" in raw
            or ">>" in raw
            or (not allow_pattern and any(char in Path(raw).name for char in "*?["))
        ):
            add_issue(run, origin, raw, "Unexpanded variable, wildcard or unsupported reference")
            return
        path = Path(raw)
        if not path.is_absolute():
            path = base / path
        path = path.absolute()
        layer = str(getattr(source, "lyrname", "") or "") if require_layer and path.suffix.casefold() == ".gpkg" else ""
        if layer_override is not None and path.suffix.casefold() == ".gpkg":
            layer = layer_override
        key = (
            run.index,
            origin,
            path,
            layer,
            scan_attributes,
            database_kind,
            scan_raster_metadata,
            scan_table_links,
            bc_activation_uuid,
        )
        if key not in seen:
            seen.add(key)
            references.append(
                _FileReference(
                    run,
                    origin,
                    path,
                    layer,
                    scan_attributes,
                    database_kind,
                    scan_raster_metadata,
                    scan_table_links,
                    bc_activation_uuid,
                )
            )
        if not any(char in path.name for char in "*?["):
            unique.setdefault(path, set())
            if layer:
                unique[path].add(layer)

    # Older PyPI builds have no deferred API. Keep the existing eager collector
    # until the validated upstream feature is released; never probe support by
    # calling context() and catching a parser TypeError.
    try:
        deferred_supported = "file_resolution" in inspect.signature(model.context).parameters
    except (TypeError, ValueError):
        deferred_supported = False
    if not deferred_supported:
        logger.warning("Installed PyTUFLOW has no deferred API; falling back to eager reference discovery")

    for run in runs:
        try:
            context = (
                model.context(run.arguments, file_resolution="deferred")
                if deferred_supported
                else model.context(run.arguments)
            )
        except Exception as exc:  # noqa: BLE001 - parser errors must fail closed
            add_issue(run, "TCF context", str(tcf), f"Could not resolve run: {exc}")
            continue

        eager_sources_available = not deferred_supported and callable(getattr(context, "database_sources", None))
        try:
            inputs = context.find_input()
            # Only multi-declaration hydraulic BC models need feature-specific
            # binding. Existing single-database eager/deferred inventories
            # remain unchanged, preserving the established 174-run baseline.
            declarations = [(i, inp) for i, inp in enumerate(inputs) if _command_name(inp) == "bc database"]
            multi_bc = deferred_supported and len(declarations) > 1
            bc_activations: tuple[_BCActivation, ...] = ()
            if multi_bc:
                activation_reader = getattr(context, "deferred_database_activations", None)
                source_reader_for_activation = getattr(context, "deferred_database_sources_for_activation", None)
                if not callable(activation_reader) or not callable(source_reader_for_activation):
                    message = "BC declaration selection requires PyTUFLOW activation APIs"
                    raise ValueError(message)
                bc_activations = tuple(activation_reader())
                declared = {(position, str(inp.uuid)) for position, inp in declarations}
                actual = {
                    (a.sequence, a.input_uuid) for a in bc_activations if getattr(a, "database_kind", None) == "bc"
                }
                if actual != declared:
                    message = "BC database declaration sequence/UUID mismatch"
                    raise ValueError(message)
                for _, declaration in declarations:
                    uuid = str(declaration.uuid)
                    by_name: dict[str, Path] = {}
                    for source in source_reader_for_activation(uuid):
                        if source.database_kind != "bc" or not source.entry_name.strip():
                            message = "Unclassified BC database Source row"
                            raise ValueError(message)
                        key = source.entry_name.strip().casefold()
                        if key in by_name:
                            message = f"Duplicate BC identifier {source.entry_name!r}"
                            raise ValueError(message)
                        by_name[key] = Path(source.path_pattern).absolute()
                    report.bc_activation_sources[(run.index, uuid)] = by_name
                    active = next(a for a in bc_activations if a.database_kind == "bc" and a.input_uuid == uuid)
                    report.bc_activation_paths[(run.index, uuid)] = active.database_path
            for position, inp in enumerate(inputs):
                if _is_non_input_command(inp):
                    continue
                origin = str(inp)
                name = _command_name(inp)
                bc_uuid = ""
                if multi_bc and _bc_source_command(name):
                    try:
                        bc_uuid = _selected_bc_uuid(position, bc_activations)
                    except ValueError as exc:
                        add_issue(run, origin, str(tcf), str(exc))
                if deferred_supported:
                    # The provisional PyTUFLOW database API owns BC and
                    # material row classification and event substitutions.
                    # Older fork builds remain unresolved rather than being
                    # guessed by this consumer.
                    database_sources_available = callable(getattr(context, "deferred_database_sources", None))
                    try:
                        for ref in inp.file_references:
                            if ref.unresolved_variables:
                                add_issue(
                                    run,
                                    ref.origin,
                                    str(ref.path_pattern),
                                    f"Unresolved PyTUFLOW variables: {', '.join(ref.unresolved_variables)}",
                                )
                                continue
                            kind = getattr(ref, "database_kind", None)
                            if kind is None:
                                # Compatibility with earlier fork builds whose
                                # BC/material references predate the typed field.
                                kind = {
                                    "bc database": "bc",
                                    "read materials file": "materials",
                                }.get(_command_name(inp))
                            supported_kinds = {
                                "bc",
                                "materials",
                                "soils",
                                "ad_bc",
                                "ad_global",
                                "pit_inlet",
                                "depth_discharge",
                                "cross_section",
                                "rainfall",
                            }
                            if ref.requires_database_scan and not (
                                database_sources_available and kind in supported_kinds
                            ):
                                add_issue(
                                    run,
                                    ref.origin,
                                    str(ref.path_pattern),
                                    f"Unsupported deferred database materialisation: {kind or 'unknown'}",
                                )
                            if getattr(ref, "requires_table_link_scan", False) and not ref.requires_attribute_scan:
                                add_issue(
                                    run,
                                    ref.origin,
                                    str(ref.path_pattern),
                                    "GIS table-link attributes cannot be inspected using this reference",
                                )
                            add_file(
                                run,
                                ref.path_pattern,
                                ref.origin or origin,
                                ref.base_directory,
                                layer_override=ref.layer_pattern or "",
                                allow_pattern=True,
                                scan_attributes=ref.requires_attribute_scan or bool(bc_uuid),
                                database_kind=kind or "",
                                scan_raster_metadata=getattr(ref, "requires_raster_metadata_scan", False),
                                scan_table_links=getattr(ref, "requires_table_link_scan", False),
                                bc_activation_uuid=bc_uuid,
                            )
                    except Exception as exc:  # noqa: BLE001 - incomplete references must fail closed
                        add_issue(run, origin, "", f"Could not collect deferred file references: {exc}")
                    continue
                if eager_sources_available:
                    # Eager .files does not include database-key curve sources.
                    # Use upstream syntax metadata to request the same shared
                    # active-feature join used by deferred collection.
                    try:
                        for ref in inp.file_references:
                            if not ref.requires_attribute_scan and ref.database_kind not in {
                                "pit_inlet",
                                "depth_discharge",
                            }:
                                continue
                            if ref.unresolved_variables:
                                add_issue(run, ref.origin, str(ref.path_pattern), "Unresolved eager input metadata")
                                continue
                            add_file(
                                run,
                                ref.path_pattern,
                                ref.origin or origin,
                                ref.base_directory,
                                layer_override=ref.layer_pattern or "",
                                allow_pattern=True,
                                scan_attributes=ref.requires_attribute_scan,
                                database_kind=ref.database_kind or "",
                                scan_table_links=ref.requires_table_link_scan,
                            )
                    except Exception as exc:  # noqa: BLE001 - never certify partial metadata
                        add_issue(run, origin, "", f"Could not collect eager secondary metadata: {exc}")
                elif _command_name(inp) in {
                    "ad bc database",
                    "ad global database",
                    "pit inlet database",
                    "depth discharge database",
                }:
                    add_issue(run, origin, "", "PyTUFLOW typed secondary-source API is unavailable in eager mode")
                try:
                    files = list(inp.files)
                except Exception as exc:  # noqa: BLE001 - unknown PyTUFLOW errors fail closed
                    add_issue(run, origin, "", f"Could not enumerate input files: {exc}")
                    continue
                parent = getattr(inp, "parent", None)
                owning_file = getattr(parent, "fpath", None)
                base = Path(owning_file).parent if owning_file else tcf.parent
                for file in files:
                    add_file(run, file, origin, base)
                # Avoid triggering PyTUFLOW's file checks for every run and
                # every file. Only query its missing-file property for inputs
                # where no path was exposed.
                if not files:
                    try:
                        if inp.has_missing_files:
                            add_issue(run, origin, "", "Input has missing dependencies with no resolved file path")
                    except Exception as exc:  # noqa: BLE001 - cannot certify missing references
                        add_issue(run, origin, "", f"Could not inspect unresolved input: {exc}")
        except Exception as exc:  # noqa: BLE001 - parser traversal errors fail closed
            add_issue(run, "Control-file traversal", str(tcf), f"Could not read model inputs: {exc}")

        if deferred_supported or eager_sources_available:
            # Only invoke this deliberate database materialisation stage after
            # collecting the syntactic references for this specific run.
            source_reader = getattr(
                context, "deferred_database_sources" if deferred_supported else "database_sources", None
            )
            if callable(source_reader):
                try:
                    if multi_bc:
                        selected_reader = context.deferred_database_sources_for_activation
                        # No aggregate view: it deliberately rejects multiple
                        # active BC declarations. Candidate BC rows were staged
                        # above and are selected from the scanned GIS Names.
                        sources = tuple(
                            source
                            for activation in bc_activations
                            if activation.database_kind != "bc"
                            for source in selected_reader(activation.input_uuid)
                        )
                    else:
                        sources = (
                            context.deferred_database_sources() if deferred_supported else context.database_sources()
                        )
                    for source in sources:
                        kind = source.database_kind
                        if kind not in {
                            "bc",
                            "materials",
                            "soils",
                            "ad_bc",
                            "ad_global",
                            "pit_inlet",
                            "depth_discharge",
                            "cross_section",
                            "rainfall",
                        }:
                            message = f"Unsupported deferred database kind: {kind}"
                            raise ValueError(message)
                        if kind == "soils":
                            message = "Unexpected file source from inline soils database"
                            raise ValueError(message)
                        if kind in {"pit_inlet", "depth_discharge"}:
                            # These cached database rows describe *possible*
                            # curves, not files required by every simulation.
                            # Only the identifiers found in active GIS features
                            # are materialised by _materialise_attribute_references.
                            # Adding all rows here generates false dependencies
                            # for unused inlet and depth/discharge curves.
                            continue
                        owner_file: Path | None = getattr(source, "owner_control_file", None)
                        # Eager rainfall grid entries belong to the owning
                        # Read Grid RF command, not to an invented command
                        # named after each time-index row. Keep the row name
                        # for the database kinds whose eager API exposes it.
                        origin = (
                            getattr(source, "owning_command", "") or "Read Grid RF"
                            if kind == "rainfall"
                            else f"{kind} source: {source.entry_name}"
                        )
                        add_file(
                            run,
                            source.path_pattern,
                            origin,
                            owner_file.parent if owner_file is not None else source.database_path.parent,
                            require_layer=False,
                            layer_override=getattr(source, "layer_pattern", None) or "",
                            allow_pattern=True,
                            database_kind=f"source:{kind}",
                            scan_raster_metadata=getattr(source, "requires_raster_metadata_scan", False),
                        )
                except Exception as exc:  # noqa: BLE001 - incomplete databases cannot pass preflight
                    add_issue(run, "Database sources", str(tcf), f"Could not inspect deferred databases: {exc}")
            else:
                # Early versions of the fork defer BC parsing but expose no
                # supported way to enumerate its event-dependent source rows.
                try:
                    if context.find_input(lhs="BC Database"):
                        add_issue(run, "BC Database", str(tcf), "Deferred BC source API is unavailable")
                except Exception as exc:  # noqa: BLE001 - fail closed if presence cannot be established
                    add_issue(run, "BC Database", str(tcf), f"Cannot identify deferred BC database: {exc}")
            continue

        # PyTUFLOW resolves the BC database's event-dependent Source column.
        # Collect the Source values now, but do not stat() the files yet.
        try:
            database = context.bc_dbase()
        except (LookupError, ValueError):
            try:
                has_database = bool(context.find_input(lhs="BC Database"))
            except Exception as exc:  # noqa: BLE001 - cannot establish whether the database is optional
                add_issue(run, "BC Database", str(tcf), f"Cannot determine active BC database: {exc}")
            else:
                if has_database:
                    add_issue(run, "BC Database", str(tcf), "Could not resolve active BC database")
            continue
        except Exception as exc:  # noqa: BLE001 - parser errors must fail closed
            add_issue(run, "BC Database", str(tcf), f"Could not load BC database: {exc}")
            continue

        if database is None:
            continue
        database_path = Path(database.fpath).absolute()
        add_file(run, database_path, "BC Database", tcf.parent, require_layer=False)
        try:
            frame: pd.DataFrame = database.df
            source_column = next((col for col in frame.columns if str(col).strip().casefold() == "source"), None)
            if source_column is None:
                add_issue(run, "BC Database", str(database_path), "No Source column found")
                continue
            for name, row in frame.iterrows():
                raw = str(row[source_column]).strip()
                if raw.casefold() in {"", "nan", "none"}:
                    continue
                try:
                    float(raw)  # Numeric constant, not a file path.
                except ValueError:
                    add_file(run, raw, f"BC source: {name}", database_path.parent, require_layer=False)
        except Exception as exc:  # noqa: BLE001 - source list must be complete
            add_issue(run, "BC Database", str(database_path), f"Could not enumerate BC sources: {exc}")
    return references, unique


def _materialise_attribute_references(  # noqa: C901 - database joins and explicit fail-closed diagnostics
    references: list[_FileReference],
    report: PreflightReport,
    *,
    maximum_directory_entries: int = 50000,
) -> list[_FileReference]:
    """Bulk-inspect GIS attribute sources with full per-run provenance.

    This shared stage is used by normal preflight and temporary differential
    diagnostics. It must never suppress partial inspection failures.
    """
    # Inspect each distinct GIS attribute-bearing layer only once.
    # Read-only bulk phase; invoke only after resolving all run contexts.
    attribute_started = time.perf_counter()
    scan_references = [ref for ref in references if ref.scan_attributes]
    curve_databases: dict[tuple[int, str], set[Path]] = {}
    for ref in references:
        if ref.database_kind in {"pit_inlet", "depth_discharge"}:
            curve_databases.setdefault((ref.run.index, ref.database_kind), set()).add(ref.path)
    pit_database_cache: dict[Path, dict[str, Path] | str] = {}
    bc_constant_cache: dict[Path, frozenset[str] | str] = {}
    if scan_references:
        requests = {(ref.path, ref.layer) for ref in scan_references}
        inspected, scan_count = inspect_gis_attribute_sources(requests)
        report.attribute_databases_scanned = scan_count
        extra_references: list[_FileReference] = []
        for ref in scan_references:
            result = inspected[(ref.path, ref.layer)]
            if result.status != "present":
                report.issues.append(
                    PreflightIssue(
                        ref.run.index,
                        ref.run.description,
                        "unresolved",
                        ref.origin,
                        f"{ref.path} >> {ref.layer}",
                        result.detail,
                    )
                )
                continue
            for path in result.sources:
                extra_references.append(_FileReference(ref.run, f"GIS attribute: {ref.origin}", path))
            if ref.bc_activation_uuid:
                sources = report.bc_activation_sources.get((ref.run.index, ref.bc_activation_uuid))
                if sources is None:
                    report.issues.append(
                        PreflightIssue(
                            ref.run.index,
                            ref.run.description,
                            "unresolved",
                            ref.origin,
                            str(ref.path),
                            "Active BC database source map was not collected",
                        )
                    )
                else:
                    for identifier in result.bc_identifiers:
                        linked = sources.get(identifier.casefold())
                        if linked is None:
                            database_path = report.bc_activation_paths[(ref.run.index, ref.bc_activation_uuid)]
                            if database_path not in bc_constant_cache:
                                try:
                                    bc_constant_cache[database_path] = read_bc_inline_constant_names(database_path)
                                except (OSError, UnicodeError, ValueError) as exc:
                                    bc_constant_cache[database_path] = str(exc)
                            inline_names = bc_constant_cache[database_path]
                            if isinstance(inline_names, frozenset) and identifier.casefold() in inline_names:
                                continue  # Numeric constant: no secondary file to validate.
                            detail = (
                                f"Cannot classify BC constant row: {inline_names}"
                                if isinstance(inline_names, str)
                                else f"Active BC Database has no Source row for {identifier!r}"
                            )
                            report.issues.append(
                                PreflightIssue(
                                    ref.run.index,
                                    ref.run.description,
                                    "unresolved",
                                    ref.origin,
                                    str(ref.path),
                                    detail,
                                )
                            )
                        else:
                            extra_references.append(
                                _FileReference(
                                    ref.run,
                                    f"bc source: {identifier}",
                                    linked,
                                )
                            )
            # Resolve source identifiers using the database type that owns
            # this GIS element, never interpreting a curve ID as a filename.
            for identifiers, database_kind in (
                (result.network_identifiers, "depth_discharge"),
                (result.pit_identifiers, "pit_inlet"),
            ):
                if not identifiers:
                    continue
                available = curve_databases.get((ref.run.index, database_kind), set())
                if len(available) != 1:
                    report.issues.append(
                        PreflightIssue(
                            ref.run.index,
                            ref.run.description,
                            "unresolved",
                            ref.origin,
                            str(ref.path),
                            f"{database_kind} database is absent or ambiguous for GIS curve identifiers",
                        )
                    )
                    continue
                database_path = next(iter(available))
                if database_path not in pit_database_cache:
                    try:
                        pit_database_cache[database_path] = read_pit_inlet_database(database_path)
                    except (OSError, UnicodeError, ValueError) as exc:
                        pit_database_cache[database_path] = str(exc)
                mapping = pit_database_cache[database_path]
                if isinstance(mapping, str):
                    report.issues.append(
                        PreflightIssue(
                            ref.run.index,
                            ref.run.description,
                            "unresolved",
                            ref.origin,
                            str(database_path),
                            f"Cannot resolve {database_kind} database: {mapping}",
                        )
                    )
                    continue
                for identifier in identifiers:
                    linked = mapping.get(identifier.casefold())
                    if linked is None:
                        report.issues.append(
                            PreflightIssue(
                                ref.run.index,
                                ref.run.description,
                                "unresolved",
                                ref.origin,
                                str(database_path),
                                f"{database_kind} database has no entry for {identifier!r}",
                            )
                        )
                        continue
                    extra_references.append(_FileReference(ref.run, f"{database_kind} curve: {identifier}", linked))

        # File-valued GIS attributes may themselves contain wildcard paths.
        # Expand those patterns once across all simulations, not once per run.
        extra_patterns = {ref.path for ref in extra_references if any(char in ref.path.name for char in "*?[")}
        if extra_patterns:
            expanded, failures, extra_scans = expand_file_patterns(
                extra_patterns, maximum_directory_entries=maximum_directory_entries
            )
            report.directories_scanned += extra_scans
            extra_materialised: list[_FileReference] = []
            for ref in extra_references:
                if ref.path not in extra_patterns:
                    extra_materialised.append(ref)
                    continue
                if ref.path in failures:
                    problem = failures[ref.path]
                    report.issues.append(
                        PreflightIssue(
                            ref.run.index,
                            ref.run.description,
                            "missing" if problem.status == "missing" else "unresolved",
                            ref.origin,
                            str(ref.path),
                            problem.detail,
                        )
                    )
                    continue
                extra_materialised.extend(_FileReference(ref.run, ref.origin, path) for path in expanded[ref.path])
            extra_references = extra_materialised

        distinct_extra = {(ref.run.index, ref.origin, ref.path, ref.layer): ref for ref in extra_references}
        references.extend(distinct_extra.values())
    report.attribute_seconds = time.perf_counter() - attribute_started
    return references


def check_simulation_inputs(  # noqa: C901 - wildcard materialisation and run-level result routing
    tcf: Path,
    runs: Sequence[RunRequest],
    *,
    check_gpkg_layers: bool = True,
    directory_scan_threshold: int = 8,
    maximum_directory_entries: int = 50000,
) -> PreflightReport:
    """Resolve all dependencies first, then check each unique file only once.

    Dense input directories are listed once (bounded to avoid huge network
    scans); sparse directories use direct metadata checks. Filesystem results
    are then mapped back onto every run that needs each file. Output locations
    are not treated as required inputs. The checker does not launch TUFLOW.
    """
    if not tcf.is_file():
        message = f"TUFLOW control file not found: {tcf}"
        raise FileNotFoundError(message)
    if not runs:
        message = "No simulations were supplied for input preflight"
        raise ValueError(message)
    # Import only on demand; PyTUFLOW is a required ryan-tools dependency.
    from pytuflow import TCF

    factory = cast(_ModelFactory, TCF)
    report = PreflightReport(run_count=len(runs))
    start = time.perf_counter()
    model: _Model = factory(str(tcf.absolute()))
    report.parse_seconds = time.perf_counter() - start

    # Phase 1: PyTUFLOW run-state resolution; this can itself access files.
    # Time it separately from the explicit deduplicated filesystem inventory.
    start = time.perf_counter()
    references, unique_paths = _collect_references(tcf.absolute(), runs, model, report)
    report.context_seconds = time.perf_counter() - start

    # Expand each distinct wildcard pattern once. A missing, oversized or
    # unsupported directory fails only the runs that reference that pattern.
    patterns = {ref.path for ref in references if any(char in ref.path.name for char in "*?[")}
    if patterns:
        expanded, failures, wildcard_scans = expand_file_patterns(
            patterns, maximum_directory_entries=maximum_directory_entries
        )
        report.directories_scanned += wildcard_scans
        materialised: list[_FileReference] = []
        for ref in references:
            if ref.path not in patterns:
                materialised.append(ref)
                continue
            if ref.path in failures:
                failure = failures[ref.path]
                status: IssueStatus = "missing" if failure.status == "missing" else "unresolved"
                report.issues.append(
                    PreflightIssue(
                        ref.run.index, ref.run.description, status, ref.origin, str(ref.path), failure.detail
                    )
                )
                continue
            for path in expanded[ref.path]:
                materialised.append(
                    _FileReference(
                        ref.run,
                        ref.origin,
                        path,
                        ref.layer,
                        ref.scan_attributes,
                        ref.database_kind,
                        ref.scan_raster_metadata,
                        ref.scan_table_links,
                    )
                )
                unique_paths.setdefault(path, set())
                if ref.layer:
                    unique_paths[path].add(ref.layer)
        references = materialised

    references = _materialise_attribute_references(
        references, report, maximum_directory_entries=maximum_directory_entries
    )
    for ref in references:
        unique_paths.setdefault(ref.path, set())
        if ref.layer:
            unique_paths[ref.path].add(ref.layer)

    # Validate unique raster source metadata only after primary filesystem checks
    # establish the file exists. Never read raster data in a run context.
    raster_started = time.perf_counter()
    raster_references = [ref for ref in references if ref.scan_raster_metadata]
    if raster_references:
        raster_paths = {ref.path for ref in raster_references}
        metadata, raster_opens = inspect_raster_metadata(raster_paths)
        report.raster_files_scanned = raster_opens
        for ref in raster_references:
            result = metadata[ref.path]
            if result.status != "present":
                report.issues.append(
                    PreflightIssue(
                        ref.run.index,
                        ref.run.description,
                        "unresolved",
                        ref.origin,
                        str(ref.path),
                        result.detail,
                    )
                )
    report.raster_seconds = time.perf_counter() - raster_started

    # Phase 2: stat or list only distinct required paths, with one GeoPackage
    # query per database for all of its requested layers.
    if not check_gpkg_layers:
        unique_paths = {path: set[str]() for path in unique_paths}
    start = time.perf_counter()
    inventory = check_unique_dependencies(
        unique_paths,
        scan_threshold=directory_scan_threshold,
        maximum_directory_entries=maximum_directory_entries,
    )
    report.inventory_seconds = time.perf_counter() - start
    report.checked_paths.update(str(path) for path in unique_paths)
    report.directories_scanned += inventory.directories_scanned
    report.individually_checked = inventory.individually_checked

    # Phase 3: map results back to affected simulations and source commands.
    for reference in references:
        result = inventory.files[reference.path]
        if result.status == "present" and reference.layer and check_gpkg_layers:
            result = inventory.layers.get(
                (reference.path, reference.layer),
                result,
            )
        if result.status != "present":
            name = str(reference.path)
            if reference.layer:
                name = f"{name} >> {reference.layer}"
            report.issues.append(
                PreflightIssue(
                    reference.run.index,
                    reference.run.description,
                    result.status,
                    reference.origin,
                    name,
                    result.detail,
                )
            )

    logger.info(
        "Preflight: {} runs, {} unique files, {} directory scans, {} attribute GIS-source opens, "
        "{} raster metadata opens, {} direct checks, {} issues across {} runs; "
        "PyTUFLOW parse {:.2f}s, contexts {:.2f}s, GIS attributes {:.2f}s, "
        "raster metadata {:.2f}s, inventory {:.2f}s",
        report.run_count,
        len(report.checked_paths),
        report.directories_scanned,
        report.attribute_databases_scanned,
        report.raster_files_scanned,
        report.individually_checked,
        len(report.issues),
        report.affected_runs,
        report.parse_seconds,
        report.context_seconds,
        report.attribute_seconds,
        report.raster_seconds,
        report.inventory_seconds,
    )
    return report
