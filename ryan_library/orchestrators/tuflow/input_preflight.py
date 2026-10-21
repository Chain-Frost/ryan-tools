"""Read-only preflight of TUFLOW input files for exact proposed run combinations.

Uses PyTUFLOW's scenario-aware run state, and validates all resolved input
references plus source files in the active BC database. Never runs TUFLOW.
"""

from __future__ import annotations

import csv
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Protocol, cast

import pandas as pd
from loguru import logger

from ...functions.tuflow.dependency_index import check_unique_dependencies

IssueStatus = Literal["missing", "unresolved"]


class _Input(Protocol):
    """Minimal read-only input surface used from PyTUFLOW."""

    @property
    def files(self) -> list[Path]: ...

    @property
    def has_missing_files(self) -> bool: ...


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


class _Model(Protocol):
    """TUFLOW project control file with a context resolution method."""

    def context(self, flags: str) -> _Context: ...


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


# Output paths are destinations, not source inputs: TUFLOW may create them.
_OUTPUT_COMMANDS = frozenset(
    {
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


def _collect_references(  # noqa: C901 - event-aware collection and BC source traversal
    tcf: Path,
    runs: Sequence[RunRequest],
    model: _Model,
    report: PreflightReport,
) -> tuple[list[_FileReference], dict[Path, set[str]]]:
    """Phase 1: resolve all run-specific inputs without existence checking."""
    references: list[_FileReference] = []
    unique: dict[Path, set[str]] = {}
    seen: set[tuple[int, str, Path, str]] = set()

    def add_issue(run: RunRequest, origin: str, reference: str, detail: str) -> None:
        report.issues.append(PreflightIssue(run.index, run.description, "unresolved", origin, reference, detail))

    def add_file(run: RunRequest, source: object, origin: str, base: Path, *, require_layer: bool = True) -> None:
        raw = str(getattr(source, "dbpath", source)).strip()
        if not raw or raw == "." or "<<" in raw or ">>" in raw or any(char in raw for char in "*?"):
            add_issue(run, origin, raw, "Unexpanded variable, wildcard or unsupported reference")
            return
        path = Path(raw)
        if not path.is_absolute():
            path = base / path
        path = path.absolute()
        layer = str(getattr(source, "lyrname", "") or "") if require_layer and path.suffix.casefold() == ".gpkg" else ""
        key = (run.index, origin, path, layer)
        if key not in seen:
            seen.add(key)
            references.append(_FileReference(run, origin, path, layer))
        unique.setdefault(path, set())
        if layer:
            unique[path].add(layer)

    for run in runs:
        try:
            context = model.context(run.arguments)
        except Exception as exc:  # noqa: BLE001 - parser errors must fail closed
            add_issue(run, "TCF context", str(tcf), f"Could not resolve run: {exc}")
            continue

        try:
            inputs = context.find_input()
            for inp in inputs:
                if _command_name(inp) in _OUTPUT_COMMANDS:
                    continue
                origin = str(inp)
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


def check_simulation_inputs(
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
    report.directories_scanned = inventory.directories_scanned
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
        "Preflight: {} runs, {} unique files, {} scans, {} direct checks, {} issues across {} runs; "
        "PyTUFLOW parse {:.2f}s, contexts {:.2f}s, inventory {:.2f}s",
        report.run_count,
        len(report.checked_paths),
        report.directories_scanned,
        report.individually_checked,
        len(report.issues),
        report.affected_runs,
        report.parse_seconds,
        report.context_seconds,
        report.inventory_seconds,
    )
    return report
