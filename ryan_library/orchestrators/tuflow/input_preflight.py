"""Read-only preflight of TUFLOW input files for exact proposed run combinations.

Uses PyTUFLOW's scenario-aware run state, and validates all resolved input
references plus source files in the active BC database. Never runs TUFLOW.
"""

from __future__ import annotations

import csv
import sqlite3
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from loguru import logger

IssueStatus = Literal["missing", "unresolved"]


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
    issues: list[PreflightIssue] = field(default_factory=list)
    checked_paths: set[str] = field(default_factory=set)

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


def _database_layer_exists(path: Path, layer: str) -> bool:
    """Check a GeoPackage layer with SQLite without requiring GDAL or QGIS."""
    if not layer:
        return True
    try:
        with sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True) as connection:
            row = connection.execute(
                "SELECT 1 FROM gpkg_contents WHERE lower(table_name) = lower(?)",
                (layer,),
            ).fetchone()
        return row is not None
    except (sqlite3.DatabaseError, OSError):
        return False


def check_simulation_inputs(  # noqa: C901 - explicit fail-closed checks across TUFLOW input families
    tcf: Path,
    runs: Sequence[RunRequest],
    *,
    check_gpkg_layers: bool = True,
) -> PreflightReport:
    """Check every active file input and BC source for all proposed runs.

    The PyTUFLOW dependency is optional until invoked. Missing, unparseable or
    unresolved inputs fail closed and are returned in the report. The model
    control files and input data are never modified.
    """
    if not tcf.is_file():
        message = f"TUFLOW control file not found: {tcf}"
        raise FileNotFoundError(message)
    if not runs:
        message = "No simulations were supplied for input preflight"
        raise ValueError(message)
    try:
        from pytuflow import TCF
    except ImportError as exc:
        message = "PyTUFLOW is required; install with: python -m pip install pytuflow>=1.2"
        raise RuntimeError(message) from exc

    report = PreflightReport(run_count=len(runs))
    cache: dict[tuple[str, str], bool] = {}
    model = TCF(str(tcf.absolute()))

    def add_issue(run: RunRequest, status: IssueStatus, origin: str, reference: str, detail: str) -> None:
        report.issues.append(
            PreflightIssue(
                run_index=run.index,
                flags=run.description,
                status=status,
                origin=origin,
                reference=reference,
                detail=detail,
            )
        )

    def inspect_file(
        run: RunRequest, file: object, origin: str, *, base: Path, require_layer: bool = True
    ) -> None:
        raw = str(getattr(file, "dbpath", file))
        if not raw or raw == "." or "<<" in raw or ">>" in raw or any(char in raw for char in "*?"):
            add_issue(run, "unresolved", origin, raw, "Unexpanded variable, wildcard or unsupported reference")
            return
        path = Path(raw)
        if not path.is_absolute():
            path = base / path
        path = path.absolute()
        layer = str(getattr(file, "lyrname", "") or "") if require_layer and check_gpkg_layers else ""
        if path.suffix.lower() != ".gpkg":
            layer = ""
        key = (str(path).casefold(), layer.casefold())
        if key not in cache:
            cache[key] = path.is_file() and (not layer or _database_layer_exists(path, layer))
        report.checked_paths.add(str(path))
        if not cache[key]:
            detail = f"Missing GeoPackage layer {layer}" if path.is_file() and layer else "Input file does not exist"
            add_issue(run, "missing", origin, f"{path} >> {layer}" if layer else str(path), detail)

    for run in runs:
        try:
            context = model.context(run.arguments)
        except Exception as exc:  # noqa: BLE001 - unknown PyTUFLOW exceptions must be reported as unresolved
            add_issue(run, "unresolved", "TCF context", str(tcf), f"Could not resolve run: {exc}")
            continue

        try:
            inputs: Iterable[object] = context.find_input()
            for inp in inputs:
                origin = str(inp)
                try:
                    files = list(inp.files)
                except Exception as exc:  # noqa: BLE001 - unknown PyTUFLOW exceptions must be reported as unresolved
                    add_issue(run, "unresolved", origin, "", f"Could not enumerate input files: {exc}")
                    continue
                parent = getattr(inp, "parent", None)
                owning_file = getattr(parent, "fpath", None)
                base = Path(owning_file).parent if owning_file else tcf.absolute().parent
                for file in files:
                    inspect_file(run, file, origin, base=base)
                try:
                    missing_flag = bool(inp.has_missing_files)
                except Exception as exc:  # noqa: BLE001 - unknown PyTUFLOW exceptions must be reported as unresolved
                    add_issue(run, "unresolved", origin, "", f"Could not evaluate missing-file state: {exc}")
                    continue
                if missing_flag and not files:
                    add_issue(run, "unresolved", origin, "", "Input reports missing dependencies without a resolved path")
        except Exception as exc:  # noqa: BLE001 - unknown PyTUFLOW exceptions must be reported as unresolved
            add_issue(run, "unresolved", "Control-file traversal", str(tcf), str(exc))

        # PyTUFLOW does not expose BC source CSVs as ordinary control-file
        # commands. Inspect the resolved database's Source column separately.
        try:
            bc_database = context.bc_dbase()
        except (LookupError, ValueError):
            # Models with no BC Database are valid (e.g. direct GIS boundaries).
            if context.find_input(lhs="BC Database"):
                add_issue(run, "unresolved", "BC Database", str(tcf), "Could not resolve active BC database")
            continue
        except Exception as exc:  # noqa: BLE001 - unknown PyTUFLOW exceptions must be reported as unresolved
            add_issue(run, "unresolved", "BC Database", str(tcf), f"Could not load BC database: {exc}")
            continue

        if bc_database is None:
            continue
        database_path = Path(bc_database.fpath).absolute()
        inspect_file(run, database_path, "BC Database", base=tcf.absolute().parent, require_layer=False)
        if not database_path.is_file():
            continue

        try:
            df = bc_database.df
            source_column = next((column for column in df.columns if str(column).strip().lower() == "source"), None)
            if source_column is None:
                add_issue(run, "unresolved", "BC Database", str(database_path), "No Source column found")
                continue
            for name, row in df.iterrows():
                raw = str(row[source_column]).strip()
                if raw.lower() in {"", "nan", "none"}:
                    continue
                # Numeric constant BC values do not refer to a source file.
                try:
                    float(raw)
                except ValueError:
                    inspect_file(run, raw, f"BC source: {name}", base=database_path.parent, require_layer=False)
        except Exception as exc:  # noqa: BLE001 - unknown PyTUFLOW exceptions must be reported as unresolved
            add_issue(run, "unresolved", "BC Database", str(database_path), f"Could not inspect BC sources: {exc}")

    logger.info(
        "Preflight: {} runs, {} unique files, {} issues affecting {} runs",
        report.run_count,
        len(report.checked_paths),
        len(report.issues),
        report.affected_runs,
    )
    return report
