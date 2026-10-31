"""Read-only TUFLOW input preflight through PyTUFLOW's public batch validator.

The command planner owns run order, source provenance, effective TCF and cwd.
This consumer maps upstream zero-based validation positions to supplied outer
run indices exactly once. It does not inspect files independently or run TUFLOW.
"""

from __future__ import annotations

import csv
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Protocol, cast

from ...functions.tuflow.pytuflow_compatibility import require_validation_api

IssueStatus = Literal["missing", "unresolved", "unsupported"]


class _Finding(Protocol):
    run_index: int
    flags: str
    status: str
    kind: str
    path: Path | None
    detail: str
    layer: str | None
    origin: str
    input_uuid: str
    command_index: int
    owner_control_file: Path | None
    database_kind: str | None
    source_pattern: Path | None
    csv_columns: tuple[str, ...] | None


class _DependencyRun(Protocol):
    index: int
    error: str | None


class _Dependencies(Protocol):
    runs: tuple[_DependencyRun, ...]
    passed: bool


class _PhysicalReport(Protocol):
    run_count: int
    findings: tuple[_Finding, ...]
    passed: bool
    files_examined: int
    geopackages_examined: int
    csv_headers_examined: int
    directories_scanned: int


class _BatchResult(Protocol):
    dependencies: _Dependencies
    validation: _PhysicalReport
    passed: bool
    parse_seconds: float
    collection_seconds: float
    inspection_seconds: float
    gis_sources_added: int
    gis_files_scanned: int


@dataclass(frozen=True, slots=True)
class RunRequest:
    """One actual planned occurrence, with lossless structured model switches."""

    index: int
    flags: Mapping[str, str]

    @property
    def arguments(self) -> str:
        """Return a human-readable representation, never used for validation."""
        return " ".join(f"-{key} {value}" for key, value in self.flags.items())

    @property
    def description(self) -> str:
        """Show switches in a predictable order for reports."""
        return " ".join(f"-{key} {value}" for key, value in sorted(self.flags.items()))


@dataclass(frozen=True, slots=True)
class PreflightIssue:
    """Failed requirement with upstream source and original planned-run identity."""

    run_index: int
    flags: str
    status: IssueStatus
    origin: str
    reference: str
    detail: str
    kind: str = ""
    path: Path | None = None
    layer: str | None = None
    owner_control_file: Path | None = None
    database_kind: str | None = None
    input_uuid: str = ""
    command_index: int = -1
    csv_columns: tuple[str, ...] | None = None
    source_pattern: Path | None = None


@dataclass(slots=True)
class PreflightReport:
    """Only a complete upstream run set with no findings can pass."""

    run_count: int
    completed_runs: int = 0
    upstream_passed: bool = False
    issues: list[PreflightIssue] = field(default_factory=list[PreflightIssue])
    checked_paths: set[str] = field(default_factory=set[str])
    files_examined: int = 0
    geopackages_examined: int = 0
    csv_headers_checked: int = 0
    directories_scanned: int = 0
    gis_sources_added: int = 0
    gis_files_scanned: int = 0
    parse_seconds: float = 0.0
    context_seconds: float = 0.0
    inventory_seconds: float = 0.0

    @property
    def passed(self) -> bool:
        """Fail closed for unavailable, omitted or partially mapped validations."""
        return bool(self.run_count) and self.completed_runs == self.run_count and self.upstream_passed and not self.issues

    @property
    def affected_runs(self) -> int:
        """Number of distinct planned occurrences with unsuccessful requirements."""
        return len({issue.run_index for issue in self.issues})

    def write_csv(self, destination: Path) -> Path:
        """Create an issue report without overwriting an existing file."""
        destination = destination.absolute()
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("x", newline="", encoding="utf-8-sig") as output:
            writer = csv.writer(output)
            writer.writerow(
                (
                    "run_index", "flags", "status", "origin", "reference", "detail",
                    "kind", "path", "layer", "owner_control_file", "database_kind",
                    "input_uuid", "command_index", "csv_columns", "source_pattern",
                )
            )
            for issue in self.issues:
                writer.writerow(
                    (
                        issue.run_index, issue.flags, issue.status, issue.origin, issue.reference, issue.detail,
                        issue.kind, str(issue.path) if issue.path is not None else "", issue.layer or "",
                        str(issue.owner_control_file) if issue.owner_control_file is not None else "",
                        issue.database_kind or "", issue.input_uuid, issue.command_index,
                        ",".join(issue.csv_columns) if issue.csv_columns is not None else "",
                        str(issue.source_pattern) if issue.source_pattern is not None else "",
                    )
                )
        return destination


def _nonnegative_count(value: object, label: str) -> int:
    """Reject missing, invalid or negative public inspection counters."""
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        message = f"PyTUFLOW returned invalid {label}: {value!r}"
        raise ValueError(message)
    return value


def _issue(finding: _Finding, request: RunRequest) -> PreflightIssue:
    """Translate the complete public finding without synthesising input context."""
    if finding.status not in {"missing", "unresolved", "unsupported"}:
        message = f"Unknown PyTUFLOW validation status: {finding.status!r}"
        raise ValueError(message)
    reference = str(finding.path) if finding.path is not None else (
        str(finding.source_pattern) if finding.source_pattern is not None else "<run>"
    )
    if finding.layer is not None:
        reference = f"{reference} >> {finding.layer}"
    return PreflightIssue(
        run_index=request.index,
        flags=request.description,
        status=cast(IssueStatus, finding.status),
        origin=finding.origin,
        reference=reference,
        detail=finding.detail,
        kind=finding.kind,
        path=finding.path,
        layer=finding.layer,
        owner_control_file=finding.owner_control_file,
        database_kind=finding.database_kind,
        input_uuid=finding.input_uuid,
        command_index=finding.command_index,
        csv_columns=finding.csv_columns,
        source_pattern=finding.source_pattern,
    )


def check_simulation_inputs(  # noqa: C901 - fail-closed public result validation
    tcf: Path | str | object,
    runs: Sequence[RunRequest],
    *,
    check_gpkg_layers: bool = True,
    io_workers: int = 4,
    maximum_directory_entries: int = 50_000,
) -> PreflightReport:
    """Validate exact ordered runs with PyTUFLOW PR17's public API.

    Every run retains its supplied global identity. Validation is only invoked
    explicitly; no import of PyTUFLOW occurs during help, planning or export.
    Existing raster files are checked as input paths, not decoded for headers
    or pixel contents. The caller owns grouping, model cwd and error policy.
    """
    if not runs:
        message = "No simulations were supplied for input preflight"
        raise ValueError(message)
    if len({run.index for run in runs}) != len(runs) or any(run.index < 1 for run in runs):
        message = "Input preflight requires distinct positive planned-run indices"
        raise ValueError(message)
    if not 1 <= io_workers <= 32:
        message = "io_workers must be between 1 and 32"
        raise ValueError(message)
    if maximum_directory_entries < 1:
        message = "maximum_directory_entries must be positive"
        raise ValueError(message)

    api = require_validation_api()
    # Do not use a display string: flag values may contain whitespace. PyTUFLOW
    # accepts these dictionaries as the authoritative model run arguments.
    structured = [dict(run.flags) for run in runs]
    model: object = Path(tcf).absolute() if isinstance(tcf, (str, Path)) else tcf
    options = api.options_factory(
        check_files=True,
        check_gpkg_layers=check_gpkg_layers,
        check_bc_csv_headers=True,
        io_workers=io_workers,
        maximum_directory_entries=maximum_directory_entries,
    )
    result = cast(_BatchResult, api.validate_batch_inputs(model, structured, options=options))

    # A missing/malformed public result is a failure, never an empty success.
    if (
        result is None
        or not isinstance(getattr(result, "passed", None), bool)
        or not hasattr(result, "dependencies")
        or not hasattr(result, "validation")
    ):
        message = "PyTUFLOW returned no complete batch validation result"
        raise ValueError(message)
    deps, physical = result.dependencies, result.validation
    run_results = getattr(deps, "runs", None)
    findings = getattr(physical, "findings", None)
    if not isinstance(run_results, (list, tuple)) or not isinstance(findings, (list, tuple)):
        message = "PyTUFLOW omitted batch run results or physical findings"
        raise ValueError(message)
    count = len(runs)
    if _nonnegative_count(getattr(physical, "run_count", None), "run_count") != count:
        message = "PyTUFLOW returned an incomplete physical-validation run count"
        raise ValueError(message)
    indices = [getattr(item, "index", None) for item in run_results]
    if len(indices) != count or sorted(indices, key=str) != sorted(range(count), key=str):
        message = "PyTUFLOW returned missing, duplicate or invalid batch run indices"
        raise ValueError(message)
    by_position = dict(enumerate(runs))
    report = PreflightReport(run_count=count)
    for finding in findings:
        position = getattr(finding, "run_index", None)
        if type(position) is not int or position not in by_position:
            message = f"PyTUFLOW finding refers to unknown batch position: {position!r}"
            raise ValueError(message)
        if getattr(finding, "status", None) == "present":
            if finding.path is not None:
                report.checked_paths.add(str(finding.path))
            continue
        report.issues.append(_issue(finding, by_position[position]))
        if finding.path is not None:
            report.checked_paths.add(str(finding.path))

    for run_result in run_results:
        error = getattr(run_result, "error", None)
        position = run_result.index
        if error and not any(issue.run_index == by_position[position].index for issue in report.issues):
            request = by_position[position]
            report.issues.append(
                PreflightIssue(request.index, request.description, "unresolved", "TCF run context", str(model), str(error))
            )

    if not result.passed and not report.issues:
        message = "PyTUFLOW reported an unsuccessful batch without findings"
        raise ValueError(message)
    if result.passed and report.issues:
        message = "PyTUFLOW reported a passing batch containing failed input findings"
        raise ValueError(message)
    if not isinstance(getattr(deps, "passed", None), bool) or not isinstance(physical.passed, bool):
        message = "PyTUFLOW omitted completeness status for dependencies or physical validation"
        raise ValueError(message)
    if result.passed != (deps.passed and physical.passed):
        message = "PyTUFLOW returned inconsistent dependency/validation status"
        raise ValueError(message)

    report.completed_runs = count
    report.upstream_passed = result.passed
    report.files_examined = _nonnegative_count(getattr(physical, "files_examined", None), "files_examined")
    report.geopackages_examined = _nonnegative_count(
        getattr(physical, "geopackages_examined", None), "geopackages_examined"
    )
    report.csv_headers_checked = _nonnegative_count(
        getattr(physical, "csv_headers_examined", None), "csv_headers_examined"
    )
    report.directories_scanned = _nonnegative_count(
        getattr(physical, "directories_scanned", None), "directories_scanned"
    )
    report.gis_sources_added = _nonnegative_count(getattr(result, "gis_sources_added", None), "gis_sources_added")
    report.gis_files_scanned = _nonnegative_count(getattr(result, "gis_files_scanned", None), "gis_files_scanned")
    for label, attribute in (
        ("parse_seconds", "parse_seconds"),
        ("context_seconds", "collection_seconds"),
        ("inventory_seconds", "inspection_seconds"),
    ):
        value = getattr(result, attribute, None)
        if not isinstance(value, (int, float)) or value < 0:
            message = f"PyTUFLOW returned invalid {attribute}"
            raise ValueError(message)
        setattr(report, label, float(value))
    return report
