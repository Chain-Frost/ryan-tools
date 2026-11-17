"""Read-only multi-TCF preflight using one authoritative command-file plan."""

from __future__ import annotations

import csv
import json
import os
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path, PurePath, PureWindowsPath
from typing import TYPE_CHECKING

from ...functions.tuflow.command_plan import CommandPlan, CommandPlanError, PlannedRun

if TYPE_CHECKING:
    from .input_preflight import PreflightIssue, PreflightReport


@dataclass(frozen=True, slots=True)
class CommandPreflightIssue:
    """A dependency finding with its original command-file provenance."""

    run: PlannedRun
    finding: PreflightIssue


@dataclass(frozen=True, slots=True)
class CommandPreflightIssueGroup:
    """Presentation grouping only: every attributed finding is preserved."""

    issues: tuple[CommandPreflightIssue, ...]
    source: str


def group_preflight_issues(issues: Sequence[CommandPreflightIssue]) -> tuple[CommandPreflightIssueGroup, ...]:
    """Combine identical run/source/status/type keys without resolving or folding paths.

    A BC database may request one missing CSV many times. The exact findings
    remain in the report and CSV, including the requested boundaries/columns.
    Never group across distinct runs or statuses, or infer physical identity.
    """
    groups: dict[tuple[int, str, str, str], list[CommandPreflightIssue]] = {}
    for issue in issues:
        finding = issue.finding
        source = (
            str(finding.path)
            if finding.path is not None
            else str(finding.source_pattern)
            if finding.source_pattern is not None
            else finding.reference
        )
        key = (issue.run.index, finding.status, finding.kind or finding.origin, source)
        groups.setdefault(key, []).append(issue)
    return tuple(CommandPreflightIssueGroup(tuple(entries), key[3]) for key, entries in groups.items())


@dataclass(slots=True)
class CommandPreflightReport:
    """Aggregate results; an exception or partial group can never pass."""

    plan: CommandPlan
    issues: list[CommandPreflightIssue] = field(default_factory=list[CommandPreflightIssue])
    warnings: list[CommandPreflightIssue] = field(default_factory=list[CommandPreflightIssue])
    checked_paths: set[str] = field(default_factory=set[str])
    groups_checked: int = 0
    completed_runs: int = 0
    parse_seconds: float = 0.0
    context_seconds: float = 0.0
    inventory_seconds: float = 0.0
    files_examined: int = 0
    geopackages_examined: int = 0
    csv_headers_checked: int = 0
    directories_scanned: int = 0
    gis_sources_added: int = 0
    gis_files_scanned: int = 0

    @property
    def run_count(self) -> int:
        return len(self.plan.runs)

    @property
    def passed(self) -> bool:
        return bool(self.run_count) and self.completed_runs == self.run_count and not self.issues

    @property
    def affected_runs(self) -> int:
        return len({issue.run.index for issue in self.issues})

    def write_csv(self, destination: Path) -> Path:
        """Create a lossless per-reference register, never overwriting an existing file."""
        destination = destination.absolute()
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("x", newline="", encoding="utf-8-sig") as output:
            writer = csv.writer(output)
            writer.writerow(
                (
                    "source_file",
                    "line_start",
                    "line_end",
                    "run_index",
                    "tcf",
                    "working_directory",
                    "flags",
                    "status",
                    "origin",
                    "reference",
                    "detail",
                    "kind",
                    "path",
                    "layer",
                    "owner_control_file",
                    "database_kind",
                    "input_uuid",
                    "command_index",
                    "csv_columns_json",
                    "source_pattern",
                    "command_text",
                    "line_number",
                    "severity",
                )
            )
            for severity, findings in (("error", self.issues), ("warning", self.warnings)):
                for issue in findings:
                    run, finding = issue.run, issue.finding
                    writer.writerow(
                        (
                            str(run.source),
                            run.line_start,
                            run.line_end,
                            run.index,
                            str(run.tcf),
                            str(run.working_directory),
                            " ".join(f"-{k} {v}" for k, v in run.flag_pairs),
                            finding.status,
                            finding.origin,
                            finding.reference,
                            finding.detail,
                            finding.kind,
                            str(finding.path) if finding.path is not None else "",
                            finding.layer or "",
                            str(finding.owner_control_file) if finding.owner_control_file is not None else "",
                            finding.database_kind or "",
                            finding.input_uuid,
                            finding.command_index,
                            json.dumps(finding.csv_columns, ensure_ascii=False) if finding.csv_columns is not None else "",
                            str(finding.source_pattern) if finding.source_pattern is not None else "",
                            finding.command_text,
                            finding.line_number if finding.line_number is not None else "",
                            severity,
                        )
                    )
        return destination


def _native_path(path: PurePath) -> Path:
    if isinstance(path, PureWindowsPath) != (os.name == "nt"):
        message = "Preflight must use the operating system matching the plan's path flavour"
        raise CommandPlanError(message)
    return Path(str(path))


def _check_group(
    runs: tuple[PlannedRun, ...],
    *,
    check_gpkg_layers: bool,
    io_workers: int = 4,
    maximum_directory_entries: int = 50_000,
) -> PreflightReport:
    """Call the one scope-01 validator with unchanged global run identities."""
    # Imported lazily: planning/--plan-only must not require the GIS stack.
    # Scope 01 translates PyTUFLOW's zero-based positions to these global
    # request IDs. There must be no second local/global translation here.
    from .input_preflight import RunRequest, check_simulation_inputs

    requests = [RunRequest(run.index, run.flags) for run in runs]
    return check_simulation_inputs(
        _native_path(runs[0].tcf),
        requests,
        check_gpkg_layers=check_gpkg_layers,
        io_workers=io_workers,
        maximum_directory_entries=maximum_directory_entries,
    )


def _validate_options(*, io_workers: int, maximum_directory_entries: int) -> None:
    if not 1 <= io_workers <= 32:
        message = "io_workers must be between 1 and 32"
        raise ValueError(message)
    if maximum_directory_entries < 1:
        message = "maximum_directory_entries must be positive"
        raise ValueError(message)


def check_command_plan(
    plan: CommandPlan,
    *,
    check_gpkg_layers: bool = True,
    io_workers: int = 4,
    maximum_directory_entries: int = 50_000,
) -> CommandPreflightReport:
    """Check every planned run, grouped by effective TCF and launch directory.

    Preserve the global source order/indices even for interleaved TCF groups.
    Scope 01's public PyTUFLOW consumer owns input interpretation. No shell,
    solver, chdir, revalidation backend or cross-TCF cache is introduced.
    Directory changes are modelled assuming success; an absent destination
    therefore fails closed instead of silently changing shell semantics.
    """
    _validate_options(io_workers=io_workers, maximum_directory_entries=maximum_directory_entries)
    indices = [run.index for run in plan.runs]
    if (
        not indices
        or any(type(index) is not int or index < 1 for index in indices)
        or len(set(indices)) != len(indices)
    ):
        message = "Run plan must contain distinct positive original run indices"
        raise CommandPlanError(message)
    positions = {index: position for position, index in enumerate(indices)}
    for requirement in plan.directories:
        if not _native_path(requirement.path).is_dir():
            message = (
                f"{requirement.source}:{requirement.line}: working directory is unavailable; "
                "cannot assume the batch directory change succeeds"
            )
            raise CommandPlanError(message)
    groups: dict[tuple[PurePath, PurePath], list[PlannedRun]] = {}
    for run in plan.runs:
        _native_path(run.tcf)
        groups.setdefault((run.tcf, run.working_directory), []).append(run)
    combined = CommandPreflightReport(plan)
    for grouped_runs in groups.values():
        runs = tuple(grouped_runs)
        by_index = {run.index: run for run in runs}
        try:
            report = _check_group(
                runs,
                check_gpkg_layers=check_gpkg_layers,
                io_workers=io_workers,
                maximum_directory_entries=maximum_directory_entries,
            )
            if report.run_count != len(runs) or any(
                finding.run_index not in by_index for finding in (*report.issues, *report.warnings)
            ):
                message = "Incomplete or incorrectly indexed preflight group report"
                raise ValueError(message)
            if not report.passed and not report.issues:
                message = "Validation group reports failure but supplies no attributable findings"
                raise ValueError(message)
            findings = [CommandPreflightIssue(by_index[issue.run_index], issue) for issue in report.issues]
            combined.issues.extend(findings)
            combined.warnings.extend(
                CommandPreflightIssue(by_index[warning.run_index], warning) for warning in report.warnings
            )
            combined.checked_paths.update(report.checked_paths)
            combined.parse_seconds += report.parse_seconds
            combined.context_seconds += report.context_seconds
            combined.inventory_seconds += report.inventory_seconds
            combined.files_examined += report.files_examined
            combined.geopackages_examined += report.geopackages_examined
            combined.csv_headers_checked += report.csv_headers_checked
            combined.directories_scanned += report.directories_scanned
            combined.gis_sources_added += report.gis_sources_added
            combined.gis_files_scanned += report.gis_files_scanned
            combined.completed_runs += len(runs)
            combined.groups_checked += 1
        except Exception as exc:  # noqa: BLE001 - every group failure invalidates all its runs; other groups still checked
            from .input_preflight import PreflightIssue

            combined.issues.extend(
                CommandPreflightIssue(
                    run,
                    PreflightIssue(
                        run.index,
                        " ".join(f"-{key} {value}" for key, value in run.flag_pairs),
                        "unresolved",
                        "TCF preflight",
                        str(run.tcf),
                        f"{type(exc).__name__}: {exc}",
                    ),
                )
                for run in runs
            )
    combined.issues.sort(key=lambda issue: positions[issue.run.index])
    combined.warnings.sort(key=lambda warning: positions[warning.run.index])
    return combined
