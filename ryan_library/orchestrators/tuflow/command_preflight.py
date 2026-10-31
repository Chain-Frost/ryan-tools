"""Read-only multi-TCF preflight using one authoritative command-file plan."""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass, field
from pathlib import Path, PurePath, PureWindowsPath
from typing import TYPE_CHECKING, Literal

from ...functions.tuflow.command_plan import CommandPlan, CommandPlanError, PlannedRun

if TYPE_CHECKING:
    from .input_preflight import PreflightReport


@dataclass(frozen=True, slots=True)
class CommandPreflightIssue:
    """A dependency finding with its original command-file provenance."""

    run: PlannedRun
    status: str
    origin: str
    reference: str
    detail: str


@dataclass(slots=True)
class CommandPreflightReport:
    """Aggregate results; an exception or partial group can never pass."""

    plan: CommandPlan
    issues: list[CommandPreflightIssue] = field(default_factory=list[CommandPreflightIssue])
    checked_paths: set[str] = field(default_factory=set[str])
    groups_checked: int = 0
    completed_runs: int = 0
    parse_seconds: float = 0.0
    context_seconds: float = 0.0
    inventory_seconds: float = 0.0
    csv_headers_checked: int = 0
    csv_column_selections_checked: int = 0
    dependency_fragment_evaluations: int = 0
    dependency_fragment_reuses: int = 0
    dependency_fallback_runs: int = 0

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
        """Create a new private issue register; never overwrite an input/report."""
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
                )
            )
            for issue in self.issues:
                run = issue.run
                writer.writerow(
                    (
                        str(run.source),
                        run.line_start,
                        run.line_end,
                        run.index,
                        str(run.tcf),
                        str(run.working_directory),
                        " ".join(f"-{k} {v}" for k, v in run.flag_pairs),
                        issue.status,
                        issue.origin,
                        issue.reference,
                        issue.detail,
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
    context_mode: Literal["individual", "batch"],
    files_only: bool = False,
    io_workers: int = 4,
) -> PreflightReport:
    # Keep planning/--plan-only independent of the PyTUFLOW/GIS import stack.
    from .input_preflight import RunRequest, check_batch_files, check_simulation_inputs

    # Local zero-based positions are the PyTUFLOW batch contract. Never send
    # global positions upstream; results are mapped back by the caller.
    requests: list[RunRequest] = [RunRequest(position, run.flags) for position, run in enumerate(runs)]
    if files_only:
        return check_batch_files(
            _native_path(runs[0].tcf), requests, check_gpkg_layers=check_gpkg_layers, io_workers=io_workers
        )
    return check_simulation_inputs(
        _native_path(runs[0].tcf), requests, check_gpkg_layers=check_gpkg_layers, context_mode=context_mode
    )


def _validate_options(*, context_mode: Literal["individual", "batch"], io_workers: int) -> None:
    if not 1 <= io_workers <= 32:
        message = "io_workers must be between 1 and 32"
        raise ValueError(message)
    if context_mode not in {"individual", "batch"}:
        message = f"Unsupported input preflight context mode: {context_mode}"
        raise ValueError(message)


def check_command_plan(
    plan: CommandPlan,
    *,
    check_gpkg_layers: bool = True,
    context_mode: Literal["individual", "batch"] = "individual",
    files_only: bool = False,
    io_workers: int = 4,
) -> CommandPreflightReport:
    """Check every planned run, grouped by effective TCF and launch directory.

    Preserve the global source order/indices even for interleaved TCF groups.
    Existing PyTUFLOW collection/materialisation owns model syntax. No shell,
    solver, chdir, new graph cache or cross-TCF inventory cache is introduced.
    Directory changes are modelled assuming success; an absent destination
    therefore fails closed instead of silently changing shell semantics.
    """
    _validate_options(context_mode=context_mode, io_workers=io_workers)
    if not plan.runs or [run.index for run in plan.runs] != list(range(1, len(plan.runs) + 1)):
        message = "Run plan must contain a complete sequential set of global indices"
        raise CommandPlanError(message)
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
        by_index = dict(enumerate(runs))
        try:
            if files_only:
                report = _check_group(
                    runs,
                    check_gpkg_layers=check_gpkg_layers,
                    context_mode=context_mode,
                    files_only=True,
                    io_workers=io_workers,
                )
            else:
                report = _check_group(runs, check_gpkg_layers=check_gpkg_layers, context_mode=context_mode)
            if report.run_count != len(runs) or any(issue.run_index not in by_index for issue in report.issues):
                message = "Incomplete or incorrectly indexed preflight group report"
                raise ValueError(message)
            if not report.passed and not report.issues:
                message = "Validation group reports failure but supplies no attributable findings"
                raise ValueError(message)
            findings = [
                CommandPreflightIssue(
                    by_index[issue.run_index], issue.status, issue.origin, issue.reference, issue.detail
                )
                for issue in report.issues
            ]
            combined.issues.extend(findings)
            combined.checked_paths.update(report.checked_paths)
            combined.parse_seconds += report.parse_seconds
            combined.context_seconds += report.context_seconds
            combined.inventory_seconds += report.inventory_seconds
            if files_only:
                combined.csv_headers_checked += report.csv_headers_checked
                combined.csv_column_selections_checked += report.csv_column_selections_checked
                combined.dependency_fragment_evaluations += report.dependency_fragment_evaluations
                combined.dependency_fragment_reuses += report.dependency_fragment_reuses
                combined.dependency_fallback_runs += report.dependency_fallback_runs
            combined.completed_runs += len(runs)
            combined.groups_checked += 1
        except Exception as exc:  # noqa: BLE001 - every group failure invalidates all its runs; other groups still checked
            combined.issues.extend(
                CommandPreflightIssue(run, "unresolved", "TCF preflight", str(run.tcf), f"{type(exc).__name__}: {exc}")
                for run in runs
            )
    combined.issues.sort(key=lambda issue: issue.run.index)
    return combined
