"""Read-only multi-TCF preflight using one authoritative command-file plan."""

from __future__ import annotations

import csv
import os
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
                        issue.finding.status,
                        issue.finding.origin,
                        issue.finding.reference,
                        issue.finding.detail,
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
            if report.run_count != len(runs) or any(issue.run_index not in by_index for issue in report.issues):
                message = "Incomplete or incorrectly indexed preflight group report"
                raise ValueError(message)
            if not report.passed and not report.issues:
                message = "Validation group reports failure but supplies no attributable findings"
                raise ValueError(message)
            findings = [CommandPreflightIssue(by_index[issue.run_index], issue) for issue in report.issues]
            combined.issues.extend(findings)
            combined.checked_paths.update(report.checked_paths)
            combined.parse_seconds += report.parse_seconds
            combined.context_seconds += report.context_seconds
            combined.inventory_seconds += report.inventory_seconds
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
    return combined
