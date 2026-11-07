"""Infer TLF ensembles with optional successful-completion verification."""

import os
import re
from dataclasses import dataclass
from itertools import product
from math import prod
from pathlib import Path


@dataclass(frozen=True)
class TPCountTable:
    """TP inventory for one scenario combination; rows are AEPs, columns durations."""

    scenario: str
    aeps: tuple[str, ...]
    durations: tuple[str, ...]
    counts: tuple[tuple[int, ...], ...]
    expected_tps: int
    missing_summary: tuple[str, ...] = ()


@dataclass(frozen=True)
class EnsembleInventory:
    """Observed logs and inferred gaps; presence does not establish successful completion."""

    log_count: int
    run_count: int
    expected_count: int
    family_count: int
    missing: tuple[str, ...]
    tp_tables: tuple[TPCountTable, ...] = ()
    ungridded_missing: tuple[str, ...] = ()
    verified_count: int | None = None


def infer_ensemble_inventory(
    names: list[str], *, max_combinations: int = 1_000_000, successful_names: set[str] | None = None
) -> EnsembleInventory:
    """Cross observed underscore-token values within compatible filename layouts.

    TP-bearing layouts expect TP01 through TP10 in addition to any observed TPs.
    All other tokens (including scenarios) use observed values. Different token
    counts or event positions form independent families. If successful_names is supplied,
    only those names count towards coverage; expectations still use every observed name.
    """
    if max_combinations < 1:
        message = "max_combinations must be positive"
        raise ValueError(message)
    families: dict[tuple[str, ...], set[tuple[str, ...]]] = {}
    log_count = 0
    for name in names:
        if not name.lower().endswith(".tlf"):
            continue
        log_count += 1
        tokens = _filename_tokens(name)
        layout = tuple(_token_kind(token) for token in tokens)
        families.setdefault(layout, set()).add(tokens)
    if not families:
        message = "No TLF logs found"
        raise ValueError(message)
    missing: list[str] = []
    tables: list[TPCountTable] = []
    ungridded: list[str] = []
    expected_count = 0
    successful = {_filename_tokens(name) for name in successful_names} if successful_names is not None else None
    for layout, runs in sorted(families.items()):
        values = [sorted({run[index] for run in runs}) for index in range(len(layout))]
        for index, kind in enumerate(layout):
            if kind == "TP":
                values[index] = sorted(set(values[index]) | {f"TP{tp:02d}" for tp in range(1, 11)})
        expected_count += prod(len(items) for items in values)
        if expected_count > max_combinations:
            message = f"Inferred combinations exceed limit {max_combinations}; narrow the scan root"
            raise ValueError(message)
        eligible = runs if successful is None else runs & successful
        gaps = ["_".join(run) for run in product(*values) if run not in eligible]
        missing.extend(gaps)
        family_tables = _tp_count_tables(layout, eligible, values)
        tables.extend(family_tables)
        if not family_tables:
            ungridded.extend(gaps)
    return EnsembleInventory(
        log_count,
        sum(map(len, families.values())),
        expected_count,
        len(families),
        tuple(missing),
        tuple(tables),
        tuple(ungridded),
        sum(len(runs & successful) for runs in families.values()) if successful is not None else None,
    )


def _numeric_order(value: str) -> tuple[float, str]:
    match = re.search(r"\d+(?:\.\d+)?", value)
    return (float(match[0]) if match else 0, value)


def _tp_count_tables(
    layout: tuple[str, ...], runs: set[tuple[str, ...]], values: list[list[str]]
) -> list[TPCountTable]:
    # Ambiguous repeated event fields remain in the filename report rather than being collapsed.
    if layout.count("TP") != 1 or layout.count("AEP") > 1 or layout.count("Duration") > 1:
        return []
    tp_index = layout.index("TP")
    aep_index = layout.index("AEP") if "AEP" in layout else None
    duration_index = layout.index("Duration") if "Duration" in layout else None
    scenario_indices = [index for index in range(len(layout)) if index not in {tp_index, aep_index, duration_index}]
    aeps = tuple(sorted(values[aep_index], key=_numeric_order)) if aep_index is not None else ("No AEP",)
    durations = (
        tuple(sorted(values[duration_index], key=_numeric_order)) if duration_index is not None else ("No duration",)
    )
    counts: dict[tuple[tuple[str, ...], str, str], int] = {}
    for run in runs:
        key = (
            tuple(run[index] for index in scenario_indices),
            run[aep_index] if aep_index is not None else "No AEP",
            run[duration_index] if duration_index is not None else "No duration",
        )
        counts[key] = counts.get(key, 0) + 1
    return [
        TPCountTable(
            scenario="_".join(scenario) or "All scenarios",
            aeps=aeps,
            durations=durations,
            counts=tuple(tuple(counts.get((scenario, aep, duration), 0) for duration in durations) for aep in aeps),
            expected_tps=len(values[tp_index]),
            missing_summary=_compact_missing(scenario, scenario_indices, aeps, durations, runs, layout, values),
        )
        for scenario in product(*(values[index] for index in scenario_indices))
    ]


def _tp_ranges(tps: list[str]) -> str:
    """Compact sorted TP codes into inclusive consecutive ranges."""
    numbers = sorted(int(tp[2:]) for tp in tps)
    groups: list[list[int]] = []
    for number in numbers:
        if groups and number == groups[-1][-1] + 1:
            groups[-1].append(number)
        else:
            groups.append([number])
    return ",".join(f"TP{group[0]:02d}" + (f"-{group[-1]:02d}" if len(group) > 1 else "") for group in groups)


def _compact_missing(
    scenario: tuple[str, ...],
    scenario_indices: list[int],
    aeps: tuple[str, ...],
    durations: tuple[str, ...],
    runs: set[tuple[str, ...]],
    layout: tuple[str, ...],
    values: list[list[str]],
) -> tuple[str, ...]:
    tp_index = layout.index("TP")
    template = [items[0] for items in values]
    for index, value in zip(scenario_indices, scenario, strict=True):
        template[index] = value
    summaries: list[str] = []
    for aep in aeps:
        grouped: dict[str, list[str]] = {}
        for duration in durations:
            if "AEP" in layout:
                template[layout.index("AEP")] = aep
            if "Duration" in layout:
                template[layout.index("Duration")] = duration
            absent: list[str] = []
            for tp in values[tp_index]:
                template[tp_index] = tp
                if tuple(template) not in runs:
                    absent.append(tp)
            if absent:
                label = "all TP" if len(absent) == len(values[tp_index]) else _tp_ranges(absent)
                grouped.setdefault(label, []).append(duration)
        if grouped:
            summaries.append(f"{aep}: " + "; ".join(f"{','.join(ds)} {tp}" for tp, ds in grouped.items()))
    return tuple(summaries)


def _filename_tokens(name: str) -> tuple[str, ...]:
    stem = name[:-4]
    if stem.lower().endswith(".hpc"):
        stem = stem[:-4]
    tokens = tuple(
        f"TP{int(token[2:]):02d}" if re.fullmatch(r"TP\d+", token, re.IGNORECASE) else token
        for token in stem.split("_")
    )
    if any(not token for token in tokens):
        message = f"Empty filename field: {name}"
        raise ValueError(message)
    return tokens


def _token_kind(token: str) -> str:
    if re.fullmatch(r"TP\d+", token):
        return "TP"
    if re.fullmatch(r"\d+(?:\.\d+)?(?:p|%|AEP)", token, re.IGNORECASE):
        return "AEP"
    # Uppercase M commonly denotes a scenario/grid label (e.g. 16M), not storm minutes.
    if re.fullmatch(r"\d+(?:\.\d+)?(?:m|h|min|hr)", token):
        return "Duration"
    return "Scenario"


def scan_ensemble_inventory(
    root: Path, *, recursive: bool = True, max_combinations: int = 1_000_000, verify_success: bool = False
) -> EnsembleInventory:
    """Scan filenames only, rejecting inaccessible directories rather than claiming completeness.

    Directory links are not followed. Run identity is the filename, irrespective
    of containing folder, so duplicated archived logs do not add runs.
    """
    root = root.absolute()
    if not root.is_dir():
        raise NotADirectoryError(root)
    names: list[str] = []
    logs: list[Path] = []

    def fail(error: OSError) -> None:
        raise error

    for directory, directories, files in os.walk(root, onerror=fail, followlinks=False):
        directories[:] = [
            name
            for name in directories
            if not (Path(directory) / name).is_symlink() and not (Path(directory) / name).is_junction()
        ]
        names.extend(name for name in files if name.lower().endswith(".tlf"))
        if verify_success:
            logs.extend(Path(directory) / name for name in files if name.lower().endswith(".tlf"))
        if not recursive:
            directories.clear()
    return infer_ensemble_inventory(
        names,
        max_combinations=max_combinations,
        successful_names=_successful_log_names(logs) if verify_success else None,
    )


def _successful_log_names(logs: list[Path]) -> set[str]:
    """Prefer the newest primary TLF over HPC companions; never reuse an older successful attempt."""
    selected: dict[tuple[str, ...], tuple[tuple[bool, int], Path]] = {}
    for path in logs:
        key = _filename_tokens(path.name)
        priority = (not path.name.lower().endswith(".hpc.tlf"), path.stat().st_mtime_ns)
        if key not in selected or priority > selected[key][0]:
            selected[key] = (priority, path)
    successful: set[str] = set()
    for _, path in selected.values():
        before = path.stat()
        with path.open(encoding="utf-8", errors="replace") as stream:
            finished = any(line.strip() == "Simulation FINISHED" for line in stream)
        after = path.stat()
        if finished and (before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns):
            successful.add(path.name)
    return successful
