"""Infer missing filename permutations from TLF inventories, without reading result data."""

import os
import re
from dataclasses import dataclass
from itertools import product
from math import prod
from pathlib import Path


@dataclass(frozen=True)
class EnsembleInventory:
    """Observed logs and inferred gaps; presence does not establish successful completion."""

    log_count: int
    run_count: int
    expected_count: int
    family_count: int
    missing: tuple[str, ...]


def infer_ensemble_inventory(names: list[str], *, max_combinations: int = 1_000_000) -> EnsembleInventory:
    """Cross observed underscore-token values within compatible filename layouts.

    TP-bearing layouts expect TP01 through TP10 in addition to any observed TPs.
    All other tokens (including scenarios) use observed values. Different token
    counts or event positions form independent families. No completion is inferred.
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
    expected_count = 0
    for layout, runs in sorted(families.items()):
        values = [sorted({run[index] for run in runs}) for index in range(len(layout))]
        for index, kind in enumerate(layout):
            if kind == "TP":
                values[index] = sorted(set(values[index]) | {f"TP{tp:02d}" for tp in range(1, 11)})
        expected_count += prod(len(items) for items in values)
        if expected_count > max_combinations:
            message = f"Inferred combinations exceed limit {max_combinations}; narrow the scan root"
            raise ValueError(message)
        missing.extend("_".join(run) for run in product(*values) if run not in runs)
    return EnsembleInventory(log_count, sum(map(len, families.values())), expected_count, len(families), tuple(missing))


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
    if re.fullmatch(r"\d+(?:\.\d+)?(?:m|h|min|hr)", token, re.IGNORECASE):
        return "Duration"
    return "Scenario"


def scan_ensemble_inventory(
    root: Path, *, recursive: bool = True, max_combinations: int = 1_000_000
) -> EnsembleInventory:
    """Scan filenames only, rejecting inaccessible directories rather than claiming completeness.

    Directory links are not followed. Run identity is the filename, irrespective
    of containing folder, so duplicated archived logs do not add runs.
    """
    root = root.absolute()
    if not root.is_dir():
        raise NotADirectoryError(root)
    names: list[str] = []

    def fail(error: OSError) -> None:
        raise error

    for directory, directories, files in os.walk(root, onerror=fail, followlinks=False):
        directories[:] = [
            name
            for name in directories
            if not (Path(directory) / name).is_symlink() and not (Path(directory) / name).is_junction()
        ]
        names.extend(name for name in files if name.lower().endswith(".tlf"))
        if not recursive:
            directories.clear()
    return infer_ensemble_inventory(names, max_combinations=max_combinations)
