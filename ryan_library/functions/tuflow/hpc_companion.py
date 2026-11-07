"""Opt-in, read-only observations from a conventional TUFLOW HPC companion log.

A companion row is not a full-timestep record. This reader never infers a
true solver-wide nWet maximum or merges ambiguous domain cell populations.
No companion read happens unless the caller explicitly opts in.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Literal

from loguru import logger

CompanionStatus = Literal[
    "missing", "observed", "incomplete", "changed_during_read", "multiple_domains", "unreadable"
]

_MAX_LINE_BYTES = 8192
_STEP_TIME = re.compile(r"\d+:[0-5]\d:[0-5]\d(?:\.\d+)?")
_COUNT_PATTERNS: dict[str, re.Pattern[str]] = {
    "HPC_Active_2D_Cells": re.compile(r"^\s*Number active cells\s*=\s*(\d+)\s*$", re.IGNORECASE),
    "HPC_Sealed_Wall_Cells": re.compile(r"^\s*Number sealed wall cells\s*=\s*(\d+)\s*$", re.IGNORECASE),
    "HPC_Groundwater_Cells": re.compile(r"^\s*Number groundwater cells\s*=\s*(\d+)\s*$", re.IGNORECASE),
}


def _matched_count(line: str) -> tuple[str, int] | None:
    for field, pattern in _COUNT_PATTERNS.items():
        if match := pattern.match(line):
            return field, int(match.group(1))
    return None


def _sampled_wet_count(line: str, wet_index: int | None) -> int | None:
    if wet_index is None:
        return None
    parts = line.split()
    if len(parts) <= max(wet_index, 1) or len(parts) < 9:
        return None
    if not parts[0].isdigit() or not _STEP_TIME.fullmatch(parts[1]):
        return None
    return int(parts[wet_index]) if parts[wet_index].isdigit() else None


def read_hpc_companion_metrics(main_log_path: Path) -> dict[str, str | int]:
    """Observe a matching *.hpc.tlf without locking it or claiming completion.

    Matches only the main log's conventional same-folder, same-run-code name.
    Unknown counts are omitted. Repeated cell headers signal ambiguous
    domains; they are not added. Samples do not establish all-timestep coverage.
    Changing/truncated logs are reported rather than silently trusted.
    """
    companion = main_log_path.with_name(f"{main_log_path.stem}.hpc.tlf")
    result: dict[str, str | int] = {"HPC_Companion_Path": str(companion)}
    try:
        before = companion.stat()
    except FileNotFoundError:
        result["HPC_Companion_Status"] = "missing"
        return result
    except OSError as exc:
        logger.warning("Unable to inspect HPC companion {}: {}", companion, exc)
        result["HPC_Companion_Status"] = "unreadable"
        return result

    count_values: dict[str, int] = {}
    count_occurrences: dict[str, int] = {}
    wet_index: int | None = None
    wet_samples = 0
    observed_wet_max: int | None = None
    partial = False

    try:
        with companion.open("rb") as stream:
            while raw := stream.readline(_MAX_LINE_BYTES):
                if not raw.endswith(b"\n"):
                    # Skip partial EOF lines or oversized lines in bounded chunks.
                    partial = True
                    if len(raw) == _MAX_LINE_BYTES:
                        while segment := stream.readline(_MAX_LINE_BYTES):
                            if segment.endswith(b"\n"):
                                break
                    continue
                try:
                    line = raw.decode("utf-8")
                except UnicodeDecodeError:
                    line = raw.decode("cp1252", errors="replace")

                if match := _matched_count(line):
                    field, count = match
                    count_occurrences[field] = count_occurrences.get(field, 0) + 1
                    count_values.setdefault(field, count)

                words = line.split()
                if len(words) >= 9 and words[0].casefold() == "istep":
                    index = next((i for i, word in enumerate(words) if word.casefold() == "nwet"), None)
                    if index is not None:
                        wet_index = index
                        continue
                sample = _sampled_wet_count(line, wet_index)
                if sample is not None:
                    wet_samples += 1
                    observed_wet_max = sample if observed_wet_max is None else max(observed_wet_max, sample)

        after = companion.stat()
        changed = (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns)
    except OSError as exc:
        logger.warning("Unable to read HPC companion {}: {}", companion, exc)
        result["HPC_Companion_Status"] = "unreadable"
        return result

    if any(n > 1 for n in count_occurrences.values()):
        result["HPC_Companion_Status"] = "multiple_domains"
        # Even identical domain values do not prove a single global population.
        return result

    result.update(count_values)
    if wet_index is not None:
        result["HPC_nWet_Sampled_Records"] = wet_samples
        if observed_wet_max is not None:
            result["HPC_nWet_Observed_Max"] = observed_wet_max

    status: CompanionStatus = "observed"
    if changed:
        status = "changed_during_read"
    elif partial or ("HPC_Active_2D_Cells" not in result and wet_samples == 0):
        status = "incomplete"
    result["HPC_Companion_Status"] = status
    return result
