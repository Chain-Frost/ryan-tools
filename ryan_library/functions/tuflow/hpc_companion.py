"""Opt-in, read-only observations from a conventional TUFLOW HPC companion log.

A companion row is not a full-timestep record. This reader never infers a
true solver-wide nWet maximum or merges ambiguous domain cell populations.
No companion read happens unless the caller explicitly opts in.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from loguru import logger

CompanionStatus = Literal["missing", "observed", "incomplete", "changed_during_read", "multiple_domains", "unreadable"]

_MAX_LINE_BYTES = 8192
_STEP_TIME = re.compile(r"\d+:[0-5]\d:[0-5]\d(?:\.\d+)?")
_COUNT_PATTERNS: dict[str, re.Pattern[str]] = {
    "HPC_Active_2D_Cells": re.compile(r"^\s*Number active cells\s*=\s*(\d+)\s*$", re.IGNORECASE),
    "HPC_Sealed_Wall_Cells": re.compile(r"^\s*Number sealed wall cells\s*=\s*(\d+)\s*$", re.IGNORECASE),
    "HPC_Groundwater_Cells": re.compile(r"^\s*Number groundwater cells\s*=\s*(\d+)\s*$", re.IGNORECASE),
}


def _matched_count(line: str) -> tuple[str, int] | None:
    for field_name, pattern in _COUNT_PATTERNS.items():
        if match := pattern.match(line):
            return field_name, int(match.group(1))
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


@dataclass(slots=True)
class _CompanionObservation:
    counts: dict[str, int] = field(default_factory=dict)
    duplicated_count_fields: set[str] = field(default_factory=set)
    wet_index: int | None = None
    wet_samples: int = 0
    wet_max: int | None = None
    partial: bool = False

    def add_line(self, line: str) -> None:
        if item := _matched_count(line):
            field_name, count = item
            if field_name in self.counts:
                self.duplicated_count_fields.add(field_name)
            else:
                self.counts[field_name] = count

        words = line.split()
        if len(words) >= 9 and words[0].casefold() == "istep":
            index = next((i for i, word in enumerate(words) if word.casefold() == "nwet"), None)
            if index is not None:
                self.wet_index = index
                return

        sample = _sampled_wet_count(line, self.wet_index)
        if sample is not None:
            self.wet_samples += 1
            self.wet_max = sample if self.wet_max is None else max(self.wet_max, sample)


def _complete_hpc_lines(path: Path, observation: _CompanionObservation) -> Iterator[str]:
    """Stream only complete bounded lines; do not consume partially written rows."""
    with path.open("rb") as stream:
        while raw := stream.readline(_MAX_LINE_BYTES):
            if not raw.endswith(b"\n"):
                observation.partial = True
                # Discard the rest of any oversized line, in bounded chunks.
                if len(raw) == _MAX_LINE_BYTES:
                    while segment := stream.readline(_MAX_LINE_BYTES):
                        if segment.endswith(b"\n"):
                            break
                continue
            try:
                yield raw.decode("utf-8")
            except UnicodeDecodeError:
                yield raw.decode("cp1252", errors="replace")


def _observation_status(observation: _CompanionObservation, *, changed: bool) -> CompanionStatus:
    if observation.duplicated_count_fields:
        return "multiple_domains"
    if changed:
        return "changed_during_read"
    if observation.partial or ("HPC_Active_2D_Cells" not in observation.counts and observation.wet_samples == 0):
        return "incomplete"
    return "observed"


def read_hpc_companion_metrics(main_log_path: Path) -> dict[str, str | int]:
    """Read an explicitly requested, matching HPC log, with sampled provenance.

    A companion has the same run-code stem and folder as the main TLF.
    The reader makes no exclusive lock and does not claim full-run sample
    coverage or aggregate counts from a potentially multiple-domain log.
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

    observation = _CompanionObservation()
    try:
        for line in _complete_hpc_lines(companion, observation):
            observation.add_line(line)
        after = companion.stat()
    except OSError as exc:
        logger.warning("Unable to read HPC companion {}: {}", companion, exc)
        result["HPC_Companion_Status"] = "unreadable"
        return result

    changed = (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns)
    status = _observation_status(observation, changed=changed)
    result["HPC_Companion_Status"] = status
    if status == "multiple_domains":
        return result

    result.update(observation.counts)
    if observation.wet_index is not None:
        result["HPC_nWet_Sampled_Records"] = observation.wet_samples
    if observation.wet_max is not None:
        result["HPC_nWet_Observed_Max"] = observation.wet_max
    return result
