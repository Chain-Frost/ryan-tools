"""Reusable simulation argument parsing for TUFLOW launchers and preflight checks."""

from __future__ import annotations

import re
from collections.abc import Sequence
from pathlib import Path

_FLAG_PATTERN = re.compile(r"^-(?P<key>[es][1-9])$", re.IGNORECASE)
_PAIR_PATTERN = re.compile(r"(?<!\S)-(?P<key>[es][1-9])\s+(?P<value>\S+)", re.IGNORECASE)


def extract_run_flags(tokens: Sequence[str]) -> dict[str, str]:
    """Extract event/scenario switches from a subprocess argument sequence.

    Preserve the launcher's exact event names while trimming padding inserted
    for its batch-file output. Conflicting duplicate switches are rejected.
    """
    flags: dict[str, str] = {}
    for index, token in enumerate(tokens):
        match = _FLAG_PATTERN.fullmatch(token.strip())
        if match is None:
            continue
        key = match.group("key").lower()
        if index + 1 >= len(tokens) or not tokens[index + 1].strip():
            raise ValueError(f"Run switch -{key} has no value")
        value = tokens[index + 1].strip().strip('"')
        if value.startswith("-"):
            raise ValueError(f"Run switch -{key} has an invalid value: {value}")
        if key in flags and flags[key] != value:
            raise ValueError(f"Conflicting values for -{key}: {flags[key]} and {value}")
        flags[key] = value
    return flags


def extract_flag_pairs(line: str) -> list[tuple[str, str]]:
    """Extract ordered event/scenario pairs from an existing batch command line."""
    return [(match.group("key").lower(), match.group("value")) for match in _PAIR_PATTERN.finditer(line)]


def read_run_commands(path: Path) -> list[dict[str, str]]:
    """Read exact simulation combinations from launcher-exported batch commands.

    Requires at least one event/scenario switch on each simulation line.
    Silently ignores SET/PAUSE/REM lines and empty/comment lines.
    """
    if not path.is_file():
        raise FileNotFoundError(f"Run commands file does not exist: {path}")
    runs: list[dict[str, str]] = []
    for line_number, raw in enumerate(path.read_text(encoding="utf-8-sig").splitlines(), start=1):
        line = raw.strip()
        if not line or line.lstrip().startswith(("#", ";", "::", "//")):
            continue
        if re.match(r"^(?:@?rem|@?echo|set|pause)\b", line, re.IGNORECASE):
            continue
        pairs = extract_flag_pairs(line)
        if not pairs:
            continue
        switches = [token for token in line.split() if _FLAG_PATTERN.fullmatch(token)]
        if len(pairs) != len(switches):
            raise ValueError(f"Malformed event/scenario switch on line {line_number} of {path}")
        flags: dict[str, str] = {}
        for key, value in pairs:
            if value.startswith("-"):
                raise ValueError(f"Missing value for -{key} on line {line_number}")
            if key in flags and flags[key] != value:
                raise ValueError(f"Conflicting -{key} values on line {line_number}")
            flags[key] = value.strip('"')
        runs.append(flags)
    if not runs:
        raise ValueError(f"No event/scenario combinations found in {path}")
    return runs
