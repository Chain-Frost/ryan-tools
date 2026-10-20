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
            message = f"Run switch -{key} has no value"
            raise ValueError(message)
        value = tokens[index + 1].strip().strip('"')
        if value.startswith("-"):
            message = f"Run switch -{key} has an invalid value: {value}"
            raise ValueError(message)
        if key in flags and flags[key] != value:
            message = f"Conflicting values for -{key}: {flags[key]} and {value}"
            raise ValueError(message)
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
        message = f"Run commands file does not exist: {path}"
        raise FileNotFoundError(message)
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
            message = f"Malformed event/scenario switch on line {line_number} of {path}"
            raise ValueError(message)
        flags: dict[str, str] = {}
        for key, value in pairs:
            if value.startswith("-"):
                message = f"Missing value for -{key} on line {line_number}"
                raise ValueError(message)
            if key in flags and flags[key] != value:
                message = f"Conflicting -{key} values on line {line_number}"
                raise ValueError(message)
            flags[key] = value.strip('"')
        runs.append(flags)
    if not runs:
        message = f"No event/scenario combinations found in {path}"
        raise ValueError(message)
    return runs
