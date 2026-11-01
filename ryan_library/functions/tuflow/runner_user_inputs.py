"""Normalize user-editable TUFLOW runner values before planning or I/O.

The copied wrapper accepts concise strings, Paths and lists. This module
converts those conveniences into one strict internal representation and emits
actionable ValueErrors rather than silently coercing incorrect inputs.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from enum import StrEnum
from pathlib import Path


def choice[T: StrEnum](name: str, value: object, options: type[T]) -> T:
    """Accept short enum members or the equivalent raw string, case-insensitively."""
    if isinstance(value, options):
        return value
    if isinstance(value, str):
        for item in options:
            if item.value.casefold() == value.casefold():
                return item
    examples = ", ".join(f"{options.__name__}.{item.name} ({item.value!r})" for item in options)
    message = f"{name}={value!r} is invalid. Valid choices: {examples}."
    raise ValueError(message)


def path(name: str, value: object) -> Path:
    """Preserve mapped-drive and UNC spelling; never resolve filesystem aliases."""
    if isinstance(value, str):
        if not value.strip():
            message = f"{name} is blank. Enter a path string or Path object."
            raise ValueError(message)
        if value != value.strip():
            message = f"{name} has leading/trailing whitespace in {value!r}; remove it."
            raise ValueError(message)
    elif not isinstance(value, Path):
        message = f"{name} requires a path string or Path, not {type(value).__name__}."
        raise ValueError(message)
    return Path(value)


def input_paths(value: object) -> tuple[Path, ...]:
    """A raw string names one source; a list or tuple supplies multiple sources."""
    if value is None:
        return ()
    sources: Sequence[object]
    if isinstance(value, (str, Path)):
        sources = (value,)
    elif isinstance(value, (list, tuple)):
        sources = value
    else:
        message = "INPUT_FILES must be one path or a list/tuple, e.g. ['runs1.bat', 'runs2.txt']."
        raise ValueError(message)
    result = tuple(path(f"INPUT_FILES[{index}]", source) for index, source in enumerate(sources))
    for source in result:
        if source.suffix.casefold() not in {".bat", ".cmd", ".txt"}:
            message = f"INPUT_FILES expects .bat, .cmd or .txt sources; got {source!s}."
            raise ValueError(message)
    return result


def gpu_groups(value: object) -> tuple[tuple[str, ...], ...]:
    """Top-level entries are concurrent slots; nested GPU strings share one slot."""
    if value is None:
        return ()
    candidates: Sequence[object]
    if isinstance(value, str):
        candidates = (value,)
    elif isinstance(value, (list, tuple)):
        candidates = value
    else:
        message = "GPU_DEVICES requires '-pu1', ['-pu0', '-pu1'], or [['-pu0', '-pu1']]."
        raise ValueError(message)
    groups: list[tuple[str, ...]] = []
    for index, group in enumerate(candidates):
        tokens: Sequence[object]
        if isinstance(group, str):
            tokens = (group,)
        elif isinstance(group, (list, tuple)):
            tokens = group
        else:
            message = f"GPU_DEVICES[{index}] must be a '-puN' string or list/tuple of '-puN' strings."
            raise ValueError(message)
        message = f"GPU_DEVICES[{index}] needs one or more '-puN' strings; use [] for automatic placement."
        if not tokens:
            raise ValueError(message)
        validated: list[str] = []
        for token in tokens:
            if not isinstance(token, str):
                raise ValueError(message)
            validated.append(token)
        groups.append(tuple(validated))
    return tuple(groups)


def batch_commands(value: object) -> str:
    """Validate the user-edited engine command string before any output."""
    if not isinstance(value, str):
        message = "BATCH_COMMANDS must be a string, e.g. '-b -t -nlc'."
        raise ValueError(message)
    return value


def boolean(name: str, value: object) -> bool:
    """Do not silently treat 'False', 0 or other truthy/falsy objects as booleans."""
    if not isinstance(value, bool):
        message = f"{name} requires True or False, not {value!r}."
        raise ValueError(message)
    return value


def finite_number(name: str, value: object, *, positive: bool) -> float:
    """Avoid infinite/NaN dashboard rates, negative launch delays and bool-as-int."""
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        message = f"{name} must be a finite number ({'> 0' if positive else '>= 0'}), not {value!r}."
        raise ValueError(message)
    if (positive and value <= 0) or (not positive and value < 0):
        message = f"{name} must be {'greater than zero' if positive else 'nonnegative'}, not {value!r}."
        raise ValueError(message)
    return float(value)


def _nonempty_variable_list(key: str, entries: Sequence[object]) -> list[str]:
    """Accept an explicit list only when all entries are useful strings."""
    message = f"RUN_VARIABLES_RAW[{key!r}] needs nonempty strings, e.g. ['Dry', 'Wet Season']."
    if not entries:
        raise ValueError(message)
    strings: list[str] = []
    for item in entries:
        if not isinstance(item, str) or not item.strip():
            raise ValueError(message)
        strings.append(item)
    return strings


def run_variables(value: object) -> dict[str, str | list[str]]:
    """Reject invalid or empty eN/sN inputs rather than silently dropping them."""
    if not isinstance(value, dict):
        message = "RUN_VARIABLES_RAW must be a dictionary, e.g. {'e1': '01.0p 05.0p'}."
        raise ValueError(message)
    validated: dict[str, str | list[str]] = {}
    seen: set[str] = set()
    entries: Mapping[object, object] = value
    for key, raw in entries.items():
        if not isinstance(key, str) or re.fullmatch(r"[es][1-9]", key, flags=re.IGNORECASE) is None:
            message = f"RUN_VARIABLES_RAW key {key!r} must be e1-e9 or s1-s9."
            raise ValueError(message)
        if key.casefold() in seen:
            message = f"RUN_VARIABLES_RAW repeats variable {key!r} with different capitalization."
            raise ValueError(message)
        seen.add(key.casefold())
        if isinstance(raw, str):
            if not raw.split():
                message = f"RUN_VARIABLES_RAW[{key!r}] is blank. Remove this key to disable it."
                raise ValueError(message)
            validated[key] = raw
        elif isinstance(raw, list):
            items: Sequence[object] = raw
            validated[key] = _nonempty_variable_list(key, items)
        else:
            message = f"RUN_VARIABLES_RAW[{key!r}] requires a string or list of strings."
            raise ValueError(message)
    return validated


def priority_order(value: object) -> str | None:
    """Validate optional flag ordering and normalize keys for case-insensitive use."""
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        message = "PRIORITY_ORDER requires flag keys such as 'e3 e2 e1', or None."
        raise ValueError(message)
    keys = value.split()
    if any(re.fullmatch(r"[es][1-9]", key, flags=re.IGNORECASE) is None for key in keys):
        message = f"PRIORITY_ORDER={value!r} has invalid keys; use e1-e9 and s1-s9."
        raise ValueError(message)
    if len({key.casefold() for key in keys}) != len(keys):
        message = "PRIORITY_ORDER repeats a flag; list each eN/sN key at most once."
        raise ValueError(message)
    return " ".join(key.casefold() for key in keys)
