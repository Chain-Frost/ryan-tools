"""Normalize user-editable TUFLOW runner values before planning or I/O.

The copied wrapper accepts concise strings, Paths and lists. This module
converts those conveniences into one strict internal representation and emits
actionable ValueErrors rather than silently coercing incorrect inputs.
"""

from __future__ import annotations

import math
import re
from enum import StrEnum
from pathlib import Path
from typing import cast


def choice[T: StrEnum](name: str, value: T | str, options: type[T]) -> T:
    """Accept short enum members or the equivalent raw string, case-insensitively."""
    if isinstance(value, options):
        return value
    if isinstance(value, str):
        for item in options:
            if item.value.casefold() == value.casefold():
                return item
    examples = ", ".join(f"{options.__name__}.{item.name} ({item.value!r})" for item in options)
    raise ValueError(f"{name}={value!r} is invalid. Valid choices: {examples}.")


def path(name: str, value: str | Path) -> Path:
    """Preserve mapped-drive and UNC spelling; never resolve filesystem aliases."""
    if isinstance(value, str):
        if not value.strip():
            raise ValueError(f"{name} is blank. Enter a path string or Path object.")
        if value != value.strip():
            raise ValueError(f"{name} has leading/trailing whitespace in {value!r}; remove it.")
    elif not isinstance(value, Path):
        raise ValueError(f"{name} requires a path string or Path, not {type(value).__name__}.")
    return Path(value)


def input_paths(value: str | Path | list[str | Path] | tuple[str | Path, ...] | None) -> tuple[Path, ...]:
    """A raw string names one source; a list or tuple supplies multiple sources."""
    if value is None:
        return ()
    if isinstance(value, (str, Path)):
        sources = (value,)
    elif isinstance(value, (list, tuple)):
        sources = value
    else:
        raise ValueError("INPUT_FILES must be one path or a list/tuple, e.g. ['runs1.bat', 'runs2.txt'].")
    result = tuple(path(f"INPUT_FILES[{index}]", source) for index, source in enumerate(sources))
    for source in result:
        if source.suffix.casefold() not in {".bat", ".cmd", ".txt"}:
            raise ValueError(f"INPUT_FILES expects .bat, .cmd or .txt sources; got {source!s}.")
    return result


def gpu_groups(
    value: str | list[str | list[str] | tuple[str, ...]] | tuple[str | list[str] | tuple[str, ...], ...] | None,
) -> tuple[tuple[str, ...], ...]:
    """Top-level entries are concurrent slots; nested GPU strings share one slot."""
    if value is None:
        return ()
    if isinstance(value, str):
        candidates = (value,)
    elif isinstance(value, (list, tuple)):
        candidates = value
    else:
        raise ValueError("GPU_DEVICES requires '-pu1', ['-pu0', '-pu1'], or [['-pu0', '-pu1']].")
    groups: list[tuple[str, ...]] = []
    for index, group in enumerate(candidates):
        if isinstance(group, str):
            tokens = (group,)
        elif isinstance(group, (list, tuple)):
            tokens = tuple(group)
        else:
            raise ValueError(f"GPU_DEVICES[{index}] must be a '-puN' string or list/tuple of '-puN' strings.")
        if not tokens or any(not isinstance(token, str) for token in tokens):
            raise ValueError(f"GPU_DEVICES[{index}] needs one or more '-puN' strings; use [] for automatic placement.")
        groups.append(tokens)
    return tuple(groups)


def boolean(name: str, value: object) -> bool:
    """Do not silently treat 'False', 0 or other truthy/falsy objects as booleans."""
    if not isinstance(value, bool):
        raise ValueError(f"{name} requires True or False, not {value!r}.")
    return value


def finite_number(name: str, value: object, *, positive: bool) -> float:
    """Avoid infinite/NaN dashboard rates, negative launch delays and bool-as-int."""
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"{name} must be a finite number ({'> 0' if positive else '>= 0'}), not {value!r}.")
    if (positive and value <= 0) or (not positive and value < 0):
        raise ValueError(f"{name} must be {'greater than zero' if positive else 'nonnegative'}, not {value!r}.")
    return float(value)


def run_variables(value: object) -> dict[str, str | list[str]]:
    """Reject invalid or empty eN/sN inputs rather than silently dropping them."""
    if not isinstance(value, dict):
        raise ValueError("RUN_VARIABLES_RAW must be a dictionary, e.g. {'e1': '01.0p 05.0p'}.")
    validated: dict[str, str | list[str]] = {}
    seen: set[str] = set()
    for key, raw in value.items():
        if not isinstance(key, str) or re.fullmatch(r"[es][1-9]", key, flags=re.IGNORECASE) is None:
            raise ValueError(f"RUN_VARIABLES_RAW key {key!r} must be e1-e9 or s1-s9.")
        if key.casefold() in seen:
            raise ValueError(f"RUN_VARIABLES_RAW repeats variable {key!r} with different capitalization.")
        seen.add(key.casefold())
        if isinstance(raw, str):
            if not raw.split():
                raise ValueError(f"RUN_VARIABLES_RAW[{key!r}] is blank. Remove this key to disable it.")
            validated[key] = raw
        elif isinstance(raw, list):
            if not raw or any(not isinstance(item, str) or not item.strip() for item in raw):
                raise ValueError(f"RUN_VARIABLES_RAW[{key!r}] needs nonempty strings, e.g. ['Dry', 'Wet Season'].")
            validated[key] = cast(list[str], raw)
        else:
            raise ValueError(f"RUN_VARIABLES_RAW[{key!r}] requires a string or list of strings.")
    return validated


def priority_order(value: object) -> str | None:
    """Validate optional flag ordering and normalize keys for case-insensitive use."""
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError("PRIORITY_ORDER requires flag keys such as 'e3 e2 e1', or None.")
    keys = value.split()
    if any(re.fullmatch(r"[es][1-9]", key, flags=re.IGNORECASE) is None for key in keys):
        raise ValueError(f"PRIORITY_ORDER={value!r} has invalid keys; use e1-e9 and s1-s9.")
    if len({key.casefold() for key in keys}) != len(keys):
        raise ValueError("PRIORITY_ORDER repeats a flag; list each eN/sN key at most once.")
    return " ".join(key.casefold() for key in keys)
