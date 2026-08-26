"""Shared validation and filename helpers for TUFLOW raster-result workflows."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
import re

from ryan_library.classes.tuflow_string_classes import TuflowStringParser


def result_type_from_parser(*, parser: TuflowStringParser, result_types: Sequence[str]) -> str | None:
    """Return a requested result type identified case-insensitively by the parser."""
    if parser.data_type is None:
        return None
    canonical_types: dict[str, str] = {value.casefold(): value for value in result_types}
    return canonical_types.get(parser.data_type.casefold())


def require_component_text(*, value: str | None, component: str, filename: str) -> str:
    """Return non-empty parsed component text or identify the malformed filename."""
    if value:
        return value
    raise ValueError(f"Parsed {component} text was empty in {filename}")


def replace_filename_component(*, filename: str, old_component: str, new_component: str | None) -> str:
    """Replace or remove one complete ``_``- or ``+``-delimited component.

    Delimiters are retained exactly as supplied so the output keeps the input
    filename's separator style. When removing a component, its following
    delimiter is removed as well (or its preceding delimiter at the end).
    """
    parts: list[str] = re.split(r"([_+])", filename)
    indexes: list[int] = [
        index for index in range(0, len(parts), 2) if parts[index].casefold() == old_component.casefold()
    ]
    if len(indexes) != 1:
        raise ValueError(f"Expected one {old_component!r} component in {filename!r}; found {len(indexes)}")
    component_index = indexes[0]
    if new_component is None:
        if component_index + 1 < len(parts):
            del parts[component_index : component_index + 2]
        elif component_index > 0:
            del parts[component_index - 1 : component_index + 1]
        else:
            del parts[component_index]
    else:
        parts[component_index] = new_component
    return "".join(parts)


def format_user_template(*, template: str, values: Mapping[str, object], description: str) -> str:
    """Format a user-editable template and report unknown placeholders clearly."""
    try:
        return template.format_map(values)
    except KeyError as error:
        available = ", ".join(sorted(values))
        raise ValueError(
            f"Unknown placeholder {error.args[0]!r} in {description}; available placeholders: {available}"
        ) from error


def validate_output_filename(filename: str) -> str:
    """Reject wildcards, paths, and Windows-invalid characters in an output name."""
    invalid_characters: set[str] = set('<>:"/\\|?*')
    found_invalid: list[str] = sorted(invalid_characters.intersection(filename))
    if found_invalid:
        raise ValueError(
            f"Output filename {filename!r} contains invalid characters: {''.join(found_invalid)}. "
            "Use wildcards only in the input glob and named placeholders in the output template."
        )
    if Path(filename).name != filename or filename in {"", ".", ".."}:
        raise ValueError(f"Output filename template must produce a filename, not a path: {filename!r}")
    return filename
