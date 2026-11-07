# ryan_library/functions/tuflow/wrapper_helpers.py

# ryan_library\functions\tuflow\wrapper_helpers.py
"""Helpers shared by TUFLOW wrapper/orchestrator scripts."""

from collections.abc import Collection, Sequence
from pathlib import Path

from loguru import logger


def normalize_data_types(
    requested: Collection[str] | None,
    default: Sequence[str],
    accepted: Collection[str],
) -> tuple[list[str], list[str]]:
    """Return a de-duplicated list of requested types plus any that are invalid.

    Args:
        requested: User-requested data types (may be None).
        default: Fallback only when ``requested`` is None; explicit empty selections are retained.
        accepted: Types considered valid for the workflow.
    """
    resolved: list[str] = list(dict.fromkeys(default if requested is None else requested))
    accepted_set: set[str] = set(accepted)
    invalid: list[str] = [data_type for data_type in resolved if data_type not in accepted_set]
    return resolved, invalid


def validate_discovery_settings(
    *,
    paths_to_process: Collection[Path],
    selected_data_types: Collection[str],
    accepted_data_types: Collection[str],
    context: str,
) -> None:
    """Reject invalid scan plans before configuring logs or report outputs."""
    if not paths_to_process:
        message = f"{context}: at least one input directory is required"
        raise ValueError(message)
    if not selected_data_types:
        message = f"{context}: no data types selected"
        raise ValueError(message)
    invalid = sorted(set(selected_data_types).difference(accepted_data_types))
    if invalid:
        message = f"{context}: unsupported data types {invalid}; accepted: {sorted(set(accepted_data_types))}"
        raise ValueError(message)

    for candidate in paths_to_process:
        path = Path(candidate).expanduser().absolute()
        if not path.exists():
            message = f"{context}: input directory does not exist: {path}"
            raise FileNotFoundError(message)
        if not path.is_dir():
            message = f"{context}: input path is not a directory: {path}"
            raise NotADirectoryError(message)


def warn_on_invalid_types(
    *,
    invalid_types: Sequence[str],
    accepted_types: Collection[str],
    context: str,
) -> None:
    """Log a warning about unexpected types while continuing execution."""
    if not invalid_types:
        return

    logger.warning(
        f"{context}: unrecognised data types requested: {list(invalid_types)}. "
        f"Acceptable: {sorted(set(accepted_types))}. Will proceed with supplied list."
    )
