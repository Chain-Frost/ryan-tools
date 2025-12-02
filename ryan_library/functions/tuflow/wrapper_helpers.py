# ryan_library\functions\tuflow\wrapper_helpers.py
"""Helpers shared by TUFLOW wrapper/orchestrator scripts."""

from collections.abc import Collection, Sequence
from loguru import logger


def normalize_data_types(
    requested: Collection[str] | None,
    default: Sequence[str],
    accepted: Collection[str],
) -> tuple[list[str], list[str]]:
    """Return a de-duplicated list of requested types plus any that are invalid.

    Args:
        requested: User-requested data types (may be None).
        default: Fallback list if ``requested`` is falsy.
        accepted: Types considered valid for the workflow.
    """

    resolved: list[str] = list(dict.fromkeys(requested or default))
    accepted_set: set[str] = set(accepted)
    invalid: list[str] = [data_type for data_type in resolved if data_type not in accepted_set]
    return resolved, invalid


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
