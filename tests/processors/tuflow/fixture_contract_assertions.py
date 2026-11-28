"""Shared test-only enforcement for the pinned #228 fixture support contract."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

from ryan_library.processors.tuflow.base_processor import BaseProcessor

CONTRACT_PATH = Path(__file__).with_name("fixture_contract_wp11.json")
OUTCOMES = {"supported", "empty", "unsupported", "malformed"}


def load_declared_cases() -> dict[str, dict[str, Any]]:
    """Load the bounded manifest; registered suffixes do not imply support."""
    manifest: dict[str, Any] = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    cases: list[dict[str, Any]] = manifest["cases"]
    return {str(case["id"]): case for case in cases}


def assert_declared_outcome(processor: BaseProcessor, case: dict[str, Any]) -> None:
    """Fail on incorrect success, rejection, row population, schema or values."""
    fixture: str = case["id"]
    outcome: str = case["outcome"]
    assert outcome in OUTCOMES, f"{fixture}: unrecognised outcome {outcome!r}"

    if outcome != "supported":
        assert not processor.processed, f"{fixture}: declared {outcome} fixture processed successfully"
        assert processor.df.empty, f"{fixture}: declared {outcome} fixture retained rows after rejection"
        return

    assert processor.processed, f"{fixture}: supported fixture was rejected"
    assert not processor.df.empty, f"{fixture}: supported fixture has no processed rows"
    minimum_rows: int = case["minimum_rows"]
    assert len(processor.df) >= minimum_rows, f"{fixture}: supported fixture lost required rows"
    required_columns: list[str] = case["columns"]
    missing = sorted(set(required_columns).difference(processor.df.columns))
    assert not missing, f"{fixture}: supported fixture lost columns {missing}"
    expected_values: dict[str, str | float] = case.get("values", {})
    for column, expected in expected_values.items():
        actual = processor.df.iloc[0][column]
        if isinstance(expected, float):
            assert math.isclose(float(actual), expected, rel_tol=1e-10, abs_tol=1e-10), (
                f"{fixture}: incorrect pinned value for {column}: {actual!r} != {expected!r}"
            )
        else:
            assert str(actual) == expected, (
                f"{fixture}: incorrect pinned value for {column}: {actual!r} != {expected!r}"
            )
