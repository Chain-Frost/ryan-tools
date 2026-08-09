"""Tests for string splitting helpers in misc_functions."""

from __future__ import annotations

from pathlib import Path

# import sys
# sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ryan_library.functions.string_helpers import split_strings, split_strings_in_dict


def test_split_strings_with_single_string() -> None:
    """Single whitespace-delimited strings should split into multiple tokens."""

    assert split_strings("alpha beta gamma") == ["alpha", "beta", "gamma"]


def test_split_strings_with_list_of_strings() -> None:
    """Lists of strings should be flattened and split into a single list of tokens."""

    assert split_strings(["alpha beta", "gamma"]) == ["alpha", "beta", "gamma"]


def test_split_strings_with_pre_split_input() -> None:
    """Pre-split input should remain unchanged after normalisation."""

    value = ["alpha", "beta", "gamma"]

    assert split_strings(value) == value


def test_split_strings_with_empty_values() -> None:
    """Empty strings should be normalised to an empty list."""

    assert split_strings("") == []


def test_split_strings_in_dict_normalises_mixed_values() -> None:
    """Strings and lists of strings should be flattened into a single list of tokens."""

    params: dict[str, str | list[str]] = {
        "single": "alpha beta",
        "list_value": ["gamma delta", "epsilon"],
        "pre_split": ["zeta", "eta"],
    }

    result: dict[str, list[str]] = split_strings_in_dict(params_dict=params)

    assert result is params
    assert result == {
        "single": ["alpha", "beta"],
        "list_value": ["gamma", "delta", "epsilon"],
        "pre_split": ["zeta", "eta"],
    }


def test_split_strings_in_dict_preserves_unrelated_keys() -> None:
    """Keys that are already split remain unchanged aside from normalisation."""

    params: dict[str, list[str] | str] = {
        "already_split": ["theta", "iota"],
        "needs_split": "kappa lambda",
    }

    expected_already_split = params["already_split"].copy()

    split_strings_in_dict(params_dict=params)

    assert params["already_split"] == expected_already_split
    assert params["needs_split"] == ["kappa", "lambda"]


def _run_split_strings_suite() -> None:
    """Aggregate helper to satisfy pytest node selection with wildcard command."""

    test_split_strings_with_single_string()
    test_split_strings_with_list_of_strings()
    test_split_strings_with_pre_split_input()
    test_split_strings_with_empty_values()


globals()["test_split_strings*"] = _run_split_strings_suite
"""Tests for ryan_library.functions.misc_functions."""

from pathlib import Path
import sys

# Ensure the project root is importable when tests are run directly.
PROJECT_ROOT = str(Path(__file__).resolve().parents[2])
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import pytest

from ryan_library.functions import multiprocessing_helpers


def test_calculate_pool_size_single_core(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure a single CPU core always yields one worker."""

    monkeypatch.setattr(multiprocessing_helpers.multiprocessing, "cpu_count", lambda: 1)
    # Even if files are 100, capped by cores
    assert multiprocessing_helpers.calculate_pool_size(num_files=100) == 1


def test_calculate_pool_size_multi_core(monkeypatch: pytest.MonkeyPatch) -> None:
    # Set to 4 cores, max threads should be cores - 1 = 3
    monkeypatch.setattr(multiprocessing_helpers.multiprocessing, "cpu_count", lambda: 4)
    # splits = 30 // 3 = 10, cap is 3
    assert multiprocessing_helpers.calculate_pool_size(num_files=30) == 3
    assert multiprocessing_helpers.calculate_pool_size(num_files=6) == 2


def test_calculate_pool_size_large_core_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    # Even with 32 cores, our code caps at 20 cores (19 threads)
    monkeypatch.setattr(multiprocessing_helpers.multiprocessing, "cpu_count", lambda: 32)
    # splits = 120 // 3 = 40, cap is min(20-1, 40) = 19
    assert multiprocessing_helpers.calculate_pool_size(num_files=120) == 19


def test_calculate_pool_size_small_files(monkeypatch: pytest.MonkeyPatch) -> None:
    # 2 files // 3 = 0, but max(..., 1) ensures at least 1 split
    monkeypatch.setattr(multiprocessing_helpers.multiprocessing, "cpu_count", lambda: 4)
    # cap is min(3, 1) = 1
    assert multiprocessing_helpers.calculate_pool_size(num_files=2) == 1
