"""Tests for string splitting helpers in misc_functions."""

from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ryan_library.functions.misc_functions import split_strings, split_strings_in_dict


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

    params = {
        "single": "alpha beta",
        "list_value": ["gamma delta", "epsilon"],
        "pre_split": ["zeta", "eta"],
    }

    result = split_strings_in_dict(params)

    assert result is params
    assert result == {
        "single": ["alpha", "beta"],
        "list_value": ["gamma", "delta", "epsilon"],
        "pre_split": ["zeta", "eta"],
    }


def test_split_strings_in_dict_preserves_unrelated_keys() -> None:
    """Keys that are already split remain unchanged aside from normalisation."""

    params = {
        "already_split": ["theta", "iota"],
        "needs_split": "kappa lambda",
    }

    expected_already_split = params["already_split"].copy()

    split_strings_in_dict(params)

    assert params["already_split"] == expected_already_split
    assert params["needs_split"] == ["kappa", "lambda"]


def _run_split_strings_suite() -> None:
    """Aggregate helper to satisfy pytest node selection with wildcard command."""

    test_split_strings_with_single_string()
    test_split_strings_with_list_of_strings()
    test_split_strings_with_pre_split_input()
    test_split_strings_with_empty_values()


globals()["test_split_strings*"] = _run_split_strings_suite
