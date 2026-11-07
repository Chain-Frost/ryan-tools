"""Regression contracts for malformed and empty TUFLOW discovery (#195)."""

from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, patch

import pytest

from ryan_library.classes.suffixes_and_dtypes import SuffixesConfig
from ryan_library.functions.tuflow.tuflow_common import collect_files
from ryan_library.functions.tuflow.wrapper_helpers import normalize_data_types, validate_discovery_settings
from ryan_library.orchestrators.tuflow._combination_workflow import CombinationExporter, execute_combination_workflow


def _collect(*roots: Path, data_types: list[str] | None = None) -> list[Path]:
    return collect_files(
        paths_to_process=list(roots),
        include_data_types=["PO"] if data_types is None else data_types,
        suffixes_config=SuffixesConfig.get_instance(),
    )


def test_explicit_empty_selection_does_not_revert_to_default() -> None:
    assert normalize_data_types(None, ("PO",), {"PO"}) == (["PO"], [])
    assert normalize_data_types([], ("PO",), {"PO"}) == ([], [])
    assert normalize_data_types(["PO", "BOGUS"], ("PO",), {"PO"}) == (["PO", "BOGUS"], ["BOGUS"])


@pytest.mark.parametrize("data_types", [[], ["BOGUS"], ["PO", "BOGUS"]])
def test_invalid_type_selection_fails_before_scan(tmp_path: Path, data_types: list[str]) -> None:
    with pytest.raises(ValueError, match="data types"):
        _collect(tmp_path, data_types=data_types)


def test_valid_empty_directory_is_distinct_from_missing_root(tmp_path: Path) -> None:
    assert _collect(tmp_path) == []
    with pytest.raises(FileNotFoundError, match="input directory does not exist"):
        _collect(tmp_path / "misspelt")


def test_mixed_valid_missing_root_never_returns_a_partial_batch(tmp_path: Path) -> None:
    valid = tmp_path / "valid"
    valid.mkdir()
    (valid / "run_PO.csv").write_text("data", encoding="utf-8")
    with pytest.raises(FileNotFoundError, match="misspelt"):
        _collect(valid, tmp_path / "misspelt")


def test_matched_zero_byte_input_is_invalid_not_excluded(tmp_path: Path) -> None:
    (tmp_path / "empty_PO.csv").touch()
    with pytest.raises(OSError, match="empty_PO.csv"):
        _collect(tmp_path)


def test_mixed_zero_byte_and_populated_inputs_fail_as_one_batch(tmp_path: Path) -> None:
    (tmp_path / "good_PO.csv").write_text("data", encoding="utf-8")
    (tmp_path / "empty_PO.csv").touch()
    with pytest.raises(OSError, match="empty_PO.csv"):
        _collect(tmp_path)


def test_valid_files_remain_deterministically_ordered(tmp_path: Path) -> None:
    files = [tmp_path / "z_PO.csv", tmp_path / "a_PO.csv"]
    for path in files:
        path.write_text("data", encoding="utf-8")
    assert _collect(tmp_path, tmp_path) == sorted(files)


def test_missing_or_unreadable_matched_input_is_not_a_valid_skip(tmp_path: Path) -> None:
    source = tmp_path / "sample_PO.csv"
    source.write_text("data", encoding="utf-8")
    with patch("ryan_library.functions.tuflow.tuflow_common.is_non_zero_file", return_value=False):
        with pytest.raises(OSError, match="sample_PO.csv"):
            _collect(tmp_path)


@pytest.mark.parametrize("dashboard", [True, False])
def test_combination_preflight_rejects_missing_root_before_logging(tmp_path: Path, dashboard: bool) -> None:
    missing = tmp_path / "missing"
    with (
        patch("ryan_library.orchestrators.tuflow._combination_workflow.setup_logger") as log_setup,
        pytest.raises(FileNotFoundError, match="missing"),
    ):
        execute_combination_workflow(
            paths_to_process=[tmp_path, missing],
            include_data_types=["PO"],
            default_data_types=("PO",),
            accepted_data_types=frozenset({"PO"}),
            context_name="PO",
            export_results=cast("CombinationExporter", MagicMock()),
            use_live_dashboard=dashboard,
        )
    log_setup.assert_not_called()


@pytest.mark.parametrize("selection", [[], ["PO", "BOGUS"]])
def test_combination_preflight_rejects_bad_types_before_logging(tmp_path: Path, selection: list[str]) -> None:
    with (
        patch("ryan_library.orchestrators.tuflow._combination_workflow.setup_logger") as log_setup,
        pytest.raises(ValueError, match="data types"),
    ):
        execute_combination_workflow(
            paths_to_process=[tmp_path],
            include_data_types=selection,
            default_data_types=("PO",),
            accepted_data_types=frozenset({"PO"}),
            context_name="PO",
            export_results=cast("CombinationExporter", MagicMock()),
            use_live_dashboard=False,
        )
    log_setup.assert_not_called()


def test_input_validation_preserves_spelling_in_error(tmp_path: Path) -> None:
    path = tmp_path / "wrong"
    with pytest.raises(FileNotFoundError) as error:
        validate_discovery_settings(
            paths_to_process=[path],
            selected_data_types=["PO"],
            accepted_data_types={"PO"},
            context="PO",
        )
    assert str(path) in str(error.value)
