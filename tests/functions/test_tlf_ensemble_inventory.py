"""Regression checks for inferred filename ensembles."""

from pathlib import Path

import pytest

from ryan_library.functions.tlf_ensemble_inventory import infer_ensemble_inventory, scan_ensemble_inventory


def test_missing_whole_ensemble_and_tp() -> None:
    result = infer_ensemble_inventory(
        [
            "Model_EXG_01p_060m_TP01.tlf",
            "Model_EXG_01p_060m_TP01.hpc.tlf",
            "Model_DEV_02p_120m_TP02.tlf",
        ]
    )
    assert result.log_count == 3
    assert result.run_count == 2
    assert result.expected_count == 80
    assert len(result.missing) == 78
    assert "Model_DEV_01p_060m_TP10" in result.missing


def test_without_tp_or_duration() -> None:
    result = infer_ensemble_inventory(["Model_EXG_01p.tlf", "Model_DEV_02p.tlf"])
    assert set(result.missing) == {"Model_EXG_02p", "Model_DEV_01p"}
    assert result.expected_count == 4


def test_multiple_scenario_fields() -> None:
    result = infer_ensemble_inventory(["Model_EXG_01p_Near.tlf", "Model_DEV_02p_Far.tlf"])
    assert result.expected_count == 8
    assert "Model_DEV_01p_Near" in result.missing


def test_tp_normalization_and_layout_separation() -> None:
    result = infer_ensemble_inventory(["M_01p_tp1.tlf", "M_01p_TP01.hpc.tlf", "M_01p.tlf"])
    assert result.run_count == 2
    assert result.family_count == 2
    assert len(result.missing) == 9


def test_limits_and_no_logs() -> None:
    with pytest.raises(ValueError, match="exceed"):
        infer_ensemble_inventory(["M_TP01.tlf"], max_combinations=9)
    with pytest.raises(ValueError, match="No TLF"):
        infer_ensemble_inventory([])


def test_recursive_scan(tmp_path: Path) -> None:
    (tmp_path / "M_01p.tlf").touch()
    child = tmp_path / "child"
    child.mkdir()
    (child / "M_02p.TLF").touch()
    assert scan_ensemble_inventory(tmp_path).run_count == 2
    assert scan_ensemble_inventory(tmp_path, recursive=False).run_count == 1
    with pytest.raises(NotADirectoryError):
        scan_ensemble_inventory(tmp_path / "absent")
