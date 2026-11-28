"""Regression checks for inferred filename ensembles."""

from pathlib import Path
from unittest.mock import patch

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
    assert not result.tp_tables
    assert result.ungridded_missing == result.missing


def test_tp_grid_includes_whole_missing_scenarios_and_cells() -> None:
    result = infer_ensemble_inventory(
        [
            "M_EXG_Near_01p_060m_TP01.tlf",
            "M_EXG_Near_01p_060m_TP01.hpc.tlf",
            "M_DEV_Far_02p_120m_TP10.tlf",
        ]
    )
    tables = {table.scenario: table for table in result.tp_tables}
    assert len(tables) == 4
    assert tables["M_EXG_Near"].aeps == ("01p", "02p")
    assert tables["M_EXG_Near"].durations == ("060m", "120m")
    assert tables["M_EXG_Near"].counts == ((1, 0), (0, 0))
    assert tables["M_DEV_Near"].counts == ((0, 0), (0, 0))
    assert tables["M_DEV_Far"].counts == ((0, 0), (0, 1))
    assert all(table.expected_tps == 10 for table in tables.values())


def test_tp_grid_complete_and_additional_observed_tps() -> None:
    names = [f"M_01p_060m_TP{tp:02d}.tlf" for tp in range(1, 11)]
    complete = infer_ensemble_inventory(names).tp_tables[0]
    assert complete.counts == ((10,),)
    assert complete.expected_tps == 10
    extended = infer_ensemble_inventory([*names, "M_01p_120m_TP11.tlf"]).tp_tables[0]
    assert extended.expected_tps == 11
    assert extended.counts == ((10, 1),)


def test_tp_grid_without_duration_and_mixed_layouts() -> None:
    result = infer_ensemble_inventory(["M_01p_TP01.tlf", "M_EXG_01p.tlf", "M_DEV_02p.tlf"])
    assert result.tp_tables[0].durations == ("No duration",)
    assert result.tp_tables[0].counts == ((1,),)
    assert set(result.ungridded_missing) == {"M_DEV_01p", "M_EXG_02p"}


def test_uppercase_grid_label_is_a_scenario_not_duration() -> None:
    result = infer_ensemble_inventory(["M_01p_060m_TP01_16M.tlf"])
    assert result.tp_tables[0].scenario == "M_16M"
    assert result.tp_tables[0].durations == ("060m",)


def test_compact_missing_tp_ranges_and_shared_durations() -> None:
    names = [f"M_01p_{duration}_TP{tp:02d}.tlf" for duration in ("060m", "120m") for tp in (4, 5, 6, 7, 9, 10)]
    summary = infer_ensemble_inventory(names).tp_tables[0].missing_summary
    assert summary == ("01p: 060m,120m TP01-03,TP08",)


def test_verification_keeps_expectations_from_unfinished_runs(tmp_path: Path) -> None:
    (tmp_path / "M_01p_060m_TP01.tlf").write_text("Simulation FINISHED\n")
    (tmp_path / "M_02p_120m_TP02.tlf").write_text("Running...\n")
    (tmp_path / "M_02p_120m_TP02.hpc.tlf").write_text("Simulation FINISHED\n")
    presence = scan_ensemble_inventory(tmp_path)
    verified = scan_ensemble_inventory(tmp_path, verify_success=True)
    assert verified.expected_count == presence.expected_count == 40
    assert verified.run_count == 2
    assert verified.verified_count == 1
    assert len(verified.missing) == len(presence.missing) + 1
    assert verified.tp_tables[0].counts == ((1, 0), (0, 0))
    assert "02p: 060m,120m all TP" in verified.tp_tables[0].missing_summary


def test_verification_no_success_and_false_marker(tmp_path: Path) -> None:
    (tmp_path / "M_01p_TP01.tlf").write_text("Expected text: Simulation FINISHED\nERROR: stopped\n")
    result = scan_ensemble_inventory(tmp_path, verify_success=True)
    assert result.verified_count == 0
    assert result.expected_count == len(result.missing) == 10
    assert result.tp_tables[0].counts == ((0,),)


def test_hpc_only_success_is_supported(tmp_path: Path) -> None:
    (tmp_path / "M_01p_TP01.hpc.tlf").write_text("Simulation FINISHED\n")
    assert scan_ensemble_inventory(tmp_path, verify_success=True).verified_count == 1


def test_default_reads_no_logs_and_verification_reports_read_failure(tmp_path: Path) -> None:
    (tmp_path / "M_01p_TP01.tlf").touch()
    with patch.object(Path, "open", side_effect=PermissionError("locked log")):
        assert scan_ensemble_inventory(tmp_path).run_count == 1
        with pytest.raises(PermissionError, match="locked log"):
            scan_ensemble_inventory(tmp_path, verify_success=True)


def test_newest_primary_log_wins_over_old_success(tmp_path: Path) -> None:
    import os

    old = tmp_path / "old"
    old.mkdir()
    archived = old / "M_01p_TP01.tlf"
    archived.write_text("Simulation FINISHED\n")
    os.utime(archived, (1, 1))
    (tmp_path / archived.name).write_text("Running\n")
    assert scan_ensemble_inventory(tmp_path, verify_success=True).verified_count == 0


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
