"""Explicit Linux-only fixture correction; immutable public source archive is unchanged."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from tests.fixtures.tuflow_example_repro.run_checks import prepare_linux_example_case_overlay


@pytest.mark.skipif(sys.platform != "linux", reason="Requires native Linux symlinks")
def test_temporary_linux_case_aliases_are_complete_and_fail_closed(tmp_path: Path) -> None:
    examples = (
        ("model/Materials.csv", "model/materials.csv"),
        ("model/gis/2d_rf_EG03_007_R.shp", "model/gis/2d_RF_EG03_007_R.shp"),
        ("model/gis/1d_WLL_EG14_001_L.shp", "model/gis/1d_wll_EG14_001_L.shp"),
        ("model/gis/1d_WLLp_EG14_001_P.shp", "model/gis/1d_wllp_EG14_001_P.shp"),
        ("model/gis/2d_qnl_E13_001_R.shp", "model/gis/2d_qnl_E13_001_R.SHP"),
        ("model/gis/2d_rf_EG03_014_R.shp", "model/gis/2d_RF_EG03_014_R.shp"),
        ("model/gis/2d_qnl_E13_003_R.shp", "model/gis/2d_qnl_E13_003_R.SHP"),
        ("model/gis/2d_qnl_EG15_003_R.shp", "model/gis/2d_qnl_EG15_003_R.SHP"),
    )
    for original, _alias in examples:
        source = tmp_path / original
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text(original, encoding="utf-8")

    sidecar = tmp_path / "model/gis/2d_rf_EG03_007_R.dbf"
    sidecar.write_text("sidecar", encoding="utf-8")
    aliases = prepare_linux_example_case_overlay(tmp_path)

    assert len(aliases) == 9
    assert set(aliases) == {alias for _, alias in examples} | {"model/gis/2d_RF_EG03_007_R.dbf"}
    for original, alias in examples:
        assert (tmp_path / alias).is_symlink()
        assert (tmp_path / alias).read_text(encoding="utf-8") == original
        assert not (tmp_path / original).is_symlink()
    assert (tmp_path / "model/gis/2d_RF_EG03_007_R.dbf").is_symlink()
    with pytest.raises(ValueError, match="pre-existing"):
        prepare_linux_example_case_overlay(tmp_path)


@pytest.mark.skipif(sys.platform != "linux", reason="Requires native Linux symlinks")
def test_missing_archive_source_fails_without_creating_aliases(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="original archive file is missing"):
        prepare_linux_example_case_overlay(tmp_path)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.skipif(sys.platform == "linux", reason="Linux mode is supported here")
def test_overlay_rejected_on_other_platforms(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="native Linux"):
        prepare_linux_example_case_overlay(tmp_path)
