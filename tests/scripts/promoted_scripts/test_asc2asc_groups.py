from __future__ import annotations

from pathlib import Path

import pytest

from run_grouped_asc_to_asc_statistics import TUFLOWRaster, build_diff_commands, build_max_median_commands


def _raster(path: str, *, tp: str, duration: str, aep: str) -> TUFLOWRaster:
    return TUFLOWRaster(Path(path), tp=tp, duration=duration, aep=aep, suffix="d_Max")


def test_build_max_median_commands_groups_temporal_patterns_and_durations(tmp_path: Path) -> None:
    rasters = [
        _raster("Model_01.0p_060m_TP01_d_Max.tif", tp="TP01", duration="060m", aep="01.0p"),
        _raster("Model_01.0p_060m_TP02_d_Max.tif", tp="TP02", duration="060m", aep="01.0p"),
        _raster("Model_01.0p_120m_TP01_d_Max.tif", tp="TP01", duration="120m", aep="01.0p"),
    ]

    median, maximum = build_max_median_commands(rasters, tmp_path, "asc_to_asc")

    assert len(median) == 2
    assert len(maximum) == 1
    assert all("-statMedian" in command for command in median)
    assert maximum[0][-2] == "-statMax"


def test_build_diff_commands_rejects_ambiguous_existing_match(tmp_path: Path) -> None:
    current = [_raster("Model_01.0p_060m_TP01_d_Max.tif", tp="TP01", duration="060m", aep="01.0p")]
    existing = [
        _raster("Model_01.0p_060m_TP02_d_Max.tif", tp="TP02", duration="060m", aep="01.0p"),
        _raster("Model_01.0p_060m_TP03_d_Max.tif", tp="TP03", duration="060m", aep="01.0p"),
    ]

    with pytest.raises(ValueError, match="Ambiguous"):
        build_diff_commands(current, existing, tmp_path, "asc_to_asc")
