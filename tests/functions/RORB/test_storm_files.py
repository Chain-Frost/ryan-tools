"""Regression checks for interval rainfall conversion to RORB STM."""

from pathlib import Path

import pytest

from ryan_library.functions.RORB.storm_files import (
    ConversionError,
    RainfallSeries,
    build_stm,
    infer_model_name,
    infer_output_details,
    output_filename,
    read_rainfall_series,
    write_stm,
)


def rainfall_csv(path: Path, times: list[float], values: list[float]) -> Path:
    body = "Event ID,RF\nTime (hour),TP07_2030_SSP3-7.0\n"
    body += "".join(f"{t},{v}\n" for t, v in zip(times, values, strict=True))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    return path


def test_manual_first_ordinate_is_first_rainfall_interval(tmp_path: Path) -> None:
    path = rainfall_csv(tmp_path / "GD08_RF_01.00p00120m.CSV", [0, 1, 2], [0, 5, 10])
    series = read_rainfall_series(path, "TP07_2030_SSP3-7.0")
    assert series.values == (5, 10)
    assert series.depth_mm == 15
    storm = build_stm("MLGD", series, 200)
    assert "\n0,2\nPluviograph 1\n5\n10\n-99\n" in storm
    assert "\n0,3\nPluviograph" not in storm


def test_auto_removes_only_extra_zero_after_named_duration(tmp_path: Path) -> None:
    path = rainfall_csv(tmp_path / "GD08_RF_01.00p00120m.CSV", [0, 1, 2, 3], [0, 5, 10, 0])
    assert read_rainfall_series(path, "TP07_2030_SSP3-7.0").values == (5, 10)
    assert read_rainfall_series(path, "TP07_2030_SSP3-7.0", terminal_zero="keep").values == (5, 10, 0)


def test_keeps_terminal_zero_within_named_duration(tmp_path: Path) -> None:
    path = rainfall_csv(tmp_path / "GD08_RF_01.00p00120m.CSV", [0, 1, 2], [0, 5, 0])
    assert read_rainfall_series(path, "TP07_2030_SSP3-7.0").values == (5, 0)


def test_interior_zeros_are_not_trimmed(tmp_path: Path) -> None:
    path = rainfall_csv(tmp_path / "GD08_RF_01.00p00240m.CSV", [0, 1, 2, 3, 4, 5], [0, 5, 0, 0, 10, 0])
    series = read_rainfall_series(path, "TP07_2030_SSP3-7.0")
    assert series.values == (5, 0, 0, 10)
    assert len(series.values) == 4
    assert series.depth_mm == 15


@pytest.mark.parametrize("zero_depth", [0.01, -0.01])
def test_nonzero_time_zero_rejected(tmp_path: Path, zero_depth: float) -> None:
    path = rainfall_csv(tmp_path / "input.csv", [0, 1], [zero_depth, 5])
    with pytest.raises(ConversionError, match=r"time-zero|negative"):
        read_rainfall_series(path, "TP07_2030_SSP3-7.0")


def test_negative_interval_rejected(tmp_path: Path) -> None:
    path = rainfall_csv(tmp_path / "input.csv", [0, 1], [0, -5])
    with pytest.raises(ConversionError, match="negative rainfall"):
        read_rainfall_series(path, "TP07_2030_SSP3-7.0")


def test_irregular_timing_rejected(tmp_path: Path) -> None:
    path = rainfall_csv(tmp_path / "input.csv", [0, 1, 2.5], [0, 5, 5])
    with pytest.raises(ConversionError, match="Irregular"):
        read_rainfall_series(path, "TP07_2030_SSP3-7.0")


def test_extra_nonzero_after_named_duration_rejected(tmp_path: Path) -> None:
    path = rainfall_csv(tmp_path / "GD08_RF_01.00p00120m.CSV", [0, 1, 2, 3], [0, 5, 10, 1])
    with pytest.raises(ConversionError, match="non-zero"):
        read_rainfall_series(path, "TP07_2030_SSP3-7.0")


def test_named_duration_mismatch_can_be_explicitly_overridden(tmp_path: Path) -> None:
    path = rainfall_csv(tmp_path / "GD08_RF_01.00p00120m.CSV", [0, 1, 2, 3, 4], [0, 5, 10, 0, 0])
    with pytest.raises(ConversionError, match="duration"):
        read_rainfall_series(path, "TP07_2030_SSP3-7.0")
    assert read_rainfall_series(path, "TP07_2030_SSP3-7.0", terminal_zero="keep").values == (5, 10, 0, 0)


def test_drop_requires_terminal_zero(tmp_path: Path) -> None:
    path = rainfall_csv(tmp_path / "input.csv", [0, 1], [0, 5])
    with pytest.raises(ConversionError, match="non-zero terminal"):
        read_rainfall_series(path, "TP07_2030_SSP3-7.0", terminal_zero="drop")


def test_no_rainfall_intervals_after_drop_rejected(tmp_path: Path) -> None:
    path = rainfall_csv(tmp_path / "input.csv", [0, 1], [0, 0])
    with pytest.raises(ConversionError, match="No rainfall intervals"):
        read_rainfall_series(path, "TP07_2030_SSP3-7.0", terminal_zero="drop")


def test_filename_normalisation_and_existing_tp_parser(tmp_path: Path) -> None:
    path = tmp_path / "GD08_RF_01.00p01440m.CSV"
    assert infer_output_details(path, "TP07_2030_SSP3-7.0") == ("GD08", "01p", "1440", "tp07")
    assert output_filename(path, "TP07_2030_SSP3-7.0") == "Storm_GD08_01p_1440m_tp07.stm"
    with pytest.raises(ConversionError):
        output_filename(path, "TP07_2030_SSP3-7.0", override="C:\\escape.stm")


def test_model_name_ancestor(tmp_path: Path) -> None:
    path = tmp_path / "TUFLOW_MLGD" / "bc_dbase" / "GD08" / "rf_inflow" / "storm.CSV"
    assert infer_model_name(path) == "MLGD"


def test_run_increment_bounds() -> None:
    series = RainfallSeries("TP01", 0.5, (5.0, 10.0))
    for requested in (1, 2001):
        with pytest.raises(ConversionError):
            build_stm("MLGD", series, requested)
    assert "\n0.5,2000,1,1,0,-99\n" in build_stm("MLGD", series, 2000)


def test_stm_writer_wont_overwrite(tmp_path: Path) -> None:
    output = tmp_path / "storm.stm"
    write_stm(output, "first\n")
    with pytest.raises(ConversionError, match="exists"):
        write_stm(output, "second\n")
    assert output.read_text(encoding="utf-8") == "first\n"
    write_stm(output, "second\n", overwrite=True)
    assert output.read_text(encoding="utf-8") == "second\n"


def test_long_storm_needs_sufficient_run_increments(tmp_path: Path) -> None:
    # 72 hours at 15-minute timesteps: 288 actual intervals, excluding t=0.
    count = 288
    path = rainfall_csv(
        tmp_path / "GD08_RF_01.00p04320m.CSV",
        [index * 0.25 for index in range(count + 1)],
        [0.0, *([1.0] * count)],
    )
    series = read_rainfall_series(path, "TP07_2030_SSP3-7.0")
    assert len(series.values) == count
    assert series.depth_mm == 288.0
    with pytest.raises(ConversionError, match="shorter than rainfall"):
        build_stm("MLGD", series)
    output = build_stm("MLGD", series, run_increments=400)
    assert "\n0.25,400,1,1,0,-99\n" in output
    assert "\n0,288\nPluviograph 1\n" in output
    assert output.split("Pluviograph 1\n", 1)[1].splitlines() == ["1"] * count + ["-99"]
