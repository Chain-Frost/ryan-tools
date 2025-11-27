from pathlib import Path
import sys
import pandas as pd
from pandas import DataFrame


sys.path.insert(0, str(Path(__file__).absolute().parents[2]))

from ryan_library.functions.tuflow.pomm_utils import (
    aggregated_from_paths,
    find_aep_dur_median,
    find_aep_median_max,
)
from ryan_library.scripts.pomm_max_items import run_mean_peak_report, run_median_peak_report


DATA_DIR: Path = Path(__file__).absolute().parent.parent / "test_data" / "tuflow" / "tutorials"


def test_aggregated_from_paths_module01() -> None:
    path: Path = DATA_DIR / "Module_01" / "results"
    df: DataFrame = aggregated_from_paths(paths=[path])
    assert len(df) == 6
    assert set(df["file"].unique()) == {"M01_2.5m_001_POMM.csv", "M01_5m_001_POMM.csv"}
    for col in ["AbsMax", "Location", "Type", "trim_runcode"]:
        assert col in df.columns


def test_find_aep_dur_median_and_max() -> None:
    df = pd.DataFrame(
        {
            "aep_text": ["A", "A", "A", "A", "B"],
            "duration_text": ["D1", "D1", "D2", "D2", "D1"],
            "Location": ["L1"] * 5,
            "Type": ["Flow"] * 5,
            "trim_runcode": ["Run"] * 5,
            "AbsMax": [5, 1, 3, 7, 2],
            "tp_text": ["TP1", "TP2", "TP1", "TP2", "TP3"],
        }
    )
    med: DataFrame = find_aep_dur_median(aggregated_df=df)
    assert med["MedianAbsMax"].tolist() == [5, 7, 2]
    assert med["median_duration"].tolist() == ["D1", "D2", "D1"]
    assert med["mean_Duration"].tolist() == ["D1", "D2", "D1"]
    max_df: DataFrame = find_aep_median_max(aep_dur_median=med)
    assert len(max_df) == 2
    row_a = max_df[max_df["aep_text"] == "A"].iloc[0]
    assert row_a["MedianAbsMax"] == 7
    assert row_a["duration_text"] == "D2"


def test_run_median_peak_report_creates_excel() -> None:
    src_dir: Path = DATA_DIR / "Module_01" / "results"
    run_median_peak_report(script_directory=src_dir, log_level="INFO")
    excel_files: list[Path] = list(src_dir.glob("*_med_peaks.xlsx"))
    assert excel_files
    with pd.ExcelFile(path_or_buffer=excel_files[0]) as xl:
        assert set(["aep-dur-max", "aep-max", "POMM"]).issubset(set(xl.sheet_names))
        aep_dur_df = xl.parse(sheet_name="aep-dur-max")
        assert all("mean" not in col.lower() for col in aep_dur_df.columns)
        if not aep_dur_df.empty:
            assert "MedianAbsMax" in aep_dur_df.columns
        aep_max_df = xl.parse(sheet_name="aep-max")
        assert all("mean" not in col.lower() for col in aep_max_df.columns)
        if not aep_max_df.empty:
            assert "MedianAbsMax" in aep_max_df.columns
    for f in excel_files:
        try:
            f.unlink()
        except PermissionError:
            pass  # Best effort cleanup


def test_run_median_peak_report_skips_pomm_sheet_when_disabled() -> None:
    src_dir: Path = DATA_DIR / "Module_01" / "results"
    run_median_peak_report(script_directory=src_dir, log_level="INFO", include_pomm=False)
    excel_files: list[Path] = list(src_dir.glob("*_med_peaks.xlsx"))
    assert excel_files
    with pd.ExcelFile(path_or_buffer=excel_files[0]) as xl:
        assert "POMM" not in set(xl.sheet_names)
        assert {"aep-dur-max", "aep-max"}.issubset(set(xl.sheet_names))
    for f in excel_files:
        try:
            f.unlink()
        except PermissionError:
            pass


def test_run_mean_peak_report_creates_excel_with_mean_only_columns() -> None:
    src_dir: Path = DATA_DIR / "Module_01" / "results"
    run_mean_peak_report(script_directory=src_dir, log_level="INFO")
    excel_files: list[Path] = list(src_dir.glob("*_mean_peaks.xlsx"))
    assert excel_files
    with pd.ExcelFile(path_or_buffer=excel_files[0]) as xl:
        assert {"aep-dur-mean", "aep-mean-max"}.issubset(set(xl.sheet_names))
        aep_dur_df = xl.parse(sheet_name="aep-dur-mean")
        assert all("median" not in col.lower() for col in aep_dur_df.columns)
        if not aep_dur_df.empty:
            assert "mean_PeakFlow" in aep_dur_df.columns
        aep_max_df = xl.parse(sheet_name="aep-mean-max")
        assert all("median" not in col.lower() for col in aep_max_df.columns)
        if not aep_max_df.empty:
            assert "mean_PeakFlow" in aep_max_df.columns
    for f in excel_files:
        try:
            f.unlink()
        except PermissionError:
            pass
