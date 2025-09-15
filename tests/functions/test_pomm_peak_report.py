import shutil
from pathlib import Path
import sys

import pandas as pd  # type: ignore
import pytest

sys.path.insert(0, str(Path(__file__).absolute().parents[2]))

from ryan_library.scripts.pomm_utils import (
    aggregated_from_paths,
    find_aep_dur_median,
    find_aep_median_max,
)
from ryan_library.scripts.pomm_max_items import run_median_peak_report


DATA_DIR = Path(__file__).absolute().parent.parent / "test_data" / "tuflow" / "tutorials"


def test_aggregated_from_paths_module01() -> None:
    path = DATA_DIR / "Module_01" / "results"
    df = aggregated_from_paths([path])
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
    med = find_aep_dur_median(df)
    assert med["MedianAbsMax"].tolist() == [5, 7, 2]
    max_df = find_aep_median_max(med)
    assert len(max_df) == 2
    row_a = max_df[max_df["aep_text"] == "A"].iloc[0]
    assert row_a["MedianAbsMax"] == 7
    assert row_a["duration_text"] == "D2"


def test_run_median_peak_report_creates_excel() -> None:
    src_dir = DATA_DIR / "Module_01" / "results"
    run_median_peak_report(script_directory=src_dir, log_level="INFO")
    excel_files = list(src_dir.glob("*_med_peaks.xlsx"))
    assert excel_files
    xl = pd.ExcelFile(excel_files[0])
    assert set(["aep-dur-max", "aep-max", "POMM"]).issubset(set(xl.sheet_names))
    for f in excel_files:
        f.unlink()
