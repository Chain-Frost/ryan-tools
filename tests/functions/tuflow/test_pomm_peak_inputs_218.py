"""Real CSV/processor/Excel regressions for supported POMM peak inputs (#218)."""

from pathlib import Path

import pandas as pd
import pytest
from pandas import DataFrame

from ryan_library.functions.tuflow import pomm_utils
from ryan_library.orchestrators.tuflow.pomm_max_items import (
    export_mean_peak_report,
    export_median_peak_report,
)
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection


def _write_peak_csv_files(directory: Path) -> dict[str, Path]:
    run_code = "Model_01p_0060m_TP01"
    rll_path = directory / f"{run_code}_RLL_Qmx.csv"
    rll_path.write_text(
        "ID,Qmax,Time Qmax,dQmax,Time dQmax,H\n"
        "C1,42,1,0.2,1.2,100\n",
        encoding="utf-8",
    )

    pomm_path = directory / f"{run_code}_POMM.csv"
    # POMM is stored columnwise; the second original column becomes headers
    # after the processor's transpose. The first cell retains the run code.
    DataFrame(
        {
            0: [run_code, "ignored", "ignored", "ignored", "ignored", "ignored"],
            1: [
                "Location",
                "Time",
                "Maximum (Extracted from Time Series)",
                "Time of Maximum",
                "Minimum (Extracted From Time Series)",
                "Time of Minimum",
            ],
            2: ["Flow", "C1", 12, 1, -4, 2],
        }
    ).to_csv(pomm_path, index=False, header=False)

    return {"POMM": pomm_path, "RLL_Qmx": rll_path}


@pytest.mark.parametrize(
    "selected_formats",
    [("RLL_Qmx",), ("POMM",), ("POMM", "RLL_Qmx")],
    ids=("rll-only", "pomm-only", "mixed"),
)
def test_real_csv_processors_reach_mean_and_median_workbooks(
    selected_formats: tuple[str, ...],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only parallel transport is replaced; files, processors and exports are real."""
    paths = _write_peak_csv_files(tmp_path)
    expected_filenames = {paths[file_type].name for file_type in selected_formats}

    def serial_processing(*, file_list: list[Path], **_options: object) -> ProcessorCollection:
        collection = ProcessorCollection()
        for file_path in file_list:
            processor = BaseProcessor.from_file(file_path=file_path)
            processor.process()
            assert processor.processed, f"{file_path.name} was not processed"
            collection.add_processor(processor)
        return collection

    monkeypatch.setattr(pomm_utils, "process_files_in_parallel", serial_processing)

    aggregated = pomm_utils.aggregated_from_paths(
        paths=[tmp_path],
        include_data_types=selected_formats,
    )
    assert len(aggregated) == len(selected_formats)
    assert set(aggregated["file"].astype(str)) == expected_filenames

    expected_types: set[str] = set()
    if "POMM" in selected_formats:
        pomm = aggregated.loc[aggregated["Type"] == "Flow"].iloc[0]
        assert pomm["Location"] == "C1"
        assert pomm["AbsMax"] == 12
        assert pomm["SignedAbsMax"] == 12
        expected_types.add("Flow")
    if "RLL_Qmx" in selected_formats:
        rll = aggregated.loc[aggregated["Type"] == "Q"].iloc[0]
        assert rll["Location"] == "C1"
        assert rll["Q"] == 42
        assert rll["AbsMax"] == 42
        assert rll["SignedAbsMax"] == 42
        expected_types.add("Q")

    export_mean_peak_report(
        script_directory=tmp_path,
        paths_to_process=[tmp_path],
        include_data_types=selected_formats,
        log_level="WARNING",
    )
    export_median_peak_report(
        script_directory=tmp_path,
        paths_to_process=[tmp_path],
        include_data_types=selected_formats,
        log_level="WARNING",
    )

    for suffix, sheet_name, value_column in (
        ("_mean_peaks.xlsx", "aep-mean-max", "mean_PeakFlow"),
        ("_med_peaks.xlsx", "aep-max", "MedianAbsMax"),
    ):
        workbooks = list(tmp_path.glob(f"*{suffix}"))
        assert len(workbooks) == 1
        with pd.ExcelFile(workbooks[0]) as workbook:
            assert {sheet_name, "POMM", "Data Dictionary"}.issubset(workbook.sheet_names)
            reported = workbook.parse(sheet_name)
            raw = workbook.parse("POMM")
        assert set(raw["file"].astype(str)) == expected_filenames
        assert set(reported["Type"].astype(str)) == expected_types
        expected_values = {"Flow": 12, "Q": 42}
        for _, row in reported.iterrows():
            assert row[value_column] == expected_values[str(row["Type"])]


def test_rll_signed_peak_is_preserved_alongside_its_magnitude(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Qmax is a signed reported maximum, not a reconstructed POMM min/max."""
    rll = DataFrame({"Chan ID": ["C1"], "Q": [-7.0], "file": ["Model_RLL_Qmx.csv"]})
    monkeypatch.setattr(pomm_utils, "combine_df_from_paths", lambda **_options: rll.copy())

    aggregated = pomm_utils.aggregated_from_paths(paths=[tmp_path])
    row = aggregated.iloc[0]
    assert row["Q"] == -7.0
    assert row["SignedAbsMax"] == -7.0
    assert row["AbsMax"] == 7.0
    assert row["Type"] == "Q"
    assert row["file"] == "Model_RLL_Qmx.csv"
