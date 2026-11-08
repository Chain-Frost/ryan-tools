"""Source-backed reporting-line and reporting-region result contracts (WP12/#236)."""

from datetime import datetime
from pathlib import Path
from shutil import copy2
from unittest.mock import patch

import pandas as pd
import pytest

from ryan_library.mcp.server import inspect_tuflow_result
from ryan_library.orchestrators.tuflow.reporting_lines import export_results as export_lines
from ryan_library.orchestrators.tuflow.reporting_lines import main_processing as run_lines
from ryan_library.orchestrators.tuflow.reporting_regions import export_results as export_regions
from ryan_library.orchestrators.tuflow.reporting_regions import main_processing as run_regions
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.other_processors.RLLQProcessor import RLLQProcessor
from ryan_library.processors.tuflow.other_processors.RLRVolmxProcessor import RLRVolmxProcessor
from ryan_library.processors.tuflow.other_processors.RLRVolProcessor import RLRVolProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection

EG02: Path = Path(__file__).parents[3] / "test_data" / "tuflow" / "TUFLOW_Example_Model_Dataset" / "EG02"
CSV_DIR: Path = EG02 / "plot" / "csv"
RUN = "EG02_013"


def _processed(suffix: str, *, run: str = RUN, locations: list[str] | None = None) -> BaseProcessor:
    """Exercise the real factory and processor with pinned source bytes."""
    processor = BaseProcessor.from_file(CSV_DIR / f"{run}_{suffix}.csv", entity_filter=locations)
    processor.process()
    assert processor.processed
    return processor


def _write_series(
    directory: Path,
    *,
    suffix: str,
    tag: str,
    location: str,
    samples: list[str],
    times: list[str] | None = None,
) -> Path:
    """Build the native descriptor/time/wide-values format with an independent row index."""
    path = directory / f"{RUN}_{suffix}.csv"
    times = times if times is not None else [str(n) for n in range(len(samples))]
    assert len(times) == len(samples)
    header = f'"{RUN} [model.tcf]","Time (h)","{tag} {location} [{RUN}]"'
    rows = [f"{n + 1},{time},{value}" for n, (time, value) in enumerate(zip(times, samples, strict=True))]
    path.write_text(header + "\n" + "\n".join(rows) + "\n", encoding="utf-8")
    return path


@pytest.mark.parametrize(
    ("location", "minimum", "maximum", "last", "missing"),
    [
        ("RL_03", 0.0, 96.812, 4.890, 0),
        ("RL_04", 0.0, 75.810, 5.307, 0),
        ("RL_05", 0.0, 46.116, 0.001, 0),
    ],
)
def test_real_line_flow_filters_selected_columns_and_source_values(
    location: str, minimum: float, maximum: float, last: float, missing: int
) -> None:
    processor = _processed("RLL_Q", locations=[location])
    assert isinstance(processor, RLLQProcessor)
    assert processor.data_type == "RLL_Q"
    assert processor.dataformat == "PO"
    assert list(processor.raw_df.columns) == ["Time (h)", f"Q {location} [{RUN}]"]
    assert len(processor.df) == 181
    assert set(processor.df["Location"].astype(str)) == {location}
    assert set(processor.df["Type"].astype(str)) == {"Reporting Line Flow"}
    rows = processor.df.sort_values("Time")
    values = rows["Value"]
    assert values.isna().sum() == missing
    assert values.min() == pytest.approx(minimum)
    assert values.max() == pytest.approx(maximum)
    assert values.iloc[-1] == pytest.approx(last)
    assert rows["Time"].iloc[-1] == pytest.approx(3.0)


def test_real_line_flow_signed_negative_and_zero_are_not_missing() -> None:
    processor = _processed("RLL_Q", run="EG02_014", locations=["RL_04"])
    assert isinstance(processor, RLLQProcessor)
    assert len(processor.df) == 181
    values = processor.df["Value"]
    assert values.min() == pytest.approx(-0.874)
    assert values.max() == pytest.approx(83.651)
    assert values.eq(0).any()
    assert values.isna().sum() == 0


def test_existing_line_maxima_are_source_values_not_resampled_extrema() -> None:
    series = _processed("RLL_Q", locations=["RL_03"])
    maximums = _processed("RLL_Qmx", locations=["RL_03"])
    assert series.df["Value"].max() == pytest.approx(96.812)
    assert len(maximums.df) == 1
    row = maximums.df.iloc[0]
    assert row["Chan ID"] == "RL_03"
    assert row["Q"] == pytest.approx(96.842)
    assert row["Time"] == pytest.approx(0.9741)
    assert row["dQ"] == pytest.approx(0.1429)
    assert row["Time dQ"] == pytest.approx(0.6642)
    # Existing RLL_Qmx retains its as-supplied H sentinel; WP04 owns #219.
    assert row["H"] == -99999
    assert row["Q"] > series.df["Value"].max()


def test_real_region_volume_and_source_maxima_have_distinct_time_basis() -> None:
    series = _processed("RLR_Vol", locations=["RL_06"])
    summary = _processed("RLR_Volmx", locations=["RL_06"])
    assert isinstance(series, RLRVolProcessor)
    assert isinstance(summary, RLRVolmxProcessor)
    assert list(series.raw_df.columns) == ["Time (h)", f"Vol RL_06 [{RUN}]"]
    assert len(series.df) == 181
    assert set(series.df["Type"].astype(str)) == {"Reporting Region Volume"}
    values = series.df.sort_values("Time")["Value"]
    assert values.iloc[0] == pytest.approx(2.465)
    assert values.max() == pytest.approx(8773.318)
    assert values.iloc[-1] == pytest.approx(398.077)
    assert len(summary.df) == 1
    row = summary.df.iloc[0]
    assert row["Location"] == "RL_06"
    assert row["Vol max"] == pytest.approx(8773.601)
    assert row["Time Vol max"] == pytest.approx(1.2697)
    assert row["dVolmax"] == pytest.approx(9.2064)
    assert row["Time dVolmax"] == pytest.approx(0.6529)
    assert pd.isna(row["H"])
    assert row["Vol max"] > values.max()


def test_real_second_region_volume_supports_genuine_near_zero_values() -> None:
    series = _processed("RLR_Vol", run="EG02_014")
    summary = _processed("RLR_Volmx", run="EG02_014")
    assert series.df["Value"].iloc[0] == pytest.approx(0.004)
    assert series.df["Value"].max() == pytest.approx(70.972)
    assert summary.df["Vol max"].iloc[0] == pytest.approx(70.972)
    assert summary.df["Time Vol max"].iloc[0] == pytest.approx(3.0001)
    assert pd.isna(summary.df["H"].iloc[0])


def test_line_and_point_with_shared_identifier_remain_distinct(tmp_path: Path) -> None:
    """A reporting point and line may share a label but are not the same result."""
    point = _write_series(tmp_path, suffix="RLP_H", tag="H", location="Shared", samples=["-1.5", "0"])
    line = _write_series(tmp_path, suffix="RLL_Q", tag="Q", location="Shared", samples=["-2.0", "0"])
    collection = ProcessorCollection()
    for path in (point, line):
        processor = BaseProcessor.from_file(path, entity_filter=["Shared"])
        processor.process()
        assert processor.processed
        collection.add_processor(processor)

    values = collection.combine_raw()
    assert len(values) == 4
    assert set(values["Location"].astype(str)) == {"Shared"}
    assert set(values["Type"].astype(str)) == {"Reporting Point Water Level", "Reporting Line Flow"}
    assert values["Value"].min() == pytest.approx(-2.0)
    assert values["Value"].eq(0).sum() == 2


@pytest.mark.parametrize(
    ("suffix", "tag", "values"),
    [
        ("RLL_Q", "Q", ["-3", "0", "-99999", "**********"]),
        ("RLR_Vol", "Vol", ["-3", "0", "-99999", "**********"]),
    ],
)
def test_selected_history_preserves_negative_zero_and_missing(
    tmp_path: Path, suffix: str, tag: str, values: list[str]
) -> None:
    path = _write_series(tmp_path, suffix=suffix, tag=tag, location="Selected", samples=values)
    processor = BaseProcessor.from_file(path, entity_filter=["Selected"])
    processor.process()
    assert processor.processed
    assert processor.df["Value"].iloc[0] == pytest.approx(-3)
    assert processor.df["Value"].iloc[1] == pytest.approx(0)
    assert pd.isna(processor.df["Value"].iloc[2])
    assert pd.isna(processor.df["Value"].iloc[3])
    assert processor.df["AbsValue"].iloc[0] == pytest.approx(3)


@pytest.mark.parametrize(
    ("suffix", "tag", "values", "times"),
    [
        ("RLL_Q", "Q", ["-99999", "**********"], None),
        ("RLR_Vol", "Vol", ["nan", "4"], None),
        ("RLL_Q", "Q", ["1", "infinity"], None),
        ("RLR_Vol", "Vol", ["1", "2"], ["0.5", "0.5"]),
    ],
)
def test_history_rejects_all_missing_nonfinite_or_duplicate_times(
    tmp_path: Path,
    suffix: str,
    tag: str,
    values: list[str],
    times: list[str] | None,
) -> None:
    path = _write_series(tmp_path, suffix=suffix, tag=tag, location="Selected", samples=values, times=times)
    processor = BaseProcessor.from_file(path)
    processor.process()
    assert not processor.processed
    assert processor.df.empty
    assert not getattr(processor, "filtered_empty", False)


def test_history_absent_optional_location_is_valid_filtered_empty(tmp_path: Path) -> None:
    path = _write_series(tmp_path, suffix="RLL_Q", tag="Q", location="Found", samples=["0", "1"])
    processor = BaseProcessor.from_file(path, entity_filter=["NotPresent"])
    processor.process()
    assert not processor.processed
    assert processor.df.empty
    assert getattr(processor, "filtered_empty", False)


def test_region_summary_preserves_missing_and_negative_source_data(tmp_path: Path) -> None:
    path = tmp_path / f"{RUN}_RLR_Volmx.csv"
    path.write_text(
        f'"{RUN} [model.tcf]","ID","Vol max","Time Vol max","dVolmax","Time dVolmax","H"\n'
        '1,"Region",-2.5,1.0,-0.5,0.25,-1.0\n'
        '2,"Missing",**********,**********,-99999,**********,-99999\n',
        encoding="utf-8",
    )
    good = BaseProcessor.from_file(path, entity_filter=["Region"])
    good.process()
    assert good.processed
    assert good.df["Vol max"].iloc[0] == pytest.approx(-2.5)
    assert good.df["dVolmax"].iloc[0] == pytest.approx(-0.5)
    assert good.df["H"].iloc[0] == pytest.approx(-1.0)

    missing = BaseProcessor.from_file(path, entity_filter=["Missing"])
    missing.process()
    assert not missing.processed
    assert missing.df.empty
    assert not getattr(missing, "filtered_empty", False)


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("Time Vol max", "**********"),
        ("Vol max", "BROKEN"),
    ],
)
def test_region_summary_rejects_missing_peak_time_and_bad_numbers(tmp_path: Path, column: str, value: str) -> None:
    path = tmp_path / f"{RUN}_RLR_Volmx.csv"
    record = {
        "ID": "Region",
        "Vol max": "2.0",
        "Time Vol max": "0.5",
        "dVolmax": "0.1",
        "Time dVolmax": "0.25",
        "H": "1.0",
    }
    record[column] = value
    record_row = ",".join(("1", *record.values()))
    path.write_text(
        f'"{RUN} [model.tcf]","ID","Vol max","Time Vol max","dVolmax","Time dVolmax","H"\n{record_row}\n',
        encoding="utf-8",
    )
    processor = BaseProcessor.from_file(path)
    processor.process()
    assert not processor.processed
    assert processor.df.empty


@pytest.mark.parametrize(
    ("data_type", "location", "value_field"),
    [
        ("RLL_Q", "RL_03", "Value"),
        ("RLR_Vol", "RL_06", "Value"),
        ("RLR_Volmx", "RL_06", "Vol max"),
    ],
)
def test_generic_mcp_inspection_recognises_new_types(data_type: str, location: str, value_field: str) -> None:
    result = inspect_tuflow_result(
        file_path=str(CSV_DIR / f"{RUN}_{data_type}.csv"), locations=[location], sample_rows=2
    )
    assert result["data_type"] == data_type
    assert result["processed"]
    assert result["raw_run_code"] == RUN
    assert result["row_count"] > 0
    assert value_field in result["columns"]


@pytest.mark.parametrize(
    ("suffixes", "output_key", "sheets", "expected_series", "expected_max"),
    [
        (
            ("RLL_Q", "RLL_Qmx"),
            "reporting_lines",
            ("Line_Flow_Series", "Source_Flow_Maxima"),
            96.812,
            96.842,
        ),
        (
            ("RLR_Vol", "RLR_Volmx"),
            "reporting_regions",
            ("Region_Volume_Series", "Source_Volume_Maxima"),
            8773.318,
            8773.601,
        ),
    ],
)
def test_public_discovery_export_protects_source_maxima(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    suffixes: tuple[str, str],
    output_key: str,
    sheets: tuple[str, str],
    expected_series: float,
    expected_max: float,
) -> None:
    """Report only chosen locations and retain supplied noncoincident maxima on distinct sheets."""
    source = tmp_path / "sources"
    destination = tmp_path / "reports"
    source.mkdir()
    destination.mkdir()
    for suffix in suffixes:
        copy2(CSV_DIR / f"{RUN}_{suffix}.csv", source / f"{RUN}_{suffix}.csv")

    monkeypatch.chdir(destination)
    if output_key == "reporting_lines":
        assert run_lines(
            paths_to_process=[source], locations_to_include=["RL_03"], export_mode="both", use_live_dashboard=False
        )
    else:
        assert run_regions(
            paths_to_process=[source], locations_to_include=["RL_06"], export_mode="both", use_live_dashboard=False
        )

    source_frames = list(destination.glob(f"*_{output_key}.xlsx"))
    assert len(source_frames) == 1
    series_sheet, summary_sheet = sheets
    series = pd.read_excel(source_frames[0], sheet_name=series_sheet)  # pyright: ignore[reportUnknownMemberType]
    summary = pd.read_excel(source_frames[0], sheet_name=summary_sheet)  # pyright: ignore[reportUnknownMemberType]
    assert len(series) == 181
    assert len(summary) == 1
    assert series["Value"].max() == pytest.approx(expected_series)
    maximum_field = "Q" if output_key == "reporting_lines" else "Vol max"
    assert summary[maximum_field].iloc[0] == pytest.approx(expected_max)

    series_parquet = list(destination.glob(f"*_{output_key}_{series_sheet}.parquet.gzip"))
    summary_parquet = list(destination.glob(f"*_{output_key}_{summary_sheet}.parquet.gzip"))
    assert len(series_parquet) == 1
    assert len(summary_parquet) == 1
    assert pd.read_parquet(series_parquet[0])["Value"].max() == pytest.approx(expected_series)
    assert pd.read_parquet(summary_parquet[0])[maximum_field].iloc[0] == pytest.approx(expected_max)


@pytest.mark.parametrize(
    ("kind", "output_key", "summary_sheet"),
    [
        ("line", "reporting_lines", "Source_Flow_Maxima"),
        ("region", "reporting_regions", "Source_Volume_Maxima"),
    ],
)
def test_summary_only_source_is_supported_without_series_companion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    output_key: str,
    summary_sheet: str,
) -> None:
    collection = ProcessorCollection()
    suffix = "RLL_Qmx" if kind == "line" else "RLR_Volmx"
    collection.add_processor(_processed(suffix))
    monkeypatch.chdir(tmp_path)
    fixed = datetime(2026, 11, 8, 12, 0)
    exporter = export_lines if kind == "line" else export_regions
    with patch("ryan_library.functions.excel_export.datetime") as clock:
        clock.now.return_value = fixed
        assert exporter(results=collection, export_mode="parquet")
        # Unchanged destinations must remain protected even if the call is repeated later.
        with pytest.raises(FileExistsError):
            exporter(results=collection, export_mode="parquet")

    products = list(tmp_path.glob(f"*_{output_key}_{summary_sheet}.parquet.gzip"))
    assert len(products) == 1

def test_history_only_line_workflow_logs_missing_qmx_without_failing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reproduce native PR #270 report: partial output is valid but needs an explicit warning."""
    sources = tmp_path / "inputs"
    reports = tmp_path / "outputs"
    sources.mkdir()
    reports.mkdir()
    copy2(CSV_DIR / f"{RUN}_RLL_Q.csv", sources / f"{RUN}_RLL_Q.csv")
    log_path = tmp_path / "reporting-line.log"

    monkeypatch.chdir(reports)
    assert run_lines(
        paths_to_process=[sources],
        locations_to_include=["RL_03"],
        export_mode="parquet",
        use_live_dashboard=False,
        console_log_level="SUCCESS",
        log_file=str(log_path),
    )
    log_text = log_path.read_text(encoding="utf-8")
    assert RUN in log_text
    assert "RLL_Qmx.csv" in log_text
    assert "optional companion" in log_text
    assert "publishing available reporting products only" in log_text

    histories = list(reports.glob("*_reporting_lines_Line_Flow_Series.parquet.gzip"))
    assert len(histories) == 1
    assert not list(reports.glob("*_reporting_lines_Source_Flow_Maxima.parquet.gzip"))
    assert pd.read_parquet(histories[0])["Value"].max() == pytest.approx(96.812)


@pytest.mark.parametrize(
    ("suffix", "missing"),
    [
        ("RLL_Qmx", "RLL_Q.csv"),
        ("RLR_Volmx", "RLR_Vol.csv"),
    ],
)
def test_summary_only_reports_missing_history_without_changing_partial_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    suffix: str,
    missing: str,
) -> None:
    """A single valid summary is publishable, with an explicit missing-series diagnostic."""
    collection = ProcessorCollection()
    collection.add_processor(_processed(suffix))
    monkeypatch.chdir(tmp_path)
    with (
        patch("ryan_library.orchestrators.tuflow._reporting_location_export.logger.warning") as warning,
        patch("ryan_library.orchestrators.tuflow._reporting_location_export.ExcelExporter.export_dataframes"),
    ):
        if suffix == "RLL_Qmx":
            assert export_lines(results=collection, export_mode="parquet")
        else:
            assert export_regions(results=collection, export_mode="parquet")

    assert warning.call_count == 1
    args, _ = warning.call_args
    assert args[1] == RUN
    assert missing in args[2]


def test_multiple_runs_report_only_the_missing_run_not_global_product_presence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A complete run must not hide a missing companion in another run."""
    collection = ProcessorCollection()
    collection.add_processor(_processed("RLL_Q", run="EG02_013", locations=["RL_03"]))
    collection.add_processor(_processed("RLL_Qmx", run="EG02_013", locations=["RL_03"]))
    collection.add_processor(_processed("RLL_Q", run="EG02_014", locations=["RL_03"]))
    monkeypatch.chdir(tmp_path)
    with (
        patch("ryan_library.orchestrators.tuflow._reporting_location_export.logger.warning") as warning,
        patch("ryan_library.orchestrators.tuflow._reporting_location_export.ExcelExporter.export_dataframes"),
    ):
        assert export_lines(results=collection, export_mode="parquet")

    assert warning.call_count == 1
    args, _ = warning.call_args
    assert args[1] == "EG02_014"
    assert "*_RLL_Qmx.csv" in args[2]


def test_complete_line_pair_does_not_emit_optional_companion_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Both source products present: do not mislabel a complete report as partial."""
    collection = ProcessorCollection()
    collection.add_processor(_processed("RLL_Q", locations=["RL_03"]))
    collection.add_processor(_processed("RLL_Qmx", locations=["RL_03"]))
    monkeypatch.chdir(tmp_path)
    with (
        patch("ryan_library.orchestrators.tuflow._reporting_location_export.logger.warning") as warning,
        patch("ryan_library.orchestrators.tuflow._reporting_location_export.ExcelExporter.export_dataframes"),
    ):
        assert export_lines(results=collection, export_mode="parquet")

    warning.assert_not_called()
