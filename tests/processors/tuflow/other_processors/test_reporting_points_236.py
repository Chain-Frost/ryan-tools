"""Real and synthetic acceptance for the selected reporting-point CSV outputs (#236)."""

from pathlib import Path
from shutil import copy2

import pandas as pd
import pytest

from ryan_library.mcp.server import inspect_tuflow_result
from ryan_library.orchestrators.tuflow.reporting_points import export_results, main_processing
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.other_processors.RLPHmxProcessor import RLPHmxProcessor
from ryan_library.processors.tuflow.other_processors.RLPHProcessor import RLPHProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection

EG02: Path = Path(__file__).parents[3] / "test_data" / "tuflow" / "TUFLOW_Example_Model_Dataset" / "EG02"
PLOT: Path = EG02 / "plot" / "csv"
RUN = "EG02_013"


def _processor(path: Path, locations: list[str] | None = None) -> BaseProcessor:
    processor = BaseProcessor.from_file(path, entity_filter=locations)
    processor.process()
    assert processor.processed, path
    return processor


@pytest.mark.parametrize(
    ("location", "missing", "first_valid", "sampled_max"),
    [
        ("RL_01", 22, 39.0113, 42.4816),
        ("RL_02", 45, 41.6942, 42.3958),
    ],
)
def test_real_rlp_h_factory_filters_before_expanding(
    location: str, missing: int, first_valid: float, sampled_max: float
) -> None:
    processor = _processor(PLOT / f"{RUN}_RLP_H.csv", locations=[location])
    assert isinstance(processor, RLPHProcessor)
    assert processor.data_type == "RLP_H"
    assert processor.name_parser.raw_run_code == RUN
    assert processor.dataformat == "PO"
    assert list(processor.raw_df.columns) == ["Time (h)", f"H {location} [{RUN}]"]
    assert len(processor.df) == 181
    assert set(processor.df["Location"].astype(str)) == {location}
    assert set(processor.df["Type"].astype(str)) == {"Reporting Point Water Level"}
    values = processor.df.sort_values("Time")["Value"]
    assert values.isna().sum() == missing
    assert values.dropna().iloc[0] == pytest.approx(first_valid)
    assert values.max() == pytest.approx(sampled_max)
    assert processor.df["AbsValue"].max() == pytest.approx(sampled_max)
    assert processor.df["Time"].min() == pytest.approx(0.0)
    assert processor.df["Time"].max() == pytest.approx(3.0)


def test_real_rlp_hmx_retains_supplied_associations_without_recomputing_maxima() -> None:
    processor = _processor(PLOT / f"{RUN}_RLP_Hmx.csv", locations=["RL_01"])
    assert isinstance(processor, RLPHmxProcessor)
    assert processor.data_type == "RLP_Hmx"
    assert len(processor.df) == 1
    row = processor.df.iloc[0]
    assert row["Location"] == "RL_01"
    assert row["Type"] == "Reporting Point Water Level"
    assert row["Hmax"] == pytest.approx(42.4817)
    assert row["Time Hmax"] == pytest.approx(1.2814)
    # dHmax reflects the supplied change from -99999; retain rather than silently derive a new statistic.
    assert row["dHmax"] == pytest.approx(100038.01562)
    assert row["Time dHmax"] == pytest.approx(0.3663)
    assert pd.isna(row["Q"])
    assert "Max" not in processor.df.columns
    assert "AbsMax" not in processor.df.columns


def test_real_rlp_source_maximum_is_not_replaced_by_sampled_maximum() -> None:
    series = _processor(PLOT / f"{RUN}_RLP_H.csv", locations=["RL_01"])
    maximum = _processor(PLOT / f"{RUN}_RLP_Hmx.csv", locations=["RL_01"])
    sampled = series.df["Value"].max()
    supplied = maximum.df["Hmax"].iloc[0]
    assert sampled == pytest.approx(42.4816)
    assert supplied == pytest.approx(42.4817)
    assert supplied > sampled


def test_reporting_point_collection_and_protected_excel_parquet_export(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    series = _processor(PLOT / f"{RUN}_RLP_H.csv")
    maxima = _processor(PLOT / f"{RUN}_RLP_Hmx.csv")
    collection = ProcessorCollection()
    collection.add_processor(series)
    collection.add_processor(maxima)
    assert set(collection.get_processors_by_data_type(["RLP_H"]).combine_raw()["Location"].astype(str)) == {
        "RL_01",
        "RL_02",
    }
    collection.filter_locations(["RL_01"])
    assert set(collection.combine_raw()["Location"].astype(str)) == {"RL_01"}

    monkeypatch.chdir(tmp_path)
    assert export_results(results=collection, export_mode="both")
    workbooks = list(tmp_path.glob("*_reporting_points.xlsx"))
    assert len(workbooks) == 1
    source = pd.read_excel(workbooks[0], sheet_name="Source_Maxima")  # pyright: ignore[reportUnknownMemberType]
    history = pd.read_excel(workbooks[0], sheet_name="Water_Level_Series")  # pyright: ignore[reportUnknownMemberType]
    assert len(source) == 1
    assert len(history) == 181
    assert source["Hmax"].iloc[0] == pytest.approx(42.4817)
    assert history["Value"].isna().sum() == 22

    history_parquet = list(tmp_path.glob("*_reporting_points_Water_Level_Series.parquet.gzip"))
    maxima_parquet = list(tmp_path.glob("*_reporting_points_Source_Maxima.parquet.gzip"))
    assert len(history_parquet) == len(maxima_parquet) == 1
    assert pd.read_parquet(history_parquet[0])["Value"].max() == pytest.approx(42.4816)
    assert pd.read_parquet(maxima_parquet[0])["Time Hmax"].iloc[0] == pytest.approx(1.2814)
    with pytest.raises(FileExistsError):
        export_results(results=collection, export_mode="both")


def _make_history(directory: Path, *, up: list[str], down: list[str]) -> Path:
    path = directory / f"{RUN}_RLP_H.csv"
    header = f'"{RUN} [example.tcf]","Time (h)","H Up [{RUN}]","H Down [{RUN}]"'
    rows = [f"{i + 1},{i},{u},{d}" for i, (u, d) in enumerate(zip(up, down, strict=True))]
    path.write_text(header + "\n" + "\n".join(rows) + "\n", encoding="utf-8")
    return path


def test_history_preserves_negative_levels_zero_and_selected_null_samples(tmp_path: Path) -> None:
    path = _make_history(tmp_path, up=["-2.5", "0", "-99999", "**********"], down=["3", "4", "5", "6"])
    processor = _processor(path, locations=["Up"])
    values = processor.df["Value"].tolist()
    assert values[:2] == [-2.5, 0.0]
    assert pd.isna(values[2])
    assert pd.isna(values[3])
    assert processor.df["AbsValue"].iloc[0] == pytest.approx(2.5)
    assert len(processor.raw_df.columns) == 2


@pytest.mark.parametrize(
    ("up", "down"),
    [
        (["-99999", "**********"], ["-99999", ""]),
        (["-2", "BROKEN"], ["0", "1"]),
        (["1", "inf"], ["1", "2"]),
    ],
)
def test_history_rejects_all_missing_or_invalid_measurements(tmp_path: Path, up: list[str], down: list[str]) -> None:
    processor = BaseProcessor.from_file(_make_history(tmp_path, up=up, down=down))
    processor.process()
    assert not processor.processed
    assert processor.df.empty
    assert not getattr(processor, "filtered_empty", False)


def test_history_no_match_filter_is_not_a_parse_failure(tmp_path: Path) -> None:
    processor = BaseProcessor.from_file(_make_history(tmp_path, up=["1"], down=["2"]), entity_filter=["Absent"])
    processor.process()
    assert not processor.processed
    assert getattr(processor, "filtered_empty", False)
    assert processor.df.empty


def test_summary_handles_source_negative_and_missing_values(tmp_path: Path) -> None:
    path = tmp_path / f"{RUN}_RLP_Hmx.csv"
    path.write_text(
        f'"{RUN} [example.tcf]","ID","Hmax","Time Hmax","dHmax","Time dHmax","Q"\n'
        '1,"Up",-2.5,1.0,0.0,1.0,-3.0\n'
        '2,"Down",**********,**********,-99999,**********,-99999\n',
        encoding="utf-8",
    )
    processor = _processor(path, locations=["Up"])
    assert processor.df["Hmax"].iloc[0] == pytest.approx(-2.5)
    assert processor.df["Q"].iloc[0] == pytest.approx(-3.0)
    missing = BaseProcessor.from_file(path, entity_filter=["Down"])
    missing.process()
    assert not missing.processed
    assert missing.df.empty
    assert not getattr(missing, "filtered_empty", False)


def test_summary_rejects_duplicate_ids_and_unassociated_peak_time(tmp_path: Path) -> None:
    path = tmp_path / f"{RUN}_RLP_Hmx.csv"
    header = f'"{RUN} [example.tcf]","ID","Hmax","Time Hmax","dHmax","Time dHmax","Q"\n'
    path.write_text(header + '1,"Same",1,0.0,0,0,0\n2,"Same",2,0.5,0,0,0\n', encoding="utf-8")
    processor = BaseProcessor.from_file(path)
    processor.process()
    assert not processor.processed
    path.write_text(header + '1,"Same",1,**********,0,0,0\n', encoding="utf-8")
    processor.process()
    assert not processor.processed
    assert processor.df.empty


def test_mcp_public_inspection_preserves_selected_reporting_point_identity() -> None:
    """The generic public inspection interface needs no new reporting-point endpoint."""
    series = inspect_tuflow_result(str(PLOT / f"{RUN}_RLP_H.csv"), locations=["RL_01"], minimum_time=2.0, sample_rows=2)
    assert series["data_type"] == "RLP_H"
    assert series["processed"]
    assert series["raw_run_code"] == RUN
    assert series["time_filter"]["applied"]
    assert series["row_count"] > 0
    assert series["sample_row_count"] == 2

    maximum = inspect_tuflow_result(
        str(PLOT / f"{RUN}_RLP_Hmx.csv"), locations=["RL_01"], minimum_time=2.0, sample_rows=1
    )
    assert maximum["data_type"] == "RLP_Hmx"
    assert maximum["processed"]
    assert maximum["row_count"] == 1
    assert not maximum["time_filter"]["applied"]


def test_reporting_point_workflow_discovers_sources_and_exports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exercise the public discovery→processor→report route using real data bytes."""
    source_dir = tmp_path / "inputs"
    output_dir = tmp_path / "outputs"
    source_dir.mkdir()
    output_dir.mkdir()
    for suffix in ("RLP_H", "RLP_Hmx"):
        copy2(PLOT / f"{RUN}_{suffix}.csv", source_dir / f"{RUN}_{suffix}.csv")

    monkeypatch.chdir(output_dir)
    assert main_processing(
        paths_to_process=[source_dir],
        locations_to_include=["RL_01"],
        export_mode="parquet",
        use_live_dashboard=False,
    )

    histories = list(output_dir.glob("*_reporting_points_Water_Level_Series.parquet.gzip"))
    maxima = list(output_dir.glob("*_reporting_points_Source_Maxima.parquet.gzip"))
    assert len(histories) == 1
    assert len(maxima) == 1
    history = pd.read_parquet(histories[0])
    maximum = pd.read_parquet(maxima[0])
    assert len(history) == 181
    assert len(maximum) == 1
    assert history["Value"].max() == pytest.approx(42.4816)
    assert maximum["Hmax"].iloc[0] == pytest.approx(42.4817)
