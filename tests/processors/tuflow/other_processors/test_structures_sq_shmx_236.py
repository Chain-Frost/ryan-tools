"""Genuine EG02 and synthetic TUFLOW SQ/SHmx acceptance for #236."""

from pathlib import Path
from shutil import copy2

import pandas as pd
import pytest

from ryan_library.mcp.server import inspect_tuflow_result
from ryan_library.orchestrators.tuflow.structure_results import main_processing
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.other_processors.SHmxProcessor import SHmxProcessor
from ryan_library.processors.tuflow.other_processors.SQProcessor import SQProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection

RUN = "EG02_013"
SAMPLES = Path(__file__).parents[3] / "test_data" / "tuflow" / "TUFLOW_Example_Model_Dataset" / "EG02" / "plot" / "csv"
FIELDS = (
    "ID (Lowest)", "Q Below", "Q Above", "Q Total", "A Below", "A Above", "A Total",
    "V Below", "V Above", "V Combined", "h U/S", "h D/S", "dh", "Time",
)


def processed(path: Path, locations: list[str] | None = None) -> BaseProcessor:
    result = BaseProcessor.from_file(path, entity_filter=locations)
    result.process()
    assert result.processed, path
    return result


def test_source_sq_is_total_crossing_flow_and_selected_before_expansion() -> None:
    p = processed(SAMPLES / f"{RUN}_SQ.csv", ["FC01.1_R"])
    assert isinstance(p, SQProcessor)
    assert p.data_type == "SQ"
    assert p.dataformat == "PO"
    assert list(p.raw_df.columns) == ["Time (h)", f"SQ FC01.1_R [{RUN}]"]
    assert len(p.df) == 181
    assert set(p.df["Location"].astype(str)) == {"FC01.1_R"}
    assert set(p.df["Type"].astype(str)) == {"Structure Total Crossing Flow"}
    assert p.df["Time"].iloc[11] == pytest.approx(0.183333)
    assert p.df["Value"].iloc[11] == pytest.approx(0.0779)
    assert p.df["AbsValue"].iloc[11] == pytest.approx(0.0779)


def test_real_source_shmx_keeps_all_component_values_not_independent_maxima() -> None:
    p = processed(SAMPLES / f"{RUN}_SHmx.csv", ["FC01.1_R"])
    assert isinstance(p, SHmxProcessor)
    assert p.data_type == "SHmx"
    assert p.dataformat == "StructureState"
    assert len(p.df) == 1
    row = p.df.iloc[0]
    assert row["Location"] == "FC01.1_R"
    assert "Maximum Upstream Water Level" in str(row["Type"])
    for field, value in {
        "Q Below": 53.0, "Q Above": 12.6, "Q Total": 65.6, "A Below": 5.8,
        "A Above": 10.6, "A Total": 16.5, "V Below": 9.092,
        "V Above": 1.180, "V Combined": 3.982, "h U/S": 49.893,
        "h D/S": 47.129, "dh": 2.764, "Time": 0.959,
    }.items():
        assert row[field] == pytest.approx(value)
    # Source-rounded A Total is 16.5, NOT the reconstructed 5.8 + 10.6.
    assert row["A Total"] != pytest.approx(row["A Below"] + row["A Above"])
    assert "Qmax" not in p.df.columns
    assert "AbsMax" not in p.df.columns


def test_weir_above_flow_is_not_relabelled_below_flow() -> None:
    row = processed(SAMPLES / f"{RUN}_SHmx.csv", ["FC_weir1"]).df.iloc[0]
    assert row["Q Below"] == 0
    assert row["Q Above"] == pytest.approx(82.4)
    assert row["Q Total"] == pytest.approx(82.4)


def test_collection_filters_and_keeps_source_products_separate() -> None:
    results = ProcessorCollection()
    for suffix in ("SQ", "SHmx"):
        results.add_processor(processed(SAMPLES / f"{RUN}_{suffix}.csv", ["FC01.1_R"]))
    assert len(results.get_processors_by_data_type("SQ").combine_raw()) == 181
    assert len(results.get_processors_by_data_type("SHmx").combine_raw()) == 1
    assert results.check_duplicates() == {}


def test_source_bad_duplicate_and_missing_time_are_invalid(tmp_path: Path) -> None:
    path = tmp_path / f"{RUN}_SHmx.csv"
    head = f'"{RUN} [model.tcf]",' + ",".join(f'"{item}"' for item in FIELDS) + "\n"
    row = '1,"Same",1,2,3,4,5,6,7,8,9,10,11,12,0.5\n'
    path.write_text(head + row + row.replace("1,", "2,", 1), encoding="utf-8")
    item = BaseProcessor.from_file(path)
    item.process()
    assert not item.processed
    assert item.df.empty
    path.write_text(head + row.replace("0.5", "**********"), encoding="utf-8")
    item.process()
    assert not item.processed
    assert item.df.empty


def test_source_signed_zero_and_missing_values_are_not_fake_peaks(tmp_path: Path) -> None:
    path = tmp_path / f"{RUN}_SHmx.csv"
    head = f'"{RUN} [model.tcf]",' + ",".join(f'"{item}"' for item in FIELDS) + "\n"
    path.write_text(
        head + '1,"Negative",-3,0,-3,**********,1,1,0,0,0,4,5,-1,0.25\n'
        + '2,"Missing",-99999,0,0,0,0,0,0,0,0,-99999,3,**********,0\n',
        encoding="utf-8",
    )
    neg = processed(path, ["Negative"])
    assert neg.df["Q Total"].iloc[0] == -3
    assert neg.df["Q Above"].iloc[0] == 0
    assert pd.isna(neg.df["A Below"].iloc[0])
    missing = processed(path, ["Missing"])
    assert pd.isna(missing.df["Q Below"].iloc[0])
    assert pd.isna(missing.df["h U/S"].iloc[0])
    assert pd.isna(missing.df["dh"].iloc[0])


def test_filtered_empty_and_invalid_sq_are_distinct(tmp_path: Path) -> None:
    path = tmp_path / f"{RUN}_SQ.csv"
    path.write_text(f'"{RUN} [model.tcf]","Time (h)","SQ One [{RUN}]"\n1,0,-1\n2,1,0\n', encoding="utf-8")
    assert processed(path, ["One"]).df["Value"].tolist() == [-1.0, 0.0]
    absent = BaseProcessor.from_file(path, entity_filter=["Absent"])
    absent.process()
    assert not absent.processed
    assert absent.filtered_empty
    path.write_text(f'"{RUN} [model.tcf]","Time (h)","SQ One [{RUN}]"\n1,0,nan\n', encoding="utf-8")
    invalid = BaseProcessor.from_file(path)
    invalid.process()
    assert not invalid.processed
    assert invalid.df.empty


def test_public_mcp_inspection_recognises_two_distinct_types() -> None:
    for suffix, field in (("SQ", "Value"), ("SHmx", "Q Total")):
        result = inspect_tuflow_result(str(SAMPLES / f"{RUN}_{suffix}.csv"), locations=["FC01.1_R"], sample_rows=1)
        assert result["data_type"] == suffix
        assert result["processed"]
        assert result["row_count"] > 0
        assert field in result["columns"]


def test_protected_excel_parquet_export_preserves_source_numbers_and_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, output = tmp_path / "source", tmp_path / "output"
    source.mkdir()
    output.mkdir()
    originals: dict[Path, bytes] = {}
    for suffix in ("SQ", "SHmx"):
        file = source / f"{RUN}_{suffix}.csv"
        copy2(SAMPLES / file.name, file)
        originals[file] = file.read_bytes()

    monkeypatch.chdir(output)
    assert main_processing(
        paths_to_process=[source],
        locations_to_include=["FC01.1_R"],
        export_mode="both",
        use_live_dashboard=False,
    )
    books = list(output.glob("*_structure_results.xlsx"))
    assert len(books) == 1
    history = pd.read_excel(books[0], sheet_name="Total_Crossing_Flow")  # pyright: ignore[reportUnknownMemberType]
    state = pd.read_excel(books[0], sheet_name="Max_Upstream_Level_State")  # pyright: ignore[reportUnknownMemberType]
    assert len(history) == 181
    assert len(state) == 1
    assert state["Q Total"].iloc[0] == pytest.approx(65.6)
    assert state["A Total"].iloc[0] == pytest.approx(16.5)
    assert len(list(output.glob("*_structure_results_Total_Crossing_Flow.parquet.gzip"))) == 1
    state_files = list(output.glob("*_structure_results_Max_Upstream_Level_State.parquet.gzip"))
    assert len(state_files) == 1
    assert pd.read_parquet(state_files[0])["h U/S"].iloc[0] == pytest.approx(49.893)
    assert all(path.read_bytes() == content for path, content in originals.items())
