"""Genuine EG02 model/domain and node summary mass-balance source contracts."""

from pathlib import Path
from shutil import copy2

import pandas as pd
import pytest

from ryan_library.mcp.server import inspect_tuflow_result
from ryan_library.orchestrators.tuflow.model_mass_balance import main_processing as export_model
from ryan_library.orchestrators.tuflow.node_mass_balance_summary import main_processing as export_node
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection

ROOT = Path(__file__).parents[3] / "test_data" / "tuflow" / "TUFLOW_Example_Model_Dataset" / "EG02"
RUN = "EG02_013"


def processed(path: Path, locations: list[str] | None = None) -> BaseProcessor:
    processor = BaseProcessor.from_file(path, entity_filter=locations)
    processor.process()
    assert processor.processed, path
    return processor


@pytest.mark.parametrize(
    ("suffix", "scope", "metric", "expected"),
    [
        ("MB", "1D and 2D Model", "Q Vol In", 5.079493),
        ("MB1D", "All 1D Domains", "Q V In", 5.079493),
        ("MB_HPC", "HPC Model", "iStep", 129.0),
    ],
)
def test_real_model_domain_mass_balance_first_sample(suffix: str, scope: str, metric: str, expected: float) -> None:
    result = processed(ROOT / f"{RUN}_{suffix}.csv")
    assert result.data_type == suffix
    assert result.dataformat == "ModelMassBalanceHistory"
    assert set(result.df["Location"].astype(str)) == {scope}
    row = result.df.loc[result.df["Type"].eq(metric) & result.df["Time"].eq(0.017)]
    assert len(row) == 1
    assert row["Value"].iloc[0] == pytest.approx(expected)
    assert "AbsMax" not in result.df.columns


def test_signed_mass_error_is_not_absolute_or_cross_domain_relabelled() -> None:
    result = processed(ROOT / f"{RUN}_MB.csv")
    row = result.df.loc[result.df["Type"].eq("Vol Err") & result.df["Time"].eq(0.033)]
    assert len(row) == 1
    assert row["Value"].iloc[0] == pytest.approx(-0.001212357)
    assert "Cum Vol Err" in set(result.df["Type"].astype(str))
    assert "Cum ME (%)" in set(result.df["Type"].astype(str))


def test_longest_node_summary_suffix_is_dispatched_before_global_mb() -> None:
    model = BaseProcessor.from_file(ROOT / f"{RUN}_MB.csv")
    node = BaseProcessor.from_file(ROOT / f"{RUN}_1d_MB.csv")
    assert model.data_type == "MB"
    assert node.data_type == "MB_Node"
    assert type(model) is not type(node)


def test_node_mass_balance_summary_preserves_three_cumulative_fields() -> None:
    summary = processed(ROOT / f"{RUN}_1d_MB.csv", ["ds1.1"])
    assert summary.data_type == "MB_Node"
    assert summary.dataformat == "NodeMassBalanceSummary"
    assert len(summary.df) == 1
    row = summary.df.iloc[0]
    assert row["Location"] == "ds1.1"
    assert row["Cum %ME"] == pytest.approx(0.0)
    assert row["Cum VE"] == pytest.approx(-5.54)
    assert row["Cum VQ"] == pytest.approx(279054.156)
    assert "Time" not in summary.df.columns


def test_node_summary_filter_missing_is_not_malformed() -> None:
    p = BaseProcessor.from_file(ROOT / f"{RUN}_1d_MB.csv", entity_filter=["NotANode"])
    p.process()
    assert not p.processed
    assert getattr(p, "filtered_empty", False)


def test_distinct_model_and_node_metric_collections() -> None:
    collection = ProcessorCollection()
    for suffix in ("MB", "MB1D", "MB_HPC", "1d_MB"):
        collection.add_processor(processed(ROOT / f"{RUN}_{suffix}.csv"))
    for dtype in ("MB", "MB1D", "MB_HPC", "MB_Node"):
        assert len(collection.get_processors_by_data_type(dtype).combine_raw()) > 0


def test_invalid_model_metric_and_node_header_fail_gracefully(tmp_path: Path) -> None:
    model = tmp_path / f"{RUN}_MB.csv"
    model.write_text('"Time (h)","1D and 2D:","Q ME (%)"\n0,,infinity\n', encoding="utf-8")
    invalid = BaseProcessor.from_file(model)
    invalid.process()
    assert not invalid.processed
    node = tmp_path / f"{RUN}_1d_MB.csv"
    node.write_text('"model","Node:","One"\n"model","Wrong",1\n', encoding="utf-8")
    invalid = BaseProcessor.from_file(node)
    invalid.process()
    assert not invalid.processed


@pytest.mark.parametrize(
    "record",
    [
        "0.017,129,0.5,999",  # Extra field could shift Time into an implicit pandas index.
        "0.017,129",  # Missing field could be silently padded.
    ],
)
def test_model_history_rejects_mismatched_source_row_width(tmp_path: Path, record: str) -> None:
    model = tmp_path / f"{RUN}_MB_HPC.csv"
    model.write_text('"Time (h)","iStep","dtTarget"\n' + record + "\n", encoding="utf-8")
    invalid = BaseProcessor.from_file(model)
    invalid.process()
    assert not invalid.processed
    assert invalid.df.empty


@pytest.mark.parametrize("invalid_node_content", ["", '"bad","Node:","ds1.1"\n"bad","Wrong",1\n'])
def test_model_export_ignores_unrequested_invalid_or_empty_node_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, invalid_node_content: str
) -> None:
    source, output = tmp_path / "input", tmp_path / "output"
    source.mkdir()
    output.mkdir()
    copy2(ROOT / f"{RUN}_MB.csv", source / f"{RUN}_MB.csv")
    node = source / f"{RUN}_1d_MB.csv"
    node.write_text(invalid_node_content, encoding="utf-8")
    original = node.read_bytes()
    monkeypatch.chdir(output)

    assert export_model(paths_to_process=[source], export_mode="parquet", use_live_dashboard=False)
    exported = list(output.glob("*_model_mass_balance_Global_MB.parquet.gzip"))
    assert len(exported) == 1
    assert set(pd.read_parquet(exported[0])["Location"].astype(str)) == {"1D and 2D Model"}
    assert node.read_bytes() == original


def test_read_only_mcp_inspects_separate_source_metric_schemas() -> None:
    for suffix, field in (("MB", "Value"), ("1d_MB", "Cum VE")):
        report = inspect_tuflow_result(str(ROOT / f"{RUN}_{suffix}.csv"), sample_rows=1)
        assert report["processed"]
        assert field in report["columns"]


def test_protected_model_and_node_exports(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source, output = tmp_path / "source", tmp_path / "output"
    source.mkdir()
    output.mkdir()
    originals: dict[Path, bytes] = {}
    for suffix in ("MB", "MB1D", "MB_HPC", "1d_MB"):
        target = source / f"{RUN}_{suffix}.csv"
        copy2(ROOT / target.name, target)
        originals[target] = target.read_bytes()

    monkeypatch.chdir(output)
    assert export_model(paths_to_process=[source], export_mode="parquet", use_live_dashboard=False)
    assert export_node(
        paths_to_process=[source],
        locations_to_include=["ds1.1"],
        export_mode="parquet",
        use_live_dashboard=False,
    )
    all_model = list(output.glob("*_model_mass_balance_Global_MB.parquet.gzip"))
    nodes = list(output.glob("*_node_mass_balance_summary_Node_Cumulative_Summary.parquet.gzip"))
    assert len(all_model) == len(nodes) == 1
    model = pd.read_parquet(all_model[0])
    assert set(model["Location"].astype(str)) == {"1D and 2D Model"}
    node = pd.read_parquet(nodes[0])
    assert len(node) == 1
    assert node["Cum VE"].iloc[0] == pytest.approx(-5.54)
    assert all(path.read_bytes() == blob for path, blob in originals.items())
