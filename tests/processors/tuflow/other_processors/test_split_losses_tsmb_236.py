"""Real source and synthetic compatibility checks for #236 staged CSV readers."""

from collections.abc import Callable
from pathlib import Path
from shutil import copy2

import pandas as pd
import pytest

from ryan_library.mcp.server import inspect_tuflow_result
from ryan_library.orchestrators.tuflow.culvert_losses import main_processing as export_losses
from ryan_library.orchestrators.tuflow.node_mass_balance_history import main_processing as export_node_mb
from ryan_library.orchestrators.tuflow.split_2d_results import main_processing as export_split
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection

EG02 = Path(__file__).parents[3] / "test_data" / "tuflow" / "TUFLOW_Example_Model_Dataset" / "EG02"
CSV = EG02 / "plot" / "csv"


def processed(path: Path, locations: list[str] | None = None) -> BaseProcessor:
    item = BaseProcessor.from_file(path, entity_filter=locations)
    item.process()
    assert item.processed, path
    return item


@pytest.mark.parametrize(
    ("suffix", "source_type", "location"),
    [
        ("2d_H", "Water Level", "PO_01"),
        ("2d_Q", "Flow", "PO_02"),
        ("2d_V", "Velocity", "PO_02"),
    ],
)
def test_split_real_source_matches_combined_po_values(suffix: str, source_type: str, location: str) -> None:
    split = processed(CSV / f"EG02_010_{suffix}.csv", [location])
    combined = processed(EG02 / "EG02_010_PO.csv", [location])
    assert split.data_type == suffix
    assert split.dataformat == "SplitPO"
    assert len(split.raw_df.columns) == 2
    assert set(split.df["Type"].astype(str)) == {source_type}
    expected = combined.df.loc[combined.df["Type"].eq(source_type)].sort_values("Time")
    actual = split.df.sort_values("Time")
    assert actual["Time"].tolist() == expected["Time"].tolist()
    assert actual["Value"].tolist() == expected["Value"].tolist()


def test_split_sources_are_separate_from_po_grouping_and_allow_selected_columns() -> None:
    collection = ProcessorCollection()
    split = processed(CSV / "EG02_010_2d_H.csv", ["PO_01"])
    assert list(split.raw_df.columns) == ["Time (h)", "H PO_01 [EG02_010]"]
    collection.add_processor(split)
    assert len(collection.combine_raw()) == len(split.df)
    assert collection.po_combine().empty


def test_real_loss_components_preserve_entry_additional_exit_identity() -> None:
    item = processed(CSV / "EG02_014_1d_L.csv", ["Pipe0"])
    assert item.data_type == "L"
    assert item.dataformat == "LossCoefficient"
    assert list(item.raw_df.columns) == [
        "Time(h)",
        "Entry LC Pipe0 [EG02_014]",
        "Additional LC Pipe0 [EG02_014]",
        "Exit LC Pipe0 [EG02_014]",
    ]
    assert len(item.df) == 3 * 181
    assert set(item.df["Type"].astype(str)) == {"Entry LC", "Additional LC", "Exit LC"}
    sample = item.df.loc[item.df["Time"].eq(0.016667)].set_index("Type")["Value"]
    assert sample.loc["Entry LC"] == pytest.approx(0.5)
    assert sample.loc["Additional LC"] == pytest.approx(0)
    assert sample.loc["Exit LC"] == pytest.approx(0.86)


def test_loss_node_component_keeps_node_identity() -> None:
    item = processed(CSV / "EG02_014_1d_L.csv", ["Pit1"])
    assert set(item.df["Location"].astype(str)) == {"Pit1"}
    assert set(item.df["Type"].astype(str)) == {"LC"}


def test_node_tsmb_is_not_model_or_node_summary_balance() -> None:
    item = processed(CSV / "EG02_014_1d_TSMB.csv", ["Pit1"])
    assert item.data_type == "TSMB"
    assert item.dataformat == "NodeMassBalanceHistory"
    assert len(item.raw_df.columns) == 2
    assert len(item.df) == 181
    assert set(item.df["Location"].astype(str)) == {"Pit1"}
    assert set(item.df["Type"].astype(str)) == {"1D Node Mass Balance History"}
    assert item.df["Value"].iloc[0] == 0


def test_component_source_duplicate_rejected_but_valid_distinct_components(tmp_path: Path) -> None:
    path = tmp_path / "EG02_014_1d_L.csv"
    path.write_text(
        '"EG02_014 [model.tcf]","Time(h)","Entry LC Pipe [EG02_014]","Exit LC Pipe [EG02_014]"\n'
        "1,0,-3,0\n2,1,**********,-99999\n",
        encoding="utf-8",
    )
    valid = processed(path, ["Pipe"])
    assert len(valid.df) == 4
    assert valid.df["Value"].iloc[0] == -3
    assert pd.isna(valid.df["Value"].iloc[1])
    assert valid.df["Value"].iloc[2] == 0
    assert pd.isna(valid.df["Value"].iloc[3])
    path.write_text(
        '"EG02_014 [model.tcf]","Time(h)","Entry LC Pipe [EG02_014]","Entry LC Pipe [EG02_014]"\n'
        "1,0,1,2\n",
        encoding="utf-8",
    )
    invalid = BaseProcessor.from_file(path)
    invalid.process()
    assert not invalid.processed


@pytest.mark.parametrize(
    ("run", "dtype", "location"),
    [
        ("EG02_010", "2d_H", "PO_01"),
        ("EG02_014", "L", "Pipe0"),
        ("EG02_014", "TSMB", "Pit1"),
    ],
)
def test_mcp_identifies_new_types(run: str, dtype: str, location: str) -> None:
    source_suffix = {"2d_H": "2d_H", "L": "1d_L", "TSMB": "1d_TSMB"}[dtype]
    result = inspect_tuflow_result(
        str(CSV / f"{run}_{source_suffix}.csv"), locations=[location], sample_rows=1
    )
    assert result["processed"]
    assert result["data_type"] == dtype


@pytest.mark.parametrize(
    ("workflow", "run", "suffix", "pattern"),
    [
        (export_split, "EG02_010", "2d_H", "*_split_2d_Split_Water_Level.parquet.gzip"),
        (export_losses, "EG02_014", "1d_L", "*_loss_coefficients_Loss_Coefficient_Series.parquet.gzip"),
        (export_node_mb, "EG02_014", "1d_TSMB", "*_node_mass_balance_Node_Mass_Balance_History.parquet.gzip"),
    ],
)
def test_protected_family_exports(
    workflow: Callable[..., bool], run: str, suffix: str, pattern: str, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    src, dest = tmp_path / "src", tmp_path / "dest"
    src.mkdir()
    dest.mkdir()
    file = src / f"{run}_{suffix}.csv"
    copy2(CSV / file.name, file)
    before = file.read_bytes()
    monkeypatch.chdir(dest)
    assert workflow(
        paths_to_process=[src],
        export_mode="parquet",
        use_live_dashboard=False,
    )
    assert len(list(dest.glob(pattern))) == 1
    assert file.read_bytes() == before
