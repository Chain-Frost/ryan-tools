"""Verify existing HU/HD PO consumers against independent EG02 source values (issue #237)."""

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from ryan_library.orchestrators.tuflow.po_combine import export_results
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection

EG02: Path = Path(__file__).parents[3] / "test_data" / "tuflow" / "TUFLOW_Example_Model_Dataset" / "EG02"


@pytest.fixture(scope="module")
def bridge_po() -> BaseProcessor:
    """Use the real combined PO file; retain both measurement types at Bridge."""
    processor = BaseProcessor.from_file(EG02 / "EG02_011_PO.csv", entity_filter=["Bridge"])
    processor.process()
    assert processor.processed
    return processor


@pytest.mark.parametrize(
    ("measurement", "split_suffix", "first_wet", "valid_max"),
    [
        ("U/S Structure Water Level", "HU", 38.6572, 40.7877),
        ("D/S Structure Water Level", "HD", 38.3981, 40.6995),
    ],
)
def test_combined_po_preserves_source_hu_hd_and_split_numerical_parity(
    bridge_po: BaseProcessor,
    measurement: str,
    split_suffix: str,
    first_wet: float,
    valid_max: float,
) -> None:
    """The split and combined PO representations are the same observations, not two events."""
    rows = bridge_po.df.loc[bridge_po.df["Type"] == measurement].sort_values("Time")
    assert len(rows) == 181
    assert (rows["Location"] == "Bridge").all()
    assert (rows["Value"].iloc[:44] == -99999).all()
    assert rows["Time"].iloc[44] == pytest.approx(0.733333)
    assert rows["Value"].iloc[44] == pytest.approx(first_wet)
    assert rows["Value"].iloc[44:].max() == pytest.approx(valid_max)

    # Independent, native TUFLOW one-header CSV; no production split adapter is assumed.
    split = pd.read_csv(EG02 / "plot" / "csv" / f"EG02_011_2d_{split_suffix}.csv")
    assert len(split) == 181
    np.testing.assert_array_equal(rows["Time"].to_numpy(dtype=float), split.iloc[:, 1].to_numpy(dtype=float))
    np.testing.assert_array_equal(rows["Value"].to_numpy(dtype=float), split.iloc[:, 2].to_numpy(dtype=float))


def test_pomm_keeps_distinct_source_maxima_but_missing_minimum_requires_219() -> None:
    """Prove the raw POMM summary fields without accepting the existing invalid AbsMax."""
    processor = BaseProcessor.from_file(EG02 / "EG02_011_POMM.csv", entity_filter=["Bridge"])
    processor.process()
    assert processor.processed
    for measurement, maximum in (
        ("U/S Structure Water Level", 40.7877),
        ("D/S Structure Water Level", 40.6995),
    ):
        row = processor.df.loc[processor.df["Type"] == measurement]
        assert len(row) == 1
        assert row["Location"].iloc[0] == "Bridge"
        assert row["Max"].iloc[0] == pytest.approx(maximum)
        assert row["Tmax"].iloc[0] == pytest.approx(1.1333)
        assert row["Min"].iloc[0] == -99999
    # #219 owns normalization before AbsMax/SignedAbsMax; do not freeze the invalid result.


def test_po_hu_hd_survive_collection_filter_and_parquet_export(
    bridge_po: BaseProcessor, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test the real factory→collection→export path with data read back from disk."""
    collection = ProcessorCollection()
    collection.add_processor(bridge_po)
    collection.filter_locations(["Bridge"])
    combined = collection.po_combine()
    assert set(combined["Location"].astype(str)) == {"Bridge"}
    for measurement in ("U/S Structure Water Level", "D/S Structure Water Level"):
        assert (combined["Type"] == measurement).sum() == 181

    monkeypatch.chdir(tmp_path)
    assert export_results(results=collection, export_mode="parquet")
    reports = list(tmp_path.glob("*_combined_PO_combined_PO.parquet.gzip"))
    assert len(reports) == 1
    output = pd.read_parquet(reports[0])
    for measurement, maximum in (
        ("U/S Structure Water Level", 40.7877),
        ("D/S Structure Water Level", 40.6995),
    ):
        rows = output.loc[output["Type"] == measurement]
        assert len(rows) == 181
        assert rows.loc[rows["Value"] != -99999, "Value"].max() == pytest.approx(maximum)

 
def test_pinned_pytuflow_tpc_returns_independent_hu_hd_source_levels() -> None:
    """Existing public TPC reader must return actual levels, not merely the expected shape."""
    from pytuflow import TPC  # Delayed import keeps the ordinary PO regression separate.

    reader = TPC(EG02 / "plot" / "EG02_011.tpc")
    for data_type, first_wet, valid_max in (
        ("u-s structure water level", 38.6572, 40.7877),
        ("d-s structure water level", 38.3981, 40.6995),
    ):
        frame = reader.time_series("Bridge", data_type)
        assert frame.shape == (181, 1)
        values = frame.iloc[:, 0]
        assert (values.iloc[:44] == -99999).all()
        assert values.iloc[44] == pytest.approx(first_wet, abs=3e-6)
        assert values.iloc[44:].max() == pytest.approx(valid_max, abs=3e-6)
