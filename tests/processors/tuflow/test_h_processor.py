"""Regression coverage for TUFLOW HProcessor."""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path

import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest

from ryan_library.processors.tuflow.timeseries_1d.HProcessor import HProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection


@contextmanager
def change_working_directory(path: Path) -> None:
    """Temporarily change the working directory for processors relying on ``Path.cwd()``."""

    original_cwd = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(original_cwd)


def _write_h_csv(directory: Path, file_name: str) -> Path:
    """Create a minimal `_1d_H.csv` file for testing."""

    data = pd.DataFrame(
        {
            "Descriptor": ["row0", "row1", "row2"],
            "Time": [0.0, 1.0, 2.0],
            "H AAA.1": [1.0, 1.5, None],
            "H AAA.2": [2.0, None, None],
            "H BBB.1": [3.0, 3.5, 4.5],
            "H BBB.2": [4.0, None, 4.8],
        }
    )
    path = directory / file_name
    data.to_csv(path, index=False)
    return path


def test_h_processor_normalises_dual_timeseries(tmp_path: Path) -> None:
    """HProcessor should reshape upstream/downstream heads into tidy columns."""

    with change_working_directory(tmp_path):
        csv_path = _write_h_csv(Path.cwd(), "TestRun_1d_H.csv")
        processor = HProcessor(file_path=csv_path)
        processor.process()

    assert not processor.df.empty

    observed = processor.df[["Time", "Chan ID", "H_US", "H_DS"]].sort_values(["Chan ID", "Time"]).reset_index(drop=True)

    expected = pd.DataFrame(
        [
            (0.0, "AAA", 1.0, 2.0),
            (1.0, "AAA", 1.5, np.nan),
            (0.0, "BBB", 3.0, 4.0),
            (1.0, "BBB", 3.5, np.nan),
            (2.0, "BBB", 4.5, 4.8),
        ],
        columns=["Time", "Chan ID", "H_US", "H_DS"],
    )

    expected = expected.sort_values(["Chan ID", "Time"]).reset_index(drop=True)

    observed = observed.astype({"Chan ID": "string"})
    expected = expected.astype({"Chan ID": "string"})

    pdt.assert_frame_equal(observed, expected, check_dtype=False)

    # Ensure rows with no upstream/downstream data were discarded (AAA at Time 2.0).
    assert (observed["Chan ID"] == "AAA").sum() == 2


def test_collection_combines_h_timeseries(tmp_path: Path) -> None:
    """ProcessorCollection should preserve both H columns when aggregating timeseries."""

    with change_working_directory(tmp_path):
        csv_path = _write_h_csv(Path.cwd(), "Combo_1d_H.csv")
        processor = HProcessor(file_path=csv_path)
        processor.process()

    collection = ProcessorCollection()
    collection.add_processor(processor)

    combined = (
        collection.combine_1d_timeseries()[["internalName", "Chan ID", "Time", "H_US", "H_DS"]]
        .sort_values(["Chan ID", "Time"])
        .reset_index(drop=True)
    )

    expected = pd.DataFrame(
        [
            ("Combo", "AAA", 0.0, 1.0, 2.0),
            ("Combo", "AAA", 1.0, 1.5, np.nan),
            ("Combo", "BBB", 0.0, 3.0, 4.0),
            ("Combo", "BBB", 1.0, 3.5, np.nan),
            ("Combo", "BBB", 2.0, 4.5, 4.8),
        ],
        columns=["internalName", "Chan ID", "Time", "H_US", "H_DS"],
    )

    expected = expected.sort_values(["Chan ID", "Time"]).reset_index(drop=True)

    combined = combined.astype({"internalName": "string", "Chan ID": "string"})
    expected = expected.astype({"internalName": "string", "Chan ID": "string"})

    pdt.assert_frame_equal(combined, expected, check_dtype=False)


def test_h_processor_robustness(tmp_path: Path) -> None:
    """Test robustness against invalid files."""
    # Empty file
    empty_path = tmp_path / "Empty_Run_1d_H.csv"
    empty_path.touch()
    proc_empty = HProcessor(file_path=empty_path)
    proc_empty.process()
    assert not proc_empty.processed

    # Missing columns
    bad_path = tmp_path / "Bad_Run_1d_H.csv"
    pd.DataFrame({"Wrong": [1]}).to_csv(bad_path, index=False)
    proc_bad = HProcessor(file_path=bad_path)
    proc_bad.process()
    assert not proc_bad.processed


def test_h_processor_real_file(find_test_file):
    """Test HProcessor with a real file if available."""
    file_path = find_test_file("_1d_H.csv")
    if not file_path:
        pytest.skip("No _1d_H.csv file found in test data")

    processor = HProcessor(file_path=file_path)
    processor.process()

    if processor.processed:
        assert not processor.df.empty
        assert "H_US" in processor.df.columns or "H_DS" in processor.df.columns
