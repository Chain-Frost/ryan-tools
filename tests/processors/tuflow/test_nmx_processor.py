"""Regression coverage for TUFLOW NmxProcessor."""

from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path

import numpy as np
import pandas as pd
import pandas.testing as pdt

from ryan_library.processors.tuflow import NmxProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection


# change_working_directory removed in favor of change_cwd fixture


def _write_nmx_csv(directory: Path, file_name: str) -> Path:
    """Create a minimal `_1d_Nmx.csv` file for testing upstream/downstream pivots."""

    data = pd.DataFrame(
        {
            "Node ID": ["CULVERT.1", "CULVERT.2", "CULVERT2.1"],
            "Hmax": [1.2, 1.4, 2.0],
            "Time Hmax": [10.0, 10.0, 12.0],
        }
    )
    path = directory / file_name
    data.to_csv(path, index=False)
    return path


def test_nmx_processor_pivots_upstream_downstream_nodes(tmp_path: Path, change_cwd) -> None:
    """NmxProcessor should continue pivoting `.1`/`.2` suffixes into US/DS columns."""

    with change_cwd(tmp_path):
        csv_path = _write_nmx_csv(Path.cwd(), "NmxRun_1d_Nmx.csv")
        processor = NmxProcessor(file_path=csv_path)
        processor.process()

    observed = processor.df[["Chan ID", "Time", "US_h", "DS_h"]].sort_values("Chan ID").reset_index(drop=True)
    observed = observed.rename_axis(columns=None)

    expected = pd.DataFrame(
        [
            ("CULVERT", 10.0, 1.2, 1.4),
            ("CULVERT2", 12.0, 2.0, np.nan),
        ],
        columns=["Chan ID", "Time", "US_h", "DS_h"],
    )
    expected = expected.sort_values("Chan ID").reset_index(drop=True)
    expected = expected.rename_axis(columns=None)

    observed = observed.astype({"Chan ID": "string"})
    expected = expected.astype({"Chan ID": "string"})

    pdt.assert_frame_equal(observed, expected, check_dtype=False)

    collection = ProcessorCollection()
    collection.add_processor(processor)

    combined = (
        collection.combine_1d_maximums()[["internalName", "Chan ID", "US_h", "DS_h"]]
        .sort_values("Chan ID")
        .reset_index(drop=True)
    )
    combined = combined.rename_axis(columns=None)

    expected_combined = pd.DataFrame(
        [
            ("NmxRun", "CULVERT", 1.2, 1.4),
            ("NmxRun", "CULVERT2", 2.0, np.nan),
        ],
        columns=["internalName", "Chan ID", "US_h", "DS_h"],
    )

    expected_combined = expected_combined.sort_values("Chan ID").reset_index(drop=True)
    expected_combined = expected_combined.rename_axis(columns=None)

    combined = combined.astype({"internalName": "string", "Chan ID": "string"})
    expected_combined = expected_combined.astype({"internalName": "string", "Chan ID": "string"})

    pdt.assert_frame_equal(combined, expected_combined, check_dtype=False)


def test_nmx_processor_real_file(find_test_file):
    """Test NmxProcessor with a real file if available."""
    file_path = find_test_file("_1d_Nmx.csv")
    if not file_path:
        pytest.skip("No _1d_Nmx.csv file found in test data")

    processor = NmxProcessor(file_path=file_path)
    processor.process()

    assert processor.processed
    assert not processor.df.empty
    assert "internalName" in processor.df.columns
    # Nmx usually has Hmax, Time Hmax
    # But NmxProcessor might pivot them?
    # Let's check if it has US_h/DS_h if it did pivoting, or just Hmax if not.
    # The processor logic pivots if .1/.2 suffixes exist.
    # We just assert it processed something.


def test_nmx_processor_robustness(tmp_path, change_cwd):
    """Test robustness against invalid files."""
    with change_cwd(tmp_path):
        # Empty file
        empty_path = tmp_path / "Empty_Run_1d_Nmx.csv"
        empty_path.touch()
        proc_empty = NmxProcessor(file_path=empty_path)
        proc_empty.process()
        assert not proc_empty.processed

        # Missing columns
        bad_path = tmp_path / "Bad_Run_1d_Nmx.csv"
        pd.DataFrame({"Wrong": [1]}).to_csv(bad_path, index=False)
        proc_bad = NmxProcessor(file_path=bad_path)
        proc_bad.process()
        assert not proc_bad.processed
