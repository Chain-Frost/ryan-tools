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


@contextmanager
def change_working_directory(path: Path) -> None:
    """Temporarily change the working directory for processors relying on ``Path.cwd()``."""

    original_cwd = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(original_cwd)


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


def test_nmx_processor_pivots_upstream_downstream_nodes(tmp_path: Path) -> None:
    """NmxProcessor should continue pivoting `.1`/`.2` suffixes into US/DS columns."""

    with change_working_directory(tmp_path):
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
