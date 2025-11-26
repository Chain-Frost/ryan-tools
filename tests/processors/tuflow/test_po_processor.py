import pytest
import pandas as pd
from pathlib import Path
from ryan_library.processors.tuflow.other_processors.POProcessor import POProcessor


def test_po_processor_synthetic(tmp_path, change_cwd):
    """Test POProcessor with synthetic data."""
    with change_cwd(tmp_path):
        csv_path = tmp_path / "Test_Run_PO.csv"

        # PO Format:
        # Row 0: Measurement Type (e.g. "H", "V", "Q")
        # Row 1: Location Name (e.g. "Loc1", "Loc2")
        # Row 2+: Data (Time, Val1, Val2...)

        # One column must be Time (in either row 0 or 1)

        # Col 0: Run Info (dropped)
        # Col 1: Time
        # Col 2: Loc1 (H)
        # Col 3: Loc2 (V)

        data = [
            ["RunInfo", "Time", "H", "V"],  # Row 0: Measurement
            ["RunInfo", "Time", "Loc1", "Loc2"],  # Row 1: Location
            ["Run1", 0.0, 1.5, 0.5],
            ["Run1", 1.0, 1.6, 0.6],
        ]

        df = pd.DataFrame(data)
        df.to_csv(csv_path, index=False, header=False)

        processor = POProcessor(file_path=csv_path)
        processor.process()

    assert processor.processed
    assert not processor.df.empty

    # Check columns
    assert "Time" in processor.df.columns
    assert "Location" in processor.df.columns
    assert "Type" in processor.df.columns
    assert "Value" in processor.df.columns

    # Check values
    # Loc1, H
    loc1 = processor.df[(processor.df["Location"] == "Loc1") & (processor.df["Type"] == "H")]
    assert len(loc1) == 2
    assert float(loc1.iloc[0]["Value"]) == 1.5


def test_po_processor_robustness(tmp_path, change_cwd):
    """Test robustness against invalid files."""
    with change_cwd(tmp_path):
        # Empty file
        empty_path = tmp_path / "Empty_Run_PO.csv"
        empty_path.touch()
        proc_empty = POProcessor(file_path=empty_path)
        proc_empty.process()
        assert not proc_empty.processed

        # Missing Time column
        bad_path = tmp_path / "Bad_Run_PO.csv"
        data = [["RunInfo", "NotTime", "H"], ["RunInfo", "NotTime", "Loc1"], ["Run1", 0.0, 1.5]]
        pd.DataFrame(data).to_csv(bad_path, index=False, header=False)

        proc_bad = POProcessor(file_path=bad_path)
        proc_bad.process()
        assert not proc_bad.processed


def test_po_processor_real_file(find_test_file):
    """Test POProcessor with a real file if available."""
    file_path = find_test_file("_PO.csv")
    if not file_path:
        pytest.skip("No _PO.csv file found in test data")

    processor = POProcessor(file_path=file_path)
    processor.process()

    if processor.processed:
        assert not processor.df.empty
        assert "Value" in processor.df.columns
