import pytest
import pandas as pd
from pathlib import Path
from ryan_library.processors.tuflow.timeseries_1d.CFProcessor import CFProcessor


def test_cf_processor_synthetic(tmp_path, change_cwd):
    """Test CFProcessor with synthetic data."""
    with change_cwd(tmp_path):
        csv_path = tmp_path / "Test_Run_1d_CF.csv"

        # CFProcessor expects columns like "Time", "F Chan1", "F Chan2"
        # It cleans "F " prefix.
        df = pd.DataFrame(
            {"Descriptor": ["row1", "row2"], "Time": [0.0, 1.0], "F Chan1": [1.1, 2.2], "F Chan2": [3.3, 4.4]}
        )
        df.to_csv(csv_path, index=False)

        processor = CFProcessor(file_path=csv_path)
        processor.process()

    assert processor.processed
    assert not processor.df.empty

    # Check columns
    assert "Time" in processor.df.columns
    assert "Chan ID" in processor.df.columns
    assert "CF" in processor.df.columns

    # Check values
    # CF is coerced to string in _apply_final_transformations
    assert processor.df["CF"].dtype == "string" or processor.df["CF"].dtype == "object"

    chan1 = processor.df[processor.df["Chan ID"] == "Chan1"]
    assert len(chan1) == 2
    # Check value as string
    assert float(chan1.iloc[0]["CF"]) == 1.1


def test_cf_processor_robustness(tmp_path, change_cwd):
    """Test robustness against invalid files."""
    with change_cwd(tmp_path):
        # Empty file
        empty_path = tmp_path / "Empty_Run_1d_CF.csv"
        empty_path.touch()
        proc_empty = CFProcessor(file_path=empty_path)
        proc_empty.process()
        assert not proc_empty.processed

        # Missing Time column
        bad_path = tmp_path / "Bad_Run_1d_CF.csv"
        pd.DataFrame({"F Chan1": [1.1]}).to_csv(bad_path, index=False)
        proc_bad = CFProcessor(file_path=bad_path)
        proc_bad.process()
        assert not proc_bad.processed


def test_cf_processor_real_file(find_test_file):
    """Test CFProcessor with a real file if available."""
    file_path = find_test_file("_1d_CF.csv")
    if not file_path:
        pytest.skip("No _1d_CF.csv file found in test data")

    processor = CFProcessor(file_path=file_path)
    processor.process()

    if processor.processed:
        assert not processor.df.empty
        assert "CF" in processor.df.columns
