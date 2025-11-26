import pytest
import pandas as pd
from pathlib import Path
from ryan_library.processors.tuflow.timeseries_1d.VProcessor import VProcessor


def test_v_processor_synthetic(tmp_path, change_cwd):
    """Test VProcessor with synthetic data."""
    with change_cwd(tmp_path):
        csv_path = tmp_path / "Test_Run_1d_V.csv"

        # VProcessor expects columns like "Time", "V Chan1", "V Chan2"
        # It cleans "V " prefix.
        df = pd.DataFrame(
            {"Descriptor": ["row1", "row2"], "Time": [0.0, 1.0], "V Chan1": [0.5, 0.6], "V Chan2": [0.7, 0.8]}
        )
        df.to_csv(csv_path, index=False)

        processor = VProcessor(file_path=csv_path)
        processor.process()

    assert processor.processed
    assert not processor.df.empty

    # Check columns
    assert "Time" in processor.df.columns
    assert "Chan ID" in processor.df.columns
    assert "V" in processor.df.columns

    # Check values
    chan1 = processor.df[processor.df["Chan ID"] == "Chan1"]
    assert len(chan1) == 2
    assert chan1.iloc[0]["V"] == 0.5


def test_v_processor_robustness(tmp_path, change_cwd):
    """Test robustness against invalid files."""
    with change_cwd(tmp_path):
        # Empty file
        empty_path = tmp_path / "Empty_Run_1d_V.csv"
        empty_path.touch()
        proc_empty = VProcessor(file_path=empty_path)
        proc_empty.process()
        assert not proc_empty.processed

        # Missing Time column
        bad_path = tmp_path / "Bad_Run_1d_V.csv"
        pd.DataFrame({"V Chan1": [0.5]}).to_csv(bad_path, index=False)
        proc_bad = VProcessor(file_path=bad_path)
        proc_bad.process()
        assert not proc_bad.processed


def test_v_processor_real_file(find_test_file):
    """Test VProcessor with a real file if available."""
    file_path = find_test_file("_1d_V.csv")
    if not file_path:
        pytest.skip("No _1d_V.csv file found in test data")

    processor = VProcessor(file_path=file_path)
    processor.process()

    if processor.processed:
        assert not processor.df.empty
        assert "V" in processor.df.columns
