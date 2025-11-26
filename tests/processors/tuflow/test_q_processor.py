import pytest
import pandas as pd
from pathlib import Path
from ryan_library.processors.tuflow.timeseries_1d.QProcessor import QProcessor


def test_q_processor_synthetic(tmp_path, change_cwd):
    """Test QProcessor with synthetic data."""
    with change_cwd(tmp_path):
        csv_path = tmp_path / "Test_Run_1d_Q.csv"

        # QProcessor expects columns like "Time", "Q Chan1", "Q Chan2"
        # It cleans "Q " prefix.
        df = pd.DataFrame(
            {"Descriptor": ["row1", "row2"], "Time": [0.0, 1.0], "Q Chan1": [10.0, 20.0], "Q Chan2": [30.0, 40.0]}
        )
        df.to_csv(csv_path, index=False)

        processor = QProcessor(file_path=csv_path)
        processor.process()

    assert processor.processed
    assert not processor.df.empty

    # Check columns
    assert "Time" in processor.df.columns
    assert "Chan ID" in processor.df.columns
    assert "Q" in processor.df.columns

    # Check values
    chan1 = processor.df[processor.df["Chan ID"] == "Chan1"]
    assert len(chan1) == 2
    assert chan1.iloc[0]["Q"] == 10.0


def test_q_processor_robustness(tmp_path, change_cwd):
    """Test robustness against invalid files."""
    with change_cwd(tmp_path):
        # Empty file
        empty_path = tmp_path / "Empty_Run_1d_Q.csv"
        empty_path.touch()
        proc_empty = QProcessor(file_path=empty_path)
        proc_empty.process()
        assert not proc_empty.processed

        # Missing Time column
        bad_path = tmp_path / "Bad_Run_1d_Q.csv"
        pd.DataFrame({"Q Chan1": [10.0]}).to_csv(bad_path, index=False)
        proc_bad = QProcessor(file_path=bad_path)
        proc_bad.process()
        assert not proc_bad.processed


def test_q_processor_real_file(find_test_file):
    """Test QProcessor with a real file if available."""
    file_path = find_test_file("_1d_Q.csv")
    if not file_path:
        pytest.skip("No _1d_Q.csv file found in test data")

    processor = QProcessor(file_path=file_path)
    processor.process()

    if processor.processed:
        assert not processor.df.empty
        assert "Q" in processor.df.columns
