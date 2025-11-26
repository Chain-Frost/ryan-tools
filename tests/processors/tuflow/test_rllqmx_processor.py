import pytest
import pandas as pd
from pathlib import Path
from ryan_library.processors.tuflow.other_processors.RLLQmxProcessor import RLLQmxProcessor


def test_rllqmx_processor_synthetic(tmp_path, change_cwd):
    """Test RLLQmxProcessor with synthetic data."""
    with change_cwd(tmp_path):
        csv_path = tmp_path / "Test_Run_RLL_Qmx.csv"

        # RLLQmx expects: ID, Qmax, Time Qmax, dQmax, Time dQmax, H
        df = pd.DataFrame(
            {"ID": ["Loc1"], "Qmax": [10.0], "Time Qmax": [5.0], "dQmax": [1.0], "Time dQmax": [5.1], "H": [2.5]}
        )
        df.to_csv(csv_path, index=False)

        processor = RLLQmxProcessor(file_path=csv_path)
        processor.process()

    assert processor.processed
    assert not processor.df.empty

    # Check renames
    assert "Q" in processor.df.columns  # from Qmax
    assert "Time" in processor.df.columns  # from Time Qmax
    assert "dQ" in processor.df.columns  # from dQmax
    assert "Time dQ" in processor.df.columns  # from Time dQmax

    # Check values
    assert processor.df.iloc[0]["Q"] == 10.0


def test_rllqmx_processor_robustness(tmp_path, change_cwd):
    """Test robustness against invalid files."""
    with change_cwd(tmp_path):
        # Empty file
        empty_path = tmp_path / "Empty_Run_RLL_Qmx.csv"
        empty_path.touch()
        proc_empty = RLLQmxProcessor(file_path=empty_path)
        proc_empty.process()
        assert not proc_empty.processed

        # Missing columns
        bad_path = tmp_path / "Bad_Run_RLL_Qmx.csv"
        pd.DataFrame({"ID": ["Loc1"]}).to_csv(bad_path, index=False)
        proc_bad = RLLQmxProcessor(file_path=bad_path)
        proc_bad.process()
        assert not proc_bad.processed


def test_rllqmx_processor_real_file(find_test_file):
    """Test RLLQmxProcessor with a real file if available."""
    file_path = find_test_file("_RLL_Qmx.csv")
    if not file_path:
        pytest.skip("No _RLL_Qmx.csv file found in test data")

    processor = RLLQmxProcessor(file_path=file_path)
    processor.process()

    if processor.processed:
        assert not processor.df.empty
        assert "Q" in processor.df.columns
