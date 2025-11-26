import pytest
import pandas as pd
from pathlib import Path
from ryan_library.processors.tuflow.other_processors.ChanProcessor import ChanProcessor


def test_chan_processor_synthetic(tmp_path, change_cwd):
    """Test ChanProcessor with synthetic data."""
    with change_cwd(tmp_path):
        csv_path = tmp_path / "Test_Run_1d_Chan.csv"

        # ChanProcessor expects: Channel, LBUS Obvert, US Invert
        df = pd.DataFrame(
            {
                "Channel": ["Chan1"],
                "LBUS Obvert": [10.0],
                "US Invert": [8.0],
                "Length": [100.0],
                "n or Cd": [0.013],
                "pSlope": [0.01],
                "DS Invert": [7.0],
                "pBlockage": [0.0],
                "Flags": ["C"],
                "Extra": [1],
            }
        )
        df.to_csv(csv_path, index=False)

        processor = ChanProcessor(file_path=csv_path)
        processor.process()

    assert processor.processed
    assert not processor.df.empty

    # Check renames and calculations
    assert "Chan ID" in processor.df.columns  # Renamed from Channel
    assert "US Obvert" in processor.df.columns  # Renamed from LBUS Obvert
    assert "Height" in processor.df.columns  # Calculated

    assert processor.df.iloc[0]["Height"] == 2.0  # 10.0 - 8.0


def test_chan_processor_missing_columns(tmp_path, change_cwd):
    """Test robustness against missing columns."""
    with change_cwd(tmp_path):
        csv_path = tmp_path / "Bad_Run_1d_Chan.csv"
        # Missing US Invert
        df = pd.DataFrame({"Channel": ["Chan1"], "LBUS Obvert": [10.0]})
        df.to_csv(csv_path, index=False)

        processor = ChanProcessor(file_path=csv_path)
        processor.process()

        assert not processor.processed
        assert processor.df.empty


def test_chan_processor_real_file(find_test_file):
    """Test ChanProcessor with a real file if available."""
    file_path = find_test_file("_1d_Chan.csv")
    if not file_path:
        pytest.skip("No _1d_Chan.csv file found in test data")

    processor = ChanProcessor(file_path=file_path)
    processor.process()

    if processor.processed:
        assert not processor.df.empty
        assert "Height" in processor.df.columns
