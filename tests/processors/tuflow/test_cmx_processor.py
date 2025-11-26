import pytest
import pandas as pd
from pathlib import Path
from ryan_library.processors.tuflow import CmxProcessor


def test_cmx_processor_synthetic(tmp_path, change_cwd):
    """Test CmxProcessor with synthetic data to verify reshaping logic."""
    with change_cwd(tmp_path):
        csv_path = tmp_path / "Test_Run_1d_Cmx.csv"

        # Create synthetic Cmx data
        # Cmx format usually has: Chan ID, Time Qmax, Qmax, Time Vmax, Vmax
        df = pd.DataFrame(
            {
                "Chan ID": ["Chan1", "Chan2"],
                "Time Qmax": [10.0, 20.0],
                "Qmax": [5.5, 6.6],
                "Time Vmax": [11.0, 21.0],
                "Vmax": [1.1, 1.2],
            }
        )
        df.to_csv(csv_path, index=False)

        processor = CmxProcessor(file_path=csv_path)
        processor.process()

    assert processor.processed
    assert not processor.df.empty

    # Verify reshaping: Should have rows for Q and rows for V
    # Original: 2 rows. Reshaped: 4 rows (2 for Q, 2 for V)
    assert len(processor.df) == 4

    # Check columns
    assert "Q" in processor.df.columns
    assert "V" in processor.df.columns
    assert "Time" in processor.df.columns

    # Check values for Chan1
    chan1 = processor.df[processor.df["Chan ID"] == "Chan1"]
    # Should have one row with Q=5.5 (V=NaN) and one with V=1.1 (Q=NaN)
    row_q = chan1[chan1["Q"].notna()]
    assert len(row_q) == 1
    assert row_q.iloc[0]["Q"] == 5.5
    assert pd.isna(row_q.iloc[0]["V"])
    assert row_q.iloc[0]["Time"] == 10.0

    row_v = chan1[chan1["V"].notna()]
    assert len(row_v) == 1
    assert row_v.iloc[0]["V"] == 1.1
    assert pd.isna(row_v.iloc[0]["Q"])
    assert row_v.iloc[0]["Time"] == 11.0


def test_cmx_processor_real_file(find_test_file):
    """Test CmxProcessor with a real file if available."""
    file_path = find_test_file("_1d_Cmx.csv")
    if not file_path:
        pytest.skip("No _1d_Cmx.csv file found in test data")

    processor = CmxProcessor(file_path=file_path)
    processor.process()

    assert processor.processed
    assert not processor.df.empty
    assert "internalName" in processor.df.columns
    # Check for critical columns after reshaping
    assert "Q" in processor.df.columns
    assert "V" in processor.df.columns


def test_cmx_processor_missing_columns(tmp_path, change_cwd):
    """Test robustness against missing columns."""
    with change_cwd(tmp_path):
        csv_path = tmp_path / "Bad_Run_1d_Cmx.csv"
        # Missing Qmax
        df = pd.DataFrame(
            {
                "Chan ID": ["Chan1"],
                "Time Qmax": [10.0],
                # Qmax missing
                "Time Vmax": [11.0],
                "Vmax": [1.1],
            }
        )
        df.to_csv(csv_path, index=False)

        processor = CmxProcessor(file_path=csv_path)
        processor.process()

        # Should fail gracefully
        assert not processor.processed
        assert processor.df.empty


def test_cmx_processor_empty_file(tmp_path, change_cwd):
    """Test robustness against empty file."""
    with change_cwd(tmp_path):
        csv_path = tmp_path / "Empty_Run_1d_Cmx.csv"
        csv_path.touch()

        processor = CmxProcessor(file_path=csv_path)
        processor.process()

        assert not processor.processed
        assert processor.df.empty
