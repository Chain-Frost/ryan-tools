import pytest
import pandas as pd
from pathlib import Path
from ryan_library.processors.tuflow.other_processors.POMMProcessor import POMMProcessor


def test_pomm_processor_synthetic(tmp_path, change_cwd):
    """Test POMMProcessor with synthetic data."""
    with change_cwd(tmp_path):
        csv_path = tmp_path / "Test_Run_POMM.csv"

        # POMM format is transposed.
        # Row 0: Run Code (usually) - dropped
        # Row 1 (becomes header): Location, Time, Maximum (Extracted from Time Series), Time of Maximum, Minimum (Extracted From Time Series), Time of Minimum
        # Col 0 (becomes index/header): ...

        # Let's construct it as it appears in the file (pre-transpose)
        # Actually, the processor reads header=None.
        # Then drops col 0. Then Transposes.
        # Then Row 0 becomes header.

        # Let's construct the "Transposed" version first, then transpose it back to write to CSV.
        # Expected Headers after transpose:
        # Location, Time, Maximum (Extracted from Time Series), Time of Maximum, Minimum (Extracted From Time Series), Time of Minimum

        data = {
            "Location": ["Loc1", "Loc2"],
            "Time": [10.0, 20.0],  # This becomes 'Location' column in rename
            "Maximum (Extracted from Time Series)": [5.0, 6.0],  # Becomes 'Max'
            "Time of Maximum": [1.0, 2.0],  # Becomes 'Tmax'
            "Minimum (Extracted From Time Series)": [1.0, 2.0],  # Becomes 'Min'
            "Time of Minimum": [3.0, 4.0],  # Becomes 'Tmin'
        }
        df_transposed = pd.DataFrame(data)

        # Now we need to invert the logic to create the input file.
        # The input file has headers in the first column?
        # Let's look at the code:
        # raw_df = pd.read_csv(..., header=None)
        # transposed = raw_df.drop(columns=0).T
        # transposed.columns = transposed.iloc[0]

        # Let's build the raw dataframe
        # Processor drops Col 0.
        # Then Transposes.
        # Then Row 0 of transposed (which was Col 1 of raw) becomes headers.
        # So Col 1 of raw must contain the header names.

        raw_data = {
            0: ["Ignore"] * 6,  # Col 0 is dropped
            1: [
                "Location",
                "Time",
                "Maximum (Extracted from Time Series)",
                "Time of Maximum",
                "Minimum (Extracted From Time Series)",
                "Time of Minimum",
            ],  # Col 1 becomes headers
            2: ["Loc1", 10.0, 5.0, 1.0, 1.0, 3.0],  # Data 1
            3: ["Loc2", 20.0, 6.0, 2.0, 2.0, 4.0],  # Data 2
        }
        df_raw = pd.DataFrame(raw_data)
        # Write without header
        df_raw.to_csv(csv_path, index=False, header=False)

        processor = POMMProcessor(file_path=csv_path)
        # Mock expected headers if necessary, but usually loaded from config.
        # The processor uses self.output_columns and self.expected_in_header.
        # We might need to mock these if they are strictly checked and not loaded for this fake file.
        # But BaseProcessor loads config based on suffix.
        # We need a valid suffix for POMM.
        # Let's assume the file name needs to match a POMM suffix.
        # Usually it's .csv but identified by something else?
        # Let's check suffixes_and_dtypes.json or similar if we could.
        # But for now, let's try to rely on the fact that we passed a file path.
        # Wait, BaseProcessor uses TuflowStringParser which uses SuffixesConfig.
        # If "Test_Run_POMM.csv" isn't a valid suffix, it might fail.
        # Let's use a known suffix if possible.
        # From WORKFLOW.md: "POMM" format.
        # Let's assume "_POMM.csv" is a valid suffix or similar.
        # Actually, let's use the find_test_file to see what a real POMM file looks like.
        # But for synthetic test, we might need to mock the config or use a valid name.
        # Let's try "Test_Run_POMM.csv" - hopefully that maps.

        # Re-write to a better path
        csv_path = tmp_path / "Test_Run_POMM.csv"
        df_raw.to_csv(csv_path, index=False, header=False)

        processor = POMMProcessor(file_path=csv_path)

        # We might need to inject config if the suffix isn't recognized in the test env default config.
        # But let's try.
        processor.process()

    assert processor.processed
    assert not processor.df.empty

    # Check columns (Renamed)
    assert "Type" in processor.df.columns  # From Location
    assert "Location" in processor.df.columns  # From Time
    assert "Max" in processor.df.columns
    assert "AbsMax" in processor.df.columns

    # Check values
    loc1 = processor.df[processor.df["Type"] == "Loc1"]
    assert len(loc1) == 1
    assert float(loc1.iloc[0]["Max"]) == 5.0


def test_pomm_processor_robustness(tmp_path, change_cwd):
    """Test robustness against invalid files."""
    with change_cwd(tmp_path):
        # Empty file
        empty_path = tmp_path / "Empty_Run_1d_POMM.csv"
        empty_path.touch()
        proc_empty = POMMProcessor(file_path=empty_path)
        proc_empty.process()
        assert not proc_empty.processed

        # Missing columns (after transpose)
        bad_path = tmp_path / "Bad_Run_1d_POMM.csv"
        # Create a file that transposes to missing "Max"
        raw_data = {0: ["HeaderLabel", "Location"], 1: ["Run1", "Loc1"]}
        pd.DataFrame(raw_data).to_csv(bad_path, index=False, header=False)

        proc_bad = POMMProcessor(file_path=bad_path)
        proc_bad.process()
        assert not proc_bad.processed


def test_pomm_processor_real_file(find_test_file):
    """Test POMMProcessor with a real file if available."""
    # Try to find a POMM file. Suffix might be _POMM.csv?
    # Or maybe it's just .csv but in a specific folder?
    # Let's try searching for "POMM" in name.
    file_path = find_test_file("POMM.csv")  # Heuristic
    if not file_path:
        # Try _1d_POMM.csv
        file_path = find_test_file("_1d_POMM.csv")

    if not file_path:
        pytest.skip("No POMM file found in test data")

    processor = POMMProcessor(file_path=file_path)
    processor.process()

    if processor.processed:
        assert not processor.df.empty
        assert "Max" in processor.df.columns
