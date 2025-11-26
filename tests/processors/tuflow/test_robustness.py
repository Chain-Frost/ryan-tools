"""Robustness tests for TUFLOW processors using the Example Model Dataset."""

from __future__ import annotations

from pathlib import Path
import pytest
import pandas as pd

from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection
from ryan_library.classes.suffixes_and_dtypes import SuffixesConfig


@pytest.fixture(scope="module")
def tuflow_test_data() -> Path:
    """Return the path to the TUFLOW Example Model Dataset."""
    # Relative path from this test file to the test data directory
    # This file is in tests/processors/tuflow/
    # Test data is in tests/test_data/tuflow/TUFLOW_Example_Model_Dataset
    base_path = Path(__file__).parents[2] / "test_data" / "tuflow" / "TUFLOW_Example_Model_Dataset"
    if not base_path.exists():
        pytest.skip(f"Test data not found at {base_path}")
    return base_path


def get_processable_files(root_dir: Path) -> list[Path]:
    """Recursively find all files that have a registered processor."""
    suffixes_config = SuffixesConfig.get_instance()
    processable_files = []

    # We only care about files that map to a known data type
    suffix_map = suffixes_config.invert_suffix_to_type()
    all_suffixes = [s for suffixes in suffix_map.values() for s in suffixes]

    for path in root_dir.rglob("*"):
        if path.is_file():
            # Check if file ends with any known suffix
            if any(path.name.endswith(suffix) for suffix in all_suffixes):
                processable_files.append(path)

    return processable_files


def test_smoke_process_all_files(tuflow_test_data: Path) -> None:
    """Smoke test: Ensure all processable files can be loaded and processed without error."""
    files = get_processable_files(tuflow_test_data)
    assert files, "No processable files found in test data directory"

    failures = []
    warnings = []

    # Files known to fail processing due to malformed data or missing columns
    # We hardcode these to ensure they continue to fail gracefully (processed=False)
    # rather than crashing or being processed incorrectly.
    KNOWN_PROBLEMATIC_FILES = {
        "EG02_012_PO.csv",
        "EG02_013_PO.csv",
        "EG02_014_PO.csv",
        "EG08_010_1d_ccA_L.dbf",
        "EG08_011_1d_ccA_L.dbf",
        "EG08_012_1d_ccA_L.dbf",
        "EG08_013_1d_ccA_L.dbf",
        "EG11_005_1d_ccA_L.dbf",
        "EG11_006_1d_ccA_L.dbf",
        "EG11_006_1d_Nmx.csv",
        "EG11_005_1d_ccA_L.dbf",
        "EG11_006_1d_ccA_L.dbf",
        "EG11_006_1d_Nmx.csv",
    }

    for file_path in files:
        try:
            processor = BaseProcessor.from_file(file_path)
            processor.process()

            if file_path.name in KNOWN_PROBLEMATIC_FILES:
                if processor.processed:
                    failures.append(f"{file_path.name}: Expected failure but was processed successfully")
                continue

            if not processor.processed:
                # If not processed, check if it's an empty file
                if file_path.stat().st_size == 0:
                    # Empty file, expected to not be processed
                    continue

                # If not empty but not processed, it might be a header mismatch or other validation issue
                # We log this as a warning but don't fail the test as per user request
                warnings.append(
                    f"{file_path.name}: Processor.processed is False (Size: {file_path.stat().st_size} bytes)"
                )
                continue

            if processor.df.empty:
                # DataFrame empty is allowed (e.g. filtered out data)
                continue

        except Exception as e:
            failures.append(f"{file_path.name}: Raised exception {e}")

    if warnings:
        print(f"\nWarnings (Processed=False): {len(warnings)} files")
        # Uncomment to see details
        # print("\n".join(warnings))

    assert not failures, f"Failures occurred in {len(failures)} files:\n" + "\n".join(failures[:10])


def test_integration_collection_timeseries(tuflow_test_data: Path) -> None:
    """Integration test: Combine timeseries files for a specific run."""
    # Target EG11_001 run
    run_id = "EG11_001"
    search_dir = tuflow_test_data / "EG11"

    # Find specific timeseries files for this run
    # We look for _1d_H.csv, _1d_Q.csv, _1d_V.csv which are common timeseries
    patterns = [f"{run_id}*_1d_H.csv", f"{run_id}*_1d_Q.csv", f"{run_id}*_1d_V.csv"]
    files = []
    for pattern in patterns:
        files.extend(list(search_dir.rglob(pattern)))

    assert files, f"No timeseries files found for {run_id}"

    collection = ProcessorCollection()
    for file_path in files:
        processor = BaseProcessor.from_file(file_path)
        processor.process()
        collection.add_processor(processor)

    combined_df = collection.combine_1d_timeseries()

    assert not combined_df.empty
    assert "Time" in combined_df.columns
    assert "Chan ID" in combined_df.columns
    assert "internalName" in combined_df.columns

    # Check for expected value columns based on input files
    # H processor produces H_US, H_DS
    # Q processor produces Q
    # V processor produces V
    expected_cols = ["H_US", "H_DS", "Q", "V"]
    found_cols = [col for col in expected_cols if col in combined_df.columns]
    assert found_cols, "None of the expected value columns found in combined DataFrame"


def test_integration_collection_maximums(tuflow_test_data: Path) -> None:
    """Integration test: Combine maximums files for a specific run."""
    # Target EG11_001 run
    run_id = "EG11_001"
    search_dir = tuflow_test_data / "EG11"

    # Find specific maximums files
    # _1d_Cmx.csv (Conduits Max), _1d_Nmx.csv (Nodes Max)
    patterns = [f"{run_id}*_1d_Cmx.csv", f"{run_id}*_1d_Nmx.csv"]
    files = []
    for pattern in patterns:
        files.extend(list(search_dir.rglob(pattern)))

    # Note: The test data might not have these exact files if they weren't generated.
    # Let's check what we found.
    if not files:
        pytest.skip(f"No maximums files (Cmx/Nmx) found for {run_id}")

    collection = ProcessorCollection()
    for file_path in files:
        processor = BaseProcessor.from_file(file_path)
        processor.process()
        collection.add_processor(processor)

    combined_df = collection.combine_1d_maximums()

    assert not combined_df.empty
    assert "Chan ID" in combined_df.columns
    assert "internalName" in combined_df.columns
    # Maximums combination drops Time
    assert "Time" not in combined_df.columns
