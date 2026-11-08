"""Robustness tests for TUFLOW processors using the Example Model Dataset."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from ryan_library.classes.suffixes_and_dtypes import SuffixesConfig
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection
from tests.processors.tuflow.fixture_contract_assertions import assert_declared_outcome, load_declared_cases


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
    processable_files: list[Path] = []

    # We only care about files that map to a known data type
    suffix_map = suffixes_config.invert_suffix_to_type()
    all_suffixes = [s for suffixes in suffix_map.values() for s in suffixes]

    for path in root_dir.rglob("*"):
        # Check if the file ends with any known suffix.
        if path.is_file() and any(path.name.endswith(suffix) for suffix in all_suffixes):
            processable_files.append(path)

    return processable_files


def _unclassified_file_result(
    file_path: Path, processor: BaseProcessor, known_problematic: dict[str, str]
) -> tuple[str | None, str | None]:
    """Keep legacy unknown-format diagnostics outside the strict manifest gate."""
    if file_path.name in known_problematic:
        if processor.processed:
            return f"{file_path.name}: Expected failure but was processed successfully", None
        return None, None
    if not processor.processed and file_path.stat().st_size:
        warning = f"{file_path.name}: Processor.processed is False (Size: {file_path.stat().st_size} bytes)"
        return None, warning
    # Unclassified zero-byte or processed-empty inputs remain historical
    # diagnostic-only cases until their outcome is explicitly classified.
    return None, None


def test_smoke_process_all_files(tuflow_test_data: Path) -> None:
    """Smoke test: Ensure all processable files can be loaded and processed without error."""
    files = get_processable_files(tuflow_test_data)
    assert files, "No processable files found in test data directory"

    failures: list[str] = []
    warnings: list[str] = []
    declared_cases = load_declared_cases()
    observed_declared: set[str] = set()

    # Files known to fail processing due to malformed data or missing columns
    # We hardcode these to ensure they continue to fail gracefully (processed=False)
    # rather than crashing or being processed incorrectly.
    KNOWN_PROBLEMATIC_FILES = {
        "EG02_012_PO.csv": "malformed file - empty",
        "EG02_013_PO.csv": "malformed file - empty",
        "EG02_014_PO.csv": "malformed file - empty",
        "EG08_010_1d_ccA_L.dbf": "empty file",
        "EG08_011_1d_ccA_L.dbf": "empty file",
        "EG08_012_1d_ccA_L.dbf": "empty file",
        "EG08_013_1d_ccA_L.dbf": "empty file",
        "EG11_005_1d_ccA_L.dbf": "empty file",
        "EG11_006_1d_ccA_L.dbf": "empty file",
        "EG11_006_1d_Nmx.csv": "Nmx processor limitation - Singular node values (N1, N2), not pipe ends (US, DS)",
        "EG11_006_1d_H.csv": "H processor limitation - Singular node values (N1, N2), not pipe ends (US, DS)",
        # GPKG files with empty 1d_ccA_L layers
        "EG08_010_Results.gpkg": "empty 1d_ccA_L layer",
        "EG08_011_Results.gpkg": "empty 1d_ccA_L layer",
        "EG08_012_Results.gpkg": "empty 1d_ccA_L layer",
        "EG08_013_Results.gpkg": "empty 1d_ccA_L layer",
        "EG11_005_Results.gpkg": "empty 1d_ccA_L layer",
        "EG11_006_Results.gpkg": "empty 1d_ccA_L layer",
        # EOF files missing "CULVERT AND PIPE DATA" section
        "EG08_011.eof": "missing CULVERT AND PIPE DATA section",
        "EG08_013.eof": "missing CULVERT AND PIPE DATA section",
        "EG11_002.eof": "missing CULVERT AND PIPE DATA section",
        "EG11_005.eof": "missing CULVERT AND PIPE DATA section",
        "EG11_006.eof": "missing CULVERT AND PIPE DATA section",
        "EG11_007.eof": "missing CULVERT AND PIPE DATA section",
    }

    for file_path in files:
        if file_path.name in declared_cases:
            observed_declared.add(file_path.name)
        try:
            processor = BaseProcessor.from_file(file_path)
            processor.process()

            # Only explicitly declared fixtures have a supported/rejected contract.
            # They must never fall through to the historical warning-only path.
            if case := declared_cases.get(file_path.name):
                try:
                    assert_declared_outcome(processor, case)
                except AssertionError as exc:
                    failures.append(f"{file_path.name}: {exc}")
                continue

            failure, warning = _unclassified_file_result(file_path, processor, KNOWN_PROBLEMATIC_FILES)
            if failure is not None:
                failures.append(failure)
            if warning is not None:
                warnings.append(warning)

        except Exception as e:
            failures.append(f"{file_path.name}: Raised exception {e}")

    # Losing a registered suffix/discovery match must not silently remove a
    # fixture from this broad smoke gate.
    missing_declared = sorted(set(declared_cases) - observed_declared)
    assert not missing_declared, f"Declared fixtures missing from discovery: {missing_declared}"

    if warnings:
        print(f"\nUnclassified outcomes (not accepted as supported): {len(warnings)} files")
        print("\n".join(warnings))

    assert not failures, f"Failures occurred in {len(failures)} files:\n" + "\n".join(failures[:10])


@pytest.mark.parametrize(
    ("processed", "has_data", "message"),
    [
        (False, False, "rejected"),
        (True, False, "no processed rows"),
        (True, True, "Runcode"),
    ],
)
def test_supported_manifest_case_rejects_false_success(processed: bool, has_data: bool, message: str) -> None:
    """EG01_007 must fail its contract when it is dropped, empty or misidentified."""
    processor = MagicMock(spec=BaseProcessor)
    processor.processed = processed
    processor.df = (
        pd.DataFrame({"Runcode": ["wrong_run"], "EndStatus": ["Simulation FINISHED"]}) if has_data else pd.DataFrame()
    )
    with pytest.raises(AssertionError, match=message):
        assert_declared_outcome(processor, load_declared_cases()["EG01_007.tlf"])


def test_rejected_manifest_case_cannot_retain_processed_rows() -> None:
    """A rejected input must not expose stale data as usable measurements."""
    processor = MagicMock(spec=BaseProcessor)
    processor.processed = False
    processor.df = pd.DataFrame({"Value": [1.0]})
    with pytest.raises(AssertionError, match="retained rows"):
        assert_declared_outcome(processor, load_declared_cases()["EG02_012_PO.csv"])


def test_integration_collection_timeseries(tuflow_test_data: Path) -> None:
    """Integration test: Combine timeseries files for a specific run."""
    # Target EG11_001 run
    run_id = "EG11_001"
    search_dir = tuflow_test_data / "EG11"

    # Find specific timeseries files for this run
    # We look for _1d_H.csv, _1d_Q.csv, _1d_V.csv which are common timeseries
    patterns = [f"{run_id}*_1d_H.csv", f"{run_id}*_1d_Q.csv", f"{run_id}*_1d_V.csv"]
    files: list[Path] = []
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
    files: list[Path] = []
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
