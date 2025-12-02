import sys
import json
import pytest
from pathlib import Path

# Add project root to sys.path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from ryan_library.functions.parse_tlf import (
    search_for_completion,
    read_log_file,
    process_top_lines,
    finalise_data,
)
from ryan_library.functions.path_stuff import convert_to_relative_path

# Helper function to parse a single TLF file (same logic as in generate_tlf_snapshot.py)
def parse_tlf_file(logfile_path: Path) -> dict:
    """Parses a single TLF file and returns the result as a dictionary."""
    sim_complete = 0
    success = 0
    spec_events = False
    spec_scen = False
    spec_var = False
    data_dict = {}
    current_section = None

    file_size = logfile_path.stat().st_size
    is_large_file = file_size > 10 * 1024 * 1024  # 10 MB

    lines = read_log_file(
        logfile_path=logfile_path,
        is_large_file=is_large_file,
    )

    if not lines:
        return {}

    runcode = logfile_path.stem
    relative_logfile_path = convert_to_relative_path(user_path=logfile_path)

    # Search for completion in the last 100 lines
    for line in lines[-100:]:
        data_dict, sim_complete, current_section = search_for_completion(
            line=line,
            data_dict=data_dict,
            sim_complete=sim_complete,
            current_section=current_section,
        )
        if sim_complete == 2:
            data_dict["Runcode"] = runcode
            break

    if sim_complete == 2:
        data_dict, success, spec_events, spec_scen, spec_var = process_top_lines(
            logfile_path=logfile_path,
            lines=lines,
            data_dict=data_dict,
            success=success,
            spec_events=spec_events,
            spec_scen=spec_scen,
            spec_var=spec_var,
            is_large_file=is_large_file,
            runcode=runcode,
            relative_logfile_path=relative_logfile_path,
        )

        if success == 4:
            df = finalise_data(
                runcode=runcode,
                data_dict=data_dict,
                logfile_path=logfile_path,
            )
            if not df.empty:
                result = df.to_dict(orient="records")[0]
                # Convert datetime objects to strings for comparison
                for key, value in result.items():
                    if hasattr(value, "isoformat"):
                        result[key] = value.isoformat()
                return result
            
    return {}

@pytest.fixture(scope="module")
def regression_snapshot():
    snapshot_path = Path(__file__).parent.parent / "test_data" / "tlf_regression_snapshot.json"
    if not snapshot_path.exists():
        pytest.fail(f"Snapshot file not found: {snapshot_path}. Run tests/generate_tlf_snapshot.py to generate it.")
    
    with open(snapshot_path, "r", encoding="utf-8") as f:
        return json.load(f)

def test_parse_tlf_real_world(regression_snapshot):
    base_dir = Path(__file__).parent.parent / "test_data" / "tuflow" / "TUFLOW_Example_Model_Dataset" / "log"
    tlf_files = sorted([
        f for f in base_dir.glob("*.tlf")
        if not f.name.endswith(".hpc.tlf") and not f.name.endswith(".gpu.tlf")
    ])
    
    assert len(tlf_files) > 0, "No TLF files found in test data directory."
    
    for tlf_file in tlf_files:
        filename = tlf_file.name
        
        # Skip files not in snapshot (e.g. if they were added but snapshot not updated)
        # Or fail if strict regression is desired. For now, let's warn or skip.
        if filename not in regression_snapshot:
            # pytest.warns(UserWarning, match=f"File {filename} not in snapshot.")
            continue

        expected_data = regression_snapshot[filename]
        actual_data = parse_tlf_file(tlf_file)
        
        # If actual data is empty but expected is not, that's a failure
        if not actual_data and expected_data:
             pytest.fail(f"Failed to parse {filename}, but expected data exists.")
        
        # Compare keys and values
        # We can do a direct dict comparison since we serialized everything to basic types
        # However, floating point numbers might need tolerance if they were not stringified identically
        # But in generate_tlf_snapshot.py we rely on json serialization which handles floats.
        # Let's try direct comparison first.
        
        # Note: In the snapshot generation, we converted datetimes to isoformat strings.
        # The parse_tlf_file function here also does that.
        
        # Check for missing keys
        missing_keys = set(expected_data.keys()) - set(actual_data.keys())
        assert not missing_keys, f"Missing keys in {filename}: {missing_keys}"
        
        # Check for extra keys (optional, but good for regression)
        extra_keys = set(actual_data.keys()) - set(expected_data.keys())
        assert not extra_keys, f"Extra keys in {filename}: {extra_keys}"
        
        # Check values
        for key, expected_val in expected_data.items():
            actual_val = actual_data[key]
            assert actual_val == expected_val, f"Mismatch in {filename} for key '{key}': expected {expected_val}, got {actual_val}"
