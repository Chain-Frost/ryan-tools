import sys
import json
from pathlib import Path
import pandas as pd

# Add project root to sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from ryan_library.functions.parse_tlf import (
    search_for_completion,
    read_log_file,
    process_top_lines,
    finalise_data,
)
from ryan_library.functions.path_stuff import convert_to_relative_path

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
                # Convert DataFrame to dict for JSON serialization
                # We take the first record since it's a single file
                result = df.to_dict(orient="records")[0]
                # Convert any non-serializable types (like datetime) to strings
                for key, value in result.items():
                    if hasattr(value, "isoformat"):
                        result[key] = value.isoformat()
                return result
            
    return {}

def main():
    base_dir = Path(__file__).parent / "test_data" / "tuflow" / "TUFLOW_Example_Model_Dataset" / "log"
    snapshot_path = Path(__file__).parent / "test_data" / "tlf_regression_snapshot.json"
    
    results = {}
    
    tlf_files = sorted([
        f for f in base_dir.glob("*.tlf")
        if not f.name.endswith(".hpc.tlf") and not f.name.endswith(".gpu.tlf")
    ])
    print(f"Found {len(tlf_files)} TLF files.")
    
    for tlf_file in tlf_files:
        print(f"Processing {tlf_file.name}...")
        try:
            parsed_data = parse_tlf_file(tlf_file)
            if parsed_data:
                results[tlf_file.name] = parsed_data
        except Exception as e:
            print(f"Error processing {tlf_file.name}: {e}")

    print(f"Generated snapshot for {len(results)} files.")
    
    with open(snapshot_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, sort_keys=True)
    
    print(f"Snapshot saved to {snapshot_path}")

if __name__ == "__main__":
    main()
