# tests/functions/generate_expected_files.py
import json
from pathlib import Path
from typing import Any

# Define the root directory for test data
TEST_DATA_DIR = Path("tests/test_data/tuflow/tutorials")
output_json = Path("tests/test_data/expected_files.json")

# Initialize dictionaries
expected_files: dict[str, list[str] | Any] = {
    "inclusion_only": [],
    "inclusion_with_exclusions_effective": [],
    "exclusions_no_effect": [],
    "multiple_inclusions": [],
    "no_matches": [],
    "empty_root_dirs": [],
    "log_summary": [],
}

# Inclusion and exclusion patterns
inclusion_pattern = "*.hpc.dt.csv"
exclusion_patterns_effective = ["*_001.hpc.dt.csv", "*_DEV_*.hpc.dt.csv", "*_EXG_*.hpc.dt.csv"]
exclusion_patterns_no_effect = ["*.hpc.tlf", "*.gpu.tlf"]
multiple_inclusions_patterns = ["*.hpc.dt.csv", "*.tlf"]
inclusion_log_summary = ["*.tlf"]
exclusion_log_summary = ["*.hpc.tlf", "*.gputlf"]

# Populate inclusion_only
for file in TEST_DATA_DIR.rglob(inclusion_pattern):
    relative_path = file.relative_to(TEST_DATA_DIR).as_posix()
    expected_files["inclusion_only"].append(relative_path)

# Populate inclusion_with_exclusions_effective
for file in TEST_DATA_DIR.rglob(inclusion_pattern):
    if not any(file.match(pattern) for pattern in exclusion_patterns_effective):
        relative_path = file.relative_to(TEST_DATA_DIR).as_posix()
        expected_files["inclusion_with_exclusions_effective"].append(relative_path)

# Populate inclusion_with_exclusions_effective - log_summary
for pattern in inclusion_log_summary:
    for file in TEST_DATA_DIR.rglob(pattern):
        if not any(file.match(pattern) for pattern in exclusion_log_summary):
            relative_path = file.relative_to(TEST_DATA_DIR).as_posix()
            expected_files["log_summary"].append(relative_path)

expected_files["test_find_files_with_exclusions_effective"] = expected_files[
    "inclusion_with_exclusions_effective"
].copy()

# Additionally, include "*.tlf" files in inclusion_with_exclusions_effective
for file in TEST_DATA_DIR.rglob("*.tlf"):
    relative_path = file.relative_to(TEST_DATA_DIR).as_posix()
    expected_files["inclusion_with_exclusions_effective"].append(relative_path)

# Populate exclusions_no_effect (same as inclusion_only)
expected_files["exclusions_no_effect"] = expected_files["inclusion_only"].copy()

# Populate multiple_inclusions
for pattern in multiple_inclusions_patterns:
    for file in TEST_DATA_DIR.rglob(pattern):
        relative_path = file.relative_to(TEST_DATA_DIR).as_posix()
        expected_files["multiple_inclusions"].append(relative_path)

# Remove duplicates in multiple_inclusions
expected_files["multiple_inclusions"] = list(set(expected_files["multiple_inclusions"]))

# Populate no_matches with an empty list (since "*.nonexistent" matches no files)
expected_files["no_matches"] = []

# Write to JSON file
with open(output_json, "w") as f:
    json.dump(expected_files, f, indent=4)

print(f"expected_files.json has been generated successfully at {output_json.resolve()}.")
