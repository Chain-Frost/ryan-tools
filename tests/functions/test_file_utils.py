# tests/functions/test_file_utils.py

import pytest
from pathlib import Path
import logging
import json
from pprint import pprint

# Import the function to be tested
from ryan_library.functions.file_utils import find_files_parallel

# Configure logging for tests
logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG for more detailed logs during testing
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# Define the path to the test data and expected_files.json
TEST_DATA_DIR = Path(__file__).parent.parent / "test_data" / "tuflow" / "tutorials"
EXPECTED_FILES_JSON = Path(__file__).parent.parent / "test_data" / "expected_files.json"


@pytest.fixture
def load_expected_files():
    """
    Fixture to load expected files from the JSON file.
    """
    with open(EXPECTED_FILES_JSON, "r") as f:
        data = json.load(f)
    return data


@pytest.fixture
def setup_test_environment():
    """
    Fixture to set up the test environment.
    Ensure that the test data directory exists.
    """
    assert TEST_DATA_DIR.exists(), f"Test data directory does not exist: {TEST_DATA_DIR}"
    return TEST_DATA_DIR


def resolve_paths(relative_paths):
    """
    Helper function to resolve relative paths to absolute Path objects.
    """
    return [TEST_DATA_DIR / Path(p) for p in relative_paths]


def test_find_files_inclusion_only(setup_test_environment, load_expected_files):
    """
    Test the find_files_parallel function with only inclusion patterns.
    """
    root_dir = setup_test_environment

    # Define inclusion patterns with wildcard
    include_patterns = "*.hpc.dt.csv"

    # No exclusions
    exclude_patterns = None

    # Call the function
    matched_files = find_files_parallel(
        root_dirs=[root_dir],
        patterns=include_patterns,
        excludes=exclude_patterns,
        report_level=None,
        print_found_folder=False,
    )

    # Load expected files
    expected_files = resolve_paths(load_expected_files["inclusion_only"])
    pprint("matched_files: ")
    pprint(matched_files)
    pprint("expected_files: ")
    pprint(expected_files)

    # Assert that the matched files are as expected
    assert set(matched_files) == set(expected_files), "Inclusion-only test failed."


def test_find_files_with_exclusions_effective(setup_test_environment, load_expected_files):
    """
    Test the find_files_parallel function with inclusion and exclusion patterns that affect results.
    """
    root_dir = setup_test_environment

    # Define inclusion and exclusion patterns
    include_patterns = "*.hpc.dt.csv"
    # Exclude files ending with '_001.hpc.dt.csv', '_DEV_*.hpc.dt.csv', '_EXG_*.hpc.dt.csv'
    exclude_patterns = ["*_001.hpc.dt.csv", "*_DEV_*.hpc.dt.csv", "*_EXG_*.hpc.dt.csv"]

    # Call the function
    matched_files = find_files_parallel(
        root_dirs=[root_dir],
        patterns=include_patterns,
        excludes=exclude_patterns,
        report_level=None,
        print_found_folder=False,
    )

    # Load expected files
    expected_files = resolve_paths(load_expected_files["test_find_files_with_exclusions_effective"])
    pprint("matched_files: ")
    pprint(matched_files)
    pprint("expected_files: ")
    pprint(expected_files)
    # Assert that the matched files are as expected
    assert set(matched_files) == set(expected_files), "Inclusion with effective exclusions test failed."


def test_find_files_log_summary(setup_test_environment, load_expected_files):
    """
    Test the find_files_parallel function with inclusion and exclusion patterns that affect results.
    """
    root_dir = setup_test_environment

    # Define inclusion and exclusion patterns
    include_patterns = "*.tlf"
    # Exclude files ending with '_001.hpc.dt.csv', '_DEV_*.hpc.dt.csv', '_EXG_*.hpc.dt.csv'
    exclude_patterns = ["*.hpc.tlf", "*.gpu.tlf"]

    # Call the function
    matched_files = find_files_parallel(
        root_dirs=[root_dir],
        patterns=include_patterns,
        excludes=exclude_patterns,
        report_level=None,
        print_found_folder=False,
    )

    # Load expected files
    expected_files = resolve_paths(load_expected_files["log_summary"])

    # Assert that the matched files are as expected
    assert set(matched_files) == set(expected_files), "Inclusion with effective exclusions test failed."


def test_find_files_exclusions_no_effect(setup_test_environment, load_expected_files):
    """
    Test the find_files_parallel function with exclusion patterns that do not affect the outcome.
    """
    root_dir = setup_test_environment

    # Define inclusion and exclusion patterns
    include_patterns = "*.hpc.dt.csv"
    # Exclusion patterns that do not match any included files
    exclude_patterns = ["*.hpc.tlf", "*.gpu.tlf"]

    # Call the function
    matched_files = find_files_parallel(
        root_dirs=[root_dir],
        patterns=include_patterns,
        excludes=exclude_patterns,
        report_level=None,
        print_found_folder=False,
    )

    # Load expected files
    expected_files = resolve_paths(load_expected_files["exclusions_no_effect"])

    # Assert that the matched files are as expected
    assert set(matched_files) == set(expected_files), "Exclusions with no effect test failed."


def test_find_files_multiple_inclusions(setup_test_environment, load_expected_files):
    """
    Test the find_files_parallel function with multiple inclusion patterns.
    """
    root_dir = setup_test_environment

    # Define multiple inclusion patterns
    include_patterns = ["*.hpc.dt.csv", "*.tlf"]

    # No exclusions
    exclude_patterns = None

    # Call the function
    matched_files = find_files_parallel(
        root_dirs=[root_dir],
        patterns=include_patterns,
        excludes=exclude_patterns,
        report_level=None,
        print_found_folder=False,
    )

    # Load expected files
    expected_files = resolve_paths(load_expected_files["multiple_inclusions"])

    # Assert that the matched files are as expected
    assert set(matched_files) == set(expected_files), "Multiple inclusions test failed."


def test_find_files_multiple_inclusions_and_exclusions(setup_test_environment, load_expected_files):
    """
    Test the find_files_parallel function with multiple inclusion and exclusion patterns.
    """
    root_dir = setup_test_environment

    # Define multiple inclusion and exclusion patterns
    include_patterns = ["*.hpc.dt.csv", "*.tlf"]
    # Exclude specific patterns that overlap with inclusion patterns
    exclude_patterns = ["*_001.hpc.dt.csv", "*_DEV_*.hpc.dt.csv", "*_EXG_*.hpc.dt.csv"]

    # Call the function
    matched_files = find_files_parallel(
        root_dirs=[root_dir],
        patterns=include_patterns,
        excludes=exclude_patterns,
        report_level=None,
        print_found_folder=False,
    )

    # Load expected files
    expected_files = resolve_paths(load_expected_files["inclusion_with_exclusions_effective"])
    pprint("matched_files: ")
    pprint(matched_files)
    pprint("expected_files: ")
    pprint(expected_files)
    # Assert that the matched files are as expected
    assert set(matched_files) == set(expected_files), "Multiple inclusions with exclusions test failed."


def test_find_files_no_matches(setup_test_environment, load_expected_files):
    """
    Test the find_files_parallel function when no files match the inclusion patterns.
    """
    root_dir = setup_test_environment

    # Define inclusion pattern that doesn't match any file
    include_patterns = "*.nonexistent"

    # No exclusions
    exclude_patterns = None

    # Call the function
    matched_files = find_files_parallel(
        root_dirs=[root_dir],
        patterns=include_patterns,
        excludes=exclude_patterns,
        report_level=None,
        print_found_folder=False,
    )

    # Load expected files
    expected_files = resolve_paths(load_expected_files["no_matches"])

    # Assert that no files are matched
    assert set(matched_files) == set(expected_files), "No matches test failed."


# Removed the test_find_files_invalid_patterns as the function no longer requires patterns to start with a dot


def test_find_files_empty_root_dirs(load_expected_files):
    """
    Test the find_files_parallel function with an empty list of root directories.
    """
    root_dirs = []

    # Define inclusion patterns
    include_patterns = "*.hpc.dt.csv"

    # No exclusions
    exclude_patterns = None

    # Call the function
    matched_files = find_files_parallel(
        root_dirs=root_dirs,
        patterns=include_patterns,
        excludes=exclude_patterns,
        report_level=None,
        print_found_folder=False,
    )

    # Load expected files
    expected_files = resolve_paths(load_expected_files["empty_root_dirs"])

    # Assert that no files are matched
    assert set(matched_files) == set(expected_files), "Empty root_dirs test failed."


def test_find_files_with_report_level(setup_test_environment, load_expected_files, caplog):
    """
    Test the find_files_parallel function with report_level enabled.
    """
    root_dir = setup_test_environment

    # Define inclusion patterns
    include_patterns = "*.hpc.dt.csv"

    # No exclusions
    exclude_patterns = None

    # Define report_level
    report_level = 1

    with caplog.at_level(logging.INFO):
        # Call the function with report_level
        matched_files = find_files_parallel(
            root_dirs=[root_dir],
            patterns=include_patterns,
            excludes=exclude_patterns,
            report_level=report_level,
            print_found_folder=False,
        )

        # You can inspect the logs captured by caplog
        # Example assertions based on your function's logging
        # assert "Searching in folder" in caplog.text

    # Load expected files
    expected_files = resolve_paths(load_expected_files["inclusion_only"])

    # Assert that the matched files are as expected
    assert set(matched_files) == set(expected_files), "Report level test failed."

    # Optionally, add more assertions on log messages
    # Example:
    # assert any("Searching in folder" in message.message for message in caplog.records), "Expected log messages not found."
