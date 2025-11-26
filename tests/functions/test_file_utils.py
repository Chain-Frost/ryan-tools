# tests/functions/test_file_utils.py

import pytest
from pathlib import Path

from loguru import logger
import json
from pathlib import Path
from pprint import pprint

# import sys
# # Ensure the project root is on sys.path for direct test execution
# PROJECT_ROOT = Path(__file__).resolve().parents[2]
# if str(PROJECT_ROOT) not in sys.path:
#     sys.path.insert(0, str(PROJECT_ROOT))

# Import the function to be tested
from ryan_library.functions.file_utils import ensure_output_directory, find_files_parallel, is_non_zero_file

# TODO this is using the wrong logging - we should be using logru as advised in the repo.
# Configure logging for tests
logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG for more detailed logs during testing
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# Define the path to the test data and expected_files.json
TEST_DATA_DIR: Path = Path(__file__).parent.parent / "test_data" / "tuflow" / "tutorials"
EXPECTED_FILES_JSON: Path = Path(__file__).parent.parent / "test_data" / "expected_files.json"


@pytest.fixture
def load_expected_files():
    """
    Fixture to load expected files from the JSON file.
    """
    with open(EXPECTED_FILES_JSON, "r") as f:
        data = json.load(f)
    return data


@pytest.fixture
def setup_test_environment() -> Path:
    """
    Fixture to set up the test environment.
    Ensure that the test data directory exists.
    """
    assert TEST_DATA_DIR.exists(), f"Test data directory does not exist: {TEST_DATA_DIR}"
    return TEST_DATA_DIR


def resolve_paths(relative_paths: list[Path | str]) -> list[Path]:
    """
    Helper function to resolve relative paths to absolute Path objects.
    """
    return [TEST_DATA_DIR / Path(p) for p in relative_paths]


def test_find_files_inclusion_only(setup_test_environment: Path, load_expected_files) -> None:
    """
    Test the find_files_parallel function with only inclusion patterns.
    """
    root_dir: str | Path = setup_test_environment

    # Define inclusion patterns with wildcard
    include_patterns = "*.hpc.dt.csv"

    # No exclusions
    exclude_patterns = None

    # Call the function
    matched_files: list[Path] = find_files_parallel(
        root_dirs=[root_dir],
        patterns=include_patterns,
        excludes=exclude_patterns,
        report_level=None,
        print_found_folder=False,
    )

    # Load expected files
    expected_files: list[Path] = resolve_paths(relative_paths=load_expected_files["inclusion_only"])
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
    exclude_patterns: list[str] = ["*.hpc.tlf", "*.gpu.tlf"]

    # Call the function
    matched_files = find_files_parallel(
        root_dirs=[root_dir],
        patterns=include_patterns,
        excludes=exclude_patterns,
        report_level=None,
        print_found_folder=False,
    )

    # Load expected files
    expected_files: list[Path] = resolve_paths(load_expected_files["log_summary"])

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
    exclude_patterns: list[str] = ["*.hpc.tlf", "*.gpu.tlf"]

    # Call the function
    matched_files: list[Path] = find_files_parallel(
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
    include_patterns: list[str] = ["*.hpc.dt.csv", "*.tlf"]

    # No exclusions
    exclude_patterns = None

    # Call the function
    matched_files: list[Path] = find_files_parallel(
        root_dirs=[root_dir],
        patterns=include_patterns,
        excludes=exclude_patterns,
        report_level=None,
        print_found_folder=False,
    )

    # Load expected files
    expected_files: list[Path] = resolve_paths(load_expected_files["multiple_inclusions"])

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
    matched_files: list[Path] = find_files_parallel(
        root_dirs=[root_dir],
        patterns=include_patterns,
        excludes=exclude_patterns,
        report_level=None,
        print_found_folder=False,
    )

    # Load expected files
    expected_files: list[Path] = resolve_paths(relative_paths=load_expected_files["no_matches"])

    # Assert that no files are matched
    assert set(matched_files) == set(expected_files), "No matches test failed."


# Removed the test_find_files_invalid_patterns as the function no longer requires patterns to start with a dot


def test_find_files_empty_root_dirs(load_expected_files) -> None:
    """
    Test the find_files_parallel function with an empty list of root directories.
    """
    root_dirs = []

    # Define inclusion patterns
    include_patterns = "*.hpc.dt.csv"

    # No exclusions
    exclude_patterns = None

    # Call the function
    matched_files: list[Path] = find_files_parallel(
        root_dirs=root_dirs,
        patterns=include_patterns,
        excludes=exclude_patterns,
        report_level=None,
        print_found_folder=False,
    )

    # Load expected files
    expected_files: list[Path] = resolve_paths(relative_paths=load_expected_files["empty_root_dirs"])

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
    expected_files: list[Path] = resolve_paths(load_expected_files["inclusion_only"])

    # Assert that the matched files are as expected
    assert set(matched_files) == set(expected_files), "Report level test failed."

    # Optionally, add more assertions on log messages
    # Example:
    # assert any("Searching in folder" in message.message for message in caplog.records), "Expected log messages not found."


def test_ensure_output_directory_creates_missing_nested(tmp_path: Path) -> None:
    """Ensure missing nested directories are created."""

    nested_dir: Path = tmp_path / "level_one" / "level_two"
    assert not nested_dir.exists()

    ensure_output_directory(output_dir=nested_dir)

    assert nested_dir.exists()
    assert nested_dir.is_dir()


def test_ensure_output_directory_idempotent_existing(tmp_path: Path) -> None:
    """Ensure calling on an existing directory does not recreate it."""

    existing_dir: Path = tmp_path / "already_there"
    existing_dir.mkdir(parents=True)
    sentinel_file: Path = existing_dir / "sentinel.txt"
    sentinel_file.write_text("sentinel")

    ensure_output_directory(output_dir=existing_dir)

    assert existing_dir.exists()
    assert existing_dir.is_dir()
    assert sentinel_file.exists()


@pytest.mark.parametrize(
    argnames=("scenario", "expected_result", "expected_log_fragment"),
    argvalues=[
        ("missing", False, "File does not exist"),
        ("directory", False, "Path is not a file"),
        ("empty", False, "File is empty"),
        ("populated", True, None),
    ],
)
def test_is_non_zero_file_scenarios(tmp_path: Path, scenario, expected_result, expected_log_fragment) -> None:
    if scenario == "missing":
        target_path: Path = tmp_path / "missing.txt"
    elif scenario == "directory":
        target_path = tmp_path / "as_directory"
        target_path.mkdir()
    elif scenario == "empty":
        target_path = tmp_path / "empty.txt"
        target_path.touch()
    elif scenario == "populated":
        target_path = tmp_path / "data.txt"
        target_path.write_text("content", encoding="utf-8")
    else:
        raise ValueError(f"Unknown scenario: {scenario}")

    captured_messages: list[str] = []
    handler_id = logger.add(captured_messages.append, level="ERROR", format="{message}")
    try:
        result: bool = is_non_zero_file(target_path)
    finally:
        logger.remove(handler_id)

    assert result is expected_result

    if expected_log_fragment:
        assert any(
            expected_log_fragment in message for message in captured_messages
        ), f"Expected log fragment '{expected_log_fragment}' not found in {captured_messages}"
    else:
        assert not captured_messages
