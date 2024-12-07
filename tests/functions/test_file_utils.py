# tests/functions/test_file_utils.py

import pytest
from pathlib import Path
import logging

# Import the function to be tested
from ryan_library.functions.file_utils import find_files_parallel

# Configure logging for tests
logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG for more detailed logs during testing
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# Define the path to the test data
TEST_DATA_DIR = Path(__file__).parent / "test_data" / "tuflow" / "tutorials"


@pytest.fixture
def setup_test_environment():
    """
    Fixture to set up the test environment.
    Ensure that the test data directory exists.
    """
    assert (
        TEST_DATA_DIR.exists()
    ), f"Test data directory does not exist: {TEST_DATA_DIR}"
    return TEST_DATA_DIR


def test_find_files_inclusion_only(setup_test_environment):
    """
    Test the find_files_parallel function with only inclusion patterns.
    """
    root_dir = setup_test_environment

    # Define inclusion patterns
    include_patterns = ".tlf"

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

    # Define expected files based on your test data
    expected_files = [
        # Example: Path(TEST_DATA_DIR / "file1.tlf"),
        # Path(TEST_DATA_DIR / "file.hpc.tlf"),
        # Path(TEST_DATA_DIR / "file.gpu.tlf"),
    ]

    # Placeholder for user to fill in actual expected files
    # Replace the list below with your actual expected file paths
    expected_files = [
        # Path(TEST_DATA_DIR / "file1.tlf"),
        # Path(TEST_DATA_DIR / "file.hpc.tlf"),
        # Path(TEST_DATA_DIR / "file.gpu.tlf"),
    ]

    # Assert that the matched files are as expected
    assert set(matched_files) == set(expected_files), "Inclusion-only test failed."


def test_find_files_with_exclusions(setup_test_environment):
    """
    Test the find_files_parallel function with inclusion and exclusion patterns.
    """
    root_dir = setup_test_environment

    # Define inclusion and exclusion patterns
    include_patterns = ".tlf"
    exclude_patterns = [".hpc.tlf", ".gpu.tlf"]

    # Call the function
    matched_files = find_files_parallel(
        root_dirs=[root_dir],
        patterns=include_patterns,
        excludes=exclude_patterns,
        report_level=None,
        print_found_folder=False,
    )

    # Define expected files based on your test data
    expected_files = [
        # Example: Path(TEST_DATA_DIR / "file1.tlf"),
    ]

    # Placeholder for user to fill in actual expected files
    expected_files = [
        # Path(TEST_DATA_DIR / "file1.tlf"),
    ]

    # Assert that the matched files are as expected
    assert set(matched_files) == set(
        expected_files
    ), "Inclusion with exclusions test failed."


def test_find_files_multiple_inclusions(setup_test_environment):
    """
    Test the find_files_parallel function with multiple inclusion patterns.
    """
    root_dir = setup_test_environment

    # Define multiple inclusion patterns
    include_patterns = [".tlf", ".tif.ovr"]

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

    # Define expected files based on your test data
    expected_files = [
        # Path(TEST_DATA_DIR / "file1.tlf"),
        # Path(TEST_DATA_DIR / "file.hpc.tlf"),
        # Path(TEST_DATA_DIR / "file.gpu.tlf"),
        # Path(TEST_DATA_DIR / "image.tif.ovr"),
        # Path(TEST_DATA_DIR / "report.tif.ovr"),
    ]

    # Placeholder for user to fill in actual expected files
    expected_files = [
        # Path(TEST_DATA_DIR / "file1.tlf"),
        # Path(TEST_DATA_DIR / "file.hpc.tlf"),
        # Path(TEST_DATA_DIR / "file.gpu.tlf"),
        # Path(TEST_DATA_DIR / "image.tif.ovr"),
        # Path(TEST_DATA_DIR / "report.tif.ovr"),
    ]

    # Assert that the matched files are as expected
    assert set(matched_files) == set(expected_files), "Multiple inclusions test failed."


def test_find_files_multiple_inclusions_and_exclusions(setup_test_environment):
    """
    Test the find_files_parallel function with multiple inclusion and exclusion patterns.
    """
    root_dir = setup_test_environment

    # Define multiple inclusion and exclusion patterns
    include_patterns = [".tlf", ".tif.ovr"]
    exclude_patterns = [".hpc.tlf", ".gpu.tlf"]

    # Call the function
    matched_files = find_files_parallel(
        root_dirs=[root_dir],
        patterns=include_patterns,
        excludes=exclude_patterns,
        report_level=None,
        print_found_folder=False,
    )

    # Define expected files based on your test data
    expected_files = [
        # Path(TEST_DATA_DIR / "file1.tlf"),
        # Path(TEST_DATA_DIR / "image.tif.ovr"),
        # Path(TEST_DATA_DIR / "report.tif.ovr"),
    ]

    # Placeholder for user to fill in actual expected files
    expected_files = [
        # Path(TEST_DATA_DIR / "file1.tlf"),
        # Path(TEST_DATA_DIR / "image.tif.ovr"),
        # Path(TEST_DATA_DIR / "report.tif.ovr"),
    ]

    # Assert that the matched files are as expected
    assert set(matched_files) == set(
        expected_files
    ), "Multiple inclusions with exclusions test failed."


def test_find_files_no_matches(setup_test_environment):
    """
    Test the find_files_parallel function when no files match the inclusion patterns.
    """
    root_dir = setup_test_environment

    # Define inclusion pattern that doesn't match any file
    include_patterns = ".nonexistent"

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

    # Expected result is an empty list
    expected_files = []

    # Assert that no files are matched
    assert matched_files == expected_files, "No matches test failed."


def test_find_files_invalid_patterns(setup_test_environment):
    """
    Test the find_files_parallel function with invalid pattern formats.
    """
    root_dir = setup_test_environment

    # Define invalid inclusion patterns (missing leading dot)
    include_patterns = "tlf"  # Should raise ValueError

    # No exclusions
    exclude_patterns = None

    # Expecting a ValueError due to invalid pattern format
    with pytest.raises(ValueError):
        find_files_parallel(
            root_dirs=[root_dir],
            patterns=include_patterns,
            excludes=exclude_patterns,
            report_level=None,
            print_found_folder=False,
        )


def test_find_files_empty_root_dirs():
    """
    Test the find_files_parallel function with an empty list of root directories.
    """
    root_dirs = []

    # Define inclusion patterns
    include_patterns = ".tlf"

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

    # Expected result is an empty list
    expected_files = []

    # Assert that no files are matched
    assert matched_files == expected_files, "Empty root_dirs test failed."


def test_find_files_with_report_level(setup_test_environment, caplog):
    """
    Test the find_files_parallel function with report_level enabled.
    """
    root_dir = setup_test_environment

    # Define inclusion patterns
    include_patterns = ".tlf"

    # No exclusions
    exclude_patterns = None

    # Define report_level
    report_level = 1

    # Call the function with report_level
    matched_files = find_files_parallel(
        root_dirs=[root_dir],
        patterns=include_patterns,
        excludes=exclude_patterns,
        report_level=report_level,
        print_found_folder=False,
    )

    # You can inspect the logs captured by caplog
    # For example, ensure that certain log messages were emitted
    # Replace the placeholders with actual log messages based on your test data
    # Example:
    # assert "Searching in folder" in caplog.text

    # Define expected files based on your test data
    expected_files = [
        # Path(TEST_DATA_DIR / "file1.tlf"),
        # Path(TEST_DATA_DIR / "file.hpc.tlf"),
        # Path(TEST_DATA_DIR / "file.gpu.tlf"),
    ]

    # Placeholder for user to fill in actual expected files
    expected_files = [
        # Path(TEST_DATA_DIR / "file1.tlf"),
        # Path(TEST_DATA_DIR / "file.hpc.tlf"),
        # Path(TEST_DATA_DIR / "file.gpu.tlf"),
    ]

    # Assert that the matched files are as expected
    assert set(matched_files) == set(expected_files), "Report level test failed."
