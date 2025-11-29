import pytest
from pathlib import Path
from typing import Generator
import pandas as pd
from io import StringIO
import os
from contextlib import contextmanager


@pytest.fixture
def mock_csv_file(tmp_path: Path) -> Generator[Path, None, None]:
    """
    Creates a temporary CSV file with specified content.
    Usage:
        def test_something(mock_csv_file):
            csv_path = mock_csv_file
            csv_path.write_text("header1,header2\nval1,val2")
            # ... use csv_path
    """
    file_path = tmp_path / "test_file.csv"
    yield file_path
    if file_path.exists():
        file_path.unlink()


@pytest.fixture
def sample_data_path() -> Path:
    """
    Returns the path to the sample data directory.
    Assumes tests are run from the repository root.
    """
    # Adjust this path based on where real sample data is stored if available
    # For now, we point to a 'data' folder relative to this conftest
    return Path(__file__).parent / "data"


@pytest.fixture
def create_dummy_csv(tmp_path: Path):
    """
    Factory fixture to create a CSV file with specific name and content.
    """

    def _create(filename: str, content: str) -> Path:
        p = tmp_path / filename
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
        return p

    return _create


@pytest.fixture(scope="session")
def tuflow_test_data_root() -> Path:
    """Return the root path to the TUFLOW Example Model Dataset."""
    # This assumes the layout: tests/processors/tuflow/conftest.py -> ... -> tests/test_data/tuflow/TUFLOW_Example_Model_Dataset
    # Adjust parents count: conftest is in tests/processors/tuflow (3 levels deep from root)
    # test_data is in tests/test_data (2 levels deep from root)
    # So from conftest: ../../../tests/test_data/tuflow/TUFLOW_Example_Model_Dataset
    base_path = Path(__file__).parents[3] / "tests" / "test_data" / "tuflow" / "TUFLOW_Example_Model_Dataset"
    if not base_path.exists():
        # Fallback or warning
        return Path("NOT_FOUND")
    return base_path


@pytest.fixture(scope="session")
def find_test_file(tuflow_test_data_root):
    """
    Returns a function that finds the first file matching a suffix in the test data.
    Usage:
        def test_something(find_test_file):
            file_path = find_test_file("_1d_H.csv")
    """

    def _find(suffix: str) -> Path | None:
        if not tuflow_test_data_root.exists():
            return None
        # Search efficiently: stop at first NON-EMPTY match
        for path in tuflow_test_data_root.rglob(f"*{suffix}"):
            if path.is_file() and path.stat().st_size > 0:
                # Basic check: try to read first few bytes to ensure it's not binary garbage if expecting text?
                # For now, size > 0 is a good enough filter for "not empty".
                return path
        return None

    return _find


@pytest.fixture
def change_cwd():
    """Fixture that returns a context manager to temporarily change the working directory."""

    @contextmanager
    def _change_cwd(path: Path):
        original_cwd = Path.cwd()
        os.chdir(path)
        try:
            yield
        finally:
            os.chdir(original_cwd)

    return _change_cwd
