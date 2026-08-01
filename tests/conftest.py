"""Repository-wide pytest configuration."""

from pathlib import Path

import pytest

TEST_DATA_ROOT: Path = Path(__file__).resolve().parent / "test_data"
REQUIRED_TEST_DATA_PATHS: tuple[Path, ...] = (
    TEST_DATA_ROOT / "expected_files.json",
    TEST_DATA_ROOT / "raster" / "expected.json",
    TEST_DATA_ROOT / "tlf_regression_snapshot.json",
    TEST_DATA_ROOT / "tuflow",
)


@pytest.fixture(scope="session")
def raster_test_data() -> Path:
    """Return the root of the synthetic raster fixture suite."""
    return TEST_DATA_ROOT / "raster"


def pytest_sessionstart(session: pytest.Session) -> None:
    """Fail immediately when the required test-data submodule is unavailable."""
    del session
    missing_paths: list[Path] = [path for path in REQUIRED_TEST_DATA_PATHS if not path.exists()]
    if not missing_paths:
        return

    missing_text: str = "\n".join(f"- {path}" for path in missing_paths)
    raise pytest.UsageError(
        "Required test data is missing or incomplete at tests/test_data.\n"
        "Run: git submodule update --init --recursive\n"
        f"Missing paths:\n{missing_text}"
    )
