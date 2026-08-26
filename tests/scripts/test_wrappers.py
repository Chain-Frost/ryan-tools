import sys
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Add the repository root to sys.path so we can import from ryan_scripts
REPO_ROOT = Path(__file__).absolute().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Import the wrappers using importlib or direct import if possible
# Since they are scripts, we might need to import them dynamically or just import their main functions if they are exposed.
# Looking at the file content, they all have a `main` function and `if __name__ == "__main__":` block.
# However, they are in `ryan-scripts/TUFLOW-python`, which is not a package.
# We can use `importlib.util.spec_from_file_location` to import them.

import importlib.util


def import_script(path: Path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not import {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[path.stem] = module
    spec.loader.exec_module(module)
    return module


SCRIPTS_DIR = REPO_ROOT / "ryan-scripts" / "TUFLOW-python"


@pytest.fixture(autouse=True)
def restore_working_directory():
    """Keep wrapper tests from leaking their working directory into later tests."""
    original_directory = Path.cwd()
    yield
    os.chdir(original_directory)


@pytest.fixture
def pomm_combine_script():
    return import_script(SCRIPTS_DIR / "po_and_timeseries" / "combine_pomm_results.py")


@pytest.fixture
def culvert_maximums_script():
    return import_script(SCRIPTS_DIR / "culvert_results" / "combine_culvert_maximums.py")


@pytest.fixture
def log_summary_script():
    return import_script(SCRIPTS_DIR / "log_processing" / "create_log_summary_report.py")


@pytest.fixture
def closure_durations_script():
    return import_script(SCRIPTS_DIR / "po_and_timeseries" / "calculate_po_closure_durations.py")


@pytest.fixture
def culvert_timeseries_script():
    return import_script(SCRIPTS_DIR / "culvert_results" / "combine_culvert_timeseries.py")


# @pytest.fixture
# def results_styling_script():
#     return import_script(SCRIPTS_DIR / "gis_processing" / "apply_qgis_styles_to_results.py")


def test_pomm_combine_wrapper(pomm_combine_script, tmp_path):
    """Test combine_pomm_results.py calls main_processing correctly."""
    with patch.object(pomm_combine_script, "main_processing") as mock_main_processing:
        # Call main with specific arguments
        pomm_combine_script.main(
            console_log_level="DEBUG", locations_to_include=("LocA",), export_mode="parquet", working_directory=tmp_path
        )

        mock_main_processing.assert_called_once()
        call_args = mock_main_processing.call_args
        assert call_args.kwargs["console_log_level"] == "DEBUG"
        assert call_args.kwargs["locations_to_include"] == ("LocA",)
        assert call_args.kwargs["export_mode"] == "parquet"
        assert call_args.kwargs["paths_to_process"] == [tmp_path]


def test_culvert_maximums_wrapper(culvert_maximums_script, tmp_path):
    """Test combine_culvert_maximums.py calls main_processing correctly."""
    with patch.object(culvert_maximums_script, "main_processing") as mock_main_processing:
        culvert_maximums_script.main(console_log_level="INFO", working_directory=tmp_path)

        mock_main_processing.assert_called_once()
        call_args = mock_main_processing.call_args
        assert call_args.kwargs["console_log_level"] == "INFO"
        assert call_args.kwargs["paths_to_process"] == [tmp_path]
        # Check default include_data_types are passed
        assert "include_data_types" in call_args.kwargs
        assert "Nmx" in call_args.kwargs["include_data_types"]


def test_log_summary_wrapper(log_summary_script, tmp_path):
    """Test create_log_summary_report.py calls main_processing correctly."""
    with patch.object(log_summary_script, "main_processing") as mock_main_processing:
        log_summary_script.main(console_log_level="DEBUG", working_directory=tmp_path)

        mock_main_processing.assert_called_once()
        call_args = mock_main_processing.call_args
        assert call_args.kwargs["console_log_level"] == "DEBUG"


def test_closure_durations_wrapper(closure_durations_script, tmp_path):
    """Test calculate_po_closure_durations.py calls run_closure_durations correctly."""
    with patch.object(closure_durations_script, "run_closure_durations") as mock_run:
        closure_durations_script.main(
            console_log_level="INFO", locations_to_include=("Loc1", "Loc2"), working_directory=tmp_path
        )

        mock_run.assert_called_once()
        call_args = mock_run.call_args
        assert call_args.kwargs["log_level"] == "INFO"
        assert call_args.kwargs["allowed_locations"] == ("Loc1", "Loc2")
        assert call_args.kwargs["paths"] == [tmp_path]


def test_culvert_timeseries_wrapper(culvert_timeseries_script, tmp_path):
    """Test combine_culvert_timeseries.py calls main_processing correctly."""
    with patch.object(culvert_timeseries_script, "main_processing") as mock_main_processing:
        culvert_timeseries_script.main(console_log_level="DEBUG", working_directory=tmp_path)

        mock_main_processing.assert_called_once()
        call_args = mock_main_processing.call_args
        assert call_args.kwargs["console_log_level"] == "DEBUG"
        assert call_args.kwargs["paths_to_process"] == [tmp_path]


# Functional test for combine_pomm_results
def test_pomm_combine_functional(pomm_combine_script, tmp_path):
    """Functionally test combine_pomm_results.py using real data."""
    # Setup: Copy test data to tmp_path
    import shutil

    # Source data
    src_dir = REPO_ROOT / "tests" / "test_data" / "tuflow" / "tutorials" / "Module_01" / "results"

    # Copy relevant files (POMM.csv)
    for f in src_dir.glob("*_POMM.csv"):
        shutil.copy(f, tmp_path / f.name)

    # Run the wrapper and real orchestrator in the isolated temporary directory.
    pomm_combine_script.main(console_log_level="INFO", export_mode="excel", working_directory=tmp_path)

    # Verify output
    # Expecting a file like *_combined_POMM.xlsx
    output_files = list(tmp_path.glob("*_combined_POMM.xlsx"))
    assert len(output_files) > 0, "No combined POMM Excel file created"

    # Optional: Read the excel file to check contents
    import pandas as pd

    df = pd.read_excel(output_files[0])
    assert not df.empty
    assert "file" in df.columns
