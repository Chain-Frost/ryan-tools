import sys
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

@pytest.fixture
def pomm_combine_script():
    return import_script(SCRIPTS_DIR / "POMM_combine.py")

@pytest.fixture
def culvert_maximums_script():
    return import_script(SCRIPTS_DIR / "TUFLOW_Culvert_Maximums.py")

@pytest.fixture
def log_summary_script():
    return import_script(SCRIPTS_DIR / "LogSummary.py")

@pytest.fixture
def closure_durations_script():
    return import_script(SCRIPTS_DIR / "TUFLOW-find-closure-durations.py")

@pytest.fixture
def culvert_timeseries_script():
    return import_script(SCRIPTS_DIR / "TUFLOW_Culvert_Timeseries.py")

# @pytest.fixture
# def results_styling_script():
#     return import_script(SCRIPTS_DIR / "TUFLOW_Results_Styling.py")


def test_pomm_combine_wrapper(pomm_combine_script, tmp_path):
    """Test POMM_combine.py wrapper calls main_processing correctly."""
    with patch.object(pomm_combine_script, "main_processing") as mock_main_processing:
        # Call main with specific arguments
        pomm_combine_script.main(
            console_log_level="DEBUG",
            locations_to_include=("LocA",),
            export_mode="parquet",
            working_directory=tmp_path
        )
        
        mock_main_processing.assert_called_once()
        call_args = mock_main_processing.call_args
        assert call_args.kwargs["console_log_level"] == "DEBUG"
        assert call_args.kwargs["locations_to_include"] == ("LocA",)
        assert call_args.kwargs["export_mode"] == "parquet"
        assert call_args.kwargs["paths_to_process"] == [tmp_path]


def test_culvert_maximums_wrapper(culvert_maximums_script, tmp_path):
    """Test TUFLOW_Culvert_Maximums.py wrapper calls main_processing correctly."""
    with patch.object(culvert_maximums_script, "main_processing") as mock_main_processing:
        culvert_maximums_script.main(
            console_log_level="INFO",
            working_directory=tmp_path
        )
        
        mock_main_processing.assert_called_once()
        call_args = mock_main_processing.call_args
        assert call_args.kwargs["console_log_level"] == "INFO"
        assert call_args.kwargs["paths_to_process"] == [tmp_path]
        # Check default include_data_types are passed
        assert "include_data_types" in call_args.kwargs
        assert "Nmx" in call_args.kwargs["include_data_types"]


def test_log_summary_wrapper(log_summary_script, tmp_path):
    """Test LogSummary.py wrapper calls main_processing correctly."""
    with patch.object(log_summary_script, "main_processing") as mock_main_processing:
        log_summary_script.main(
            console_log_level="DEBUG",
            working_directory=tmp_path
        )
        
        mock_main_processing.assert_called_once()
        call_args = mock_main_processing.call_args
        assert call_args.kwargs["console_log_level"] == "DEBUG"


def test_closure_durations_wrapper(closure_durations_script, tmp_path):
    """Test TUFLOW-find-closure-durations.py wrapper calls run_closure_durations correctly."""
    with patch.object(closure_durations_script, "run_closure_durations") as mock_run:
        closure_durations_script.main(
            console_log_level="INFO",
            locations_to_include=("Loc1", "Loc2"),
            working_directory=tmp_path
        )
        
        mock_run.assert_called_once()
        call_args = mock_run.call_args
        assert call_args.kwargs["log_level"] == "INFO"
        assert call_args.kwargs["allowed_locations"] == ("Loc1", "Loc2")
        assert call_args.kwargs["paths"] == [tmp_path]


def test_culvert_timeseries_wrapper(culvert_timeseries_script, tmp_path):
    """Test TUFLOW_Culvert_Timeseries.py wrapper calls main_processing correctly."""
    with patch.object(culvert_timeseries_script, "main_processing") as mock_main_processing:
        culvert_timeseries_script.main(
            console_log_level="DEBUG",
            working_directory=tmp_path
        )
        
        mock_main_processing.assert_called_once()
        call_args = mock_main_processing.call_args
        assert call_args.kwargs["console_log_level"] == "DEBUG"
        assert call_args.kwargs["paths_to_process"] == [tmp_path]


# Functional Test for POMM_combine
def test_pomm_combine_functional(pomm_combine_script, tmp_path):
    """Functional test for POMM_combine.py using real data."""
    # Setup: Copy test data to tmp_path
    import shutil
    
    # Source data
    src_dir = REPO_ROOT / "tests" / "test_data" / "tuflow" / "tutorials" / "Module_01" / "results"
    
    # Copy relevant files (POMM.csv)
    for f in src_dir.glob("*_POMM.csv"):
        shutil.copy(f, tmp_path / f.name)
        
    # Run the script
    # We need to mock print_library_version and change_working_directory to avoid side effects
    # but we want the real main_processing to run.
    
    with patch("ryan_library.scripts.wrapper_utils.change_working_directory", return_value=True):
        pomm_combine_script.main(
            console_log_level="INFO",
            export_mode="excel",
            working_directory=tmp_path
        )
        
    # Verify output
    # Expecting a file like *_combined_POMM.xlsx
    output_files = list(tmp_path.glob("*_combined_POMM.xlsx"))
    assert len(output_files) > 0, "No combined POMM Excel file created"
    
    # Optional: Read the excel file to check contents
    import pandas as pd
    df = pd.read_excel(output_files[0])
    assert not df.empty
    assert "file" in df.columns
