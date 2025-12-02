import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from ryan_library.scripts.tuflow.pomm_combine import main_processing
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection

@pytest.fixture
def mock_processor_collection():
    mock_collection = MagicMock(spec=ProcessorCollection)
    # Mock the combine_raw method to return a dummy DataFrame
    import pandas as pd
    mock_collection.combine_raw.return_value = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    mock_collection.processors = [MagicMock()] # Ensure processors list is not empty
    return mock_collection

@patch("ryan_library.scripts.tuflow.pomm_combine.collect_files")
@patch("ryan_library.scripts.tuflow.pomm_combine.process_files_in_parallel")
@patch("ryan_library.scripts.tuflow.pomm_combine.export_results")
def test_pomm_combine_with_rllqmx(mock_export, mock_process, mock_collect, mock_processor_collection):
    # Setup mocks
    mock_collect.return_value = [Path("dummy_POMM.csv"), Path("dummy_RLL_Qmx.csv")]
    mock_process.return_value = mock_processor_collection
    
    # Run main_processing with multiple data types
    main_processing(
        paths_to_process=[Path(".")],
        include_data_types=["POMM", "RLL_Qmx"]
    )
    
    # Verify calls
    mock_collect.assert_called_once()
    # Check that collect_files was called with the correct data types
    args, kwargs = mock_collect.call_args
    assert set(kwargs["include_data_types"]) == {"POMM", "RLL_Qmx"}
    
    mock_process.assert_called_once()
    mock_export.assert_called_once()

@patch("ryan_library.scripts.tuflow.pomm_combine.collect_files")
@patch("ryan_library.scripts.tuflow.pomm_combine.process_files_in_parallel")
@patch("ryan_library.scripts.tuflow.pomm_combine.export_results")
def test_pomm_combine_default(mock_export, mock_process, mock_collect, mock_processor_collection):
    # Setup mocks
    mock_collect.return_value = [Path("dummy_POMM.csv")]
    mock_process.return_value = mock_processor_collection
    
    # Run main_processing with default (None)
    main_processing(paths_to_process=[Path(".")])
    
    # Verify calls
    mock_collect.assert_called_once()
    # Check that collect_files was called with default "POMM"
    args, kwargs = mock_collect.call_args
    assert kwargs["include_data_types"] == ["POMM", "RLL_Qmx"]
    
    mock_process.assert_called_once()
    mock_export.assert_called_once()
