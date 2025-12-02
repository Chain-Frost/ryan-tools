import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from ryan_library.scripts.tuflow.po_combine import main_processing
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection

@pytest.fixture
def mock_processor_collection():
    mock_collection = MagicMock(spec=ProcessorCollection)
    # Mock the po_combine method to return a dummy DataFrame
    import pandas as pd
    mock_collection.po_combine.return_value = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    mock_collection.processors = [MagicMock()] # Ensure processors list is not empty
    return mock_collection

@patch("ryan_library.scripts.tuflow.po_combine.collect_files")
@patch("ryan_library.scripts.tuflow.po_combine.process_files_in_parallel")
@patch("ryan_library.scripts.tuflow.po_combine.export_results")
def test_po_combine_main_processing(mock_export, mock_process, mock_collect, mock_processor_collection):
    # Setup mocks
    mock_collect.return_value = [Path("dummy_PO.csv")]
    mock_process.return_value = mock_processor_collection
    
    # Run main_processing
    main_processing(paths_to_process=[Path(".")])
    
    # Verify calls
    mock_collect.assert_called_once()
    mock_process.assert_called_once()
    mock_export.assert_called_once()
    
    # Check that export_results was called with the correct results
    args, kwargs = mock_export.call_args
    assert kwargs["results"] == mock_processor_collection

@patch("ryan_library.scripts.tuflow.po_combine.collect_files")
def test_po_combine_no_files(mock_collect):
    mock_collect.return_value = []
    
    # Run main_processing
    main_processing(paths_to_process=[Path(".")])
    
    # Verify collect was called but nothing else happened (no error raised)
    mock_collect.assert_called_once()

