import pytest
from pathlib import Path
from unittest.mock import patch
from ryan_library.orchestrators.tuflow.pomm_combine import main_processing


@patch("ryan_library.orchestrators.tuflow.pomm_combine.execute_combination_workflow")
def test_pomm_combine_with_rllqmx(mock_execute):
    # Run main_processing with multiple data types
    main_processing(paths_to_process=[Path(".")], include_data_types=["POMM", "RLL_Qmx"])

    # Verify calls
    mock_execute.assert_called_once()
    kwargs = mock_execute.call_args.kwargs
    assert kwargs["paths_to_process"] == [Path(".")]
    assert set(kwargs["include_data_types"]) == {"POMM", "RLL_Qmx"}
    assert kwargs["context_name"] == "POMM combination"
    assert kwargs["export_prefix"] == "combined_POMM"


@patch("ryan_library.orchestrators.tuflow.pomm_combine.execute_combination_workflow")
def test_pomm_combine_default(mock_execute):
    # Run main_processing with default (None)
    main_processing(paths_to_process=[Path(".")])

    # Verify calls
    mock_execute.assert_called_once()
    kwargs = mock_execute.call_args.kwargs
    assert kwargs["paths_to_process"] == [Path(".")]
    # It passes include_data_types=None into the workflow directly
    assert kwargs["include_data_types"] is None
    assert set(kwargs["default_data_types"]) == {"POMM", "RLL_Qmx"}
