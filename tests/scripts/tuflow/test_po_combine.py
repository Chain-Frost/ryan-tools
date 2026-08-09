from pathlib import Path
from unittest.mock import MagicMock, patch

from ryan_library.orchestrators.tuflow.po_combine import export_results, main_processing


@patch("ryan_library.orchestrators.tuflow.po_combine.execute_combination_workflow")
def test_po_combine_main_processing(mock_execute: MagicMock) -> None:
    # Run main_processing
    main_processing(paths_to_process=[Path(".")])

    # Verify calls
    mock_execute.assert_called_once()
    kwargs = mock_execute.call_args.kwargs
    assert kwargs["paths_to_process"] == [Path(".")]
    assert kwargs["context_name"] == "PO combination"
    assert kwargs["export_results"] is export_results


@patch("ryan_library.orchestrators.tuflow.po_combine.execute_combination_workflow")
def test_po_combine_no_files(mock_execute: MagicMock) -> None:
    # In the refactored version, main_processing just passes through to execute_combination_workflow.
    # The actual empty-file logic is handled and tested inside the workflow orchestrator.
    main_processing(paths_to_process=[Path(".")])
    mock_execute.assert_called_once()
