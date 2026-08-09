"""Regression tests for public PO and POMM export entry points."""

from collections.abc import Sequence
from pathlib import Path
from typing import cast
from unittest.mock import Mock, patch

import pandas as pd

from ryan_library.orchestrators.tuflow._combination_workflow import CombinationExporter, execute_combination_workflow
from ryan_library.orchestrators.tuflow.po_combine import export_results as export_po_results
from ryan_library.orchestrators.tuflow.pomm_combine import export_results as export_pomm_results
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection


class _PoResults:
    @property
    def processors(self) -> Sequence[object]:
        return [object()]

    def po_combine(self) -> pd.DataFrame:
        return pd.DataFrame({"PO": [1]})


class _PommResults:
    @property
    def processors(self) -> Sequence[object]:
        return [object()]

    def pomm_combine(self) -> pd.DataFrame:
        return pd.DataFrame(data={"POMM": [1]})


def test_shared_workflow_routes_results_to_supplied_public_exporter() -> None:
    """Processed results flow back through the domain module's public exporter."""

    results = ProcessorCollection()
    exporter = Mock()
    with (
        patch(
            "ryan_library.orchestrators.tuflow._combination_workflow.collect_files",
            return_value=[Path("results_PO.csv")],
        ),
        patch(
            "ryan_library.orchestrators.tuflow._combination_workflow.process_files_in_parallel",
            return_value=results,
        ),
        patch("ryan_library.orchestrators.tuflow._combination_workflow.setup_logger") as setup_logger,
    ):
        setup_logger.return_value.__enter__.return_value = Mock()
        execute_combination_workflow(
            paths_to_process=[],
            include_data_types=None,
            default_data_types=("PO",),
            accepted_data_types=frozenset({"PO"}),
            context_name="PO combination",
            export_results=cast(CombinationExporter, exporter),
            export_mode="both",
        )

    exporter.assert_called_once_with(results=results, export_mode="both")


def test_po_export_results_delegates_to_shared_exporter() -> None:
    """The established PO export API remains available as a thin delegate."""

    results = _PoResults()
    with patch("ryan_library.orchestrators.tuflow.po_combine._combine_and_export_results") as shared_export:
        export_po_results(results=cast(ProcessorCollection, results))

    delegated: Mock = shared_export
    delegated.assert_called_once()
    assert delegated.call_args.kwargs["results"] is results
    assert delegated.call_args.kwargs["export_prefix"] == "combined_PO"


def test_pomm_export_results_delegates_to_shared_exporter() -> None:
    """The established POMM export API remains available as a thin delegate."""

    results = _PommResults()
    with patch("ryan_library.orchestrators.tuflow.pomm_combine._combine_and_export_results") as shared_export:
        export_pomm_results(results=results)

    delegated: Mock = shared_export
    delegated.assert_called_once()
    assert delegated.call_args.kwargs["results"] is results
    assert delegated.call_args.kwargs["export_prefix"] == "combined_POMM"
