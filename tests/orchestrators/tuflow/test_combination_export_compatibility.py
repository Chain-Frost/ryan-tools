"""Regression tests for public PO and POMM export entry points."""

from collections.abc import Sequence
from pathlib import Path
from typing import cast
from unittest.mock import ANY, Mock, patch

import pandas as pd
import pytest

from ryan_library.functions.tuflow.tuflow_common import FileBatchProgress
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


@pytest.mark.parametrize("use_live_dashboard", [False, True])
def test_shared_workflow_routes_results_to_supplied_public_exporter(use_live_dashboard: bool) -> None:
    """Processed results flow back through the domain module's public exporter."""

    results = ProcessorCollection()
    exporter = Mock()

    def completed_batch(**kwargs: object) -> ProcessorCollection:
        progress = kwargs["batch_progress"]
        assert isinstance(progress, FileBatchProgress)
        progress.reset(scheduled=1)
        progress.observe("OK")
        return results

    with (
        patch(
            "ryan_library.orchestrators.tuflow._combination_workflow.collect_files",
            return_value=[Path("results_PO.csv")],
        ),
        patch(
            "ryan_library.orchestrators.tuflow._combination_workflow.process_files_in_parallel",
            side_effect=completed_batch,
        ),
        patch("ryan_library.orchestrators.tuflow._combination_workflow.setup_logger") as setup_logger,
    ):
        setup_logger.return_value.__enter__.return_value = Mock()
        execute_combination_workflow(
            paths_to_process=[Path.cwd()],
            include_data_types=None,
            default_data_types=("PO",),
            accepted_data_types=frozenset({"PO"}),
            context_name="PO combination",
            export_results=cast("CombinationExporter", exporter),
            export_mode="both",
            use_live_dashboard=use_live_dashboard,
        )

    if use_live_dashboard:
        exporter.assert_called_once_with(results=results, export_mode="both", dashboard=ANY)
    else:
        exporter.assert_called_once_with(results=results, export_mode="both")


def test_po_export_results_delegates_to_shared_exporter() -> None:
    """The established PO export API remains available as a thin delegate."""

    results = _PoResults()
    with patch("ryan_library.orchestrators.tuflow.po_combine._combine_and_export_results") as shared_export:
        export_po_results(results=cast("ProcessorCollection", results))

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


@pytest.mark.parametrize("use_live_dashboard", [False, True])
@pytest.mark.parametrize("incomplete", [False, True])
def test_combination_refuses_export_after_failed_or_missing_input(
    tmp_path: Path, use_live_dashboard: bool, incomplete: bool
) -> None:
    """Never publish a valid subset as a complete PO/POMM result batch."""
    paths = [tmp_path / "valid_PO.csv", tmp_path / "failed_PO.csv"]
    results = ProcessorCollection()
    exporter = Mock()

    def process_batch(**kwargs: object) -> ProcessorCollection:
        progress = kwargs["batch_progress"]
        assert isinstance(progress, FileBatchProgress)
        progress.reset(scheduled=2)
        progress.observe("OK")
        if not incomplete:
            progress.observe("FAIL")
            progress.failed_inputs.append((paths[1], "INVALID (non_numeric=3)"))
        return results

    with (
        patch("ryan_library.orchestrators.tuflow._combination_workflow.collect_files", return_value=paths),
        patch(
            "ryan_library.orchestrators.tuflow._combination_workflow.process_files_in_parallel",
            side_effect=process_batch,
        ),
        patch("ryan_library.orchestrators.tuflow._combination_workflow.setup_logger") as log_setup,
        patch("ryan_library.orchestrators.tuflow._combination_workflow.logger.error") as error_log,
    ):
        log_setup.return_value.__enter__.return_value = Mock()
        with pytest.raises(RuntimeError, match="report not exported"):
            execute_combination_workflow(
                paths_to_process=[tmp_path],
                include_data_types=None,
                default_data_types=("PO",),
                accepted_data_types=frozenset({"PO"}),
                context_name="PO combination",
                export_results=cast("CombinationExporter", exporter),
                use_live_dashboard=use_live_dashboard,
            )

    exporter.assert_not_called()
    if incomplete:
        assert any("returned no outcome" in str(call) for call in error_log.call_args_list)
    else:
        error_log.assert_any_call("PO combination: failed input {} ({})", paths[1], "INVALID (non_numeric=3)")
