# pyright: reportPrivateUsage=false
"""Focused TUFLOW result-dashboard processing, retry and accounting regressions."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import cast
from unittest.mock import Mock, patch

import pandas as pd

from ryan_library.functions.live_dashboard import LiveWorkflowDashboard
from ryan_library.functions.loguru_helpers import LogQueue
from ryan_library.functions.tuflow import tuflow_common as common
from ryan_library.processors.tuflow.base_processor import BaseProcessor, ProcessorStatus
from ryan_library.processors.tuflow.other_processors.POProcessor import POProcessor
from ryan_library.processors.tuflow.other_processors.RLLQmxProcessor import RLLQmxProcessor


def _processed(name: str, *, processed: bool = True, empty: bool = False) -> BaseProcessor:
    frame = pd.DataFrame() if empty else pd.DataFrame({"Q": [1.0]})
    mock = Mock(spec=BaseProcessor)
    mock.file_name = name
    mock.processed = processed
    mock.df = frame
    return cast("BaseProcessor", mock)


def test_serial_dashboard_keeps_order_and_marks_skip_failure() -> None:
    paths = [Path("b.csv"), Path("skip.csv"), Path("error.csv"), Path("a.csv")]
    dashboard = LiveWorkflowDashboard(title="Synthetic", enabled=False)
    progress = common.FileBatchProgress()
    results = [_processed("b.csv"), _processed("skip.csv", empty=True), None, _processed("a.csv")]
    with (
        patch.object(common, "calculate_pool_size", return_value=1),
        patch.object(common, "_summarize_file_batch", return_value=(4, 0, None, 0)),
        patch.object(common, "process_file", side_effect=results) as process,
    ):
        coll = common.process_files_in_parallel(
            file_list=paths,
            log_queue=cast("LogQueue", Mock()),
            dashboard=dashboard,
            batch_progress=progress,
        )
    assert [item.file_name for item in coll.processors] == ["b.csv", "a.csv"]
    assert [call.kwargs["file_path"] for call in process.call_args_list] == paths
    assert (progress.scheduled, progress.succeeded, progress.skipped, progress.failed) == (4, 2, 1, 1)
    assert [dashboard._tasks[index].status for index in range(1, 5)] == ["OK", "SKIP", "FAIL", "OK"]
    assert "Files finished" in dashboard._format_extra_metrics()
    assert "input size" not in dashboard._format_extra_metrics().lower()


def test_dashboard_transport_fallback_retries_original_files_once() -> None:
    paths = [Path("first.csv"), Path("second.csv")]
    dashboard = LiveWorkflowDashboard(title="Fallback", enabled=False)
    progress = common.FileBatchProgress()

    def broken_report(**kwargs: object) -> None:
        outcome = common.WorkflowItemOutcome[BaseProcessor | None](
            index=1,
            status="OK",
            result=_processed("first.csv"),
            has_result=True,
        )
        callback = cast("Callable[[common.WorkflowItemOutcome[BaseProcessor | None]], None]", kwargs["on_outcome"])
        callback(outcome)
        msg = "synthetic pool transport failure"
        raise OSError(msg)

    with (
        patch.object(common, "calculate_pool_size", return_value=2),
        patch.object(common, "_summarize_file_batch", return_value=(2, 0, None, 0)),
        patch.object(common, "run_dashboard_workflow_report", side_effect=broken_report),
        patch.object(common, "process_file", side_effect=[_processed("first.csv"), None]) as process,
    ):
        coll = common.process_files_in_parallel(
            file_list=paths,
            log_queue=cast("LogQueue", Mock()),
            dashboard=dashboard,
            batch_progress=progress,
        )
    assert [item.file_name for item in coll.processors] == ["first.csv"]
    assert [call.kwargs["file_path"] for call in process.call_args_list] == paths
    assert (progress.scheduled, progress.succeeded, progress.skipped, progress.failed) == (2, 1, 0, 1)
    assert dashboard._phase == "Processing (serial fallback)"
    assert [dashboard._tasks[index].status for index in (1, 2)] == ["OK", "FAIL"]


def test_legacy_pool_oserror_fallback_and_raw_frame_disposal() -> None:
    paths = [Path("a.csv"), Path("b.csv")]
    progress = common.FileBatchProgress()
    with (
        patch.object(common, "calculate_pool_size", return_value=2),
        patch.object(common, "_summarize_file_batch", return_value=(2, 0, None, 0)),
        patch.object(common, "Pool") as pool_class,
        patch.object(common, "process_file", side_effect=[_processed("a.csv"), None]) as process,
    ):
        pool_class.return_value.__enter__.return_value.starmap.side_effect = OSError("transport")
        coll = common.process_files_in_parallel(
            file_list=paths,
            log_queue=cast("LogQueue", Mock()),
            batch_progress=progress,
        )
    assert [item.file_name for item in coll.processors] == ["a.csv"]
    assert process.call_count == len(paths)
    assert (progress.succeeded, progress.failed) == (1, 1)

    processor = Mock(spec=BaseProcessor)
    processor.processed = True
    processor.validate_data.return_value = True
    with patch.object(common.BaseProcessor, "from_file", return_value=processor):
        assert common.process_file(Path("synthetic.csv")) is processor
    processor.discard_raw_dataframe.assert_called_once_with()


def test_valid_po_filtered_to_zero_is_skip_not_failure() -> None:
    """A location miss is expected, but a malformed source must still fail."""
    raw = pd.DataFrame(
        [
            ["run", "Time", "Q"],
            ["run", "Time", "OtherLocation"],
            ["run", "0.0", "12.5"],
        ]
    )
    processor = POProcessor(Path("synthetic_PO.csv"), entity_filter=frozenset({"SelectedLocation"}))
    with (
        patch.object(common.BaseProcessor, "from_file", return_value=processor),
        patch("ryan_library.processors.tuflow.other_processors.POProcessor.pd.read_csv", return_value=raw),
        patch.object(processor, "validate_data", wraps=processor.validate_data) as validate,
    ):
        actual = common.process_file(Path("synthetic_PO.csv"), entity_filters=("SelectedLocation",))

    assert actual is processor
    assert not processor.processed
    assert processor.filtered_empty
    assert processor.df.empty
    assert processor.raw_df.empty
    validate.assert_not_called()
    assert common._result_status(processor) == "SKIP"
    assert common._dashboard_detail(processor) == "no matching locations"

    dashboard = LiveWorkflowDashboard(title="Filtered PO", enabled=False)
    progress = common.FileBatchProgress()
    with (
        patch.object(common, "calculate_pool_size", return_value=1),
        patch.object(common, "_summarize_file_batch", return_value=(2, 0, None, 0)),
        patch.object(common, "process_file", side_effect=[processor, _processed("matched_PO.csv")]),
    ):
        collection = common.process_files_in_parallel(
            file_list=[Path("synthetic_PO.csv"), Path("matched_PO.csv")],
            log_queue=cast("LogQueue", Mock()),
            dashboard=dashboard,
            batch_progress=progress,
        )
    assert [item.file_name for item in collection.processors] == ["matched_PO.csv"]
    assert (progress.succeeded, progress.skipped, progress.failed) == (1, 1, 0)
    assert [dashboard._tasks[index].status for index in (1, 2)] == ["SKIP", "OK"]


def test_malformed_po_with_filter_remains_a_failure() -> None:
    """A missing/invalid PO header cannot masquerade as a filtered-empty file."""
    processor = POProcessor(Path("malformed_PO.csv"), entity_filter=frozenset({"SelectedLocation"}))
    invalid = pd.DataFrame([["bad"]])
    with (
        patch.object(common.BaseProcessor, "from_file", return_value=processor),
        patch("ryan_library.processors.tuflow.other_processors.POProcessor.pd.read_csv", return_value=invalid),
    ):
        actual = common.process_file(Path("malformed_PO.csv"), entity_filters=("SelectedLocation",))

    assert actual is processor
    assert not processor.processed
    assert not processor.filtered_empty
    assert common._result_status(processor) == "FAIL"
    assert processor.raw_df.empty


def test_valid_rll_qmx_without_selected_locations_is_skip() -> None:
    """Valid RLL_Qmx data excluded by the filter must not fail a POMM export."""
    processor = RLLQmxProcessor(Path("synthetic_RLL_Qmx.csv"), entity_filter=frozenset({"SelectedLocation"}))
    processor.df = pd.DataFrame(
        {
            "ID": ["OtherLocation"],
            "Qmax": [10.0],
            "Time Qmax": [1.0],
            "dQmax": [0.0],
            "Time dQmax": [1.0],
            "H": [5.0],
        }
    )
    with (
        patch.object(common.BaseProcessor, "from_file", return_value=processor),
        patch.object(processor, "read_maximums_csv", return_value=ProcessorStatus.SUCCESS),
        patch.object(processor, "validate_data", wraps=processor.validate_data) as validate,
    ):
        actual = common.process_file(Path("synthetic_RLL_Qmx.csv"), entity_filters=("SelectedLocation",))
    assert actual is processor
    assert processor.filtered_empty
    assert not processor.processed
    assert processor.df.empty
    validate.assert_not_called()
    assert common._result_status(processor) == "SKIP"
    assert common._dashboard_detail(processor) == "no matching locations"

    dashboard = LiveWorkflowDashboard(title="Filtered RLL Qmx", enabled=False)
    progress = common.FileBatchProgress()
    with (
        patch.object(common, "calculate_pool_size", return_value=1),
        patch.object(common, "_summarize_file_batch", return_value=(2, 0, None, 0)),
        patch.object(common, "process_file", side_effect=[processor, _processed("matching_POMM.csv")]),
    ):
        collection = common.process_files_in_parallel(
            file_list=[Path("synthetic_RLL_Qmx.csv"), Path("matching_POMM.csv")],
            log_queue=cast("LogQueue", Mock()),
            dashboard=dashboard,
            batch_progress=progress,
        )
    assert [item.file_name for item in collection.processors] == ["matching_POMM.csv"]
    assert (progress.succeeded, progress.skipped, progress.failed) == (1, 1, 0)


def test_malformed_rll_qmx_with_filter_remains_failure() -> None:
    """Missing source columns are not mistaken for a filter exclusion."""
    processor = RLLQmxProcessor(Path("malformed_RLL_Qmx.csv"), entity_filter=frozenset({"SelectedLocation"}))
    processor.df = pd.DataFrame({"ID": ["OtherLocation"], "Qmax": [10.0]})
    with (
        patch.object(common.BaseProcessor, "from_file", return_value=processor),
        patch.object(processor, "read_maximums_csv", return_value=ProcessorStatus.SUCCESS),
    ):
        actual = common.process_file(Path("malformed_RLL_Qmx.csv"), entity_filters=("SelectedLocation",))
    assert actual is processor
    assert not processor.filtered_empty
    assert not processor.processed
    assert processor.df.empty
    assert common._result_status(processor) == "FAIL"
