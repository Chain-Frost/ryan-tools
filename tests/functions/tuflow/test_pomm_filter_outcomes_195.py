# pyright: reportPrivateUsage=false
# Exercises workflow classifier internals after processing a real POMM input.
"""WP07 #195: a valid POMM location miss must not become a failed input."""

from pathlib import Path
from typing import cast
from unittest.mock import Mock, patch

import pytest

import pandas as pd

from ryan_library.functions.live_dashboard import LiveWorkflowDashboard
from ryan_library.functions.loguru_helpers import LogQueue
from ryan_library.functions.tuflow import tuflow_common as common
from ryan_library.functions.tuflow.tuflow_common import _dashboard_detail, _result_status, process_file
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.other_processors.POMMProcessor import POMMProcessor


def _write_pomm(path: Path, *, location: str = "IncludedLocation") -> None:
    """Write the supported transposed POMM layout with one valid location."""
    columns: dict[int, list[object]] = {
        0: ["Run", "Run", "Run", "Run", "Run", "Run"],
        1: [
            "Location",
            "Time",
            "Maximum (Extracted from Time Series)",
            "Time of Maximum",
            "Minimum (Extracted From Time Series)",
            "Time of Minimum",
        ],
        2: ["Sample", location, 5.0, 1.0, -1.0, 2.0],
    }
    pd.DataFrame(columns).to_csv(path, header=False, index=False)


def test_valid_pomm_with_matching_locations_remains_successful(tmp_path: Path) -> None:
    path = tmp_path / "Run_POMM.csv"
    _write_pomm(path)
    processor = POMMProcessor(file_path=path)
    with patch.object(BaseProcessor, "from_file", return_value=processor):
        actual = process_file(path)
    assert actual is processor
    assert processor.processed
    assert not processor.filtered_empty
    assert not processor.df.empty
    assert _result_status(processor) == "OK"


def test_valid_pomm_excluded_by_locations_remains_skip(tmp_path: Path) -> None:
    path = tmp_path / "Run_POMM.csv"
    _write_pomm(path)
    processor = POMMProcessor(file_path=path, entity_filter=frozenset({"OtherLocation"}))
    with patch.object(BaseProcessor, "from_file", return_value=processor):
        actual = process_file(path, entity_filters=("OtherLocation",))
    assert actual is processor
    assert processor.filtered_empty
    assert not processor.processed
    assert processor.df.empty
    assert _result_status(processor) == "SKIP"
    assert _dashboard_detail(processor) == "no matching locations"


def test_malformed_pomm_with_filter_remains_failure(tmp_path: Path) -> None:
    path = tmp_path / "Run_POMM.csv"
    # Missing mandatory transposed columns, not a valid location exclusion.
    pd.DataFrame({0: ["run", "Location"], 1: ["run", "IncludedLocation"]}).to_csv(path, header=False, index=False)
    processor = POMMProcessor(file_path=path, entity_filter=frozenset({"OtherLocation"}))
    with patch.object(BaseProcessor, "from_file", return_value=processor):
        actual = process_file(path, entity_filters=("OtherLocation",))
    assert actual is None
    assert not processor.filtered_empty
    assert not processor.processed
    assert _result_status(processor) == "FAIL"


@pytest.mark.parametrize("dashboard_enabled", [False, True])
def test_mixed_valid_pomm_filters_do_not_fail_batch(tmp_path: Path, dashboard_enabled: bool) -> None:
    """A deliberately excluded POMM file must not taint another valid result."""
    selected = tmp_path / "Selected_POMM.csv"
    excluded = tmp_path / "Excluded_POMM.csv"
    _write_pomm(selected, location="IncludedLocation")
    _write_pomm(excluded, location="OtherLocation")

    dashboard = LiveWorkflowDashboard(title="POMM selection", enabled=dashboard_enabled)
    progress = common.FileBatchProgress()
    with patch.object(common, "calculate_pool_size", return_value=1), dashboard:
        results = common.process_files_in_parallel(
            file_list=[selected, excluded],
            log_queue=cast("LogQueue", Mock()),
            entity_filters=("IncludedLocation",),
            dashboard=dashboard if dashboard_enabled else None,
            batch_progress=progress,
        )
    assert len(results.processors) == 1
    assert results.processors[0].file_path == selected
    assert (progress.succeeded, progress.skipped, progress.failed) == (1, 1, 0)
