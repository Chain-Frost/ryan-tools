"""WP07 #195: a valid POMM location miss must not become a failed input."""

from pathlib import Path
from unittest.mock import patch

import pandas as pd

from ryan_library.functions.tuflow.tuflow_common import _dashboard_detail, _result_status, process_file
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.other_processors.POMMProcessor import POMMProcessor


def _write_pomm(path: Path) -> None:
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
        2: ["Sample", "IncludedLocation", 5.0, 1.0, -1.0, 2.0],
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
    pd.DataFrame({0: ["run", "Location"], 1: ["run", "IncludedLocation"]}).to_csv(
        path, header=False, index=False
    )
    processor = POMMProcessor(file_path=path, entity_filter=frozenset({"OtherLocation"}))
    with patch.object(BaseProcessor, "from_file", return_value=processor):
        actual = process_file(path, entity_filters=("OtherLocation",))
    assert actual is None
    assert not processor.filtered_empty
    assert not processor.processed
    assert _result_status(processor) == "FAIL"
