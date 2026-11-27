"""Regression checks for reporting-processor retries and original CSV record widths."""

from pathlib import Path

import pytest

from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.other_processors.SHmxProcessor import SHmxProcessor

RUN = "EG02_013"
KINDS = ("RLL_Q", "RLP_H", "RLR_Volmx", "SHmx")


def _source(directory: Path, kind: str, *, mode: str = "full") -> Path:
    """Provide two known locations; a short row differs from an explicit blank."""
    if kind in {"RLL_Q", "RLP_H"}:
        measurement = "Q" if kind == "RLL_Q" else "H"
        header = f'"{RUN} [model.tcf]","Time (h)","{measurement} A [{RUN}]","{measurement} B [{RUN}]"'
        rows = [["1", "0", "1", "10"], ["2", "1", "2", "20"]]
        blank_at = -1
    elif kind == "RLR_Volmx":
        header = f'"{RUN} [model.tcf]","ID","Vol max","Time Vol max","dVolmax","Time dVolmax","H"'
        rows = [["1", "A", "2", "1", "0", "0", "2"], ["2", "B", "3", "1", "0", "0", "3"]]
        blank_at = -1
    elif kind == "SHmx":
        fields = SHmxProcessor.SOURCE_FIELDS
        header = f'"{RUN} [model.tcf]",' + ",".join(f'"{field}"' for field in fields)
        rows = [
            ["1", location, "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "0.5"]
            for location in ("A", "B")
        ]
        # Missing Q Below is supported; a missing associated Time is not.
        blank_at = 2
    else:
        msg = f"Unexpected reporting test type: {kind}"
        raise ValueError(msg)

    if mode == "truncated":
        rows[1].pop()
    elif mode == "blank":
        rows[1][blank_at] = ""
    elif mode != "full":
        msg = f"Unexpected reporting test mode: {mode}"
        raise ValueError(msg)

    path = directory / f"{RUN}_{kind}.csv"
    path.write_text(header + "\n" + "".join(",".join(row) + "\n" for row in rows), encoding="utf-8")
    return path


@pytest.mark.parametrize("kind", KINDS)
def test_reprocessing_clears_cached_location_filter(kind: str, tmp_path: Path) -> None:
    processor = BaseProcessor.from_file(_source(tmp_path, kind))
    processor.process()
    assert processor.processed
    assert set(processor.df["Location"].astype(str)) == {"A", "B"}

    processor.filter_locations(["A"])
    assert processor.applied_location_filter == frozenset({"A"})
    assert set(processor.df["Location"].astype(str)) == {"A"}

    # Re-reading repopulates both locations. A repeated selection must run again.
    processor.process()
    assert processor.processed
    assert processor.applied_location_filter is None
    assert set(processor.df["Location"].astype(str)) == {"A", "B"}
    processor.filter_locations(["A"])
    assert set(processor.df["Location"].astype(str)) == {"A"}


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("selected", [None, ["A"], ["NotPresent"]])
def test_truncated_record_fails_even_when_column_was_not_selected(
    kind: str, selected: list[str] | None, tmp_path: Path
) -> None:
    processor = BaseProcessor.from_file(_source(tmp_path, kind, mode="truncated"), entity_filter=selected)
    processor.process()
    assert not processor.processed
    assert processor.df.empty
    assert processor.raw_df.empty
    assert processor.input_health == "INVALID"
    assert not processor.filtered_empty


@pytest.mark.parametrize("kind", KINDS)
def test_explicit_blank_measurement_does_not_masquerade_as_truncation(kind: str, tmp_path: Path) -> None:
    processor = BaseProcessor.from_file(_source(tmp_path, kind, mode="blank"))
    processor.process()
    assert processor.processed
    assert set(processor.df["Location"].astype(str)) == {"A", "B"}


@pytest.mark.parametrize("kind", KINDS)
def test_success_failure_recovery_has_no_stale_rows_or_filters(kind: str, tmp_path: Path) -> None:
    path = _source(tmp_path, kind)
    processor = BaseProcessor.from_file(path)
    processor.process()
    assert processor.processed
    processor.filter_locations(["A"])

    _source(tmp_path, kind, mode="truncated")
    processor.process()
    assert not processor.processed
    assert processor.df.empty
    assert processor.input_health == "INVALID"
    assert processor.applied_location_filter is None

    _source(tmp_path, kind)
    processor.process()
    assert processor.processed
    assert processor.input_health == "OK"
    processor.filter_locations(["A"])
    assert set(processor.df["Location"].astype(str)) == {"A"}
