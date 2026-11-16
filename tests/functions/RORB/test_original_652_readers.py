"""Original RORBWin 6.52 reader acceptance via retained test-data submodule.

This test does not copy native files or launch any RORB executable. It runs
only when the pinned project fixture submodule is checked out.
"""

from __future__ import annotations

import hashlib
from collections import Counter
from decimal import Decimal
from pathlib import Path

import pytest

from ryan_library.functions.RORB.associations import associate_batch_csvs
from ryan_library.functions.RORB.readers import read_rorb_file

_FIXTURE_ROOT = Path(__file__).parents[2] / "test_data" / "RORB" / "12hr-areal-example"


def _original(name: str) -> Path:
    path = _FIXTURE_ROOT / name
    if not path.is_file():
        pytest.skip("Pinned RORBWin 6.52 test-data submodule was not checked out")
    return path


def test_original_652_301_row_ordinary_report_keeps_source_clock() -> None:
    path = _original("Fig6_7_ aep0p2EY_du12hourtp1.out")
    document = read_rorb_file(path)
    section = document.section("hydrographs_1")
    original_bytes = path.read_bytes()
    assert document.sha256 == hashlib.sha256(original_bytes).hexdigest()
    assert document.original_bytes == original_bytes
    assert document.status == "partial"
    assert document.metadata["program_version"] == "6.52"
    assert document.metadata["engine_identity"] == "unverified_from_report_banner"
    assert section.valid
    assert section.increment_origin == 0
    assert section.completeness == "reported_increment_count"
    assert len(section.rows) == 301
    assert len(section.source_tokens) == 301
    assert section.columns[3:] == ["Hyd0001"]
    assert [section.rows[0]["Inc"], section.rows[-1]["Inc"]] == [0, 300]
    assert section.source_tokens[0]["Time"] == "0.00"
    assert section.source_tokens[-1]["Time"] == "150.00"
    assert section.source_decimal(300, "Time") == Decimal("150.00")
    assert document.metadata["reported_duration_increments"] == 300
    assert document.metadata["time_increment_raw"] == "0.50"


def test_original_652_80_row_batch_preserves_events_and_eight_native_refs() -> None:
    path = _original("Fig6_7_batch.out")
    document = read_rorb_file(path)
    run_table = document.section("runs")
    refs = document.section("representatives")
    assert document.status == "partial"
    assert document.metadata["program_version"] == "6.52"
    assert run_table.valid
    assert refs.valid
    assert len(run_table.rows) == len(run_table.source_tokens) == 80
    assert len(refs.rows) == 8
    assert [row["Run"] for row in run_table.rows] == [run for run in range(1, 9) for _ in range(10)]
    assert [row["TPat"] for row in run_table.rows] == list(range(1, 11)) * 8
    assert [run_table.rows[i]["aep_raw"] for i in (0, 10, 30)] == ["50%", "0.5EY", "0.2EY"]
    assert [run_table.rows[i]["aep_kind"] for i in (0, 10, 30)] == ["percent", "EY", "EY"]
    assert run_table.source_tokens[0]["Peak0001"] == "55.3175"
    assert run_table.source_decimal(0, "Peak0001") == Decimal("55.3175")
    assert [row["Run"] for row in refs.rows] == list(range(1, 9))
    assert len({str(row["reference"]) for row in refs.rows}) == 8
    assert str(refs.rows[0]["reference"]) == "dur12hour_aep50tp5.out"
    assert str(refs.rows[-1]["reference"]) == "dur12hour_aep1tp4.out"


@pytest.mark.parametrize(
    ("directory", "expected_rows"),
    [
        ("12hr-areal-example", 80),
        ("24hr-areal-example", 80),
        ("48hr-areal-example", 480),
    ],
)
def test_original_652_all_batch_csv_associations(directory: str, expected_rows: int) -> None:
    """Recheck all accessible original 6.52 event/TP companions using the canonical API.

    This is 640 public examples across three original batch summaries, NOT the
    separately retained private 6.45 population of 420 pairs.
    """
    path = _FIXTURE_ROOT.parent / directory / "Fig6_7_batch.out"
    if not path.is_file():
        pytest.skip("Pinned public RORBWin 6.52 test-data submodule was not checked out")
    batch = read_rorb_file(path)
    assert batch.status == "partial"
    assert batch.section("runs").valid
    assert len(batch.section("runs").rows) == expected_rows
    result = associate_batch_csvs(batch, max_runs=500)
    counts = Counter(str(item["status"]) for item in result.rows)
    assert len(result.rows) == expected_rows
    assert counts == {"corroborated": expected_rows}, counts
    assert result.valid
