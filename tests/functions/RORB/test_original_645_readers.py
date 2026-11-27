"""Authentic, public RORBWin-reported 6.45 batch/CSV reader admission.

These are *490 alternative* source events from test-data PR #4, not the
unlocated historic 420-event population. Never launch RORB or infer physical
site identity or native critical-duration/event selection from these files.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from decimal import Decimal
from pathlib import Path

import pytest

from ryan_library.functions.RORB.associations import associate_batch_csvs, batch_event_key, compare_output_csv
from ryan_library.functions.RORB.readers import read_rorb_file

_DATA_ROOT = Path(__file__).parents[2] / "test_data" / "RORB" / "6.45-490-event-reader-example"
_DATA_COMMIT = "af0d24b7640c6a40ed4384b8f04710a91b0e3a26"
_MANIFEST_SHA256 = "7025db5681361d38f0432b0c867fa967bfcf5529d13980d2bb8ba3eaf850d903"
_READER_EVIDENCE_COMMIT = "598e5edea093df426cda64c3a528fd1c5784c612"


def _fixture(name: str) -> Path:
    path = _DATA_ROOT / name
    if not path.is_file():
        pytest.skip(f"Public 6.45 test-data PR #4 fixture at {_DATA_COMMIT} is not checked out")
    return path


def test_original_645_distributed_fixture_manifest_and_all_file_hashes() -> None:
    """Reauthenticate all 1,472 distributed inputs/outputs, not inaccessible originals."""
    manifest_bytes = _fixture("manifest.json").read_bytes()
    assert hashlib.sha256(manifest_bytes).hexdigest() == _MANIFEST_SHA256
    manifest = json.loads(manifest_bytes)
    assert manifest["dataset_id"] == "rorbwin-reported-6.45-490-event-reader-example"
    assert manifest["expected_batch_events"] == 490
    entries = manifest["files"]
    assert len(entries) == 1472
    names = [str(item["path"]) for item in entries]
    assert len(set(names)) == 1472
    assert Counter(Path(name).suffix.lower() for name in names) == {".csv": 490, ".out": 491, ".stm": 491}
    for entry in entries:
        name = str(entry["path"])
        assert Path(name).name == name, f"Not a safe single-file fixture name: {name!r}"
        path = _DATA_ROOT / name
        assert path.is_file(), name
        contents = path.read_bytes()
        assert len(contents) == entry["size_bytes"], name
        assert hashlib.sha256(contents).hexdigest() == entry["sha256"], name
        # The source originals are not distributed; their hashes are provenance,
        # not independently verified original-file byte comparisons here.
        assert len(str(entry["original_sha256"])) == 64


def test_original_645_all_490_batch_csv_associations_and_composite_event_ids() -> None:
    """Apply current reader grammar to every member, then corroborate saved event evidence."""
    batch = read_rorb_file(_fixture("RORB_14141_batch.out"))
    assert batch.kind == "batch_output"
    assert batch.status == "partial"
    assert batch.metadata["program_version"] == "6.45"
    runs = batch.section("runs")
    assert runs.valid
    assert len(runs.rows) == len(runs.source_tokens) == 490

    associations = associate_batch_csvs(batch, max_runs=500)
    counts = Counter(str(item["status"]) for item in associations.rows)
    assert len(associations.rows) == 490
    assert associations.valid
    assert counts == {"corroborated": 490}, counts

    validation = json.loads(_fixture("validation.json").read_bytes())
    assert validation["tested_reader_commit"] == _READER_EVIDENCE_COMMIT
    assert validation["manifest_files_verified"] == 1472
    assert validation["batch_rows"] == 490
    assert validation["counts"] == {"corroborated": 490}
    assert validation["association_valid"] is True
    expected_events = validation["events"]
    assert len(expected_events) == 490
    assert len({tuple(event["event_key"]) for event in expected_events}) == 490
    for row, observed, expected in zip(runs.rows, associations.rows, expected_events, strict=True):
        assert batch_event_key(row) == tuple(expected["event_key"])
        assert int(str(row["line"])) == expected["batch_line"]
        assert observed["status"] == expected["status"] == "corroborated"
        assert Path(str(observed["path"])).name == expected["csv"]


def test_original_645_501_row_output_csv_preserves_half_hour_offset() -> None:
    """Confirm source times disagree by +0.5 h; do not silently shift them."""
    stem = "RORB_14141_ aep10_du12hourtp1"
    out = read_rorb_file(_fixture(f"{stem}.out"))
    csv = read_rorb_file(_fixture(f"{stem}.csv"))
    assert out.kind == "standard_output"
    assert csv.kind == "hydrograph_csv"
    assert out.metadata["program_version"] == "6.45"
    assert out.metadata["engine_identity"] == "unverified_from_report_banner"
    assert out.section("hydrographs_1").valid
    assert csv.section("hydrographs").valid
    assert len(out.section("hydrographs_1").rows) == len(csv.section("hydrographs").rows) == 501
    assert out.section("hydrographs_1").source_decimal(0, "Time") == Decimal("0.00")
    assert csv.section("hydrographs").source_decimal(0, "Time (hrs)") == Decimal("0.500")
    comparison = compare_output_csv(out, csv)
    assert comparison["status"] == "mismatch"
    assert comparison["flows_match"] is True
    assert comparison["time_matches"] is False
    assert comparison["reference_filename_matches"] is True
    assert comparison["rows"] == 501
    assert comparison["csv_minus_output_hours"] == 0.5
