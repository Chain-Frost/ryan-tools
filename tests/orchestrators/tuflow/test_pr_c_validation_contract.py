"""Public PyTuflow PR C collection/validation contracts for the consumer.

Runs only with the exact curated development parser installed in isolated CI.
The normal ryan-tools dependency and upstream release numbering are unchanged.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from ryan_library.orchestrators.tuflow.input_preflight import RunRequest, check_batch_files


@pytest.mark.skipif(
    os.environ.get("RYAN_TEST_BATCH_PYTUFLOW") != "1",
    reason="Requires curated PyTuflow PR C collection and validation APIs",
)
def test_public_pr_c_validator_and_file_only_preflight_keep_original_run_order(tmp_path: Path) -> None:
    """Compare physical findings and ordered provenance without GIS/raster reads."""
    from pytuflow import TCF
    from pytuflow.batch_validation import validate_batch_inputs
    from pytuflow.validation import ValidationOptions

    tcf = tmp_path / "model.tcf"
    tcf.write_text("Read Grid Zpts == grid_<<~s1~>>.tif\n", encoding="utf-8")
    (tmp_path / "grid_EXG.tif").write_bytes(b"Existence-only fixture, not raster content")

    runs = (
        RunRequest(17, {"s1": "EXG"}),
        RunRequest(23, {"s1": "DEV"}),
        RunRequest(41, {"s1": "EXG"}),
    )
    flags = tuple(run.arguments for run in runs)
    model = TCF(tcf)

    projection = model.collect_file_dependencies(flags)
    assert projection.passed
    assert len(projection.runs) == len(flags)
    assert tuple(run.index for run in projection.runs) == (0, 1, 2)
    assert tuple(run.flags for run in projection.runs) == flags
    assert all(run.ordered_inputs and run.input_references for run in projection.runs)

    checked = validate_batch_inputs(
        model,
        flags,
        options=ValidationOptions(
            check_files=True,
            check_gpkg_layers=True,
            check_bc_csv_headers=True,
            io_workers=4,
        ),
    )
    assert checked.dependencies.runs == projection.runs
    assert not checked.passed
    assert checked.validation.run_count == 3
    assert {issue.run_index for issue in checked.issues} == {1}
    assert all(issue.status == "missing" and issue.origin for issue in checked.issues)
    assert all(issue.path == tmp_path / "grid_DEV.tif" for issue in checked.issues)

    consumer = check_batch_files(tcf, runs, io_workers=4)
    assert not consumer.passed
    assert consumer.run_count == 3
    assert {issue.run_index for issue in consumer.issues} == {23}
    assert all(issue.status == "missing" for issue in consumer.issues)
    assert consumer.checked_paths == {
        str(tmp_path / "grid_EXG.tif"),
        str(tmp_path / "grid_DEV.tif"),
    }
