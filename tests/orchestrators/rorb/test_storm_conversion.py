"""Batch storm conversion and reusable template integration tests."""

from pathlib import Path

import pytest

from ryan_library.functions.RORB.storm_files import ConversionError
from ryan_library.orchestrators.rorb.storm_conversion import (
    DEFAULT_INPUT_TEMPLATE,
    prepare_batch,
    prepare_single,
    write_batch,
)


def _csv(path: Path, *, extra_final: bool = False) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    final = "3,0\n" if extra_final else ""
    path.write_text(
        "Event ID,RF\nTime (hour),TP07_2030_SSP3-7.0\n0,0\n1,5\n2,10\n" + final,
        encoding="utf-8",
    )
    return path


def _table(path: Path, *rows: str) -> Path:
    path.write_text(
        "Model,TUFLOW_AEP,TUFLOW_Duration,TP\n" + "".join(row + "\n" for row in rows),
        encoding="utf-8",
    )
    return path


def test_single_file_prepares_correct_storm_without_writing(tmp_path: Path) -> None:
    csv_path = _csv(tmp_path / "TUFLOW_MLGD" / "GD08_RF_01.00p00120m.CSV")
    target = tmp_path / "output"
    job = prepare_single(csv_path, column="TP07_2030_SSP3-7.0", output_dir=target)
    assert job.output_path == target / "Storm_GD08_01p_120m_tp07.stm"
    assert job.stm_text.startswith("MLGD\nDESIGN\n")
    assert "\n0,2\nPluviograph 1\n5\n10\n-99\n" in job.stm_text
    assert not target.exists()


def test_batch_preflights_and_uses_configured_input_layout(tmp_path: Path) -> None:
    table = _table(tmp_path / "critical.csv", "GD08,01.00p,00120m,TP07", ",,,")
    bc = tmp_path / "TUFLOW_MLGD" / "bc_dbase"
    _csv(bc / "GD08" / "rf_inflow" / "GD08_RF_01.00p00120m.CSV", extra_final=True)
    root = tmp_path / "outputs"
    plan = prepare_batch(table, bc_database=bc, output_dir=root)
    assert len(plan.jobs) == 1
    assert len(plan.skipped) == 1
    assert not plan.errors
    job = plan.jobs[0]
    assert job.output_path == root / "GD08" / "Storm_GD08_01p_120m_tp07.stm"
    assert "\n0,2\nPluviograph 1\n5\n10\n-99\n" in job.stm_text
    assert not root.exists()
    assert write_batch(plan) == (job.output_path,)
    assert job.output_path.read_text(encoding="utf-8") == job.stm_text


def test_batch_no_partial_writes_on_invalid_input(tmp_path: Path) -> None:
    table = _table(tmp_path / "critical.csv", "GD08,01.00p,00120m,TP07", "GD09,01.00p,00120m,TP07")
    bc = tmp_path / "TUFLOW_MLGD" / "bc_dbase"
    _csv(bc / "GD08" / "rf_inflow" / "GD08_RF_01.00p00120m.CSV")
    root = tmp_path / "outputs"
    plan = prepare_batch(table, bc_database=bc, output_dir=root)
    assert len(plan.jobs) == 1
    assert len(plan.errors) == 1
    with pytest.raises(ConversionError, match="no outputs"):
        write_batch(plan)
    assert not root.exists()


def test_batch_duplicate_target_fails_validation(tmp_path: Path) -> None:
    table = _table(tmp_path / "critical.csv", "GD08,01.00p,00120m,TP07", "GD08,01.00p,00120m,TP07")
    bc = tmp_path / "TUFLOW_MLGD" / "bc_dbase"
    _csv(bc / "GD08" / "rf_inflow" / "GD08_RF_01.00p00120m.CSV")
    plan = prepare_batch(table, bc_database=bc, output_dir=tmp_path / "out")
    assert len(plan.jobs) == 1
    assert len(plan.errors) == 1
    assert "duplicates" in plan.errors[0]


def test_batch_configurable_templates(tmp_path: Path) -> None:
    table = _table(tmp_path / "critical.csv", "GD08,01.00p,00120m,TP07")
    root = tmp_path / "TUFLOW_MLGD" / "rainfall"
    _csv(root / "model-GD08" / "rain-00120m.CSV")
    plan = prepare_batch(
        table,
        bc_database=root,
        output_dir=tmp_path / "out",
        input_template="model-{model}/rain-{duration}.CSV",
        output_template="custom/{model}_{tp}.stm",
    )
    assert not plan.errors
    assert plan.jobs[0].output_path == tmp_path / "out" / "custom" / "GD08_TP07.stm"


def test_batch_rejects_unsafe_output_template(tmp_path: Path) -> None:
    table = _table(tmp_path / "critical.csv", "GD08,01.00p,00120m,TP07")
    root = tmp_path / "TUFLOW_MLGD" / "rainfall"
    _csv(root / "GD08" / "rf_inflow" / "GD08_RF_01.00p00120m.CSV")
    plan = prepare_batch(table, bc_database=root, output_dir=tmp_path / "out", output_template="../elsewhere.stm")
    assert plan.errors
    assert "Unsafe output" in plan.errors[0]


def test_batch_prevents_overwrites_by_default(tmp_path: Path) -> None:
    table = _table(tmp_path / "critical.csv", "GD08,01.00p,00120m,TP07")
    root = tmp_path / "TUFLOW_MLGD" / "rainfall"
    _csv(root / "GD08" / "rf_inflow" / "GD08_RF_01.00p00120m.CSV")
    out = tmp_path / "out"
    job = prepare_batch(table, bc_database=root, output_dir=out).jobs[0]
    job.output_path.parent.mkdir(parents=True, exist_ok=True)
    job.output_path.write_text("old", encoding="utf-8")
    plan = prepare_batch(table, bc_database=root, output_dir=out)
    assert plan.errors
    assert "already exists" in plan.errors[0]
    plan_overwrite = prepare_batch(table, bc_database=root, output_dir=out, overwrite=True)
    assert not plan_overwrite.errors
    write_batch(plan_overwrite, overwrite=True)
    assert job.output_path.read_text(encoding="utf-8") != "old"


def test_default_template_uses_rainfall_subfolder() -> None:
    assert "rf_inflow" in DEFAULT_INPUT_TEMPLATE
