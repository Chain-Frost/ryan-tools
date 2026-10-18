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
    plan = prepare_batch(table, bc_database=bc, output_dir=root, allow_incomplete_rows=True)
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


def test_incomplete_rows_fail_closed_by_default(tmp_path: Path) -> None:
    table = _table(tmp_path / "critical.csv", "GD08,01.00p,00120m,TP07", "GD09,,,TP07")
    bc = tmp_path / "TUFLOW_MLGD" / "bc_dbase"
    _csv(bc / "GD08" / "rf_inflow" / "GD08_RF_01.00p00120m.CSV")
    out = tmp_path / "output"
    plan = prepare_batch(table, bc_database=bc, output_dir=out)
    assert len(plan.jobs) == 1
    assert not plan.skipped
    assert any("incomplete row" in error and "--allow-incomplete" in error for error in plan.errors)
    with pytest.raises(ConversionError, match="no outputs"):
        write_batch(plan)
    assert not out.exists()


@pytest.mark.parametrize("rows", [(), (",,,",), ("GD09,,,TP07",)])
def test_empty_or_all_incomplete_batches_fail_even_when_skipping(
    tmp_path: Path, rows: tuple[str, ...]
) -> None:
    table = _table(tmp_path / "critical.csv", *rows)
    out = tmp_path / "output"
    plan = prepare_batch(
        table,
        bc_database=tmp_path / "TUFLOW_MLGD" / "bc_dbase",
        output_dir=out,
        allow_incomplete_rows=True,
    )
    assert not plan.jobs
    assert any("No valid storm conversion jobs" in error for error in plan.errors)
    with pytest.raises(ConversionError, match="no outputs"):
        write_batch(plan)
    assert not out.exists()


def test_write_batch_rejects_empty_plan_directly() -> None:
    from ryan_library.orchestrators.rorb.storm_conversion import BatchPlan

    with pytest.raises(ConversionError, match="no valid"):
        write_batch(BatchPlan((), (), ()))


@pytest.mark.parametrize(
    "table_content, message",
    [
        ("Model,TUFLOW_AEP,TP\nGD08,01.00p,TP07\n", "missing columns"),
        ("Model,TUFLOW_AEP,TUFLOW_Duration,TP,TP\n", "duplicate CSV column"),
        ("Model,TUFLOW_AEP,TUFLOW_Duration,TP\nGD08,01.00p,00120m,TP07,EXTRA\n", "extra cells"),
    ],
)
def test_malformed_critical_storm_tables_rejected(
    tmp_path: Path, table_content: str, message: str
) -> None:
    table = tmp_path / "critical.csv"
    table.write_text(table_content, encoding="utf-8")
    with pytest.raises(ConversionError, match=message):
        prepare_batch(table, bc_database=tmp_path / "TUFLOW_MLGD" / "bc_dbase", output_dir=tmp_path / "out")


def test_multiple_tp_columns_and_metadata_full_stm_golden(tmp_path: Path) -> None:
    """Synthetic, representative TUFLOW CSV with event row and extra ending zero."""
    table = _table(tmp_path / "critical.csv", "GD08,01.00p,00120m,TP07")
    bc = tmp_path / "TUFLOW_MLGD" / "bc_dbase"
    rain_path = bc / "GD08" / "rf_inflow" / "GD08_RF_01.00p00120m.CSV"
    rain_path.parent.mkdir(parents=True, exist_ok=True)
    rain_path.write_text(
        "Event ID,01.00p 120m,01.00p 120m\n"
        "Time (hour),TP01_2030_SSP3-7.0,TP07_2030_SSP3-7.0\n"
        "0,0,0\n"
        "1,100,5\n"
        "2,200,10\n"
        "3,0,0\n",
        encoding="utf-8",
    )
    plan = prepare_batch(table, bc_database=bc, output_dir=tmp_path / "stm")
    assert not plan.errors
    assert len(plan.jobs) == 1
    expected = (
        "MLGD\n"
        "DESIGN\n"
        "C time increment (hours), run increments, bursts, pluviographs, uniform flag\n"
        "1.0,200,1,1,0,-99\n"
        "C start and finish increment of the single rainfall burst\n"
        "0,2\n"
        "Pluviograph 1\n"
        "5\n"
        "10\n"
        "-99\n"
    )
    assert plan.jobs[0].stm_text == expected
    assert not plan.jobs[0].output_path.exists()
