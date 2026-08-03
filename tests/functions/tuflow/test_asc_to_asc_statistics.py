"""Unit tests for ryan_library.functions.tuflow.asc_to_asc_statistics."""

from pathlib import Path
import pytest
from unittest.mock import patch, MagicMock

from ryan_library.functions.tuflow.asc_to_asc_statistics import (
    result_type_from_parser,
    require_component_text,
    replace_filename_component,
    format_user_template,
    validate_output_filename,
    StatisticJob,
    run_statistic_job,
    run_statistic_stage,
    DashboardOptions,
)
from ryan_library.classes.tuflow_string_classes import TuflowStringParser


def test_result_type_from_parser() -> None:
    parser = MagicMock(spec=TuflowStringParser)
    parser.data_type = "h"
    assert result_type_from_parser(parser=parser, result_types=["H", "V"]) == "H"

    parser.data_type = "unknown"
    assert result_type_from_parser(parser=parser, result_types=["H", "V"]) is None

    parser.data_type = None
    assert result_type_from_parser(parser=parser, result_types=["H", "V"]) is None


def test_require_component_text() -> None:
    assert require_component_text(value="val", component="comp", filename="file.asc") == "val"

    with pytest.raises(ValueError, match="Parsed comp text was empty in file.asc"):
        require_component_text(value="", component="comp", filename="file.asc")

    with pytest.raises(ValueError):
        require_component_text(value=None, component="comp", filename="file.asc")


def test_replace_filename_component() -> None:
    # Replacement
    assert replace_filename_component(filename="a_b_c", old_component="b", new_component="z") == "a_z_c"
    assert replace_filename_component(filename="a+b+c", old_component="b", new_component="z") == "a+z+c"

    # Deletion (middle)
    assert replace_filename_component(filename="a_b_c", old_component="b", new_component=None) == "a_c"
    # Deletion (end)
    assert replace_filename_component(filename="a_b_c", old_component="c", new_component=None) == "a_b"
    # Deletion (start)
    assert replace_filename_component(filename="a_b_c", old_component="a", new_component=None) == "b_c"

    with pytest.raises(ValueError, match="Expected one 'z' component"):
        replace_filename_component(filename="a_b_c", old_component="z", new_component="x")

    with pytest.raises(ValueError, match="Expected one 'a' component"):
        replace_filename_component(filename="a_a_c", old_component="a", new_component="x")


def test_format_user_template() -> None:
    res = format_user_template(template="{a}_{b}", values={"a": "1", "b": "2"}, description="desc")
    assert res == "1_2"

    with pytest.raises(ValueError, match="Unknown placeholder 'c'"):
        format_user_template(template="{a}_{c}", values={"a": "1"}, description="desc")


def test_validate_output_filename() -> None:
    assert validate_output_filename("valid_name.asc") == "valid_name.asc"

    with pytest.raises(ValueError, match="contains invalid characters"):
        validate_output_filename("invalid:name.asc")

    with pytest.raises(ValueError, match="contains invalid characters"):
        validate_output_filename("folder/name.asc")

    with pytest.raises(ValueError, match="must produce a filename, not a path"):
        validate_output_filename(".")

    with pytest.raises(ValueError, match="must produce a filename"):
        validate_output_filename("")


@patch("subprocess.run")
def test_run_statistic_job(mock_run, tmp_path) -> None:
    exe = Path("asc_to_asc.exe")
    out_file = tmp_path / "out.asc"
    job = StatisticJob(label="j1", operation="-max", input_files=(Path("in1.asc"),), output_file=out_file)

    mock_cp = MagicMock()
    mock_cp.returncode = 0
    mock_run.return_value = mock_cp

    # Touch output file to simulate success
    out_file.touch()

    res = run_statistic_job(executable=exe, job=job)
    assert res == out_file
    mock_run.assert_called_once()

    # Test failure from returncode
    mock_cp.returncode = 1
    mock_cp.stdout = "Error running"
    with pytest.raises(RuntimeError, match="ASC_to_ASC exited with code 1"):
        run_statistic_job(executable=exe, job=job)

    # Test failure from missing output
    mock_cp.returncode = 0
    out_file.unlink()
    with pytest.raises(FileNotFoundError, match="Expected output was not created"):
        run_statistic_job(executable=exe, job=job)


@patch("ryan_library.functions.tuflow.asc_to_asc_statistics.LiveWorkflowDashboard")
@patch("ryan_library.functions.tuflow.asc_to_asc_statistics.run_statistic_job")
def test_run_statistic_stage(mock_run_job, mock_dashboard, tmp_path) -> None:
    exe = Path("asc_to_asc.exe")
    opts = DashboardOptions(enabled=False)

    job1 = StatisticJob(label="j1", operation="-max", input_files=(), output_file=tmp_path / "1.asc")
    job2 = StatisticJob(label="j2", operation="-max", input_files=(), output_file=tmp_path / "2.asc")

    mock_run_job.side_effect = [tmp_path / "1.asc", RuntimeError("fail")]

    summary = run_statistic_stage(
        executable=exe,
        jobs=[job1, job2],
        stage_name="Stage 1",
        dashboard_title="T",
        dashboard_subtitle="S",
        workers=2,
        dashboard_options=opts,
    )

    assert summary.total == 2
    assert summary.succeeded == 1
    assert summary.failed == 1
    assert not summary.ok


def test_run_statistic_stage_empty() -> None:
    summary = run_statistic_stage(
        executable=Path("e"),
        jobs=[],
        stage_name="s",
        dashboard_title="t",
        dashboard_subtitle="s",
        workers=2,
        dashboard_options=DashboardOptions(),
    )
    assert summary.total == 0
    assert summary.ok
