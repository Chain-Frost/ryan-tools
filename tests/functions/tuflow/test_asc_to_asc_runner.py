"""Unit tests for ASC_to_ASC execution and supporting workflow helpers."""

from pathlib import Path
import pytest
from unittest.mock import patch, MagicMock

from ryan_library.functions.tuflow.asc_to_asc_runner import (
    RasterOperationJob,
    run_asc_to_asc_job,
    run_python_raster_job,
)
from ryan_library.functions.tuflow.tuflow_result_naming import (
    result_type_from_parser,
    require_component_text,
    replace_filename_component,
    format_user_template,
    validate_output_filename,
)
from ryan_library.orchestrators.tuflow.asc_to_asc_batch import (
    DashboardOptions,
    run_raster_operation_stage,
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


@patch("ryan_library.functions.tuflow.asc_to_asc_runner.subprocess.run")
def test_run_asc_to_asc_job(mock_run, tmp_path: Path) -> None:
    exe = tmp_path / "asc_to_asc.exe"
    exe.touch()
    out_file = tmp_path / "out.asc"
    job = RasterOperationJob(label="j1", operation="-max", input_files=(Path("in1.asc"),), output_file=out_file)

    mock_cp = MagicMock()
    mock_cp.returncode = 0
    mock_run.return_value = mock_cp

    # Touch output file to simulate success
    out_file.touch()

    res = run_asc_to_asc_job(executable=exe, job=job)
    assert res == out_file
    mock_run.assert_called_once()
    assert "-src" in mock_run.call_args.args[0]

    # Test failure from returncode
    mock_cp.returncode = 1
    mock_cp.stdout = "Error running"
    with pytest.raises(RuntimeError, match="ASC_to_ASC exited with code 1"):
        run_asc_to_asc_job(executable=exe, job=job)

    # Test failure from missing output
    mock_cp.returncode = 0
    out_file.unlink()
    with pytest.raises(FileNotFoundError, match="Expected output was not created"):
        run_asc_to_asc_job(executable=exe, job=job)


@patch("ryan_library.functions.tuflow.asc_to_asc_runner.compute_stat")
def test_run_python_raster_job_does_not_require_executable(mock_compute, tmp_path: Path) -> None:
    output = tmp_path / "mean.tif"
    job = RasterOperationJob(
        label="mean",
        operation="-statMean",
        input_files=(tmp_path / "one.tif", tmp_path / "two.tif"),
        output_file=output,
    )

    assert run_python_raster_job(job=job) == output

    mock_compute.assert_called_once_with(
        stat_type="-statMean",
        input_files=[str(tmp_path / "one.tif"), str(tmp_path / "two.tif")],
        output_file=str(output),
        nodata_policy="require_all",
        mean_value_method="closest_source",
        write_source=False,
    )


@patch("ryan_library.functions.tuflow.asc_to_asc_runner.subprocess.run")
def test_run_asc_to_asc_stat_job_normalizes_suffixed_comparison_output(mock_run, tmp_path: Path) -> None:
    executable = tmp_path / "asc_to_asc.exe"
    executable.touch()
    requested_output = tmp_path / "comparison.tif"
    suffixed_output = tmp_path / "comparison_Mean_Val.tif"
    job = RasterOperationJob("mean", "-statMean", (tmp_path / "input.tif",), tmp_path / "native.tif")

    def create_external_output(*args, **kwargs):
        suffixed_output.touch()
        completed = MagicMock()
        completed.returncode = 0
        return completed

    mock_run.side_effect = create_external_output

    assert run_asc_to_asc_job(executable=executable, job=job, output_file=requested_output) == requested_output
    assert requested_output.is_file()
    assert not suffixed_output.exists()
    assert "-src" not in mock_run.call_args.args[0]


def test_run_raster_operation_stage_empty_without_executable() -> None:
    summary = run_raster_operation_stage(
        jobs=[],
        stage_name="s",
        dashboard_title="t",
        dashboard_subtitle="s",
        workers=2,
        dashboard_options=DashboardOptions(),
    )
    assert summary.total == 0
    assert summary.ok
