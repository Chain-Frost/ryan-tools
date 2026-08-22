"""Focused tests for native ASC_to_ASC-style raster operations."""

import csv
from pathlib import Path

import numpy as np
import pytest
import rasterio  # pyright: ignore[reportMissingTypeStubs]
from rasterio.transform import from_origin  # pyright: ignore[reportMissingTypeStubs]

from ryan_library.functions.tuflow.asc_to_asc_raster_operations import compute_stat
from ryan_library.functions.tuflow.asc_to_asc_raster_operations import flatten_nested_source_provenance
from ryan_library.orchestrators.tuflow.asc2asc_mean_then_max_by_search import (
    ParsedRaster,
    discover_max_jobs,
    discover_mean_jobs,
)

NODATA = -9999.0


def _write_raster(path: Path, values: list[float]) -> None:
    data = np.asarray([values], dtype=np.float32)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=data.shape[1],
        height=data.shape[0],
        count=1,
        dtype="float32",
        transform=from_origin(0.0, 1.0, 1.0, 1.0),
        nodata=NODATA,
    ) as destination:
        destination.write(data, 1)


def _read_raster(path: Path) -> np.ndarray:
    with rasterio.open(path) as source:
        return source.read(1)


def _compression(path: Path) -> str | None:
    with rasterio.open(path) as source:
        return None if source.compression is None else source.compression.name


@pytest.mark.parametrize(
    ("stat_type", "expected"),
    [("-statMean", [2.0, 0.0]), ("-statMax", [2.0, 0.0])],
)
def test_compute_stat_treats_nodata_as_zero(tmp_path: Path, stat_type: str, expected: list[float]) -> None:
    first = tmp_path / "first.tif"
    second = tmp_path / "second.tif"
    output = tmp_path / "mean.tif"
    _write_raster(first, [2.0, NODATA])
    _write_raster(second, [NODATA, NODATA])

    compute_stat(stat_type, [str(first), str(second)], str(output), nodata_policy="zero")

    np.testing.assert_array_equal(_read_raster(output), np.asarray([expected], dtype=np.float32))


@pytest.mark.parametrize(
    ("stat_type", "expected"),
    [("-statMean", [104.0, 104.0, NODATA]), ("-statMax", [104.0, 104.0, NODATA])],
)
def test_compute_stat_excludes_nodata_and_preserves_all_nodata(
    tmp_path: Path, stat_type: str, expected: list[float]
) -> None:
    first = tmp_path / "first.tif"
    second = tmp_path / "second.tif"
    output = tmp_path / "mean.tif"
    _write_raster(first, [102.0, NODATA, NODATA])
    _write_raster(second, [104.0, 104.0, NODATA])

    compute_stat(stat_type, [str(first), str(second)], str(output), nodata_policy="exclude")

    np.testing.assert_array_equal(_read_raster(output), np.asarray([expected], dtype=np.float32))


def test_compute_stat_can_write_arithmetic_mean(tmp_path: Path) -> None:
    first = tmp_path / "first.tif"
    second = tmp_path / "second.tif"
    output = tmp_path / "mean.tif"
    _write_raster(first, [2.0, 10.0])
    _write_raster(second, [4.0, 14.0])

    compute_stat(
        "-statMean",
        [str(first), str(second)],
        str(output),
        mean_value_method="arithmetic",
    )

    np.testing.assert_array_equal(_read_raster(output), np.asarray([[3.0, 12.0]], dtype=np.float32))


def test_compute_stat_uses_upper_median_for_even_contributors(tmp_path: Path) -> None:
    inputs: list[str] = []
    for index, value in enumerate([1.0, 3.0, 7.0, 9.0], start=1):
        input_path = tmp_path / f"input_{index}.tif"
        _write_raster(input_path, [value])
        inputs.append(str(input_path))
    output = tmp_path / "median.tif"

    compute_stat("-statMedian", inputs, str(output), write_source=True)

    np.testing.assert_array_equal(_read_raster(output), np.asarray([[7.0]], dtype=np.float32))
    np.testing.assert_array_equal(_read_raster(tmp_path / "median_src.tif"), np.asarray([[3]], dtype=np.int32))


def test_compute_stat_writes_source_ids_and_legend(tmp_path: Path) -> None:
    first = tmp_path / "first.tif"
    second = tmp_path / "second.tif"
    third = tmp_path / "third.tif"
    output = tmp_path / "maximum.tif"
    _write_raster(first, [5.0, NODATA, 8.0])
    _write_raster(second, [9.0, 4.0, 8.0])
    _write_raster(third, [7.0, 6.0, NODATA])

    compute_stat(
        "-statMax",
        [str(first), str(second), str(third)],
        str(output),
        nodata_policy="exclude",
        write_source=True,
    )

    np.testing.assert_array_equal(_read_raster(output), np.asarray([[9.0, 6.0, 8.0]], dtype=np.float32))
    np.testing.assert_array_equal(_read_raster(tmp_path / "maximum_src.tif"), np.asarray([[2, 3, 1]], dtype=np.int32))
    with (tmp_path / "maximum_src_legend.csv").open(encoding="utf-8", newline="") as stream:
        rows = list(csv.reader(stream))
    assert rows == [
        ["source_id", "source_file"],
        ["1", str(first.resolve())],
        ["2", str(second.resolve())],
        ["3", str(third.resolve())],
    ]


def test_compute_stat_source_ids_follow_closest_mean_and_nodata_policy(tmp_path: Path) -> None:
    first = tmp_path / "first.tif"
    second = tmp_path / "second.tif"
    third = tmp_path / "third.tif"
    output = tmp_path / "mean.tif"
    _write_raster(first, [2.0, NODATA, NODATA])
    _write_raster(second, [4.0, 4.0, NODATA])
    _write_raster(third, [10.0, 8.0, NODATA])

    compute_stat(
        "mean",
        [str(first), str(second), str(third)],
        str(output),
        nodata_policy="exclude",
        write_source=True,
    )

    # 4 is nearest to 16/3; the equal-distance 4/8 tie selects the higher value.
    np.testing.assert_array_equal(_read_raster(output), np.asarray([[4.0, 8.0, NODATA]], dtype=np.float32))
    np.testing.assert_array_equal(_read_raster(tmp_path / "mean_src.tif"), np.asarray([[2, 3, 0]], dtype=np.int32))


def test_compute_stat_does_not_write_source_outputs_by_default(tmp_path: Path) -> None:
    input_path = tmp_path / "input.tif"
    output = tmp_path / "minimum.tif"
    _write_raster(input_path, [3.0])

    compute_stat("min", [str(input_path)], str(output))

    assert output.is_file()
    assert not (tmp_path / "minimum_src.tif").exists()
    assert not (tmp_path / "minimum_src_legend.csv").exists()


def test_native_geotiff_outputs_use_tuflow_compression_by_default(tmp_path: Path) -> None:
    first = tmp_path / "first.tif"
    second = tmp_path / "second.tif"
    output = tmp_path / "maximum.tif"
    _write_raster(first, [1.0, 4.0])
    _write_raster(second, [3.0, 2.0])
    assert _compression(first) is None

    compute_stat("max", [str(first), str(second)], str(output), write_source=True)

    assert _compression(output) == "deflate"
    assert _compression(tmp_path / "maximum_src.tif") == "deflate"


def test_explicit_creation_options_override_tuflow_compression_defaults(tmp_path: Path) -> None:
    input_path = tmp_path / "input.tif"
    output = tmp_path / "minimum.tif"
    _write_raster(input_path, [3.0])

    compute_stat("min", [str(input_path)], str(output), extra_args=["-co", "COMPRESS=LZW"])

    assert _compression(output) == "lzw"


def test_flatten_nested_source_provenance_links_to_original_rasters(tmp_path: Path) -> None:
    originals = [
        tmp_path / name for name in ("duration1_tp1.tif", "duration1_tp2.tif", "duration2_tp1.tif", "duration2_tp2.tif")
    ]
    for index, original in enumerate(originals, start=1):
        _write_raster(original, [float(index)])

    final_output = tmp_path / "maximum.tif"
    final_source = tmp_path / "maximum_src.tif"
    first_mean_source = tmp_path / "mean_duration1_src.tif"
    second_mean_source = tmp_path / "mean_duration2_src.tif"
    _write_raster(final_source, [1.0, 2.0, 2.0, 0.0])
    _write_raster(first_mean_source, [2.0, 1.0, 1.0, 0.0])
    _write_raster(second_mean_source, [1.0, 2.0, 0.0, 0.0])

    flatten_nested_source_provenance(
        output_file=str(final_output),
        nested_source_files=[str(first_mean_source), str(second_mean_source)],
        original_input_groups=[
            [str(originals[0]), str(originals[1])],
            [str(originals[2]), str(originals[3])],
        ],
    )

    np.testing.assert_array_equal(_read_raster(final_source), np.asarray([[2, 4, 0, 0]], dtype=np.int32))
    with (tmp_path / "maximum_src_legend.csv").open(encoding="utf-8", newline="") as stream:
        rows = list(csv.reader(stream))
    assert rows == [
        ["source_id", "source_file"],
        *[[str(index), str(path.resolve())] for index, path in enumerate(originals, start=1)],
    ]


def _parsed_raster(*, tmp_path: Path, result_type: str) -> ParsedRaster:
    return ParsedRaster(
        path=tmp_path / f"input_{result_type}.tif",
        grid_directory=tmp_path,
        scenario="EXG",
        aep="1%AEP",
        duration="60m",
        duration_minutes=60.0,
        tp_number=1,
        result_type=result_type,
        trim_run_code="model_EXG",
        mean_name=f"mean_{result_type}.tif",
        max_name=f"max_{result_type}.tif",
    )


def test_mean_and_max_jobs_use_result_type_nodata_policy(tmp_path: Path) -> None:
    expected_policies = {
        "d_HR_Max": "zero",
        "V_Max": "zero",
        "h_Max": "exclude",
        "h_HR_Max": "exclude",
    }

    for result_type, expected_policy in expected_policies.items():
        mean_jobs, incomplete = discover_mean_jobs(
            rasters=[_parsed_raster(tmp_path=tmp_path, result_type=result_type)],
            output_root=tmp_path / "output",
            expected_tps=frozenset({1}),
        )
        assert not incomplete
        assert mean_jobs[0].job.nodata_policy == expected_policy

        max_jobs = discover_max_jobs(mean_jobs=mean_jobs, output_root=tmp_path / "output")
        assert max_jobs[0][0].nodata_policy == expected_policy
