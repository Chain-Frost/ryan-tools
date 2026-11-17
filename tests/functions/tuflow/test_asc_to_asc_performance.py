"""Exact raster and provenance contracts for the bounded execution path."""

import csv
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Protocol

import numpy as np
import numpy.typing as npt
import pytest
import rasterio
from rasterio.env import get_gdal_config
from rasterio.transform import from_origin
from rasterio.windows import Window

from ryan_library.functions.tuflow import asc_to_asc_raster_operations as operations
from ryan_library.functions.tuflow.asc_to_asc_raster_operations import MeanValueMethod, NodataPolicy, StatisticType
from ryan_library.functions.tuflow.asc_to_asc_runner import RasterOperationJob, run_python_raster_job
from ryan_library.functions.tuflow.raster_performance import RasterExecutionOptions, RasterOperationTimings

NODATA = -9999.0
TRANSFORM = from_origin(300000.0, 6500000.0, 2.0, 2.0)
type RasterDtype = Literal["float32", "float64"]
type Candidate = tuple[float, int, bool]


def _write_raster(
    path: Path,
    values: npt.NDArray[np.float64],
    *,
    dtype: RasterDtype = "float32",
    mask: npt.NDArray[np.bool_] | None = None,
    tiled: bool = False,
    nodata: float | None = NODATA,
) -> None:
    options: dict[str, Any] = {"tiled": tiled, "blockysize": 32 if tiled else 1}
    if tiled:
        options["blockxsize"] = 32
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=values.shape[1],
        height=values.shape[0],
        count=1,
        dtype=dtype,
        nodata=nodata,
        transform=TRANSFORM,
        crs="EPSG:28350",
        compress="DEFLATE",
        predictor=2,
        num_threads="1",
        **options,
    ) as destination:
        destination.write(values.astype(dtype), 1)
        if mask is not None:
            destination.write_mask(np.asarray(mask, dtype=np.uint8) * 255)


def _read_raster(path: Path) -> npt.NDArray[np.generic]:
    with rasterio.open(path) as source:
        return source.read(1)


def _source_path(output: Path) -> Path:
    return output.with_name(f"{output.stem}_src.tif")


def _legend(output: Path) -> list[dict[str, str]]:
    with output.with_name(f"{output.stem}_src_legend.csv").open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


def _sample_ensemble() -> tuple[npt.NDArray[np.float64], npt.NDArray[np.bool_]]:
    # Every column is one independently checkable cell; dyadic input values keep
    # the scalar arithmetic exact while masks and selection rules vary.
    columns = [
        [NODATA, np.nan, np.inf, -np.inf, NODATA, np.nan, np.inf, -np.inf, NODATA, NODATA],
        [2, 4, 2, 4, 2, 4, 2, 4, 2, 4],
        [100, 1, 3, 7, 9, 1, 3, 7, 9, 1],
        [NODATA, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        [1, 3, 7, 7, 7, 7, 7, 9, 9, 9],
        [NODATA, np.nan, np.inf, -np.inf, 2, 4, 6, 8, 10, 12],
        [-1, 0, 1, 2, -2, 0.5, -0.5, 0.25, -0.25, 0],
        [1, 4, 10, 1, 4, 10, 1, 4, 10, 1],
        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        [8, 8, 8, 8, 8, 8, 8, 8, 8, 8],
        [NODATA, NODATA, NODATA, NODATA, NODATA, NODATA, NODATA, NODATA, 4, NODATA],
        [2, 4, 6, 8, 10, 12, 14, 16, 18, 20],
    ]
    values = np.asarray(columns, dtype=np.float64).T.reshape(10, 3, 4)
    masks = np.ones(values.shape, dtype=np.bool_)
    masks[0, 0, 2] = False
    masks[5, 1, 2] = False
    masks[:, 2, 3] = False
    return values, masks


def _select_cell(
    candidates: list[Candidate], stat_type: StatisticType, mean_method: MeanValueMethod, policy: NodataPolicy
) -> tuple[float, int]:
    if stat_type == "mean":
        mean = sum(item[0] for item in candidates) / len(candidates)
        if mean_method == "asc_to_asc":
            selected = min((item for item in candidates if item[0] >= mean), key=lambda item: item[0])
        else:
            selected = min(candidates, key=lambda item: (abs(item[0] - mean), -item[0]))
        return (selected[0] if mean_method == "closest_source" else mean), selected[1]
    if stat_type == "median":
        # The native implementation's stable rank preserves the caller order
        # within duplicate values, including when the middle lies inside a tie.
        selected = sorted(candidates, key=lambda item: item[0])[len(candidates) // 2]
        return selected[0], selected[1]
    selected = (max if stat_type == "max" else min)(candidates, key=lambda item: item[0])
    value, source_id, _ = selected
    if policy == "zero":
        source_id = next((item[1] for item in candidates if item[0] == value and item[2]), source_id)
    return value, source_id


def _reference(
    values: npt.NDArray[np.float64],
    masks: npt.NDArray[np.bool_],
    stat_type: StatisticType,
    mean_method: MeanValueMethod,
    policy: NodataPolicy,
) -> tuple[npt.NDArray[np.float64], npt.NDArray[np.uint8]]:
    result = np.full(values.shape[1:], NODATA, dtype=np.float64)
    source_ids = np.zeros(result.shape, dtype=np.uint8)
    for row, column in np.ndindex(result.shape):
        candidates: list[Candidate] = []
        valid_count = 0
        for source_id in range(1, values.shape[0] + 1):
            value = float(values[source_id - 1, row, column])
            valid = bool(masks[source_id - 1, row, column] and np.isfinite(value) and value != NODATA)
            valid_count += int(valid)
            if valid or policy == "zero":
                candidates.append((value if valid else 0.0, source_id, valid))
        if not valid_count or (policy == "require_all" and valid_count != values.shape[0]):
            continue
        result[row, column], source_ids[row, column] = _select_cell(candidates, stat_type, mean_method, policy)
    return result, source_ids


@pytest.mark.parametrize("dtype", ["float32", "float64"])
@pytest.mark.parametrize("policy", ["require_all", "zero", "exclude"])
@pytest.mark.parametrize(
    ("stat_type", "mean_method"),
    [
        ("mean", "asc_to_asc"),
        ("mean", "arithmetic"),
        ("mean", "closest_source"),
        ("min", "asc_to_asc"),
        ("max", "asc_to_asc"),
        ("median", "asc_to_asc"),
    ],
)
def test_source_enabled_statistics_match_independent_cell_contracts(
    tmp_path: Path, dtype: RasterDtype, policy: NodataPolicy, stat_type: StatisticType, mean_method: MeanValueMethod
) -> None:
    values, masks = _sample_ensemble()
    inputs = [tmp_path / f"tp_{index:02d}.tif" for index in range(values.shape[0])]
    for index, path in enumerate(inputs):
        _write_raster(path, values[index], dtype=dtype, mask=masks[index])
    expected_values, expected_ids = _reference(values, masks, stat_type, mean_method, policy)
    output = tmp_path / "statistic.tif"

    operations.compute_stat(
        stat_type,
        [str(path) for path in inputs],
        str(output),
        nodata_policy=policy,
        mean_value_method=mean_method,
        write_source=True,
        execution_options=RasterExecutionOptions(cache_mb=8, chunk_mb=1),
    )

    assert _read_raster(output).tobytes() == expected_values.astype(dtype).tobytes()
    np.testing.assert_array_equal(_read_raster(_source_path(output)), expected_ids)
    rows = _legend(output)
    assert [Path(row["source_file"]) for row in rows] == [path.absolute() for path in inputs]
    assert [int(row["cell_count"]) for row in rows] == [
        int(np.count_nonzero(expected_ids == index)) for index in range(1, len(inputs) + 1)
    ]
    for path in (output, _source_path(output)):
        with rasterio.open(path) as dataset:
            assert dataset.transform == TRANSFORM
            assert dataset.crs is not None
            assert dataset.crs.to_epsg() == 28350
            assert dataset.compression is not None
            assert dataset.compression.name == "deflate"
    with rasterio.open(output) as dataset:
        assert dataset.dtypes == (dtype,)
        np.testing.assert_array_equal(dataset.read_masks(1) != 0, expected_ids != 0)


@pytest.mark.parametrize("stat_type", ["min", "max"])
def test_zero_policy_valid_source_tie_preserves_existing_value_bits(tmp_path: Path, stat_type: StatisticType) -> None:
    inputs = [tmp_path / "invalid.tif", tmp_path / "valid.tif"]
    _write_raster(inputs[0], np.asarray([[NODATA, -0.0, 0.0]], dtype=np.float64), dtype="float64")
    _write_raster(inputs[1], np.asarray([[-0.0, 0.0, -0.0]], dtype=np.float64), dtype="float64")
    output = tmp_path / "extreme.tif"

    operations.compute_stat(
        stat_type, [str(path) for path in inputs], str(output), nodata_policy="zero", write_source=True
    )

    # A real -0 wins attribution over substituted +0, but the original selected
    # value stays +0. Equal real zeros preserve the first source and its sign.
    assert _read_raster(output).tobytes() == np.asarray([[0.0, -0.0, 0.0]], dtype=np.float64).tobytes()
    np.testing.assert_array_equal(_read_raster(_source_path(output)), [[2, 1, 1]])


@pytest.mark.parametrize(("width", "expected_mean"), [(1, 0.6), (2, 0.2)])
def test_float64_mean_retains_existing_numpy_reduction_order(tmp_path: Path, width: int, expected_mean: float) -> None:
    values = [1e16, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, -1e16, 1.0, 1.0]
    inputs = [tmp_path / f"tp_{index}.tif" for index in range(len(values))]
    for path, value in zip(inputs, values, strict=True):
        _write_raster(path, np.full((1, width), value), dtype="float64")
    output = tmp_path / "mean.tif"

    operations.compute_stat(
        "mean", [str(path) for path in inputs], str(output), mean_value_method="asc_to_asc", write_source=True
    )

    assert _read_raster(output).tobytes() == np.full((1, width), expected_mean, dtype=np.float64).tobytes()
    np.testing.assert_array_equal(_read_raster(_source_path(output)), np.full((1, width), 2, dtype=np.uint8))


def test_single_pixel_tile_edge_keeps_legacy_multicell_mean_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    values = [1e16, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, -1e16, 1.0, 1.0]
    inputs = [tmp_path / f"tp_{index}.tif" for index in range(len(values))]
    for path, value in zip(inputs, values, strict=True):
        _write_raster(path, np.full((33, 33), value), dtype="float64", tiled=True)

    def tile_windows(*_args: object, **_kwargs: object) -> Iterable[tuple[tuple[int, int], Window]]:
        for row, height in ((0, 32), (32, 1)):
            for column, width in ((0, 32), (32, 1)):
                yield (row, column), Window(column, row, width, height)

    monkeypatch.setattr(operations, "_chunk_windows", tile_windows)
    output = tmp_path / "mean.tif"
    operations.compute_stat(
        "mean", [str(path) for path in inputs], str(output), mean_value_method="asc_to_asc", write_source=True
    )

    # The original full-width reduction gives 0.2 everywhere. The new 1x1
    # bottom-right window must not switch that cell to NumPy's 0.6 pairwise sum.
    assert _read_raster(output).tobytes() == np.full((33, 33), 0.2, dtype=np.float64).tobytes()
    np.testing.assert_array_equal(_read_raster(_source_path(output)), np.full((33, 33), 2, dtype=np.uint8))


def test_closest_mean_keeps_last_equal_zero_value_and_first_source(tmp_path: Path) -> None:
    inputs = [tmp_path / "first.tif", tmp_path / "second.tif"]
    _write_raster(inputs[0], np.asarray([[-0.0, 0.0]]), dtype="float64")
    _write_raster(inputs[1], np.asarray([[0.0, -0.0]]), dtype="float64")
    output = tmp_path / "mean.tif"
    operations.compute_stat(
        "mean", [str(path) for path in inputs], str(output), mean_value_method="closest_source", write_source=True
    )

    assert _read_raster(output).tobytes() == np.asarray([[0.0, -0.0]], dtype=np.float64).tobytes()
    np.testing.assert_array_equal(_read_raster(_source_path(output)), [[1, 1]])


@dataclass(slots=True)
class _ReadObservations:
    values: list[tuple[Path, Window | None]] = field(default_factory=list[tuple[Path, Window | None]])
    masks: list[Path] = field(default_factory=list[Path])
    cache_bytes: list[int] = field(default_factory=list[int])


class _InputReader(Protocol):
    def read(self, band_index: int, *, window: object | None = None) -> npt.NDArray[np.generic]: ...

    def read_masks(self, band_index: int, *, window: object | None = None) -> npt.NDArray[np.uint8]: ...


class _ObservedDataset:
    def __init__(self, dataset: _InputReader, path: Path, observations: _ReadObservations) -> None:
        self.dataset = dataset
        self.path = path
        self.observations = observations

    def __getattr__(self, name: str) -> Any:
        return getattr(self.dataset, name)

    def read(self, band_index: int, *, window: object | None = None) -> npt.NDArray[np.generic]:
        assert window is None or isinstance(window, Window)
        self.observations.values.append((self.path, window))
        cache_bytes = get_gdal_config("GDAL_CACHEMAX")
        assert isinstance(cache_bytes, int)
        self.observations.cache_bytes.append(cache_bytes)
        return self.dataset.read(band_index, window=window)

    def read_masks(self, band_index: int, *, window: object | None = None) -> npt.NDArray[np.uint8]:
        self.observations.masks.append(self.path)
        return self.dataset.read_masks(band_index, window=window)


def _observe_reads(monkeypatch: pytest.MonkeyPatch) -> _ReadObservations:
    observations = _ReadObservations()
    original_open = getattr(operations, "_open_raster")  # noqa: B009 - private instrumentation boundary

    def observed_open(path: Path, mode: str = "r", profile: dict[str, object] | None = None) -> Any:
        dataset = original_open(path, mode, profile)
        if mode == "r":
            return _ObservedDataset(dataset, path, observations)
        return dataset

    monkeypatch.setattr(operations, "_open_raster", observed_open)
    return observations


def test_cache_units_and_mask_read_elision_preserve_independent_masks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    all_valid = tmp_path / "all_valid.tif"
    sentinel = tmp_path / "nodata_only.tif"
    explicit = tmp_path / "independent_mask.tif"
    _write_raster(all_valid, np.asarray([[2.0, 4.0, 6.0]]), nodata=None)
    _write_raster(sentinel, np.asarray([[4.0, NODATA, 8.0]]))
    _write_raster(explicit, np.asarray([[100.0, 10.0, 12.0]]), mask=np.asarray([[False, True, False]], dtype=np.bool_))
    observations = _observe_reads(monkeypatch)
    timings = RasterOperationTimings()
    output = tmp_path / "mean.tif"

    operations.compute_stat(
        "mean",
        [str(all_valid), str(sentinel), str(explicit)],
        str(output),
        nodata_policy="exclude",
        mean_value_method="asc_to_asc",
        write_source=True,
        execution_options=RasterExecutionOptions(cache_mb=8, chunk_mb=1),
        timings=timings,
    )

    assert observations.cache_bytes
    assert set(observations.cache_bytes) == {8 * 1024 * 1024}
    assert observations.masks == [explicit]
    np.testing.assert_array_equal(_read_raster(output), [[3.0, 7.0, 7.0]])
    np.testing.assert_array_equal(_read_raster(_source_path(output)), [[2, 3, 2]])
    assert (timings.width, timings.height, timings.input_count) == (3, 1, 3)
    assert timings.gdal_cache_bytes == 8 * 1024 * 1024
    assert timings.windows == 1
    assert timings.total_seconds > 0
    assert all(seconds >= 0 for seconds in timings.phase_seconds().values())


@pytest.mark.parametrize("tiled", [False, True])
def test_block_edge_windows_keep_values_and_sources_exact_with_small_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tiled: bool
) -> None:
    base_values, base_masks = _sample_ensemble()
    height, width = 131, 277
    values = np.tile(base_values, (1, 44, 70))[:, :height, :width]
    masks = np.tile(base_masks, (1, 44, 70))[:, :height, :width]
    inputs = [tmp_path / f"tp_{index}.tif" for index in range(len(values))]
    for index, path in enumerate(inputs):
        _write_raster(path, values[index], mask=masks[index], tiled=tiled)
    expected_values, expected_ids = _reference(base_values, base_masks, "mean", "asc_to_asc", "zero")
    expected_values = np.tile(expected_values, (44, 70))[:height, :width]
    expected_ids = np.tile(expected_ids, (44, 70))[:height, :width]
    observations = _observe_reads(monkeypatch)
    output = tmp_path / "mean.tif"
    timings = RasterOperationTimings()

    operations.compute_stat(
        "mean",
        [str(path) for path in inputs],
        str(output),
        nodata_policy="zero",
        mean_value_method="asc_to_asc",
        write_source=True,
        execution_options=RasterExecutionOptions(cache_mb=8, chunk_mb=1),
        timings=timings,
    )

    assert _read_raster(output).tobytes() == expected_values.astype("float32").tobytes()
    np.testing.assert_array_equal(_read_raster(_source_path(output)), expected_ids)
    first_windows = [window for path, window in observations.values if path == inputs[0]]
    assert len(first_windows) > 1
    coverage = np.zeros((height, width), dtype=np.uint8)
    for window in first_windows:
        assert window is not None
        row, column = int(window.row_off), int(window.col_off)
        rows, columns = int(window.height), int(window.width)
        assert row >= 0
        assert column >= 0
        assert row + rows <= height
        assert column + columns <= width
        if tiled:
            assert row % 32 == 0
            assert column % 32 == 0
        coverage[row : row + rows, column : column + columns] += 1
    np.testing.assert_array_equal(coverage, np.ones_like(coverage))
    assert timings.windows == len(first_windows)
    assert sum(int(row["cell_count"]) for row in _legend(output)) == int(np.count_nonzero(expected_ids))


@pytest.mark.parametrize("method", ["asc_to_asc", "arithmetic"])
def test_value_only_mean_avoids_source_selection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, method: MeanValueMethod
) -> None:
    inputs = [tmp_path / "first.tif", tmp_path / "second.tif"]
    _write_raster(inputs[0], np.asarray([[2.0, NODATA]]))
    _write_raster(inputs[1], np.asarray([[4.0, 8.0]]))

    def unexpected_source_selection(*_args: object, **_kwargs: object) -> None:
        pytest.fail("Value-only arithmetic means must not select a source index")

    monkeypatch.setattr(operations, "_mean_source_values", unexpected_source_selection)
    output = tmp_path / "mean.tif"
    timings = RasterOperationTimings()
    operations.compute_stat(
        "mean",
        [str(path) for path in inputs],
        str(output),
        nodata_policy="zero",
        mean_value_method=method,
        write_source=False,
        timings=timings,
    )

    np.testing.assert_array_equal(_read_raster(output), [[3.0, 4.0]])
    assert not _source_path(output).exists()
    assert not output.with_name("mean_src_legend.csv").exists()


@pytest.mark.parametrize("policy", ["zero", "exclude", "require_all"])
def test_fused_maximum_provenance_matches_two_pass_values_ids_and_counts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, policy: NodataPolicy
) -> None:
    groups: list[list[str]] = []
    mean_paths: list[str] = []
    for group_index, group_values in enumerate(
        (
            [[NODATA, 0, 5, 3, NODATA], [NODATA, NODATA, 9, 3, NODATA]],
            [[0, 0, 3, 1, NODATA], [0, 0, 3, 3, NODATA], [0, 0, 3, 5, NODATA]],
        ),
    ):
        group: list[str] = []
        for source_index, values in enumerate(group_values):
            path = tmp_path / f"duration_{group_index}_tp_{source_index}.tif"
            _write_raster(path, np.asarray([values], dtype=np.float64))
            group.append(str(path))
        mean = tmp_path / f"duration_{group_index}_mean.tif"
        operations.compute_stat(
            "mean", group, str(mean), nodata_policy=policy, mean_value_method="asc_to_asc", write_source=True
        )
        groups.append(group)
        mean_paths.append(str(mean))
    source_paths = [str(_source_path(Path(path))) for path in mean_paths]
    two_pass = tmp_path / "two_pass.tif"
    operations.compute_stat("max", mean_paths, str(two_pass), nodata_policy=policy, write_source=True)
    operations.flatten_nested_source_provenance(
        output_file=str(two_pass), nested_source_files=source_paths, original_input_groups=groups
    )
    expected_values = _read_raster(two_pass)
    expected_source = _read_raster(_source_path(two_pass))
    expected_legend = _legend(two_pass)

    def unexpected_flatten(**_kwargs: object) -> None:
        pytest.fail("The fused maximum must not rewrite an intermediate source raster")

    monkeypatch.setattr(operations, "flatten_nested_source_provenance", unexpected_flatten)
    fused = tmp_path / "fused.tif"
    operations.compute_stat(
        "max",
        mean_paths,
        str(fused),
        nodata_policy=policy,
        write_source=True,
        nested_source_files=source_paths,
        original_input_groups=groups,
        execution_options=RasterExecutionOptions(cache_mb=8, chunk_mb=1),
    )

    assert _read_raster(fused).tobytes() == expected_values.tobytes()
    np.testing.assert_array_equal(_read_raster(_source_path(fused)), expected_source)
    assert _legend(fused) == expected_legend


def test_fused_maximum_runner_does_not_invoke_legacy_flatten(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    mean = tmp_path / "duration_mean.tif"
    original = tmp_path / "tp01.tif"
    _write_raster(mean, np.asarray([[5.0, NODATA]]))
    _write_raster(_source_path(mean), np.asarray([[1.0, 0.0]]))
    _write_raster(original, np.asarray([[5.0, NODATA]]))

    def unexpected_flatten(**_kwargs: object) -> None:
        pytest.fail("The native maximum runner must use integrated provenance")

    from ryan_library.functions.tuflow import asc_to_asc_runner as runner

    monkeypatch.setattr(runner, "flatten_nested_source_provenance", unexpected_flatten, raising=False)
    output = tmp_path / "maximum.tif"
    result = run_python_raster_job(
        job=RasterOperationJob(
            label="maximum",
            operation="-statMax",
            input_files=(mean,),
            output_file=output,
            nodata_policy="zero",
            write_source=True,
            original_input_groups=((original,),),
        )
    )

    assert result == output
    np.testing.assert_array_equal(_read_raster(output), [[5.0, NODATA]])
    np.testing.assert_array_equal(_read_raster(_source_path(output)), [[1, 0]])
    assert _legend(output) == [{"source_id": "1", "source_file": str(original), "cell_count": "1"}]


def test_fused_maximum_invalid_local_ids_stay_zero_without_changing_values(tmp_path: Path) -> None:
    mean = tmp_path / "mean.tif"
    nested = _source_path(mean)
    originals = [tmp_path / "tp01.tif", tmp_path / "tp02.tif"]
    _write_raster(mean, np.full((1, 5), 10.0))
    _write_raster(nested, np.asarray([[1.0, 2.0, 3.0, 0.0, -1.0]]))
    for path in originals:
        _write_raster(path, np.full((1, 5), 10.0))
    output = tmp_path / "maximum.tif"

    operations.compute_stat(
        "max",
        [str(mean)],
        str(output),
        write_source=True,
        nested_source_files=[str(nested)],
        original_input_groups=[[str(path) for path in originals]],
    )

    np.testing.assert_array_equal(_read_raster(output), np.full((1, 5), 10.0))
    np.testing.assert_array_equal(_read_raster(_source_path(output)), [[1, 2, 0, 0, 0]])
    assert [int(row["cell_count"]) for row in _legend(output)] == [1, 1]


@pytest.mark.parametrize(
    ("case", "message"),
    [
        ("unpaired", "supplied together"),
        ("wrong_operation", "maximum statistic"),
        ("disabled_source", "source output enabled"),
        ("count_mismatch", "one nested source"),
        ("empty_group", "at least one original input"),
        ("misaligned", "dimensions do not match"),
    ],
)
def test_fused_provenance_rejects_invalid_contract_before_replacing_outputs(
    tmp_path: Path, case: str, message: str
) -> None:
    mean = tmp_path / "mean.tif"
    nested = _source_path(mean)
    original = tmp_path / "original.tif"
    _write_raster(mean, np.asarray([[10.0]]))
    _write_raster(original, np.asarray([[10.0]]))
    _write_raster(nested, np.ones((1, 2 if case == "misaligned" else 1)))
    arguments: dict[str, Any] = {
        "write_source": case != "disabled_source",
        "nested_source_files": [] if case == "count_mismatch" else [str(nested)],
        "original_input_groups": [[str(original)]],
    }
    if case == "unpaired":
        arguments["original_input_groups"] = None
    elif case == "empty_group":
        arguments["original_input_groups"] = [[]]
    output = tmp_path / "maximum.tif"
    old_outputs = [output, _source_path(output), output.with_name("maximum_src_legend.csv")]
    for path in old_outputs:
        path.write_bytes(b"previous completed output")

    with pytest.raises(ValueError, match=message):
        operations.compute_stat("mean" if case == "wrong_operation" else "max", [str(mean)], str(output), **arguments)

    assert all(path.read_bytes() == b"previous completed output" for path in old_outputs)
    assert not list(tmp_path.glob(".*.tmp*"))


@pytest.mark.parametrize("operation", ["max", "diff", "flatten"])
def test_other_raster_operations_apply_cache_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    first = tmp_path / "first.tif"
    second = tmp_path / "second.tif"
    _write_raster(first, np.asarray([[4.0, 2.0]]))
    _write_raster(second, np.asarray([[2.0, 1.0]]))
    observations = _observe_reads(monkeypatch)
    options = RasterExecutionOptions(cache_mb=8, chunk_mb=1)
    output = tmp_path / "output.tif"
    if operation == "max":
        operations.compute_max([str(first), str(second)], str(output), execution_options=options)
        np.testing.assert_array_equal(_read_raster(output), [[4.0, 2.0]])
    elif operation == "diff":
        operations.compute_diff(str(first), str(second), str(output), nowetdry=True, execution_options=options)
        np.testing.assert_array_equal(_read_raster(output), [[2.0, 1.0]])
    else:
        _write_raster(_source_path(output), np.asarray([[1.0, 1.0]]))
        operations.flatten_nested_source_provenance(
            output_file=str(output),
            nested_source_files=[str(second)],
            original_input_groups=[[str(first), str(second)]],
            execution_options=options,
        )
        np.testing.assert_array_equal(_read_raster(_source_path(output)), [[2, 1]])

    assert observations.cache_bytes
    assert set(observations.cache_bytes) == {8 * 1024 * 1024}
