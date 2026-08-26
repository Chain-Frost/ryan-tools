"""Optional cell-exact parity tests against a user-supplied ASC_to_ASC build."""

from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Protocol, cast

import numpy as np
import numpy.typing as npt
import pytest
import rasterio  # pyright: ignore[reportMissingTypeStubs]
from rasterio.transform import from_origin  # pyright: ignore[reportMissingTypeStubs, reportUnknownVariableType]

from ryan_library.functions.tuflow.asc_to_asc_raster_operations import MeanValueMethod
from ryan_library.functions.tuflow.asc_to_asc_runner import (
    RasterOperationJob,
    run_asc_to_asc_job,
    run_python_raster_job,
)

NODATA: float = -9999.0


class _RasterWriter(Protocol):
    def write(self, data: npt.NDArray[np.float32], indexes: int) -> None: ...

    def close(self) -> None: ...


class _RasterReader(Protocol):
    transform: object
    crs: object | None
    nodata: float | int | None

    def read(self, indexes: int) -> npt.NDArray[np.float32]: ...

    def read_masks(self, indexes: int) -> npt.NDArray[np.uint8]: ...

    def close(self) -> None: ...


@pytest.fixture(scope="module")
def asc_to_asc_executable() -> Path:
    """Load the executable selected explicitly for parity testing."""
    configured: str | None = os.environ.get("ASC_TO_ASC_EXE")
    if configured is None:
        pytest.skip("Set ASC_TO_ASC_EXE to run executable parity tests")
    executable: Path = Path(configured).expanduser().resolve()
    if not executable.is_file():
        pytest.fail(f"ASC_TO_ASC_EXE does not identify a file: {executable}")
    return executable


@pytest.fixture()
def parity_inputs(tmp_path: Path) -> tuple[Path, ...]:
    """Create clean, aligned, all-valid inputs with deterministic selections."""
    input_directory: Path = tmp_path / "inputs"
    input_directory.mkdir()
    arrays = (
        np.asarray([[0.0, 3.0, 9.0, 2.0], [1.0, 8.0, 4.0, 12.0]], dtype=np.float32),
        np.asarray([[4.0, 3.0, 7.0, 6.0], [5.0, 2.0, 10.0, 8.0]], dtype=np.float32),
        np.asarray([[11.0, 9.0, 3.0, 10.0], [9.0, 6.0, 2.0, 4.0]], dtype=np.float32),
    )
    paths: list[Path] = []
    for index, array in enumerate(arrays, start=1):
        path: Path = input_directory / f"source_{index}.tif"
        destination = cast(
            _RasterWriter,
            rasterio.open(  # pyright: ignore[reportUnknownMemberType]
                fp=path,
                mode="w",
                driver="GTiff",
                width=array.shape[1],
                height=array.shape[0],
                count=1,
                dtype="float32",
                crs="EPSG:7850",
                transform=from_origin(400_000.0, 6_500_000.0, 2.0, 2.0),
                nodata=NODATA,
                compress="DEFLATE",
                predictor=2,
            ),
        )
        try:
            destination.write(array, 1)
        finally:
            destination.close()
        paths.append(path)
    return tuple(paths)


def _read_raster_contract(path: Path) -> tuple[np.ndarray, np.ndarray, object, object, float | int | None]:
    source = cast(_RasterReader, rasterio.open(path))  # pyright: ignore[reportUnknownMemberType]
    try:
        values = source.read(1)
        mask = source.read_masks(1)
        return values, mask, source.transform, source.crs, source.nodata
    finally:
        source.close()


def _find_external_source_raster(output_directory: Path) -> Path:
    candidates: list[Path] = [
        path
        for path in output_directory.glob("*.tif")
        if "src" in path.stem.casefold() and not path.stem.casefold().startswith("native")
    ]
    assert len(candidates) == 1, f"Expected one ASC_to_ASC source raster; found {candidates}"
    return candidates[0]


def _assert_external_legend_lists_inputs(output_directory: Path, input_files: tuple[Path, ...]) -> None:
    candidates: list[Path] = [
        path for path in output_directory.glob("*.csv") if not path.stem.casefold().startswith("native")
    ]
    assert len(candidates) == 1, f"Expected one ASC_to_ASC source legend; found {candidates}"
    legend_text: str = candidates[0].read_text(encoding="utf-8", errors="replace")
    for input_file in input_files:
        assert input_file.name in legend_text


@pytest.mark.slow
@pytest.mark.parametrize(
    ("operation", "mean_value_method"),
    [
        ("-statMean", "asc_to_asc"),
        ("-statMedian", "closest_source"),
        ("-statMin", "closest_source"),
        ("-statMax", "closest_source"),
    ],
)
def test_native_statistics_match_asc_to_asc_exactly(
    tmp_path: Path,
    asc_to_asc_executable: Path,
    parity_inputs: tuple[Path, ...],
    operation: str,
    mean_value_method: MeanValueMethod,
) -> None:
    """Generate both result sets and compare values, masks, alignment and source IDs exactly."""
    output_directory: Path = tmp_path / operation.removeprefix("-").casefold()
    output_directory.mkdir()
    native_output: Path = output_directory / "native.tif"
    external_output: Path = output_directory / "external.tif"
    native_job = RasterOperationJob(
        label=f"native {operation}",
        operation=operation,
        input_files=parity_inputs,
        output_file=native_output,
        nodata_policy="require_all",
        mean_value_method=mean_value_method,
        write_source=True,
    )

    run_python_raster_job(job=native_job)
    run_asc_to_asc_job(executable=asc_to_asc_executable, job=native_job, output_file=external_output)

    native_contract = _read_raster_contract(native_output)
    external_contract = _read_raster_contract(external_output)
    np.testing.assert_array_equal(native_contract[0], external_contract[0])
    np.testing.assert_array_equal(native_contract[1], external_contract[1])
    assert native_contract[2:] == external_contract[2:]

    external_source: Path = _find_external_source_raster(output_directory)
    np.testing.assert_array_equal(
        _read_raster_contract(native_output.with_name("native_src.tif"))[0],
        _read_raster_contract(external_source)[0],
    )
    _assert_external_legend_lists_inputs(output_directory, parity_inputs)

    with native_output.with_name("native_src_legend.csv").open(encoding="utf-8", newline="") as stream:
        native_legend: list[list[str]] = list(csv.reader(stream))
    assert [Path(row[1]).name for row in native_legend[1:]] == [path.name for path in parity_inputs]
