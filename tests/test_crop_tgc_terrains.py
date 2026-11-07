from __future__ import annotations

# Rasterio does not currently expose complete read-dataset type information.
# pyright: reportUnknownArgumentType=false, reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false, reportAttributeAccessIssue=false
# pyright: reportMissingTypeStubs=false
import importlib.util
import sys
from pathlib import Path
from typing import Any

import fiona
import numpy as np
import pytest
import rasterio
from affine import Affine
from numpy.typing import NDArray
from rasterio.transform import from_origin

SCRIPT = Path(__file__).parents[1] / "ryan-scripts" / "TUFLOW-python" / "crop_tgc_terrains.py"
SPEC = importlib.util.spec_from_file_location("crop_tgc_terrains", SCRIPT)
assert SPEC is not None
assert SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def _write_raster(
    path: Path,
    data: NDArray[np.floating[Any]],
    *,
    transform: Affine | None = None,
) -> None:
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=data.shape[1],
        height=data.shape[0],
        count=1,
        dtype="float32",
        crs="EPSG:28350",
        transform=transform if transform is not None else from_origin(0, 100, 10, 10),
        nodata=-9999.0,
    ) as dataset:
        dataset.write(data.astype("float32"), 1)


def test_pytuflow_selects_one_scenario_without_constructing_full_tgc(
    tmp_path: Path,
) -> None:
    base = tmp_path / "base.tif"
    exg = tmp_path / "exg.tif"
    dev = tmp_path / "dev.tif"
    for path in (base, exg, dev):
        _write_raster(path, np.ones((2, 2)))

    tgc = tmp_path / "geometry.tgc"
    tgc.write_text(
        "Read Grid Zpts == base.tif\n"
        "If Scenario == EXG\n"
        "  Read Grid Zpts == exg.tif\n"
        "Else If Scenario == DEV\n"
        "  Read Grid Zpts == dev.tif\n"
        "End If\n"
        "If Scenario == EXTENT_A\n"
        "  Pause == unrelated extent branch\n"
        "Else\n"
        "  Pause == unrelated extent fallback\n"
        "End If\n",
        encoding="utf-8",
    )

    selected = MODULE.resolve_terrain_layers(tgc, "EXG")

    assert [item.source.name for item in selected] == ["base.tif", "exg.tif"]
    assert [item.line_number for item in selected] == [1, 3]


def test_retained_coverage_includes_requested_seam_buffer() -> None:
    lower = np.ones((15, 15), dtype=bool)
    higher = np.zeros((15, 15), dtype=bool)
    higher[4:11, 4:11] = True

    allocated = MODULE.allocate_retained_coverage((lower, higher), buffer_cells=2)
    retained_lower, lower_responsibility = allocated[0]
    retained_higher, higher_responsibility = allocated[1]

    assert lower_responsibility == lower.size - np.count_nonzero(higher)
    assert higher_responsibility == np.count_nonzero(higher)
    assert retained_higher[7, 7]
    assert retained_lower[4, 7]  # two-cell seam allowance beneath the higher layer
    assert not retained_lower[7, 7]  # deep redundant overlap is removed


def test_write_creates_new_rasters_manifest_and_patched_tgc(tmp_path: Path) -> None:
    lower = np.full((10, 10), 10.0, dtype="float32")
    higher = np.full((10, 10), -9999.0, dtype="float32")
    higher[:, 5:] = 20.0
    _write_raster(tmp_path / "lower.tif", lower)
    _write_raster(tmp_path / "higher.tif", higher)
    tgc = tmp_path / "geometry.tgc"
    tgc.write_text(
        "Read Grid Zpts == lower.tif\nRead Grid Zpts == higher.tif\n",
        encoding="utf-8",
    )

    output = tmp_path / "cropped"
    payload = MODULE.run(
        tgc_path=tgc,
        scenario="EXG",
        bounds=MODULE.AnalysisBounds(0, 0, 100, 100),
        crs="EPSG:28350",
        seam_buffer=20,
        planning_resolution=10,
        output_directory=output,
        write=True,
    )

    outputs = [Path(item["output"]) for item in payload["layers"]]
    assert all(path.is_file() for path in outputs)
    assert (output / "terrain_crop_manifest.json").is_file()
    qa = output / "terrain_coverage_qa.gpkg"
    assert qa.is_file()
    assert payload["coverage_qa"]["gpkg"] == str(qa)
    assert payload["coverage_qa"]["original_uncovered_planning_cells"] == 0
    assert payload["coverage_qa"]["final_uncovered_planning_cells"] == 0
    assert payload["coverage_qa"]["lost_original_planning_cells"] == 0
    assert set(fiona.listlayers(qa)) == {
        "original_raster_footprints",
        "original_valid_coverage",
        "clipped_out",
        "minimum_coverage_extent",
        "final_clipped_extent",
        "coverage_gaps",
        "lost_valid_coverage",
    }
    with fiona.open(qa, layer="original_raster_footprints") as layer:
        assert len(layer) == 2
    with fiona.open(qa, layer="original_valid_coverage") as layer:
        assert len(layer) == 2
    with fiona.open(qa, layer="clipped_out") as layer:
        removed = list(layer)
        assert removed
        assert {record["properties"]["layer_order"] for record in removed} == {1}
    with fiona.open(qa, layer="final_clipped_extent") as layer:
        assert len(layer) == 2
    with fiona.open(qa, layer="minimum_coverage_extent") as layer:
        assert len(layer) == 1
        assert next(iter(layer))["properties"]["area_m2"] == pytest.approx(10000.0)
    with fiona.open(qa, layer="coverage_gaps") as layer:
        assert len(layer) == 0
    with fiona.open(qa, layer="lost_valid_coverage") as layer:
        assert len(layer) == 0
    patched = Path(payload["patched_tgc"])
    assert patched.is_file()
    patched_text = patched.read_text(encoding="utf-8")
    assert "cropped\\001_lower_cropped.tif" in patched_text
    assert "cropped\\002_higher_cropped.tif" in patched_text

    with rasterio.open(outputs[0], mode="r") as opened:
        data = opened.read(1)
        assert np.any(data == 10.0)
        assert opened.width == 7
        assert np.all(data == 10.0)
        assert opened.compression is not None
        assert opened.compression.value.upper() == "DEFLATE"
        assert opened.tags(ns="IMAGE_STRUCTURE")["PREDICTOR"] == "2"
        assert opened.block_shapes[0][1] == opened.width

    assert payload["output_raster_profile"] == {
        "name": "tuflow",
        "creation_options": ["COMPRESS=DEFLATE", "PREDICTOR=2", "BIGTIFF=IF_SAFER", "NUM_THREADS=ALL_CPUS"],
    }

    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        MODULE.run(
            tgc_path=tgc,
            scenario="EXG",
            bounds=MODULE.AnalysisBounds(0, 0, 100, 100),
            crs="EPSG:28350",
            seam_buffer=20,
            planning_resolution=10,
            output_directory=output,
            write=True,
        )
