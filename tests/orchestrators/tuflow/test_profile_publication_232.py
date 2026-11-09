"""Filesystem publication regressions for TUFLOW water-level profiles (#232)."""

# Rasterio and GeoPandas expose incomplete third-party typing.
# pyright: reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportPrivateUsage=false

import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from typing import Any

import geopandas as gpd
import matplotlib as mpl
import numpy as np
import pytest
import rasterio
from matplotlib.figure import Figure
from rasterio.transform import from_origin
from shapely.geometry import LineString

mpl.use("Agg")

from ryan_library.orchestrators.tuflow import water_level_profiles as profiles
from ryan_library.orchestrators.tuflow.water_level_profiles import WaterLevelProfileConfig


def _profile_case(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    names: tuple[str, ...] = ("A",),
    overwrite_existing: bool = False,
) -> WaterLevelProfileConfig:
    lines_path = tmp_path / "profiles.gpkg"
    gpd.GeoDataFrame(
        {"Code": list(names)},
        geometry=[LineString(((0.5, 3.5 - n), (3.5, 3.5 - n))) for n in range(len(names))],
        crs="EPSG:28351",
    ).to_file(lines_path, layer="profiles")
    terrain = tmp_path / "terrain.tif"
    water = tmp_path / "results" / "Model_01.00p_h_HR_Max.tif"
    water.parent.mkdir()
    for path, value in ((terrain, 1.0), (water, 2.0)):
        with rasterio.open(
            path,
            "w",
            driver="GTiff",
            width=4,
            height=4,
            count=1,
            dtype="float64",
            crs="EPSG:28351",
            transform=from_origin(0.0, 4.0, 1.0, 1.0),
            nodata=-999.0,
        ) as output:
            output.write(np.full((4, 4), value, dtype=np.float64), 1)

    def fake_sampling(*_args: Any, **_kwargs: Any) -> tuple[Any, Any]:
        return np.array([0.0, 1.0], dtype=np.float64), np.array([1.0, 2.0], dtype=np.float64)

    monkeypatch.setattr(profiles, "sample_raster_along_line", fake_sampling)
    return WaterLevelProfileConfig(
        lines_gpkg=lines_path,
        lines_layer_name="profiles",
        name_field="Code",
        terrain_raster=terrain,
        tuflow_results_dir=water.parent,
        output_dir=tmp_path / "plots",
        target_aeps=("01.00p",),
        overwrite_existing=overwrite_existing,
    )


def _assert_no_staging_files(directory: Path) -> None:
    assert not tuple(directory.glob(".*.staged.png"))


def test_competing_publishers_only_one_wins(tmp_path: Path) -> None:
    destination = tmp_path / "A_profile.png"
    stages = (tmp_path / ".owned-one.png", tmp_path / ".owned-two.png")
    for stage, value in zip(stages, (b"first", b"second"), strict=True):
        stage.write_bytes(value)

    barrier = Barrier(2)

    def attempt(stage: Path) -> str:
        barrier.wait(timeout=10)
        try:
            profiles._publish_profile_png(stage, destination, overwrite_existing=False)
        except FileExistsError:
            return "EXISTS"
        return "PUBLISHED"

    with ThreadPoolExecutor(max_workers=2) as workers:
        results = list(workers.map(attempt, stages))

    assert sorted(results) == ["EXISTS", "PUBLISHED"]
    assert destination.read_bytes() in (b"first", b"second")
    # Windows rename consumes the winning stage; POSIX hard-link creation does not.
    # The failed caller's staged file remains under its own ownership.
    assert sum(stage.exists() for stage in stages) == (1 if os.name == "nt" else 2)


@pytest.mark.skipif(os.name != "nt", reason="Windows SMB rename status handling")
@pytest.mark.parametrize("competing_destination", [False, True])
def test_smb_access_denied_keeps_permission_errors_distinct(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, competing_destination: bool
) -> None:
    stage, destination = tmp_path / "owned.png", tmp_path / "output.png"
    stage.write_bytes(b"ours")

    def denied_rename(self: Path, target: Path) -> Path:
        del self
        if competing_destination:
            target.write_bytes(b"competitor")
        raise PermissionError(5, "SMB access denied")

    monkeypatch.setattr(Path, "rename", denied_rename)
    expected = FileExistsError if competing_destination else PermissionError
    with pytest.raises(expected):
        profiles._publish_profile_png(stage, destination, overwrite_existing=False)
    assert stage.read_bytes() == b"ours"
    if competing_destination:
        assert destination.read_bytes() == b"competitor"


def test_destination_created_after_preflight_is_preserved(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _profile_case(tmp_path, monkeypatch)
    original = profiles._publish_profile_png

    def competing_publish(stage: Path, destination: Path, *, overwrite_existing: bool) -> None:
        destination.write_bytes(b"concurrent output")
        original(stage, destination, overwrite_existing=overwrite_existing)

    monkeypatch.setattr(profiles, "_publish_profile_png", competing_publish)
    with pytest.raises(FileExistsError, match="already exists"):
        profiles.run_water_level_profile_workflow(config)

    assert (config.output_dir / "A_profile.png").read_bytes() == b"concurrent output"
    _assert_no_staging_files(config.output_dir)


@pytest.mark.parametrize("overwrite_existing", [False, True])
def test_case_alias_is_not_a_second_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, overwrite_existing: bool
) -> None:
    config = _profile_case(tmp_path, monkeypatch, overwrite_existing=overwrite_existing)
    config.output_dir.mkdir()
    alias = config.output_dir / "a_PROFILE.PNG"
    alias.write_bytes(b"untouched")

    with pytest.raises(FileExistsError, match="already exists"):
        profiles.run_water_level_profile_workflow(config)

    assert alias.read_bytes() == b"untouched"
    assert len(list(config.output_dir.iterdir())) == 1


def test_render_failure_preserves_original_png(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _profile_case(tmp_path, monkeypatch, overwrite_existing=True)
    config.output_dir.mkdir()
    destination = config.output_dir / "A_profile.png"
    destination.write_bytes(b"original")

    def fail_render(_self: Figure, file_object: Any, *_args: Any, **_kwargs: Any) -> None:
        file_object.write(b"partial")
        msg = "render failed"
        raise OSError(msg)

    monkeypatch.setattr(Figure, "savefig", fail_render)
    with pytest.raises(OSError, match="render failed"):
        profiles.run_water_level_profile_workflow(config)

    assert destination.read_bytes() == b"original"
    _assert_no_staging_files(config.output_dir)


def test_failed_publication_cleans_only_owned_staging(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _profile_case(tmp_path, monkeypatch, overwrite_existing=True)
    config.output_dir.mkdir()
    destination = config.output_dir / "A_profile.png"
    destination.write_bytes(b"original")
    foreign = config.output_dir / ".A_profile.foreign.staged.png"
    foreign.write_bytes(b"other invocation")

    def fail_publish(_stage: Path, _destination: Path, *, overwrite_existing: bool) -> None:
        assert overwrite_existing
        msg = "publish failed"
        raise OSError(msg)

    monkeypatch.setattr(profiles, "_publish_profile_png", fail_publish)
    with pytest.raises(OSError, match="publish failed"):
        profiles.run_water_level_profile_workflow(config)

    assert destination.read_bytes() == b"original"
    assert foreign.read_bytes() == b"other invocation"
    assert list(config.output_dir.glob(".*.staged.png")) == [foreign]


def test_explicit_overwrite_replaces_only_complete_render(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _profile_case(tmp_path, monkeypatch, overwrite_existing=True)
    config.output_dir.mkdir()
    destination = config.output_dir / "A_profile.png"
    destination.write_bytes(b"original")

    assert profiles.run_water_level_profile_workflow(config) == (destination,)
    assert destination.read_bytes().startswith(b"\x89PNG\r\n\x1a\n")
    _assert_no_staging_files(config.output_dir)


def test_later_failure_preserves_earlier_profile(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _profile_case(tmp_path, monkeypatch, names=("A", "B"))
    original = profiles._publish_profile_png

    def fail_second(stage: Path, destination: Path, *, overwrite_existing: bool) -> None:
        if destination.name == "B_profile.png":
            msg = "second profile failed"
            raise OSError(msg)
        original(stage, destination, overwrite_existing=overwrite_existing)

    monkeypatch.setattr(profiles, "_publish_profile_png", fail_second)
    with pytest.raises(OSError, match="second profile failed"):
        profiles.run_water_level_profile_workflow(config)

    assert (config.output_dir / "A_profile.png").is_file()
    assert not (config.output_dir / "B_profile.png").exists()
    _assert_no_staging_files(config.output_dir)
