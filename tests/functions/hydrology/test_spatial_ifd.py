"""Synthetic spatial IFD regression tests; no BoM equivalence claim."""

import json
from decimal import Decimal
from pathlib import Path

import numpy as np
import pytest

from ryan_library.functions.hydrology.spatial_ifd import (
    GridSpec,
    IFDSource,
    Point,
    PointCache,
    collect_grid,
    parse_ifd,
    write_ascii_grid,
)

# Artificial source keys, not asserted to be the production API's keys.
SOURCE = IFDSource(("AllIFDDatasets", "Synthetic unadjusted IFD"), "test-v1")
GRID = GridSpec(Decimal("-23.000"), Decimal("119.000"), 2, 2)


def response(point: Point) -> bytes:
    depth = 100 + float((point.longitude - Decimal("119")) * 400) - float((point.latitude + 23) * 800)
    table = {"index": [60, 120], "columns": [1, 10], "data": [[depth, depth / 2], [depth * 2, depth]]}
    return json.dumps(
        {
            "input_data": {"Latitude": float(point.latitude), "Longitude": float(point.longitude)},
            "layers": {"AllIFDDatasets": {"Synthetic unadjusted IFD": table}},
        }
    ).encode()


def test_resume_overlap_and_offline_events(tmp_path: Path) -> None:
    path = tmp_path / "ifd.sqlite3"
    seen: list[Point] = []

    def interrupted(point: Point) -> bytes:
        seen.append(point)
        if len(seen) == 3:
            msg = "interrupted"
            raise OSError(msg)
        return response(point)

    with pytest.raises(OSError, match="interrupted"):
        collect_grid(GRID, SOURCE, PointCache(path), 60, 1, max_new_points=4, fetcher=interrupted)

    seen.clear()

    def fetch(point: Point) -> bytes:
        seen.append(point)
        return response(point)

    result = collect_grid(GRID, SOURCE, PointCache(path), 60, 1, max_new_points=2, fetcher=fetch)
    assert (result.cached_cells, result.fetched_cells) == (2, 2)
    assert len(seen) == 2
    np.testing.assert_allclose(result.depths_mm, [[100, 110], [120, 130]])
    np.testing.assert_allclose(result.multipliers(100), [[1, 1.1], [1.2, 1.3]])

    seen.clear()
    offline = collect_grid(GRID, SOURCE, PointCache(path), 120, 10, fetcher=fetch)
    np.testing.assert_array_equal(offline.depths_mm, result.depths_mm)
    assert offline.cached_cells == 4
    assert not seen

    overlap = GridSpec(Decimal("-23"), Decimal("119.025"), 2, 1)
    subset = collect_grid(overlap, SOURCE, PointCache(path), 60, 1, fetcher=fetch)
    np.testing.assert_array_equal(subset.depths_mm, [[110], [130]])
    assert not seen


def test_budget_preflight_and_new_snapshot(tmp_path: Path) -> None:
    cache = PointCache(tmp_path / "ifd.sqlite3")
    seen: list[Point] = []

    def fetch(point: Point) -> bytes:
        seen.append(point)
        return response(point)

    with pytest.raises(ValueError, match="budget"):
        collect_grid(GRID, SOURCE, cache, 60, 1, max_new_points=3, fetcher=fetch)
    assert not seen
    collect_grid(GRID, SOURCE, cache, 60, 1, max_new_points=4, fetcher=fetch)
    new_source = IFDSource(SOURCE.table_path, "test-v2")
    with pytest.raises(ValueError, match="budget"):
        collect_grid(GRID, new_source, cache, 60, 1, fetcher=fetch)
    assert len(seen) == 4


def test_invalid_responses_never_admitted(tmp_path: Path) -> None:
    cache = PointCache(tmp_path / "ifd.sqlite3")
    point = Point(Decimal("-23"), Decimal("119"))
    other = Point(Decimal("-24"), Decimal("119"))
    for raw in (b"<html>error</html>", b"{}", response(other)):
        with pytest.raises(ValueError):
            cache.store(point, SOURCE, raw)
        assert cache.get(point, SOURCE) is None
    wrong = IFDSource(("AllIFDDatasets", "Other dataset"), SOURCE.snapshot)
    with pytest.raises(ValueError, match="Missing exact"):
        cache.store(point, wrong, response(point))
    assert cache.get(point, SOURCE) is None


def test_exact_events_and_immutable_snapshot(tmp_path: Path) -> None:
    cache = PointCache(tmp_path / "ifd.sqlite3")
    point = Point(Decimal("-23.000"), Decimal("119.000"))
    raw = response(point)
    table = parse_ifd(raw, point, SOURCE)
    with pytest.raises(ValueError, match="not tabulated"):
        table.depth(90, 1)
    with pytest.raises(ValueError, match="not tabulated"):
        table.depth(60, 0.01)
    cache.store(point, SOURCE, raw)
    replacement = raw.replace(b"100.0", b"101.0")
    assert cache.store(point, SOURCE, replacement).depth(60, 1) == 100
    assert cache.get(Point(Decimal("-23"), Decimal("119")), SOURCE) == raw


def test_ascii_orientation_and_no_overwrite(tmp_path: Path) -> None:
    rasterio = pytest.importorskip("rasterio")
    path = tmp_path / "depth.asc"
    values = np.array([[100.0, 110.0], [120.0, 130.0]])
    write_ascii_grid(path, GRID, values)
    original = path.read_bytes()
    with rasterio.open(path) as dataset:
        np.testing.assert_array_equal(dataset.read(1), values)
        assert dataset.xy(0, 0) == pytest.approx((119, -23))
        assert dataset.xy(1, 1) == pytest.approx((119.025, -23.025))
        assert dataset.res == pytest.approx((0.025, 0.025))
    with pytest.raises(FileExistsError):
        write_ascii_grid(path, GRID, values)
    assert path.read_bytes() == original


def test_reject_recommended_and_invalid_grid() -> None:
    with pytest.raises(ValueError, match="Recommended"):
        IFDSource(("RecIFD", "Default Historical (1961-1990) Baseline"), "test")
    with pytest.raises(ValueError, match="cells"):
        GridSpec(Decimal("-23"), Decimal("119"), 101, 100)
