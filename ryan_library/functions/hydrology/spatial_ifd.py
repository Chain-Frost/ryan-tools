"""Resumable point-sampled IFD grids; source equivalence is not assumed."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from collections.abc import Callable, Iterator, Sequence
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from email.utils import parsedate_to_datetime
from math import isfinite
from pathlib import Path
from typing import cast
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

import numpy as np
import numpy.typing as npt

ARR_ENDPOINT = "https://data.arr-software.org/"
MAX_GRID_CELLS = 10_000
_MAX_RESPONSE_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class Point:
    """Exact decimal-degree coordinate, without implicit rounding or snapping."""

    latitude: Decimal
    longitude: Decimal

    def __post_init__(self) -> None:
        if not self.latitude.is_finite() or not self.longitude.is_finite():
            msg = "Coordinates must be finite"
            raise ValueError(msg)
        if not -90 <= self.latitude <= 90 or not -180 <= self.longitude <= 180:
            msg = "Coordinates are outside geographic bounds"
            raise ValueError(msg)

    def params(self) -> dict[str, str]:
        """Request all IFD datasets, not the jurisdiction-default BoMIFD option."""
        return {
            "lat_coord": format(self.latitude.normalize(), "f"),
            "lon_coord": format(self.longitude.normalize(), "f"),
            "type": "json",
            "AllIFDDatasets": "1",
        }


@dataclass(frozen=True, slots=True)
class GridSpec:
    """North-west cell centre, explicit dimensions and angular spacing (WGS84)."""

    north_lat: Decimal
    west_lon: Decimal
    rows: int
    cols: int
    spacing: Decimal = Decimal("0.025")

    def __post_init__(self) -> None:
        if self.rows <= 0 or self.cols <= 0 or self.rows * self.cols > MAX_GRID_CELLS:
            msg = f"Grid must contain 1-{MAX_GRID_CELLS} cells"
            raise ValueError(msg)
        if not self.spacing.is_finite() or self.spacing <= 0:
            msg = "Grid spacing must be finite and positive"
            raise ValueError(msg)
        Point(self.north_lat, self.west_lon)
        half = self.spacing / 2
        Point(self.north_lat + half, self.west_lon - half)
        Point(
            self.north_lat - (self.rows - 1) * self.spacing - half,
            self.west_lon + (self.cols - 1) * self.spacing + half,
        )

    def points(self) -> Iterator[tuple[int, int, Point]]:
        """Yield cells in north-to-south, west-to-east raster order."""
        for row in range(self.rows):
            for col in range(self.cols):
                yield row, col, Point(self.north_lat - row * self.spacing, self.west_lon + col * self.spacing)

    @property
    def xllcorner(self) -> Decimal:
        return self.west_lon - self.spacing / 2

    @property
    def yllcorner(self) -> Decimal:
        return self.north_lat - (self.rows - 1) * self.spacing - self.spacing / 2


@dataclass(frozen=True, slots=True)
class IFDSource:
    """Exact table path under JSON layers and operator-defined cache snapshot.

    The snapshot is a local namespace, NOT a claim about the server's data version.
    Use a new namespace for an intentional refresh; never replace old responses.
    """

    table_path: tuple[str, ...]
    snapshot: str

    def __post_init__(self) -> None:
        if len(self.table_path) < 2 or any(not key.strip() for key in self.table_path):
            msg = "Specify the exact layer and IFD table path"
            raise ValueError(msg)
        if self.table_path[0] == "RecIFD":
            msg = "Recommended IFDs are not a substitute for an explicitly selected source dataset"
            raise ValueError(msg)
        if not self.snapshot.strip():
            msg = "A nonempty cache snapshot is required"
            raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class IFDTable:
    durations_min: tuple[int, ...]
    aeps_percent: tuple[float, ...]
    depths_mm: tuple[tuple[float, ...], ...]

    def depth(self, duration_min: int, aep_percent: float) -> float:
        """Exact tabulated event only; no interpolation or extrapolation."""
        try:
            row = self.durations_min.index(duration_min)
            col = self.aeps_percent.index(aep_percent)
        except ValueError as exc:
            msg = f"Requested duration {duration_min} min / AEP {aep_percent}% is not tabulated"
            raise ValueError(msg) from exc
        return self.depths_mm[row][col]


def _mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        msg = "Expected a JSON object"
        raise ValueError(msg)
    return cast("dict[str, object]", value)


def _array(value: object) -> list[object]:
    if not isinstance(value, list):
        msg = "Expected a JSON array"
        raise ValueError(msg)
    return cast("list[object]", value)


def _number(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        msg = "Expected a numeric JSON value"
        raise ValueError(msg)
    result = float(value)
    if not isfinite(result):
        msg = "Nonfinite IFD value"
        raise ValueError(msg)
    return result


def _duration(value: object) -> int:
    number = _number(value)
    if not number.is_integer() or number <= 0:
        msg = "Duration must be positive whole minutes"
        raise ValueError(msg)
    return int(number)


def _aep(value: object) -> float:
    number = _number(value)
    if not 0 < number <= 100:
        msg = "AEP must be in percent, in (0, 100]"
        raise ValueError(msg)
    return number


def _positive_depth(value: object) -> float:
    number = _number(value)
    if number <= 0:
        msg = "IFD depth must be positive (missing or NoData depths are not accepted)"
        raise ValueError(msg)
    return number


def parse_ifd(raw: bytes, point: Point, source: IFDSource) -> IFDTable:
    """Validate coordinate identity and one explicitly selected split-format table.

    The table structure is based on PyTUFLOW's recorded ARR responses. The
    production AllIFDDatasets path still requires genuine-response acceptance.
    """
    root = _mapping(cast("object", json.loads(raw)))
    inputs = _mapping(root.get("input_data"))
    if (
        Decimal(str(_number(inputs.get("Latitude")))) != point.latitude
        or Decimal(str(_number(inputs.get("Longitude")))) != point.longitude
    ):
        msg = "ARR response coordinates do not match the requested point"
        raise ValueError(msg)
    value: object = root.get("layers")
    for key in source.table_path:
        mapping = _mapping(value)
        if key not in mapping:
            msg = f"Missing exact IFD source key {key!r}; available keys: {sorted(mapping)}"
            raise ValueError(msg)
        value = mapping[key]
    table = _mapping(value)
    durations = tuple(_duration(item) for item in _array(table.get("index")))
    aeps = tuple(_aep(item) for item in _array(table.get("columns")))
    depths = tuple(tuple(_positive_depth(item) for item in _array(row)) for row in _array(table.get("data")))
    if not durations or not aeps or len(set(durations)) != len(durations) or len(set(aeps)) != len(aeps):
        msg = "IFD axes must be nonempty and unique"
        raise ValueError(msg)
    if len(depths) != len(durations) or any(len(row) != len(aeps) for row in depths):
        msg = "IFD table dimensions do not match its axes"
        raise ValueError(msg)
    return IFDTable(durations, aeps, depths)


def _cache_key(point: Point, source: IFDSource) -> str:
    return json.dumps(
        {"schema": 1, "endpoint": ARR_ENDPOINT, "params": point.params(), "snapshot": source.snapshot},
        sort_keys=True,
        separators=(",", ":"),
    )


class PointCache:
    """Immutable raw point responses, committed separately for restart safety.

    Use a local filesystem database. One downloader process per cache is the
    supported usage; SQLite transactions also protect the stored rows.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(path)) as connection, connection:
            connection.execute(
                """CREATE TABLE IF NOT EXISTS spatial_ifd_points_v1 (
                    request_key TEXT PRIMARY KEY,
                    retrieved_utc TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    response BLOB NOT NULL
                )"""
            )

    def get(self, point: Point, source: IFDSource) -> bytes | None:
        with closing(sqlite3.connect(self.path)) as connection:
            row = cast(
                "tuple[bytes, str] | None",
                connection.execute(
                    "SELECT response, sha256 FROM spatial_ifd_points_v1 WHERE request_key = ?",
                    (_cache_key(point, source),),
                ).fetchone(),
            )
        if row is None:
            return None
        raw, digest = row
        if hashlib.sha256(raw).hexdigest() != digest:
            msg = "Cached IFD response checksum mismatch; inspect the cache or use a new snapshot"
            raise ValueError(msg)
        return raw

    def store(self, point: Point, source: IFDSource, raw: bytes) -> IFDTable:
        """Admit only a validated response; retain the first committed payload."""
        parse_ifd(raw, point, source)
        with closing(sqlite3.connect(self.path)) as connection, connection:
            connection.execute(
                "INSERT OR IGNORE INTO spatial_ifd_points_v1 VALUES (?, ?, ?, ?)",
                (_cache_key(point, source), datetime.now(UTC).isoformat(), hashlib.sha256(raw).hexdigest(), raw),
            )
        saved = self.get(point, source)
        if saved is None:
            msg = "Committed IFD response could not be read"
            raise RuntimeError(msg)
        return parse_ifd(saved, point, source)


def _retry_delay(header: str | None, attempt: int) -> float:
    if not header:
        return float(2**attempt)
    try:
        seconds = float(header)
    except ValueError:
        try:
            date = parsedate_to_datetime(header)
            seconds = (date - datetime.now(UTC)).total_seconds()
        except (TypeError, ValueError, OverflowError):
            seconds = 0.0
    if not isfinite(seconds) or seconds > 60:
        msg = "Server requested a long retry delay; stop and resume the download later"
        raise RuntimeError(msg)
    return max(float(2**attempt), seconds)


def fetch_arr_point(point: Point) -> bytes:
    """Fetch one documented point request, serially and with bounded retries.

    No automatic access to alternate hosts or datasets is configured. The caller
    must decide whether the requested batch is acceptable to the service.
    """
    url = f"{ARR_ENDPOINT}?{urlencode(point.params())}"
    for attempt in range(4):
        time.sleep(1.0)
        try:
            # Fixed HTTPS service and URL-encoded numeric coordinates.
            with urlopen(url, timeout=60) as response:  # noqa: S310
                raw = response.read(_MAX_RESPONSE_BYTES + 1)
            if len(raw) > _MAX_RESPONSE_BYTES:
                msg = "ARR response exceeds the configured size limit"
                raise ValueError(msg)
            return raw
        except HTTPError as exc:
            try:
                if attempt == 3 or not (exc.code == 429 or 500 <= exc.code < 600):
                    raise
                delay = _retry_delay(exc.headers.get("Retry-After"), attempt)
            finally:
                exc.close()
            time.sleep(delay)
        except (URLError, TimeoutError):
            if attempt == 3:
                raise
            time.sleep(2**attempt)
    msg = "Unreachable ARR retry state"
    raise RuntimeError(msg)


@dataclass(frozen=True, slots=True)
class SpatialIFDGrid:
    spec: GridSpec
    source: IFDSource
    duration_min: int
    aep_percent: float
    depths_mm: npt.NDArray[np.float64]
    cached_cells: int
    fetched_cells: int

    def multipliers(self, reference_depth_mm: float) -> npt.NDArray[np.float64]:
        """Divide by an explicitly supplied positive point or catchment mean depth."""
        return self.depths_mm / _positive_depth(reference_depth_mm)


@dataclass(frozen=True, slots=True)
class IFDEvent:
    """An exact source-table event, with duration in minutes and AEP in percent."""

    duration_min: int
    aep_percent: float

    def __post_init__(self) -> None:
        _duration(self.duration_min)
        _aep(self.aep_percent)


@dataclass(frozen=True, slots=True)
class CacheCoverage:
    """Validated coverage for the selected source and events, without network access."""

    cached_cells: int
    missing_points: tuple[Point, ...]


def _event_depths(table: IFDTable, events: Sequence[IFDEvent]) -> tuple[float, ...]:
    return tuple(table.depth(event.duration_min, event.aep_percent) for event in events)


def _validate_events(events: Sequence[IFDEvent]) -> None:
    if not events or len(set(events)) != len(events):
        msg = "Events must be nonempty and unique"
        raise ValueError(msg)


def inspect_cache(
    spec: GridSpec, source: IFDSource, cache: PointCache, events: Sequence[IFDEvent]
) -> CacheCoverage:
    """Report missing coordinates; corrupt, wrong-source or unsupported data raise.

    This does not contact the network or count unvalidated cached payloads as
    hits. A missing coordinate is distinct from a cache validation failure.
    """
    _validate_events(events)
    missing: list[Point] = []
    for _, _, point in spec.points():
        raw = cache.get(point, source)
        if raw is None:
            missing.append(point)
        else:
            _event_depths(parse_ifd(raw, point, source), events)
    return CacheCoverage(spec.rows * spec.cols - len(missing), tuple(missing))


def collect_grids(
    spec: GridSpec,
    source: IFDSource,
    cache: PointCache,
    events: Sequence[IFDEvent],
    *,
    max_new_points: int = 0,
    fetcher: Callable[[Point], bytes] = fetch_arr_point,
) -> tuple[SpatialIFDGrid, ...]:
    """Collect all selected events, parsing each cached point just once.

    Complete cached-cell validation and the missing-point budget check before
    contacting the service. Each new payload must contain every requested
    event and is committed before moving on. Return no partial event grids on
    failure, but retain validated cached responses for restart.
    """
    events = tuple(events)
    _validate_events(events)
    if not 0 <= max_new_points <= MAX_GRID_CELLS:
        msg = "New-point budget is outside the supported range"
        raise ValueError(msg)
    if len(events) * spec.rows * spec.cols > 10_000_000:
        msg = "Batch exceeds ten million event-cell values; split event selection"
        raise ValueError(msg)

    depths = np.empty((len(events), spec.rows, spec.cols), dtype=np.float64)
    missing: list[tuple[int, int, Point]] = []
    for row, col, point in spec.points():
        raw = cache.get(point, source)
        if raw is None:
            missing.append((row, col, point))
        else:
            depths[:, row, col] = _event_depths(parse_ifd(raw, point, source), events)
    if len(missing) > max_new_points:
        msg = f"{len(missing)} missing points exceed the explicit new-point budget ({max_new_points})"
        raise ValueError(msg)

    for row, col, point in missing:
        raw = fetcher(point)
        _event_depths(parse_ifd(raw, point, source), events)
        table = cache.store(point, source, raw)
        depths[:, row, col] = _event_depths(table, events)

    fetched = len(missing)
    cached = spec.rows * spec.cols - fetched
    return tuple(
        SpatialIFDGrid(spec, source, event.duration_min, event.aep_percent, depths[index], cached, fetched)
        for index, event in enumerate(events)
    )


def collect_grid(
    spec: GridSpec,
    source: IFDSource,
    cache: PointCache,
    duration_min: int,
    aep_percent: float,
    *,
    max_new_points: int = 0,
    fetcher: Callable[[Point], bytes] = fetch_arr_point,
) -> SpatialIFDGrid:
    """Collect a single event using the same offline-first batch workflow."""
    return collect_grids(
        spec,
        source,
        cache,
        (IFDEvent(duration_min, aep_percent),),
        max_new_points=max_new_points,
        fetcher=fetcher,
    )[0]


def write_ascii_grid(path: Path, spec: GridSpec, values: npt.NDArray[np.float64]) -> None:
    """Write a complete ESRI ASCII grid in WGS84 degrees without overwriting.

    ASCII does not embed a CRS. Assign EPSG:4326 on import; this is not a projected
    model grid. The caller chooses depth (mm) or dimensionless multiplier values.
    """
    if values.shape != (spec.rows, spec.cols) or not np.isfinite(values).all() or np.any(values <= 0):
        msg = "Grid must have the stated dimensions and finite positive values"
        raise ValueError(msg)
    header = (
        f"ncols {spec.cols}\nnrows {spec.rows}\nxllcorner {spec.xllcorner:f}\n"
        f"yllcorner {spec.yllcorner:f}\ncellsize {spec.spacing:f}\nNODATA_value -9999\n"
    )
    # Format all values before creating the destination. No silent overwrite.
    lines = [" ".join(format(float(value), ".17g") for value in row) for row in values]
    with path.open("x", encoding="ascii", newline="\n") as stream:
        stream.write(header + "\n".join(lines) + "\n")
