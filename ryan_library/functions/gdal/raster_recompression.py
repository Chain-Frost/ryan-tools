"""Lossless GeoTIFF inspection, copying and exact decoded-pixel validation.

The streaming copy is intentionally limited to ordinary single-band Float32
GeoTIFFs. It reads each source pixel window once, records a SHA-256 digest of
the decoded bytes, writes the compressed output, and validates the reopened
output against those digests. More complex rasters should use GDAL Translate
and :func:`verify_rasters_exact` instead.
"""

from __future__ import annotations

# Pyright: Rasterio currently has no bundled type stubs.
# pyright: reportMissingTypeStubs=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownMemberType=false
# pyright: reportUnknownParameterType=false
# pyright: reportUnknownVariableType=false
# pyright: reportAttributeAccessIssue=false
# pyright: reportCallIssue=false
# pyright: reportUnnecessaryContains=false

import hashlib
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import numpy as np
import rasterio
from rasterio.enums import MaskFlags
from rasterio.windows import Window

CompressionName = Literal["DEFLATE", "ZSTD"]

_IGNORED_METADATA_NAMESPACES = frozenset({"DERIVED_SUBDATASETS", "IMAGE_STRUCTURE"})
_STATISTICS_PREFIX = "STATISTICS_"


@dataclass(frozen=True, slots=True)
class FileSnapshot:
    """Record the file properties used to detect concurrent modification."""

    path: Path
    size: int
    mtime_ns: int


@dataclass(frozen=True, slots=True)
class RasterInspection:
    """Describe storage characteristics without reading complete pixel arrays."""

    path: Path
    snapshot: FileSnapshot
    driver: str
    compression: str
    width: int
    height: int
    band_count: int
    data_types: tuple[str, ...]
    overview_count: int
    stream_eligible: bool
    stream_ineligible_reason: str


@dataclass(frozen=True, slots=True)
class RasterSignature:
    """Substantive raster metadata that must survive lossless recompression."""

    width: int
    height: int
    band_count: int
    data_types: tuple[str, ...]
    crs_wkt: str | None
    transform: tuple[float, ...]
    nodata_values: tuple[float | None, ...]
    scales: tuple[float, ...]
    offsets: tuple[float, ...]
    color_interpretations: tuple[str, ...]
    units: tuple[str | None, ...]
    descriptions: tuple[str | None, ...]
    mask_flags: tuple[tuple[str, ...], ...]
    dataset_metadata: tuple[tuple[str, tuple[tuple[str, str], ...]], ...]
    band_metadata: tuple[tuple[tuple[str, tuple[tuple[str, str], ...]], ...], ...]


@dataclass(frozen=True, slots=True)
class WindowDigest:
    """Decoded byte digest for one full-width raster window."""

    row_offset: int
    row_count: int
    digest: bytes


@dataclass(frozen=True, slots=True)
class StreamCopyEvidence:
    """Evidence produced while copying a constrained source exactly once."""

    source_signature: RasterSignature
    windows: tuple[WindowDigest, ...]


def snapshot_file(path: Path) -> FileSnapshot:
    """Return a stable file snapshot used before destructive replacement."""
    resolved = path.resolve()
    stat = resolved.stat()
    return FileSnapshot(path=resolved, size=stat.st_size, mtime_ns=stat.st_mtime_ns)


def snapshot_is_unchanged(snapshot: FileSnapshot) -> bool:
    """Return whether a file still matches an earlier snapshot."""
    try:
        return snapshot_file(snapshot.path) == snapshot
    except OSError:
        return False


def inspect_geotiff(path: Path) -> RasterInspection:
    """Inspect GeoTIFF storage and determine constrained streaming eligibility."""
    resolved = path.resolve()
    file_snapshot = snapshot_file(resolved)
    with rasterio.open(resolved) as dataset:
        compression = (
            dataset.compression.value.upper()
            if dataset.compression is not None
            else "NONE"
        )
        overview_count = sum(
            len(dataset.overviews(band)) for band in range(1, dataset.count + 1)
        )
        eligible, reason = stream_copy_eligibility(dataset)
        return RasterInspection(
            path=resolved,
            snapshot=file_snapshot,
            driver=dataset.driver,
            compression=compression,
            width=dataset.width,
            height=dataset.height,
            band_count=dataset.count,
            data_types=tuple(dataset.dtypes),
            overview_count=overview_count,
            stream_eligible=eligible,
            stream_ineligible_reason=reason,
        )


def stream_copy_eligibility(dataset: rasterio.io.DatasetReader) -> tuple[bool, str]:
    """Return whether a dataset fits the deliberately narrow one-read contract."""
    if dataset.driver != "GTiff":
        return False, f"driver is {dataset.driver}, not GTiff"
    if dataset.count != 1:
        return False, f"band count is {dataset.count}, not one"
    if dataset.dtypes != ("float32",):
        return False, f"data type is {dataset.dtypes[0]}, not Float32"
    if dataset.crs is None:
        return False, "CRS is missing"
    if dataset.transform.b != 0.0 or dataset.transform.d != 0.0:
        return False, "grid transform is rotated or skewed"
    if dataset.subdatasets:
        return False, "dataset exposes subdatasets"
    gcps, _gcp_crs = dataset.gcps
    if gcps:
        return False, "dataset uses ground-control points"
    if dataset.overviews(1):
        return False, "band has overviews"
    flags = frozenset(dataset.mask_flag_enums[0])
    if flags not in ({MaskFlags.all_valid}, {MaskFlags.nodata}):
        return (
            False,
            f"mask flags require explicit mask copying: {sorted(flag.name for flag in flags)}",
        )
    try:
        dataset.colormap(1)
    except ValueError:
        pass
    else:
        return False, "band has a colour table"
    return True, ""


def capture_raster_signature(dataset: rasterio.io.DatasetReader) -> RasterSignature:
    """Capture metadata whose equality is required after recompression."""
    crs_wkt = dataset.crs.to_wkt() if dataset.crs is not None else None
    dataset_metadata = _capture_metadata_domains(dataset)
    band_metadata = tuple(
        _capture_metadata_domains(dataset, band) for band in range(1, dataset.count + 1)
    )
    return RasterSignature(
        width=dataset.width,
        height=dataset.height,
        band_count=dataset.count,
        data_types=tuple(dataset.dtypes),
        crs_wkt=crs_wkt,
        transform=tuple(dataset.transform),
        nodata_values=tuple(dataset.nodatavals),
        scales=tuple(dataset.scales),
        offsets=tuple(dataset.offsets),
        color_interpretations=tuple(value.name for value in dataset.colorinterp),
        units=tuple(dataset.units),
        descriptions=tuple(dataset.descriptions),
        mask_flags=tuple(
            tuple(sorted(flag.name for flag in flags))
            for flags in dataset.mask_flag_enums
        ),
        dataset_metadata=dataset_metadata,
        band_metadata=band_metadata,
    )


def stream_copy_float32_geotiff(
    source: Path,
    output: Path,
    *,
    compression: CompressionName = "DEFLATE",
    threads: str = "ALL_CPUS",
    max_window_cells: int = 1_000_000,
) -> StreamCopyEvidence:
    """Compress one constrained Float32 GeoTIFF with one source-pixel read.

    The output is fully reopened and checked against decoded-byte digests. The
    source is not reread for pixel verification.
    """
    source = source.resolve()
    output = output.resolve()
    if output.exists():
        raise FileExistsError(f"Streaming output already exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)

    with rasterio.open(source) as original:
        eligible, reason = stream_copy_eligibility(original)
        if not eligible:
            raise ValueError(f"Streaming copy is not eligible: {reason}")
        source_signature = capture_raster_signature(original)
        profile = dict(original.profile)
        for key in ("blockxsize", "blockysize", "compress", "tiled"):
            profile.pop(key, None)
        profile.update(
            driver="GTiff",
            compress=compression,
            predictor=2,
            BIGTIFF="IF_SAFER",
            NUM_THREADS=threads,
        )
        rows = _window_rows(original.width, original.height, max_window_cells)
        evidence: list[WindowDigest] = []
        with (
            rasterio.Env(GDAL_NUM_THREADS=threads),
            rasterio.open(output, "w", **profile) as converted,
        ):
            _copy_raster_metadata(original, converted)
            for row_offset in range(0, original.height, rows):
                row_count = min(rows, original.height - row_offset)
                window = Window(0, row_offset, original.width, row_count)
                values = original.read(1, window=window, masked=False)
                evidence.append(
                    WindowDigest(row_offset, row_count, _array_digest(values))
                )
                converted.write(values, 1, window=window)

    result = StreamCopyEvidence(
        source_signature=source_signature, windows=tuple(evidence)
    )
    difference = verify_stream_copy_output(
        output, result, expected_compression=compression
    )
    if difference is not None:
        raise RuntimeError(f"Streaming output verification failed: {difference}")
    return result


def verify_stream_copy_output(
    output: Path,
    evidence: StreamCopyEvidence,
    *,
    expected_compression: CompressionName = "DEFLATE",
) -> str | None:
    """Verify a reopened streaming output without rereading its source pixels."""
    with rasterio.open(output.resolve()) as converted:
        metadata_difference = _signature_difference(
            evidence.source_signature,
            capture_raster_signature(converted),
        )
        if metadata_difference is not None:
            return metadata_difference
        storage_difference = _storage_difference(converted, expected_compression)
        if storage_difference is not None:
            return storage_difference
        for record in evidence.windows:
            window = Window(0, record.row_offset, converted.width, record.row_count)
            values = converted.read(1, window=window, masked=False)
            if _array_digest(values) != record.digest:
                return f"decoded pixel bytes differ at rows {record.row_offset}-{record.row_offset + record.row_count - 1}"
    return None


def verify_rasters_exact(
    source: Path,
    output: Path,
    *,
    expected_compression: CompressionName = "DEFLATE",
    max_window_cells: int = 1_000_000,
) -> str | None:
    """Compare substantive metadata, masks and decoded pixel bytes exactly."""
    with (
        rasterio.open(source.resolve()) as original,
        rasterio.open(output.resolve()) as converted,
    ):
        metadata_difference = _signature_difference(
            capture_raster_signature(original), capture_raster_signature(converted)
        )
        if metadata_difference is not None:
            return metadata_difference
        storage_difference = _storage_difference(converted, expected_compression)
        if storage_difference is not None:
            return storage_difference
        rows = _window_rows(original.width, original.height, max_window_cells)
        for band in range(1, original.count + 1):
            for row_offset in range(0, original.height, rows):
                row_count = min(rows, original.height - row_offset)
                window = Window(0, row_offset, original.width, row_count)
                before = original.read(band, window=window, masked=False)
                after = converted.read(band, window=window, masked=False)
                if before.shape != after.shape or before.dtype != after.dtype:
                    return f"band {band} shape or data type differs at row {row_offset}"
                if not np.array_equal(before.view(np.uint8), after.view(np.uint8)):
                    return f"band {band} decoded pixel bytes differ at row {row_offset}"
                if not np.array_equal(
                    original.read_masks(band, window=window),
                    converted.read_masks(band, window=window),
                ):
                    return f"band {band} validity mask differs at row {row_offset}"
    return None


def compare_raster_metadata(source: Path, other: Path) -> str | None:
    """Compare substantive metadata without reading complete pixel arrays."""
    with (
        rasterio.open(source.resolve()) as original,
        rasterio.open(other.resolve()) as candidate,
    ):
        return _signature_difference(
            capture_raster_signature(original), capture_raster_signature(candidate)
        )


def stage_raster_with_dependencies(source: Path, directory: Path) -> Path:
    """Copy a GeoTIFF and GDAL-reported sidecars to a local directory.

    Each physical file is hash-checked after copying. Metadata is then compared
    between the original and staged datasets without reading raster pixels.
    """
    source = source.resolve()
    directory = directory.resolve()
    directory.mkdir(parents=True, exist_ok=True)
    with rasterio.open(source) as dataset:
        reported_files = tuple(dataset.files)
    physical_files = tuple(
        (candidate if candidate.is_absolute() else source.parent / candidate).resolve()
        for candidate in (Path(name) for name in reported_files)
    )
    destinations: set[Path] = set()
    staged_source = directory / source.name
    for physical_file in physical_files:
        destination = directory / physical_file.name
        if destination in destinations:
            raise ValueError(
                f"Raster dependencies have duplicate names: {physical_file.name}"
            )
        destinations.add(destination)
        copy_file_with_digest(physical_file, destination)
    if not staged_source.is_file():
        raise RuntimeError(
            f"GDAL did not report the source TIFF as a physical file: {source}"
        )
    difference = compare_raster_metadata(source, staged_source)
    if difference is not None:
        raise RuntimeError(f"Staged raster metadata differs: {difference}")
    return staged_source


def copy_file_with_digest(
    source: Path, destination: Path, *, chunk_size: int = 8 * 1024 * 1024
) -> bytes:
    """Copy a file once while hashing its source bytes, then verify the copy."""
    source = source.resolve()
    destination = destination.resolve()
    if destination.exists():
        raise FileExistsError(f"Copy destination already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    with source.open("rb") as input_file, destination.open("xb") as output_file:
        while chunk := input_file.read(chunk_size):
            digest.update(chunk)
            output_file.write(chunk)
        output_file.flush()
        os.fsync(output_file.fileno())
    shutil.copystat(source, destination)
    expected = digest.digest()
    if hash_file(destination, chunk_size=chunk_size) != expected:
        destination.unlink(missing_ok=True)
        raise OSError(f"Copied file digest differs: {source} -> {destination}")
    return expected


def hash_file(path: Path, *, chunk_size: int = 8 * 1024 * 1024) -> bytes:
    """Return the SHA-256 digest of a file."""
    digest = hashlib.sha256()
    with path.resolve().open("rb") as input_file:
        while chunk := input_file.read(chunk_size):
            digest.update(chunk)
    return digest.digest()


def _window_rows(width: int, height: int, max_window_cells: int) -> int:
    if max_window_cells < 1:
        raise ValueError("max_window_cells must be positive")
    return max(1, min(height, max_window_cells // max(1, width)))


def _array_digest(values: np.ndarray) -> bytes:
    contiguous = np.ascontiguousarray(values)
    return hashlib.sha256(memoryview(contiguous).cast("B")).digest()


def _clean_tags(tags: dict[str, str]) -> tuple[tuple[str, str], ...]:
    return tuple(
        sorted(
            (key, value)
            for key, value in tags.items()
            if not key.startswith(_STATISTICS_PREFIX)
        )
    )


def _capture_metadata_domains(
    dataset: rasterio.io.DatasetReader,
    band: int = 0,
) -> tuple[tuple[str, tuple[tuple[str, str], ...]], ...]:
    domains: list[tuple[str, tuple[tuple[str, str], ...]]] = [
        ("", _clean_tags(dataset.tags(band)))
    ]
    for namespace in sorted(dataset.tag_namespaces(band)):
        if namespace in _IGNORED_METADATA_NAMESPACES:
            continue
        domains.append((namespace, _clean_tags(dataset.tags(band, ns=namespace))))
    return tuple(domains)


def _copy_raster_metadata(
    source: rasterio.io.DatasetReader,
    destination: rasterio.io.DatasetWriter,
) -> None:
    destination.update_tags(**source.tags())
    for namespace in source.tag_namespaces():
        if namespace not in _IGNORED_METADATA_NAMESPACES:
            destination.update_tags(ns=namespace, **source.tags(ns=namespace))
    destination.scales = source.scales
    destination.offsets = source.offsets
    destination.units = source.units
    destination.descriptions = source.descriptions
    destination.colorinterp = source.colorinterp
    for band in range(1, source.count + 1):
        destination.update_tags(band, **source.tags(band))
        for namespace in source.tag_namespaces(band):
            if namespace not in _IGNORED_METADATA_NAMESPACES:
                destination.update_tags(
                    band, ns=namespace, **source.tags(band, ns=namespace)
                )


def _signature_difference(
    expected: RasterSignature, actual: RasterSignature
) -> str | None:
    fields = (
        "width",
        "height",
        "band_count",
        "data_types",
        "crs_wkt",
        "transform",
        "nodata_values",
        "scales",
        "offsets",
        "color_interpretations",
        "units",
        "descriptions",
        "mask_flags",
        "dataset_metadata",
        "band_metadata",
    )
    for field in fields:
        if getattr(expected, field) != getattr(actual, field):
            return f"{field.replace('_', ' ')} differs"
    return None


def _storage_difference(
    dataset: rasterio.io.DatasetReader, expected_compression: CompressionName
) -> str | None:
    compression = (
        dataset.compression.value.upper() if dataset.compression is not None else "NONE"
    )
    if compression != expected_compression:
        return f"compression is {compression}, expected {expected_compression}"
    for band in range(1, dataset.count + 1):
        if dataset.overviews(band):
            return f"band {band} has overviews"
    return None
