"""Audit and safely replace GeoTIFFs with verified compressed copies."""

from __future__ import annotations

import shutil
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from loguru import logger

from ryan_library.functions.file_verification import has_linked_component
from ryan_library.functions.gdal.raster_processing import (
    RasterProfile,
    translate_to_geotiff,
)
from ryan_library.functions.gdal.raster_recompression import (
    RasterInspection,
    copy_file_with_digest,
    inspect_geotiff,
    raster_has_external_dependencies,
    snapshot_is_unchanged,
    stage_raster_with_dependencies,
    stream_copy_float32_geotiff,
    verify_rasters_exact,
)

RecompressionStrategy = Literal["auto", "stream", "translate"]


@dataclass(frozen=True, slots=True)
class RecompressionResult:
    """Outcome for one discovered GeoTIFF."""

    path: Path
    status: Literal["replaced", "skipped", "failed"]
    strategy: str
    reason: str
    original_bytes: int
    final_bytes: int


def audit_geotiffs(directory: Path, *, recursive: bool = True) -> tuple[RasterInspection, ...]:
    """Inspect all TIFF files without reading their complete pixel arrays."""
    return tuple(inspect_geotiff(path) for path in discover_geotiffs(directory, recursive=recursive))


def discover_geotiffs(directory: Path, *, recursive: bool = True) -> tuple[Path, ...]:
    """Return a stable list of TIFF files below a directory."""
    root = directory.absolute()
    if not root.is_dir():
        message = f"Raster directory does not exist: {root}"
        raise NotADirectoryError(message)
    iterator = root.rglob("*") if recursive else root.iterdir()
    return tuple(sorted(path for path in iterator if path.is_file() and path.suffix.lower() in {".tif", ".tiff"}))


def recompress_geotiffs(
    directory: Path,
    *,
    recursive: bool = True,
    profile: RasterProfile = "tuflow",
    strategy: RecompressionStrategy = "auto",
    staging_directory: Path | None = None,
    max_window_cells: int = 1_000_000,
) -> tuple[RecompressionResult, ...]:
    """Compress uncompressed TIFFs and atomically replace only verified sources.

    ``auto`` uses the one-source-read implementation only for its deliberately
    narrow single-band Float32 contract. Every other input uses GDAL Translate.
    A staging directory keeps the Translate conversion and its pixel comparison
    off a slower source volume.
    """
    paths = discover_geotiffs(directory, recursive=recursive)
    return tuple(
        _recompress_one(
            path,
            profile=profile,
            strategy=strategy,
            staging_directory=staging_directory,
            max_window_cells=max_window_cells,
        )
        for path in paths
    )


def _recompress_one(
    source: Path,
    *,
    profile: RasterProfile,
    strategy: RecompressionStrategy,
    staging_directory: Path | None,
    max_window_cells: int,
) -> RecompressionResult:
    inspection = inspect_geotiff(source)
    if inspection.compression != "NONE":
        return RecompressionResult(
            source,
            "skipped",
            "audit",
            f"already {inspection.compression}",
            inspection.snapshot.size,
            inspection.snapshot.size,
        )
    safety_failure = _replacement_safety_failure(source, inspection)
    if safety_failure is not None:
        return safety_failure
    compression = "DEFLATE" if profile == "tuflow" else "ZSTD"
    use_stream = strategy == "stream" or (strategy == "auto" and profile == "tuflow" and inspection.stream_eligible)
    chosen = "stream" if use_stream else "translate"
    if strategy == "stream" and (profile != "tuflow" or not inspection.stream_eligible):
        reason = inspection.stream_ineligible_reason or "stream mode requires the tuflow profile"
        return RecompressionResult(
            source,
            "failed",
            "stream",
            reason,
            inspection.snapshot.size,
            inspection.snapshot.size,
        )

    stage_root = staging_directory.absolute() if staging_directory else source.parent
    stage_root.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="ryan-recompress-", dir=stage_root))
    deployed = source.with_name(f".{source.name}.ryan-{uuid.uuid4().hex}.tmp")
    try:
        output = work / "converted.tif"
        if chosen == "stream":
            stream_copy_float32_geotiff(
                source,
                output,
                compression="DEFLATE",
                max_window_cells=max_window_cells,
            )
        else:
            compare_source = source
            if staging_directory is not None:
                compare_source = stage_raster_with_dependencies(source, work / "source")
            translate_to_geotiff(compare_source, output, profile=profile)
            difference = verify_rasters_exact(
                compare_source,
                output,
                expected_compression=compression,
                max_window_cells=max_window_cells,
            )
            if difference is not None:
                message = f"Exact verification failed: {difference}"
                raise RuntimeError(message)
        _ensure_self_contained_output(output)
        if not snapshot_is_unchanged(inspection.snapshot):
            message = "source changed during conversion"
            raise RuntimeError(message)
        # Copy helper verifies the destination SHA-256; avoid hashing it again.
        copy_file_with_digest(output, deployed)
        if not snapshot_is_unchanged(inspection.snapshot) or has_linked_component(source):
            message = "source changed or became linked before replacement"
            raise RuntimeError(message)
        deployed.replace(source)
        final_size = source.stat().st_size
        logger.info("Replaced {} using {} compression", source, chosen)
        return RecompressionResult(
            source,
            "replaced",
            chosen,
            "verified exact copy",
            inspection.snapshot.size,
            final_size,
        )
    except Exception as exc:  # noqa: BLE001 - return a per-file failure and leave source untouched
        logger.exception("Could not safely recompress {}", source)
        return RecompressionResult(
            source,
            "failed",
            chosen,
            str(exc),
            inspection.snapshot.size,
            inspection.snapshot.size,
        )
    finally:
        deployed.unlink(missing_ok=True)
        shutil.rmtree(work, ignore_errors=True)


def _replacement_safety_failure(source: Path, inspection: RasterInspection) -> RecompressionResult | None:
    """Reject inputs that a single-file atomic overwrite cannot safely handle."""
    if has_linked_component(source):
        reason = "symlink or junction in source path"
    elif raster_has_external_dependencies(source):
        reason = "external raster sidecars require manual handling"
    else:
        return None
    size = inspection.snapshot.size
    return RecompressionResult(source, "failed", "safety", reason, size, size)


def _ensure_self_contained_output(output: Path) -> None:
    """Refuse deployment of a compressed TIFF whose metadata needs sidecars."""
    if raster_has_external_dependencies(output):
        message = "compressed output uses external sidecars; replacement cancelled"
        raise RuntimeError(message)
