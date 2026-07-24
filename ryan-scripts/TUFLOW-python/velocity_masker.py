#!/usr/bin/env python
"""Trim *_V_Max rasters so that only cells with depth >= threshold retain velocity values."""
from __future__ import annotations

from pathlib import Path
from typing import Sequence

import numpy as np
import rasterio
from loguru import logger
from rasterio.enums import Resampling
from rasterio.warp import reproject

from ryan_library.classes.tuflow_string_classes import TuflowStringParser
from ryan_library.functions.loguru_helpers import setup_logger

TARGET_DIRECTORIES: Sequence[Path] = (
    # Path(
    # r"Q:\BGER\PER\RP20180.387 WYLOO NANUTARRA ACCESS ROAD - FMG\TUFLOW_Metawandy\results\v13\FullCatchment\EXG\Max"),
    # Path(
    #     r"Q:\BGER\PER\RP20180.387 WYLOO NANUTARRA ACCESS ROAD - FMG\TUFLOW_Metawandy\results\v13\AccessRoad\EXG\Max"
    # ),
    # Path(
    #     r"Q:\BGER\PER\RP20180.387 WYLOO NANUTARRA ACCESS ROAD - FMG\TUFLOW_Metawandy\results\v13\PilaruCreek\EXG\Max"
    # ),
    # Path(
    #     r"Q:\BGER\PER\RP20180.387 WYLOO NANUTARRA ACCESS ROAD - FMG\TUFLOW_Metawandy\results\v13\PilaruTrib\EXG\Max"
    # ),
    # Path(
    # r"Q:\BGER\PER\RP20180.387 WYLOO NANUTARRA ACCESS ROAD - FMG\TUFLOW_Metawandy\results\v13\PilaruUpper\EXG\Max"
    # ),
    Path(
        r"Q:\BGER\PER\RP20180.387 WYLOO NANUTARRA ACCESS ROAD - FMG\TUFLOW_Metawandy\results\v13"
    ),
)
DEPTH_THRESHOLD = 0.05  # metres
LOG_LEVEL = "INFO"
LOG_FILE: Path | None = (
    None  # e.g. Path(__file__).with_suffix(".log") if a file sink is desired
)


def derive_depth_path(velocity_path: Path) -> Path:
    return velocity_path.with_name(
        name=velocity_path.name.replace("_V_Max", "_d_HR_Max", count=1)
    )


def derive_original_velocity_path(velocity_path: Path) -> Path:
    return velocity_path.with_name(
        name=f"{velocity_path.stem}_original{velocity_path.suffix}"
    )


def extract_aep_label(path: Path) -> str:
    try:
        parser = TuflowStringParser(path.name)
        if parser.aep:
            return parser.aep.text_repr
    except Exception:
        logger.warning("Unable to parse AEP token from {}", path.name)
    return "Unknown"


def ensure_original_backup(velocity_path: Path) -> Path | None:
    original_path: Path = derive_original_velocity_path(velocity_path=velocity_path)
    if original_path.exists():
        return original_path
    if not velocity_path.exists():
        logger.error(
            "Neither {velocity} nor {backup} exists; cannot obtain a velocity source.",
            velocity=velocity_path.name,
            backup=original_path.name,
        )
        return None
    logger.info("Backing up {} -> {}", velocity_path.name, original_path.name)
    velocity_path.rename(target=original_path)
    return original_path


def build_aligned_mask(
    depth_path: Path,
    velocity_dataset: rasterio.io.DatasetReader,  # pyright: ignore[reportAttributeAccessIssue]
    threshold: float,
) -> np.ndarray:
    with rasterio.open(depth_path) as depth_ds:
        depth_values = depth_ds.read(1, masked=True).filled(-np.inf)
        fine_mask = (depth_values >= threshold).astype(np.uint8)

        aligned_mask = np.zeros(
            (velocity_dataset.height, velocity_dataset.width), dtype=np.uint8
        )
        reproject(
            source=fine_mask,
            destination=aligned_mask,
            src_transform=depth_ds.transform,
            src_crs=depth_ds.crs,
            dst_transform=velocity_dataset.transform,
            dst_crs=velocity_dataset.crs,
            resampling=Resampling.max,
            src_nodata=0,
            dst_nodata=0,
        )
    return aligned_mask.astype(bool)


def mask_velocity_data(
    velocity_dataset: rasterio.io.DatasetReader,
    mask: np.ndarray,
) -> tuple[np.ndarray, dict]:
    profile = velocity_dataset.profile.copy()
    dtype = np.dtype(profile["dtype"])
    nodata = profile.get("nodata")
    fill_scalar = np.array(0 if nodata is None else nodata).astype(dtype).item()

    velocity_values = (
        velocity_dataset.read(1, masked=True)
        .filled(fill_scalar)
        .astype(dtype, copy=False)
    )
    trimmed = np.where(mask, velocity_values, fill_scalar).astype(dtype, copy=False)

    return trimmed[np.newaxis, ...], profile


def process_velocity_file(
    velocity_path: Path, depth_path: Path, threshold: float, aep_label: str
) -> bool:
    original_path: Path | None = ensure_original_backup(velocity_path=velocity_path)
    if original_path is None:
        return False
    if not depth_path.exists():
        logger.error(
            "Missing depth raster for {} ({}).", velocity_path.name, depth_path.name
        )
        return False

    logger.info(
        "[{}] Processing {} with mask {}",
        aep_label,
        velocity_path.name,
        depth_path.name,
    )
    try:
        with rasterio.open(original_path) as vel_ds:
            mask = build_aligned_mask(depth_path, vel_ds, threshold)
            masked_data, profile = mask_velocity_data(vel_ds, mask)

        kept_cells = int(mask.sum())
        total_cells: int = mask.size
        profile.update(count=1)

        with rasterio.open(velocity_path, "w", **profile) as dst:
            dst.write(masked_data)

        logger.success(
            "[{aep}] Updated {file} (kept {kept}/{total} cells, {pct:.1f}%).",
            aep=aep_label,
            file=velocity_path.name,
            kept=kept_cells,
            total=total_cells,
            pct=100 * kept_cells / total_cells if total_cells else 0.0,
        )
        return True
    except Exception:
        logger.exception("Failed to process %s", velocity_path.name)
        return False


def process_root(root: Path, threshold: float) -> None:
    resolved: Path = root.expanduser().resolve()
    if not resolved.exists():
        logger.error("Skipping {} because it does not exist.", resolved)
        return

    velocity_files: list[Path] = sorted(resolved.glob(pattern="*_V_Max.tif"))
    if not velocity_files:
        logger.warning("No *_V_Max.tif rasters found under {}", resolved)
        return

    logger.info("Found {} velocity rasters in {}", len(velocity_files), resolved)
    success_count = 0
    for velocity_path in velocity_files:
        depth_path: Path = derive_depth_path(velocity_path=velocity_path)
        aep_label: str = extract_aep_label(path=velocity_path)
        if process_velocity_file(
            velocity_path=velocity_path,
            depth_path=depth_path,
            threshold=threshold,
            aep_label=aep_label,
        ):
            success_count += 1

    if success_count == len(velocity_files):
        logger.success(
            "Completed masking for all {} rasters in {}.", success_count, resolved
        )
    else:
        logger.error(
            "Masked {kept} of {total} rasters in {root}; please review the log for failures.",
            kept=success_count,
            total=len(velocity_files),
            root=resolved,
        )


def main() -> None:
    log_file_str: str | None = str(LOG_FILE) if LOG_FILE else None
    with setup_logger(console_log_level=LOG_LEVEL.upper(), log_file=log_file_str):
        logger.info(
            "Velocity masker starting for {} folder(s).", len(TARGET_DIRECTORIES)
        )
        for target in TARGET_DIRECTORIES:
            process_root(root=target, threshold=DEPTH_THRESHOLD)


if __name__ == "__main__":
    main()
