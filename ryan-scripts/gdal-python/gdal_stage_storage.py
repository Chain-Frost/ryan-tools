"""Calculate experimental stage-storage CSV and plot outputs from single-band DEMs."""

# moved from unsorted, not tested in production yet - 2026-08-20

from __future__ import annotations

from pathlib import Path

WRAPPER_VERSION = "2026-08-20.1"
DEFAULT_WORKING_DIR = Path(".")
DEFAULT_INPUT_DIRECTORY = Path(".")
DEFAULT_PATTERNS = ["*.tif"]
DEFAULT_MIN_LEVEL: float | None = None
DEFAULT_MAX_LEVEL: float | None = None
DEFAULT_STEP_SIZE = 0.01
DEFAULT_CREATE_PLOT = True
DEFAULT_OVERWRITE = False
DEFAULT_DRY_RUN = False

import argparse
import csv
from uuid import uuid4

import matplotlib.pyplot as plt
import numpy as np
from loguru import logger

from ryan_library.functions.wrapper_utils import (
    add_execution_cli_arguments,
    change_working_directory,
    pause_console,
    print_wrapper_banner,
)
from ryan_library.functions.gdal.stage_storage import compute_stage_storage, find_elevation_bounds


def _temporary_output(output_path: Path) -> Path:
    return output_path.with_name(f".{output_path.stem}.{uuid4().hex}.tmp{output_path.suffix}")


def _write_csv(output_path: Path, volumes: dict[float, float]) -> None:
    temporary_path = _temporary_output(output_path)
    try:
        with temporary_path.open("w", encoding="utf-8", newline="") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(["Level (m)", "Volume (m3)"])
            writer.writerows(volumes.items())
        temporary_path.replace(output_path)
    finally:
        temporary_path.unlink(missing_ok=True)


def _write_plot(output_path: Path, dem_name: str, volumes: dict[float, float]) -> None:
    temporary_path = _temporary_output(output_path)
    figure, axes = plt.subplots(figsize=(10, 6))
    try:
        levels = list(volumes)
        volume_values = list(volumes.values())
        axes.plot(levels, volume_values, color="blue", linewidth=2)  # pyright: ignore[reportUnknownMemberType]
        axes.fill_between(levels, volume_values, color="blue", alpha=0.1)  # pyright: ignore[reportUnknownMemberType]
        axes.set_title(f"Stage-Storage Curve: {dem_name}")  # pyright: ignore[reportUnknownMemberType]
        axes.set_xlabel("Elevation level (mAHD)")  # pyright: ignore[reportUnknownMemberType]
        axes.set_ylabel("Cumulative volume (m³)")  # pyright: ignore[reportUnknownMemberType]
        axes.grid(True, linestyle="--", alpha=0.7)  # pyright: ignore[reportUnknownMemberType]
        figure.tight_layout()
        figure.savefig(temporary_path, dpi=300)  # pyright: ignore[reportUnknownMemberType]
        temporary_path.replace(output_path)
    finally:
        plt.close(figure)
        temporary_path.unlink(missing_ok=True)


def _discover_dems(input_directory: Path, patterns: list[str]) -> list[Path]:
    return sorted(
        {path.resolve() for pattern in patterns for path in input_directory.glob(pattern) if path.is_file()},
        key=lambda path: str(path).casefold(),
    )


def main(args: argparse.Namespace) -> int:
    """Validate settings, calculate each curve and report partial failures."""
    input_value = args.input_directory if args.input_directory is not None else DEFAULT_INPUT_DIRECTORY
    patterns = args.patterns if args.patterns is not None else DEFAULT_PATTERNS
    minimum_override = args.min_level if args.min_level is not None else DEFAULT_MIN_LEVEL
    maximum_override = args.max_level if args.max_level is not None else DEFAULT_MAX_LEVEL
    step = args.step if args.step is not None else DEFAULT_STEP_SIZE
    create_plot = args.create_plot if args.create_plot is not None else DEFAULT_CREATE_PLOT
    overwrite = args.overwrite if args.overwrite is not None else DEFAULT_OVERWRITE
    dry_run = args.dry_run if args.dry_run is not None else DEFAULT_DRY_RUN
    input_directory = Path(input_value).resolve()
    target_directory = (
        Path(args.working_directory).resolve() if args.working_directory else DEFAULT_WORKING_DIR.resolve()
    )
    if not change_working_directory(target_dir=target_directory):
        return 1
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)

    if not input_directory.is_dir():
        logger.error("Input directory does not exist: {}", input_directory)
        return 1
    if not np.isfinite(step) or step <= 0:
        logger.error("--step must be a positive finite number.")
        return 2
    if minimum_override is not None and not np.isfinite(minimum_override):
        logger.error("--min-level must be finite.")
        return 2
    if maximum_override is not None and not np.isfinite(maximum_override):
        logger.error("--max-level must be finite.")
        return 2

    input_files = _discover_dems(input_directory, patterns)
    if not input_files:
        logger.warning("No files matching {} found in {}", patterns, input_directory)
        return 0

    successes = 0
    for dem_path in input_files:
        csv_path = input_directory / f"volumes_{dem_path.stem}.csv"
        png_path = csv_path.with_suffix(".png")
        expected_outputs = [csv_path, png_path] if create_plot else [csv_path]
        if not overwrite and any(path.exists() for path in expected_outputs):
            logger.warning("Output exists; skipping {} without --overwrite.", dem_path)
            successes += 1
            continue
        try:
            observed_minimum, observed_maximum = find_elevation_bounds(dem_path)
            minimum = observed_minimum if minimum_override is None else minimum_override
            maximum = observed_maximum if maximum_override is None else maximum_override
            if maximum < minimum:
                raise ValueError(f"Maximum level {maximum} is below minimum level {minimum}")
            levels = np.arange(minimum, maximum + step / 2.0, step, dtype=np.float64).tolist()
            volumes = compute_stage_storage(dem_path, levels=levels)
            if not dry_run:
                _write_csv(csv_path, volumes)
                if create_plot:
                    _write_plot(png_path, dem_path.stem, volumes)
            logger.success("Processed {} into {} stage levels.", dem_path, len(volumes))
            successes += 1
        except Exception:
            logger.exception("Failed to process {}", dem_path)

    logger.success("Stage-storage completed: {}/{} DEMs successful.", successes, len(input_files))
    return 0 if successes == len(input_files) else 1


def _parse_cli_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Calculate stage-storage volume curves for DEMs.")
    parser.add_argument("input_directory", nargs="?", type=Path, help="Override DEFAULT_INPUT_DIRECTORY.")
    parser.add_argument("--patterns", nargs="+", default=None, help="Override DEFAULT_PATTERNS.")
    parser.add_argument("--min-level", type=float, help="Minimum stage; defaults to the exact DEM minimum.")
    parser.add_argument("--max-level", type=float, help="Maximum stage; defaults to the exact DEM maximum.")
    parser.add_argument("--step", type=float, default=None, help="Override DEFAULT_STEP_SIZE.")
    parser.add_argument("--plot", dest="create_plot", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--overwrite", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--dry-run", action=argparse.BooleanOptionalAction, default=None)
    add_execution_cli_arguments(parser)
    return parser.parse_args(argv)


if __name__ == "__main__":
    cli_args = _parse_cli_arguments()
    try:
        result = main(cli_args)
    except Exception:
        logger.exception("Wrapper failed.")
        result = 1
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, leading_blank_line=True)
    if not cli_args.no_pause:
        pause_console()
    raise SystemExit(result)
