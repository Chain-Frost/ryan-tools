"""Convert rasters to XYZ or CSV through GDAL's streaming ``gdal2xyz`` utility."""

from __future__ import annotations

from pathlib import Path

WRAPPER_VERSION = "2026-08-12.1"
DEFAULT_INPUT_DIRECTORY = Path(".")
DEFAULT_PATTERNS = ["*.tif"]
DEFAULT_RECURSIVE = False
DEFAULT_FORMAT = "csv"
DEFAULT_KEEP_NODATA = False
DEFAULT_OVERWRITE = False
DEFAULT_WORKERS = 2
DEFAULT_DRY_RUN = False

import argparse
import concurrent.futures
import subprocess
import sys
from uuid import uuid4

from loguru import logger

from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.wrapper_utils import (
    add_execution_cli_arguments,
    change_working_directory,
    pause_console,
    print_wrapper_banner,
)


def _temporary_output(output_path: Path) -> Path:
    return output_path.with_name(f".{output_path.stem}.{uuid4().hex}.tmp{output_path.suffix}")


def convert_raster(
    input_path: Path,
    output_path: Path,
    *,
    csv_output: bool,
    skip_nodata: bool,
    overwrite: bool,
    dry_run: bool,
) -> bool:
    """Convert one raster through a temporary output and promote it on success."""
    if output_path.exists() and not overwrite:
        logger.warning("Output exists; skipping without --overwrite: {}", output_path)
        return True

    temporary_path = _temporary_output(output_path)
    command = [sys.executable, "-m", "osgeo_utils.gdal2xyz"]
    if csv_output:
        command.append("-csv")
    if skip_nodata:
        command.append("-skipnodata")
    command.extend([str(input_path), str(temporary_path)])

    if dry_run:
        logger.info("[DRY-RUN] {}", subprocess.list2cmdline(command))
        return True

    try:
        result = subprocess.run(command, check=False, text=True, capture_output=True)
        if result.returncode != 0:
            logger.error(
                "gdal2xyz failed for {} with code {}: {}",
                input_path,
                result.returncode,
                result.stderr.strip(),
            )
            return False
        if not temporary_path.is_file():
            logger.error("gdal2xyz reported success without creating {}", temporary_path)
            return False
        if overwrite:
            output_path.unlink(missing_ok=True)
        temporary_path.replace(output_path)
        logger.success("Created {}", output_path)
        return True
    except OSError as error:
        logger.error("Could not run gdal2xyz for {}: {}", input_path, error)
        return False
    finally:
        temporary_path.unlink(missing_ok=True)


def discover_rasters(input_directory: Path, patterns: list[str], *, recursive: bool) -> list[Path]:
    """Return a deterministic, deduplicated list of matching raster files."""
    discovered: set[Path] = set()
    for pattern in patterns:
        matches = input_directory.rglob(pattern) if recursive else input_directory.glob(pattern)
        discovered.update(path.resolve() for path in matches if path.is_file())
    return sorted(discovered, key=lambda path: str(path).casefold())


def main(args: argparse.Namespace) -> int:
    """Discover and convert rasters with bounded external-process concurrency."""
    input_value = args.input_directory if args.input_directory is not None else DEFAULT_INPUT_DIRECTORY
    patterns = args.patterns if args.patterns is not None else DEFAULT_PATTERNS
    recursive = args.recursive if args.recursive is not None else DEFAULT_RECURSIVE
    output_format = args.format if args.format is not None else DEFAULT_FORMAT
    keep_nodata = args.keep_nodata if args.keep_nodata is not None else DEFAULT_KEEP_NODATA
    overwrite = args.overwrite if args.overwrite is not None else DEFAULT_OVERWRITE
    workers = args.workers if args.workers is not None else DEFAULT_WORKERS
    dry_run = args.dry_run if args.dry_run is not None else DEFAULT_DRY_RUN
    input_directory = Path(input_value).resolve()
    target_directory = Path(args.working_directory).resolve() if args.working_directory else input_directory
    if not change_working_directory(target_dir=target_directory):
        return 1
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)

    if not input_directory.is_dir():
        logger.error("Input directory does not exist: {}", input_directory)
        return 1
    if workers < 1:
        logger.error("--workers must be at least 1.")
        return 2

    input_files = discover_rasters(input_directory, patterns, recursive=recursive)
    if not input_files:
        logger.warning("No files matching {} found in {}", patterns, input_directory)
        return 0

    jobs = [(input_path, input_path.with_suffix(f".{output_format}")) for input_path in input_files]
    outputs = [output.resolve() for _, output in jobs]
    if len(outputs) != len(set(outputs)):
        logger.error("Input patterns would create duplicate output paths.")
        return 1

    logger.info("Converting {} rasters with at most {} workers.", len(jobs), workers)
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                convert_raster,
                input_path,
                output_path,
                csv_output=output_format == "csv",
                skip_nodata=not keep_nodata,
                overwrite=overwrite,
                dry_run=dry_run,
            )
            for input_path, output_path in jobs
        ]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]

    successes = sum(results)
    logger.success("Conversion completed: {}/{} files successful.", successes, len(jobs))
    return 0 if successes == len(jobs) else 1


def _parse_cli_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert raster files to XYZ or CSV point data.")
    parser.add_argument("input_directory", nargs="?", type=Path, help="Override DEFAULT_INPUT_DIRECTORY.")
    parser.add_argument("--patterns", nargs="+", default=None, help="Override DEFAULT_PATTERNS.")
    parser.add_argument("--recursive", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--format", choices=["csv", "xyz"], default=None, help="Override DEFAULT_FORMAT.")
    parser.add_argument("--keep-nodata", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--overwrite", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--workers", type=int, default=None, help="Override DEFAULT_WORKERS.")
    parser.add_argument("--dry-run", action=argparse.BooleanOptionalAction, default=None)
    add_execution_cli_arguments(parser)
    return parser.parse_args(argv)


if __name__ == "__main__":
    cli_args = _parse_cli_arguments()
    with setup_logger(console_log_level=cli_args.console_log_level or "INFO", log_file="gdal_raster_to_xyz.log"):
        try:
            result = main(cli_args)
        except Exception:
            logger.exception("Wrapper failed.")
            result = 1
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, leading_blank_line=True)
    if not cli_args.no_pause:
        pause_console()
    raise SystemExit(result)
