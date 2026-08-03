r"""Convert LAZ point clouds to LAS using bounded, chunked processing.

Files are streamed in chunks so a large point cloud is not loaded wholly into
memory. Directory structure is preserved when recursive discovery is enabled.

Examples::

    python laz_to_las.py "D:\PointClouds" --output-directory "D:\PointClouds\LAS"
    python laz_to_las.py "D:\PointClouds" --recursive --workers 2 --overwrite

Common scenario::

    # Convert one source folder into a separate LAS folder.
    python laz_to_las.py "D:\Classified_LAZ" --output-directory "D:\Classified_LAS"
"""

from __future__ import annotations

from pathlib import Path

WRAPPER_VERSION = "2026-08-02.3"

WORKING_DIR: Path = Path(__file__).resolve().parent
OUTPUT_DIRECTORY: Path | None = None
CONSOLE_LOG_LEVEL = "INFO"
RECURSIVE = False
WORKERS: int | None = 1
CHUNK_SIZE = 1_000_000
OVERWRITE = False

import argparse

from loguru import logger

from ryan_library.functions.lidar_processing import convert_laz_directory
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.wrapper_utils import (
    change_working_directory,
    pause_console,
    print_wrapper_banner,
)


def main(
    *,
    working_directory: Path | None = None,
    output_directory: Path | None = None,
    console_log_level: str | None = None,
    recursive: bool | None = None,
    workers: int | None = None,
    chunk_size: int | None = None,
    overwrite: bool | None = None,
) -> int:
    """Resolve wrapper settings and convert discovered LAZ files through the shared library."""
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    target_directory = (working_directory or WORKING_DIR).resolve()
    configured_output = output_directory or OUTPUT_DIRECTORY or target_directory / "LAS"
    resolved_output = configured_output.resolve()
    if not change_working_directory(target_dir=target_directory):
        return 1
    with setup_logger(console_log_level=console_log_level or CONSOLE_LOG_LEVEL):
        try:
            outputs = convert_laz_directory(
                target_directory,
                resolved_output,
                recursive=RECURSIVE if recursive is None else recursive,
                workers=workers if workers is not None else WORKERS,
                chunk_size=chunk_size or CHUNK_SIZE,
                overwrite=OVERWRITE if overwrite is None else overwrite,
            )
            logger.info(f"Converted or retained {len(outputs)} LAS file(s) in: {resolved_output}")
        except Exception:
            logger.exception("LAZ-to-LAS conversion failed.")
            return 1
    return 0


def _parse_cli_arguments() -> argparse.Namespace:
    """Parse optional command-line overrides for the editable constants."""
    parser = argparse.ArgumentParser(
        description="Convert LAZ files to uncompressed LAS using chunked I/O.",
        epilog=r"""Processing scenario:
  python laz_to_las.py "D:\Classified_LAZ" --output-directory "D:\Classified_LAS"

This streams points through laspy/lazrs and preserves the source directory
tree when --recursive is used.""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("directory", nargs="?", type=Path, help="Directory containing LAZ files.")
    parser.add_argument("--output-directory", type=Path, help="Default: an LAS subdirectory under the input.")
    parser.add_argument("--console-log-level")
    parser.add_argument("--recursive", action="store_true", default=None)
    parser.add_argument("--workers", type=int, help="Concurrent files; default: 1 to limit disk contention.")
    parser.add_argument("--chunk-size", type=int, help="Points transferred per chunk; default: 1000000.")
    parser.add_argument("--overwrite", action="store_true", default=None)
    parser.add_argument("--no-pause", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_cli_arguments()
    result = main(
        working_directory=args.directory,
        output_directory=args.output_directory,
        console_log_level=args.console_log_level,
        recursive=args.recursive,
        workers=args.workers,
        chunk_size=args.chunk_size,
        overwrite=args.overwrite,
    )
    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not args.no_pause:
        pause_console()
    raise SystemExit(result)
