r"""Audit GeoTIFF compression and optionally replace files with verified copies.

Examples::

    python recompress_geotiffs.py "D:\Model\Results"
    python recompress_geotiffs.py "D:\Model\Results" --replace
    python recompress_geotiffs.py "Q:\Project\Results" --replace --staging-directory "D:\RasterStage"
"""

from __future__ import annotations

from pathlib import Path

WRAPPER_VERSION = "2026-10-15.1"
WORKING_DIR = Path(__file__).absolute().parent
PROFILE = "tuflow"
STRATEGY = "auto"
RECURSIVE = True

import argparse

from loguru import logger

from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.wrapper_utils import (
    change_working_directory,
    pause_console,
    print_wrapper_banner,
)
from ryan_library.orchestrators.gdal.raster_recompression import (
    audit_geotiffs,
    recompress_geotiffs,
)


def main(
    *,
    working_directory: Path | None = None,
    replace: bool = False,
    assume_yes: bool = False,
    recursive: bool = RECURSIVE,
    profile: str = PROFILE,
    strategy: str = STRATEGY,
    staging_directory: Path | None = None,
) -> int:
    """Audit by default; replace only after explicit confirmation."""
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    target = (working_directory or WORKING_DIR).absolute()
    if not change_working_directory(target_dir=target):
        return 1
    with setup_logger(console_log_level="INFO"):
        try:
            inspections = audit_geotiffs(target, recursive=recursive)
            uncompressed = [item for item in inspections if item.compression == "NONE"]
            logger.info(
                "Audited {} TIFF(s): {} uncompressed",
                len(inspections),
                len(uncompressed),
            )
            for item in uncompressed:
                eligibility = "eligible" if item.stream_eligible else item.stream_ineligible_reason
                logger.info("UNCOMPRESSED {} (stream: {})", item.path, eligibility)
            if not replace or not uncompressed:
                return 0
            prompt = f"Type REPLACE to replace {len(uncompressed)} verified file(s): "
            if not assume_yes and input(prompt).strip() != "REPLACE":
                logger.warning("Replacement cancelled.")
                return 1
            results = recompress_geotiffs(
                target,
                recursive=recursive,
                profile=profile,  # type: ignore[arg-type]
                strategy=strategy,  # type: ignore[arg-type]
                staging_directory=staging_directory,
            )
            replaced = sum(result.status == "replaced" for result in results)
            failed = sum(result.status == "failed" for result in results)
            saved = sum(result.original_bytes - result.final_bytes for result in results if result.status == "replaced")
            logger.info(
                "Complete: {} replaced, {} failed, {:.2f} MiB saved",
                replaced,
                failed,
                saved / 1024**2,
            )
            return 1 if failed else 0
        except Exception:  # noqa: BLE001 - top-level CLI converts failures to exit status
            logger.exception("GeoTIFF audit or recompression failed.")
            return 1


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit and safely recompress GeoTIFFs without overviews.")
    parser.add_argument("directory", nargs="?", type=Path)
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Replace uncompressed sources after exact verification.",
    )
    parser.add_argument("--yes", action="store_true", help="Confirm replacement non-interactively.")
    parser.add_argument("--strategy", choices=("auto", "stream", "translate"), default=STRATEGY)
    parser.add_argument("--profile", choices=("tuflow", "efficient"), default=PROFILE)
    parser.add_argument(
        "--staging-directory",
        type=Path,
        help="Local workspace for safe Translate and comparison.",
    )
    parser.add_argument("--no-recursive", action="store_false", dest="recursive", default=RECURSIVE)
    parser.add_argument("--no-pause", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_cli_arguments()
    result = main(
        working_directory=args.directory,
        replace=args.replace,
        assume_yes=args.yes,
        recursive=args.recursive,
        profile=args.profile,
        strategy=args.strategy,
        staging_directory=args.staging_directory,
    )
    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not args.no_pause:
        pause_console()
    raise SystemExit(result)
