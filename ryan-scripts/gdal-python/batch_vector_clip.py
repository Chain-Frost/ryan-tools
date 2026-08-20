"""Experimentally replace ``ogr2ogr_clipper.bat`` with bounded, checked clipping jobs."""

# moved from unsorted, not tested in production yet - 2026-08-20

from __future__ import annotations

from pathlib import Path

WRAPPER_VERSION = "2026-08-20.1"
DEFAULT_INPUTS = [Path("input.shp")]
DEFAULT_EXTENTS = [Path("extent.shp")]
DEFAULT_OUTPUT_DIR: Path | None = None
DEFAULT_WORKERS = 4
DEFAULT_EXECUTABLE = "ogr2ogr"
DEFAULT_OVERWRITE = False
DEFAULT_DRY_RUN = False

import argparse
import concurrent.futures
import shutil
import subprocess
from uuid import uuid4

from loguru import logger

from ryan_library.functions.path_stuff import to_path_list, to_single_path
from ryan_library.functions.wrapper_utils import pause_console, print_wrapper_banner


def _dataset_members(path: Path) -> list[Path]:
    """Return files belonging to an output dataset, including Shapefile sidecars."""
    if path.suffix.casefold() == ".shp":
        return sorted(candidate for candidate in path.parent.glob(f"{path.stem}.*") if candidate.is_file())
    return [path] if path.exists() else []


def _remove_dataset(path: Path) -> None:
    for member in _dataset_members(path):
        member.unlink(missing_ok=True)


def _promote_dataset(temporary_path: Path, output_path: Path, *, overwrite: bool) -> None:
    existing = _dataset_members(output_path)
    if existing and not overwrite:
        raise FileExistsError(f"Output already exists: {output_path}")
    if overwrite:
        _remove_dataset(output_path)

    temporary_members = _dataset_members(temporary_path)
    if not temporary_members:
        raise RuntimeError(f"ogr2ogr did not create an output dataset: {temporary_path}")
    moved: list[Path] = []
    try:
        for member in temporary_members:
            suffix_text = member.name[len(temporary_path.stem) :]
            destination = output_path.with_name(f"{output_path.stem}{suffix_text}")
            member.replace(destination)
            moved.append(destination)
    except Exception:
        for member in moved:
            member.unlink(missing_ok=True)
        raise


def _temporary_dataset(output_path: Path) -> Path:
    return output_path.with_name(f".{output_path.stem}.{uuid4().hex}.tmp{output_path.suffix}")


def clip_vector(
    input_path: Path,
    extent_path: Path,
    output_path: Path,
    *,
    executable: str,
    overwrite: bool,
    dry_run: bool,
) -> bool:
    """Run one ogr2ogr clip through a temporary dataset and promote it on success."""
    if _dataset_members(output_path) and not overwrite:
        logger.warning("Output exists; skipping without --overwrite: {}", output_path)
        return True

    temporary_path = _temporary_dataset(output_path)
    command = [executable, str(temporary_path), str(input_path), "-clipsrc", str(extent_path)]
    if dry_run:
        logger.info("[DRY-RUN] {}", subprocess.list2cmdline(command))
        return True

    try:
        result = subprocess.run(command, check=False, text=True, capture_output=True)
        if result.returncode != 0:
            logger.error(
                "ogr2ogr failed for {} with {} using code {}: {}",
                input_path,
                extent_path,
                result.returncode,
                result.stderr.strip(),
            )
            return False
        _promote_dataset(temporary_path, output_path, overwrite=overwrite)
        logger.success("Created {}", output_path)
        return True
    except OSError as error:
        logger.error("Could not execute ogr2ogr for {}: {}", input_path, error)
        return False
    except Exception:
        logger.exception("Could not finalise clipped output {}", output_path)
        return False
    finally:
        _remove_dataset(temporary_path)


def _resolve_executable(value: str, *, dry_run: bool) -> str:
    candidate = Path(value)
    if candidate.parent != Path(".") or candidate.is_absolute():
        if not dry_run and not candidate.is_file():
            raise FileNotFoundError(f"ogr2ogr executable does not exist: {candidate}")
        return str(candidate)
    discovered = shutil.which(value)
    if discovered is None and not dry_run:
        raise FileNotFoundError(f"ogr2ogr was not found on PATH: {value}")
    return discovered or value


def build_output_jobs(inputs: list[Path], extents: list[Path], output_dir: Path) -> list[tuple[Path, Path, Path]]:
    jobs = [
        (input_path, extent_path, output_dir / f"{input_path.stem}_clip_{extent_path.stem}{input_path.suffix}")
        for input_path in inputs
        for extent_path in extents
    ]
    outputs = [output_path.resolve() for _, _, output_path in jobs]
    if len(outputs) != len(set(outputs)):
        raise ValueError("Input or extent names would create duplicate output paths")
    return jobs


def main(args: argparse.Namespace) -> int:
    """Validate inputs, execute bounded clipping jobs and report partial failure."""
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    input_values = args.inputs if args.inputs else DEFAULT_INPUTS
    extent_values = args.extents if args.extents else DEFAULT_EXTENTS
    output_value = args.output_dir if args.output_dir is not None else DEFAULT_OUTPUT_DIR
    executable_value = args.executable if args.executable is not None else DEFAULT_EXECUTABLE
    workers = args.workers if args.workers is not None else DEFAULT_WORKERS
    overwrite = args.overwrite if args.overwrite is not None else DEFAULT_OVERWRITE
    dry_run = args.dry_run if args.dry_run is not None else DEFAULT_DRY_RUN
    if workers < 1:
        logger.error("--workers must be at least 1.")
        return 2

    inputs = to_path_list(input_values)
    extents = to_path_list(extent_values)
    invalid = [path for path in [*inputs, *extents] if not path.is_file()]
    if invalid:
        logger.error("Input dataset does not exist: {}", invalid[0])
        return 1

    output_dir = to_single_path(output_value) if output_value is not None else inputs[0].parent
    if not dry_run:
        output_dir.mkdir(parents=True, exist_ok=True)
    executable = _resolve_executable(executable_value, dry_run=dry_run)
    jobs = build_output_jobs(inputs, extents, output_dir)
    logger.info("Prepared {} clipping jobs with at most {} workers.", len(jobs), workers)

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                clip_vector,
                input_path,
                extent_path,
                output_path,
                executable=executable,
                overwrite=overwrite,
                dry_run=dry_run,
            )
            for input_path, extent_path, output_path in jobs
        ]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]

    successes = sum(results)
    logger.success("Clipping completed: {}/{} jobs successful.", successes, len(jobs))
    return 0 if successes == len(jobs) else 1


def _parse_cli_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=f"Clip vector datasets by multiple extent datasets (v{WRAPPER_VERSION})."
    )
    parser.add_argument("inputs", nargs="*", type=Path, help="Override DEFAULT_INPUTS.")
    parser.add_argument("--extents", "-e", nargs="+", type=Path, default=None, help="Override DEFAULT_EXTENTS.")
    parser.add_argument("--output-dir", "-o", help="Output directory; defaults to the first input directory.")
    parser.add_argument("--executable", default=None, help="Override DEFAULT_EXECUTABLE.")
    parser.add_argument("--workers", type=int, default=None, help="Override DEFAULT_WORKERS.")
    parser.add_argument(
        "--overwrite", action="store_const", const=True, default=None, help="Override DEFAULT_OVERWRITE."
    )
    parser.add_argument("--dry-run", action="store_const", const=True, default=None, help="Override DEFAULT_DRY_RUN.")
    parser.add_argument("--no-pause", action="store_true", help="Do not pause when the wrapper finishes.")
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
        pause_console(collect_before_pause=True)
    raise SystemExit(result)
