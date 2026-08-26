"""Build and run grouped TUFLOW ``asc_to_asc`` median, maximum and difference commands."""

# moved from unsorted, not tested in production yet - 2026-08-20

from __future__ import annotations

from pathlib import Path

WRAPPER_VERSION = "2026-08-20.1"
DEFAULT_WORKING_DIR = Path(".")
DEFAULT_MODE = "max-median"
DEFAULT_INPUT_DIRECTORIES = [Path(".")]
DEFAULT_OUTPUT_DIR: Path | None = None
DEFAULT_SUFFIX = "d_Max"
DEFAULT_EXECUTABLE = "asc_to_asc_w64.exe"
DEFAULT_WORKERS = 4
DEFAULT_DRY_RUN = False

import argparse
import concurrent.futures
import re
import shutil
import subprocess

from loguru import logger

from ryan_library.classes.tuflow_string_classes import TuflowStringParser
from ryan_library.functions.path_stuff import to_single_path
from ryan_library.functions.wrapper_utils import (
    add_execution_cli_arguments,
    change_working_directory,
    pause_console,
    print_wrapper_banner,
)


class TUFLOWRaster:
    """Parsed filename fields used to group experimental ASC-to-ASC commands."""

    def __init__(self, full_path: Path, tp: str, duration: str, aep: str, suffix: str) -> None:
        self.full_path = full_path
        self.filename = full_path.stem
        self.tp = tp
        self.duration = duration
        self.aep = aep
        self.suffix = suffix


def trim_filename(filename: str, parts_to_remove: list[str]) -> str:
    """Remove complete underscore- or plus-delimited filename tokens."""
    removed = {part.casefold() for part in parts_to_remove if part}
    return "_".join(part for part in re.split(r"_|\+", filename) if part.casefold() not in removed)


def parse_tuflow_raster(file_path: Path, suffix: str) -> TUFLOWRaster | None:
    """Return grouping fields for a raster, or None when required tokens are absent."""
    parser = TuflowStringParser(file_path)
    if parser.tp is None or parser.duration is None or parser.aep is None:
        return None
    return TUFLOWRaster(
        full_path=file_path,
        tp=parser.tp.text_repr,
        duration=parser.duration.text_repr,
        aep=parser.aep.text_repr,
        suffix=suffix,
    )


def _discover_rasters(input_dir: Path, suffix: str) -> list[TUFLOWRaster]:
    rasters: list[TUFLOWRaster] = []
    for path in sorted(input_dir.rglob(f"*{suffix}.tif")):
        parsed = parse_tuflow_raster(path, suffix)
        if parsed is None:
            logger.warning("Skipping {}: missing TP, duration or AEP token.", path.name)
        else:
            rasters.append(parsed)
    return rasters


def build_max_median_commands(
    rasters: list[TUFLOWRaster], output_dir: Path, executable: str
) -> tuple[list[list[str]], list[list[str]]]:
    """Build median-across-TP and maximum-across-duration command stages."""
    median_groups: dict[tuple[str, str, str], list[TUFLOWRaster]] = {}
    aep_groups: dict[tuple[str, str], list[TUFLOWRaster]] = {}
    for raster in rasters:
        median_groups.setdefault((raster.aep, raster.duration, raster.suffix), []).append(raster)
        aep_groups.setdefault((raster.aep, raster.suffix), []).append(raster)

    median_commands: list[list[str]] = []
    for group in median_groups.values():
        example = group[0]
        starred_name = re.sub(re.escape(example.tp), "*", example.filename, flags=re.IGNORECASE) + ".tif"
        input_pattern = example.full_path.parent / starred_name
        output_name = trim_filename(example.filename, [example.tp]) + "_Median_Val.tif"
        median_commands.append(
            [executable, "-b", "-tif", "-out", str(output_dir / output_name), "-statMedian", str(input_pattern)]
        )

    maximum_commands: list[list[str]] = []
    for group in aep_groups.values():
        example = group[0]
        without_tp = trim_filename(example.filename, [example.tp])
        starred_name = re.sub(re.escape(example.duration), "*", without_tp, flags=re.IGNORECASE) + "_Median_Val.tif"
        output_name = trim_filename(example.filename, [example.tp, example.duration]) + ".tif"
        maximum_commands.append(
            [
                executable,
                "-b",
                "-tif",
                "-out",
                str(output_dir / output_name),
                "-statMax",
                str(output_dir / starred_name),
            ]
        )
    return median_commands, maximum_commands


def build_diff_commands(
    current_rasters: list[TUFLOWRaster],
    existing_rasters: list[TUFLOWRaster],
    output_dir: Path,
    executable: str,
) -> list[list[str]]:
    """Build difference commands for unambiguous scenario-name matches."""
    removable_parts = {
        value for raster in [*current_rasters, *existing_rasters] for value in (raster.tp, raster.aep, raster.duration)
    }

    existing_by_scenario: dict[str, list[TUFLOWRaster]] = {}
    for raster in existing_rasters:
        key = trim_filename(raster.filename, sorted(removable_parts)).casefold()
        existing_by_scenario.setdefault(key, []).append(raster)

    commands: list[list[str]] = []
    for current in current_rasters:
        key = trim_filename(current.filename, sorted(removable_parts)).casefold()
        matches = existing_by_scenario.get(key, [])
        if not matches:
            logger.warning("No existing-scenario match for {}", current.full_path)
            continue
        if len(matches) > 1:
            match_names = ", ".join(str(match.full_path) for match in matches)
            raise ValueError(f"Ambiguous existing-scenario matches for {current.full_path}: {match_names}")
        output_path = output_dir / f"{current.full_path.stem}_DIFF.tif"
        commands.append(
            [executable, "-b", "-out", str(output_path), "-dif", str(current.full_path), str(matches[0].full_path)]
        )
    return commands


def _execute_command(command: list[str]) -> bool:
    logger.debug("Command: {}", subprocess.list2cmdline(command))
    try:
        result = subprocess.run(command, check=False, text=True, capture_output=True)
    except OSError as error:
        logger.error("Could not execute ASC-to-ASC command: {}", error)
        return False
    if result.returncode != 0:
        logger.error("ASC-to-ASC failed with code {}: {}", result.returncode, result.stderr.strip())
        return False
    if result.stdout.strip():
        logger.debug("ASC-to-ASC output: {}", result.stdout.strip())
    return True


def _run_commands(commands: list[list[str]], *, dry_run: bool, workers: int) -> bool:
    if dry_run:
        for command in commands:
            logger.info("[DRY-RUN] {}", subprocess.list2cmdline(command))
        return True
    if not commands:
        logger.warning("No commands were generated.")
        return False

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        results = list(executor.map(_execute_command, commands))
    return all(results)


def _resolve_executable(value: str, *, dry_run: bool) -> str:
    candidate = Path(value)
    if candidate.parent != Path(".") or candidate.is_absolute():
        if not dry_run and not candidate.is_file():
            raise FileNotFoundError(f"ASC-to-ASC executable does not exist: {candidate}")
        return str(candidate)
    discovered = shutil.which(value)
    if discovered is None and not dry_run:
        raise FileNotFoundError(f"ASC-to-ASC executable was not found on PATH: {value}")
    return discovered or value


def main(args: argparse.Namespace, *, working_directory: Path | None = None) -> int:
    """Run the selected grouping workflow and return a process exit code."""
    target_directory = (working_directory or DEFAULT_WORKING_DIR).resolve()
    if not change_working_directory(target_dir=target_directory):
        return 1
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)

    mode = args.mode if args.mode is not None else DEFAULT_MODE
    input_values = args.input if args.input else DEFAULT_INPUT_DIRECTORIES
    output_value = args.output_dir if args.output_dir is not None else DEFAULT_OUTPUT_DIR
    suffix = args.suffix if args.suffix is not None else DEFAULT_SUFFIX
    executable_value = args.executable if args.executable is not None else DEFAULT_EXECUTABLE
    workers = args.workers if args.workers is not None else DEFAULT_WORKERS
    dry_run = args.dry_run if args.dry_run is not None else DEFAULT_DRY_RUN
    if workers < 1:
        logger.error("--workers must be at least 1.")
        return 2
    executable = _resolve_executable(executable_value, dry_run=dry_run)
    input_directories = [to_single_path(value) for value in input_values]
    if any(not directory.is_dir() for directory in input_directories):
        logger.error("Every input must be an existing directory: {}", input_directories)
        return 1

    if mode == "max-median":
        if len(input_directories) != 1:
            logger.error("max-median mode requires exactly one input directory.")
            return 2
        output_dir = to_single_path(output_value) if output_value is not None else input_directories[0]
        output_dir.mkdir(parents=True, exist_ok=True)
        rasters = _discover_rasters(input_directories[0], suffix)
        if not rasters:
            logger.error("No valid rasters matched suffix {}", suffix)
            return 1
        median_commands, maximum_commands = build_max_median_commands(rasters, output_dir, executable)
        if not _run_commands(median_commands, dry_run=dry_run, workers=workers):
            return 1
        return 0 if _run_commands(maximum_commands, dry_run=dry_run, workers=workers) else 1

    if len(input_directories) != 2:
        logger.error("diff mode requires exactly two input directories.")
        return 2
    output_dir = to_single_path(output_value) if output_value is not None else input_directories[0] / "diff_output"
    output_dir.mkdir(parents=True, exist_ok=True)
    commands = build_diff_commands(
        _discover_rasters(input_directories[0], suffix),
        _discover_rasters(input_directories[1], suffix),
        output_dir,
        executable,
    )
    return 0 if _run_commands(commands, dry_run=dry_run, workers=workers) else 1


def _parse_cli_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=f"Automate ASC-to-ASC operations across grouped rasters (v{WRAPPER_VERSION})."
    )
    parser.add_argument("--mode", choices=["max-median", "diff"], default=None)
    parser.add_argument("input", nargs="*", type=Path, help="Override DEFAULT_INPUT_DIRECTORIES.")
    parser.add_argument("--output-dir", "-o", help="Output directory.")
    parser.add_argument("--suffix", default=None, help="Override DEFAULT_SUFFIX.")
    parser.add_argument("--executable", default=None, help="Override DEFAULT_EXECUTABLE.")
    parser.add_argument("--workers", type=int, default=None, help="Override DEFAULT_WORKERS.")
    parser.add_argument("--dry-run", action="store_const", const=True, default=None)
    add_execution_cli_arguments(parser)
    return parser.parse_args(argv)


if __name__ == "__main__":
    cli_args = _parse_cli_arguments()
    try:
        result = main(cli_args, working_directory=cli_args.working_directory)
    except Exception:
        logger.exception("Wrapper failed.")
        result = 1
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, leading_blank_line=True)
    if not cli_args.no_pause:
        pause_console(collect_before_pause=True)
    raise SystemExit(result)
