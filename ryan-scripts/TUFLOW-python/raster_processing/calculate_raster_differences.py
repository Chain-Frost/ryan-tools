"""Calculate differences between a list of rasters and a single base raster.

Edit the defaults below for ordinary copied-wrapper use. Command-line values
override those defaults when supplied.
"""

from __future__ import annotations

from pathlib import Path
from typing import NamedTuple, Any, cast

WRAPPER_VERSION = "2026-08-22.1"

DEFAULT_WORKING_DIR = Path(".")
DEFAULT_CURRENT_RASTERS: list[Path] = [Path("MDHR_01.0p_0360m_TP10_Post_v03_d_HR_Max.tif")]
DEFAULT_SUBTRACT_RASTER: Path = Path("MDHR_01.0p_0360m_TP07_Pre_v03_d_HR_Max.tif")
DEFAULT_CHANGE = True
DEFAULT_NO_WET_DRY = False
DEFAULT_COMBINE_WD = True
DEFAULT_COMPARE_ASC2ASC = False
DEFAULT_CREATION_OPTIONS: list[str] = []
DEFAULT_PAUSE = True

import argparse
import subprocess
import sys
import time

import numpy as np
import rasterio  # pyright: ignore[reportMissingTypeStubs]
from loguru import logger

from ryan_library.functions.tuflow.asc_to_asc_raster_operations import compute_diff
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_wrapper_banner


class WrapperConfiguration(NamedTuple):
    current_rasters: list[str]
    subtract_raster: str
    change: bool
    no_wet_dry: bool
    combine_wd: bool
    compare_asc2asc: bool
    creation_options: list[str]
    pause: bool


def resolve_configuration(args: argparse.Namespace) -> WrapperConfiguration:
    """Apply explicit CLI overrides to the editable wrapper defaults."""
    current_rasters = args.current_rasters if args.current_rasters else DEFAULT_CURRENT_RASTERS
    subtract_raster = args.subtract_raster if args.subtract_raster is not None else DEFAULT_SUBTRACT_RASTER
    change = args.change if args.change is not None else DEFAULT_CHANGE
    no_wet_dry = args.no_wet_dry if args.no_wet_dry is not None else DEFAULT_NO_WET_DRY
    combine_wd = args.combine_wd if args.combine_wd is not None else DEFAULT_COMBINE_WD
    compare_asc2asc = args.compare_asc2asc if args.compare_asc2asc is not None else DEFAULT_COMPARE_ASC2ASC
    creation_options = args.creation_options if args.creation_options is not None else DEFAULT_CREATION_OPTIONS
    pause = DEFAULT_PAUSE and not args.no_pause
    return WrapperConfiguration(
        current_rasters=[str(Path(value)) for value in current_rasters],
        subtract_raster=str(Path(subtract_raster)),
        change=change,
        no_wet_dry=no_wet_dry,
        combine_wd=combine_wd,
        compare_asc2asc=compare_asc2asc,
        creation_options=list(creation_options),
        pause=pause,
    )


def compare_rasters(raster1_path: Path, raster2_path: Path) -> bool:
    """Compare two rasters to ensure their data arrays are almost exactly equal."""
    with (
        rasterio.open(raster1_path) as _src1,  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        rasterio.open(raster2_path) as _src2,  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
    ):
        src1 = cast(Any, _src1)
        src2 = cast(Any, _src2)
        data1 = np.asarray(src1.read(1), dtype=np.float64)
        data2 = np.asarray(src2.read(1), dtype=np.float64)

        nodata1 = float(src1.nodata) if src1.nodata is not None else None
        nodata2 = float(src2.nodata) if src2.nodata is not None else None

        valid1 = np.isfinite(data1)
        if nodata1 is not None and np.isfinite(nodata1):
            valid1 &= data1 != nodata1

        valid2 = np.isfinite(data2)
        if nodata2 is not None and np.isfinite(nodata2):
            valid2 &= data2 != nodata2

        if not np.array_equal(valid1, valid2):
            logger.warning("Rasters have different valid masks (nodata locations).")
            return False

        return bool(np.allclose(data1[valid1], data2[valid2], equal_nan=True))


def run_asc2asc(current_raster: Path, subtract_raster: Path, output_file: Path, change: bool, nowetdry: bool) -> None:
    """Run asc_to_asc.exe for comparison."""
    asc2asc_exe = r"C:\TUFLOW\asc_to_asc.2024-06-AB\asc_to_asc_w64.exe"
    command = [asc2asc_exe, "-b", "-out", str(output_file)]
    if change:
        command.append("-change")
    if nowetdry:
        command.append("-nowetdry")
    command.extend(["-dif", str(current_raster), str(subtract_raster)])

    logger.debug("Running asc2asc: {}", " ".join(command))
    result = subprocess.run(
        command,
        input="\n",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        errors="replace",
    )
    if result.returncode != 0:
        logger.error(f"asc_to_asc failed with return code {result.returncode}:\n{result.stdout}")
        raise RuntimeError("asc_to_asc execution failed.")


def process_raster(current_raster: str, configuration: WrapperConfiguration) -> None:
    """Process a single raster difference."""
    current_path = Path(current_raster)
    subtract_path = Path(configuration.subtract_raster)
    output_path = current_path.with_name(f"{current_path.stem}_DIFF{current_path.suffix}")
    creation_arguments = [item for option in configuration.creation_options for item in ("-co", option)]

    logger.info("Computing Python difference for {}...", current_path.name)
    start_py = time.perf_counter()
    compute_diff(
        file1=current_raster,
        file2=configuration.subtract_raster,
        output_file=str(output_path),
        change=configuration.change,
        nowetdry=configuration.no_wet_dry,
        combine_wd=configuration.combine_wd,
        extra_args=creation_arguments,
    )
    py_duration = time.perf_counter() - start_py
    logger.success("Python computation complete in {:.2f}s.", py_duration)

    if configuration.compare_asc2asc:
        asc2asc_output_path = current_path.with_name(f"{current_path.stem}_DIFF_asc2asc{current_path.suffix}")
        logger.info("Computing asc2asc difference for {}...", current_path.name)
        start_a2a = time.perf_counter()
        run_asc2asc(current_path, subtract_path, asc2asc_output_path, configuration.change, configuration.no_wet_dry)
        a2a_duration = time.perf_counter() - start_a2a
        logger.success("asc2asc computation complete in {:.2f}s.", a2a_duration)

        logger.info("Comparing Python results against asc2asc...")
        is_match = compare_rasters(output_path, asc2asc_output_path)
        if is_match:
            logger.success("Results match exactly between Python and asc2asc!")
        else:
            logger.error("Results differ between Python and asc2asc.")

        if not configuration.change and not configuration.no_wet_dry and not configuration.combine_wd:
            py_wd_path = current_path.with_name(f"{current_path.stem}_DIFF_wd{current_path.suffix}")
            a2a_wd_path = current_path.with_name(f"{current_path.stem}_DIFF_asc2asc_wd{current_path.suffix}")
            logger.info("Comparing wet/dry outputs...")
            is_wd_match = compare_rasters(py_wd_path, a2a_wd_path)
            if is_wd_match:
                logger.success("Wet/dry results match exactly!")
            else:
                logger.error("Wet/dry results differ.")


def main(args: argparse.Namespace, *, working_directory: Path | None = None) -> int:
    """Resolve configuration, run raster operations and return a process exit code."""
    target_directory = (working_directory or DEFAULT_WORKING_DIR).resolve()
    if not change_working_directory(target_dir=target_directory):
        return 1
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    configuration = resolve_configuration(args)

    if not Path(configuration.subtract_raster).is_file():
        logger.error(f"Subtract raster does not exist: {configuration.subtract_raster}")
        return 1

    try:
        for current_raster in configuration.current_rasters:
            if not Path(current_raster).is_file():
                logger.error(f"Current raster does not exist: {current_raster}")
                continue
            process_raster(current_raster, configuration)
    except Exception:
        logger.exception("Workflow failed.")
        return 1
    return 0


def _parse_cli_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Calculate raster differences against a single base raster.")
    parser.add_argument("--subtract", dest="subtract_raster", type=Path, help="Base raster to subtract.")
    parser.add_argument("-change", dest="change", action="store_true", default=None, help="Treat nodata as zero.")
    parser.add_argument("--no-change", dest="change", action="store_false")
    parser.add_argument("-nowetdry", dest="no_wet_dry", action="store_true", default=None, help="Skip wet/dry test.")
    parser.add_argument("--wet-dry", dest="no_wet_dry", action="store_false")
    parser.add_argument(
        "--combine-wd",
        dest="combine_wd",
        action="store_true",
        default=None,
        help="Combine WD values into difference raster.",
    )
    parser.add_argument("--separate-wd", dest="combine_wd", action="store_false")
    parser.add_argument(
        "--compare", dest="compare_asc2asc", action="store_true", default=None, help="Compare with asc_to_asc.exe."
    )
    parser.add_argument("--no-compare", dest="compare_asc2asc", action="store_false")
    parser.add_argument(
        "-co",
        dest="creation_options",
        action="append",
        default=None,
        help="Raster creation option NAME=VALUE; repeat as required.",
    )
    parser.add_argument("-b", "--no-pause", action="store_true", help="Do not pause when processing finishes.")
    parser.add_argument(
        "current_rasters", nargs="*", type=Path, help="Current rasters; override DEFAULT_CURRENT_RASTERS."
    )

    values = list(sys.argv[1:] if argv is None else argv)
    return parser.parse_args(values)


if __name__ == "__main__":
    cli_args = _parse_cli_arguments()
    result = main(cli_args)
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, leading_blank_line=True)
    if resolve_configuration(cli_args).pause:
        pause_console(collect_before_pause=True)
    raise SystemExit(result)
