r"""Review matching hourly TUFLOW HPC dt/Nu/Nc/Nd GeoTIFF outputs.

Copy this wrapper into a TUFLOW HourlyDt/grids folder, edit DEFAULT_* below,
then run with Python 3.14 and the matching installed ryan-tools package.

    py -3.14 create_hourly_dt_hotspots.py --dry-run --no-pause
    py -3.14 create_hourly_dt_hotspots.py --grid-dir "Q:\project\results\HourlyDt\grids" --no-pause

Outputs a GeoPackage with per-hour low-dt, per-hour near-control-limit and
cross-hour low-dt-union point layers. The whole-run *_dt_Min.tif is NOT used.
See HOURLY_DT_HOTSPOTS.md beside this wrapper for interpretation and examples.
"""

from __future__ import annotations

from pathlib import Path

WRAPPER_VERSION = "2026-10-28.1"

# Edit these values for a copied project wrapper. CLI arguments override them.
DEFAULT_GRID_DIR = Path(__file__).absolute().parent
DEFAULT_OUTPUT = Path("HourlyDt_hotspots.gpkg")  # Relative to the grid directory.
DEFAULT_PREFIX: str | None = None
DEFAULT_MIN_HOUR: int | None = None
DEFAULT_MAX_HOUR: int | None = None
DEFAULT_LOW_DT_SECONDS = 5.0
DEFAULT_CRITICAL_DT_SECONDS = 3.0
DEFAULT_NEAR_LIMIT_FRACTION = 0.90
DEFAULT_NU_LIMIT = 1.0
DEFAULT_NC_LIMIT = 1.0
DEFAULT_ND_LIMIT = 0.3
DEFAULT_CONNECTIVITY = 8
DEFAULT_MIN_COMPONENT_CELLS = 1
DEFAULT_CONSOLE_LOG_LEVEL = "INFO"

import argparse

from loguru import logger

from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.wrapper_utils import add_no_pause_cli_argument, pause_console, print_wrapper_banner
from ryan_library.orchestrators.tuflow.hourly_dt_hotspots import HourlyHotspotConfig, run_hourly_dt_hotspot_workflow


def _parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--grid-dir", type=Path, help="Directory containing one TUFLOW simulation's hourly TIF grids")
    parser.add_argument("--output", type=Path, help="Output GeoPackage (relative paths are within the grid directory)")
    parser.add_argument("--prefix", help="Select a simulation prefix when the directory contains multiple runs")
    parser.add_argument("--min-hour", type=int, help="First hourly output timestamp to process (inclusive)")
    parser.add_argument("--max-hour", type=int, help="Last hourly output timestamp to process (inclusive)")
    parser.add_argument("--low-dt", type=float, help="Low timestep threshold in seconds")
    parser.add_argument("--critical-dt", type=float, help="Critical timestep threshold in seconds")
    parser.add_argument("--near-limit", type=float, help="Fraction of each control-number limit treated as near")
    parser.add_argument("--nu-limit", type=float, help="Nu nominal limit (default 1.0)")
    parser.add_argument("--nc-limit", type=float, help="Nc nominal limit (default 1.0)")
    parser.add_argument("--nd-limit", type=float, help="Nd nominal limit (default 0.3)")
    parser.add_argument("--connectivity", type=int, choices=(4, 8), help="Connected-cell adjacency (4 or 8)")
    parser.add_argument("--min-component-cells", type=int, help="Minimum number of connected cells for a point")
    parser.add_argument(
        "--overwrite", action="store_true", help="Replace a preexisting output only after a successful run"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Validate raster names and metadata without writing output"
    )
    parser.add_argument("--console-log-level", choices=("DEBUG", "INFO", "SUCCESS", "WARNING"), help="Console detail")
    add_no_pause_cli_argument(parser=parser)
    return parser.parse_args()


def main(args: argparse.Namespace) -> int:
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    grid_dir = (args.grid_dir if args.grid_dir is not None else DEFAULT_GRID_DIR).absolute()
    output_choice = args.output if args.output is not None else DEFAULT_OUTPUT
    output = (output_choice if output_choice.is_absolute() else grid_dir / output_choice).absolute()
    config = HourlyHotspotConfig(
        grid_dir=grid_dir,
        output=output,
        prefix=args.prefix if args.prefix is not None else DEFAULT_PREFIX,
        min_hour=args.min_hour if args.min_hour is not None else DEFAULT_MIN_HOUR,
        max_hour=args.max_hour if args.max_hour is not None else DEFAULT_MAX_HOUR,
        low_dt=args.low_dt if args.low_dt is not None else DEFAULT_LOW_DT_SECONDS,
        critical_dt=args.critical_dt if args.critical_dt is not None else DEFAULT_CRITICAL_DT_SECONDS,
        near_limit_fraction=args.near_limit if args.near_limit is not None else DEFAULT_NEAR_LIMIT_FRACTION,
        control_limits={
            "Nu": args.nu_limit if args.nu_limit is not None else DEFAULT_NU_LIMIT,
            "Nc": args.nc_limit if args.nc_limit is not None else DEFAULT_NC_LIMIT,
            "Nd": args.nd_limit if args.nd_limit is not None else DEFAULT_ND_LIMIT,
        },
        connectivity=args.connectivity if args.connectivity is not None else DEFAULT_CONNECTIVITY,
        min_component_cells=(
            args.min_component_cells if args.min_component_cells is not None else DEFAULT_MIN_COMPONENT_CELLS
        ),
        overwrite=args.overwrite,
        dry_run=args.dry_run,
    )
    try:
        # The wrapper owns logging configuration; the reusable library only emits log records.
        with setup_logger(
            console_log_level=args.console_log_level or DEFAULT_CONSOLE_LOG_LEVEL,
            log_file=(str(output.with_suffix(".log")) if not config.dry_run and output.parent.is_dir() else None),
            file_log_level="DEBUG",
        ):
            try:
                run_hourly_dt_hotspot_workflow(config)
            except Exception:
                logger.exception("Hourly timestep hotspot workflow failed")
                return 1
    except Exception as error:
        print(f"ERROR: Could not configure logging: {error}")
        return 1
    return 0


if __name__ == "__main__":
    parsed = _parse_arguments()
    code = main(parsed)
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, leading_blank_line=True)
    if not parsed.no_pause:
        pause_console()
    raise SystemExit(code)
