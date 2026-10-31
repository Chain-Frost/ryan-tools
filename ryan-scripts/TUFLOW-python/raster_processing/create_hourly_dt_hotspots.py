r"""Analyse TUFLOW hourly raster or XMDF mesh control-number outputs.

GeoTIFF mode reads same-hour dt/Nu/Nc/Nd quartets. XMDF mode reads stored
mesh-vertex snapshots: Nu/Nc/Nd are required, dt is optional, and no
raster-cell connected areas are assigned to mesh nodes.

    py -3.14 create_hourly_dt_hotspots.py --grid-dir "Q:\project\results\HourlyDt\grids" --no-pause
    py -3.14 create_hourly_dt_hotspots.py --source xmdf --xmdf "Q:\project\results\run.xmdf" --xmdf-crs EPSG:7851 --dry-run --no-pause

Edit DEFAULT_* settings or use CLI overrides. The matching ryan-tools wheel is required.
"""

from __future__ import annotations

from pathlib import Path

WRAPPER_VERSION = "2026-10-28.2"

# Editable project settings. CLI values take precedence over these defaults.
DEFAULT_SOURCE = "tif"
DEFAULT_GRID_DIR = Path(__file__).absolute().parent
DEFAULT_OUTPUT = Path("HourlyDt_hotspots.gpkg")
DEFAULT_XMDF_FILES: tuple[Path, ...] = ()
DEFAULT_XMDF_OUTPUT = Path("XMDF_control_hotspots.gpkg")
DEFAULT_XMDF_CRS: str | None = None  # XMDF has no embedded CRS; supply EPSG code or WKT.
DEFAULT_XMDF_MESH: Path | None = None
DEFAULT_NU_DATASET: str | None = None
DEFAULT_NC_DATASET: str | None = None
DEFAULT_ND_DATASET: str | None = None
DEFAULT_DT_DATASET: str | None = None
DEFAULT_START_TIME_HR: float | None = None
DEFAULT_END_TIME_HR: float | None = None
DEFAULT_TIME_STRIDE = 1
DEFAULT_MAX_POINTS_PER_TYPE = 1000
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
from ryan_library.orchestrators.tuflow.xmdf_hotspots import XmdfHotspotConfig, run_xmdf_hotspot_workflow


def _parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", choices=("tif", "xmdf"), help="Input type (default: editable DEFAULT_SOURCE)")
    parser.add_argument("--grid-dir", type=Path, help="Hourly GeoTIFF grid directory (TIF mode only)")
    parser.add_argument("--xmdf", nargs="+", type=Path, help="One or more existing XMDF mesh result files")
    parser.add_argument("--xmdf-crs", help="Required CRS for XMDF mesh coordinates, e.g. EPSG:7851")
    parser.add_argument("--xmdf-mesh", type=Path, help="Associated .2dm file (one XMDF only; otherwise auto-find)")
    parser.add_argument("--nu-dataset", help="Explicit Nu XMDF data type name if auto-detection is ambiguous")
    parser.add_argument("--nc-dataset", help="Explicit Nc XMDF data type name if auto-detection is ambiguous")
    parser.add_argument("--nd-dataset", help="Explicit Nd XMDF data type name if auto-detection is ambiguous")
    parser.add_argument("--dt-dataset", help="Optional temporal dt XMDF data type (if actually stored)")
    parser.add_argument("--start-time", type=float, help="Earliest XMDF output time in model hours")
    parser.add_argument("--end-time", type=float, help="Latest XMDF output time in model hours")
    parser.add_argument("--time-stride", type=int, help="Sample every Nth stored XMDF output (default all)")
    parser.add_argument("--max-points-per-type", type=int, help="Maximum reported mesh vertices per type and time")
    parser.add_argument("--output", type=Path, help="Output GeoPackage (relative to input directory)")
    parser.add_argument("--prefix", help="Select a simulation prefix when the TIF directory contains multiple runs")
    parser.add_argument("--min-hour", type=int, help="First TIF hour to process (inclusive)")
    parser.add_argument("--max-hour", type=int, help="Last TIF hour to process (inclusive)")
    parser.add_argument("--low-dt", type=float, help="Low timestep threshold in seconds")
    parser.add_argument("--critical-dt", type=float, help="Critical TIF timestep threshold in seconds")
    parser.add_argument("--near-limit", type=float, help="Fraction of each control limit treated as near")
    parser.add_argument("--nu-limit", type=float, help="Nu nominal limit (default 1.0)")
    parser.add_argument("--nc-limit", type=float, help="Nc nominal limit (default 1.0)")
    parser.add_argument("--nd-limit", type=float, help="Nd nominal limit (default 0.3)")
    parser.add_argument("--connectivity", type=int, choices=(4, 8), help="TIF connected-cell adjacency")
    parser.add_argument("--min-component-cells", type=int, help="TIF minimum connected-component size")
    parser.add_argument("--overwrite", action="store_true", help="Explicitly replace a pre-existing output on success")
    parser.add_argument("--dry-run", action="store_true", help="Validate inputs/metadata without writing GeoPackage")
    parser.add_argument("--console-log-level", choices=("DEBUG", "INFO", "SUCCESS", "WARNING"), help="Console detail")
    add_no_pause_cli_argument(parser=parser)
    return parser.parse_args()


def _control_limits(args: argparse.Namespace) -> dict[str, float]:
    return {
        "Nu": args.nu_limit if args.nu_limit is not None else DEFAULT_NU_LIMIT,
        "Nc": args.nc_limit if args.nc_limit is not None else DEFAULT_NC_LIMIT,
        "Nd": args.nd_limit if args.nd_limit is not None else DEFAULT_ND_LIMIT,
    }


def _output_path(args: argparse.Namespace, base: Path, default_name: Path) -> Path:
    choice = args.output if args.output is not None else default_name
    return (choice if choice.is_absolute() else base / choice).absolute()


def _run_tif(args: argparse.Namespace) -> tuple[Path, HourlyHotspotConfig]:
    if args.xmdf is not None or any(
        value is not None
        for value in (
            args.xmdf_crs,
            args.xmdf_mesh,
            args.nu_dataset,
            args.nc_dataset,
            args.nd_dataset,
            args.dt_dataset,
            args.start_time,
            args.end_time,
            args.time_stride,
            args.max_points_per_type,
        )
    ):
        message = "XMDF options are not valid with --source tif"
        raise ValueError(message)
    grid_dir = (args.grid_dir if args.grid_dir is not None else DEFAULT_GRID_DIR).absolute()
    output = _output_path(args, grid_dir, DEFAULT_OUTPUT)
    config = HourlyHotspotConfig(
        grid_dir=grid_dir,
        output=output,
        prefix=args.prefix if args.prefix is not None else DEFAULT_PREFIX,
        min_hour=args.min_hour if args.min_hour is not None else DEFAULT_MIN_HOUR,
        max_hour=args.max_hour if args.max_hour is not None else DEFAULT_MAX_HOUR,
        low_dt=args.low_dt if args.low_dt is not None else DEFAULT_LOW_DT_SECONDS,
        critical_dt=args.critical_dt if args.critical_dt is not None else DEFAULT_CRITICAL_DT_SECONDS,
        near_limit_fraction=args.near_limit if args.near_limit is not None else DEFAULT_NEAR_LIMIT_FRACTION,
        control_limits=_control_limits(args),
        connectivity=args.connectivity if args.connectivity is not None else DEFAULT_CONNECTIVITY,
        min_component_cells=(
            args.min_component_cells if args.min_component_cells is not None else DEFAULT_MIN_COMPONENT_CELLS
        ),
        overwrite=args.overwrite,
        dry_run=args.dry_run,
    )
    return output, config


def _run_xmdf(args: argparse.Namespace) -> tuple[Path, XmdfHotspotConfig]:
    if any(
        value is not None
        for value in (
            args.grid_dir,
            args.prefix,
            args.min_hour,
            args.max_hour,
            args.connectivity,
            args.min_component_cells,
        )
    ):
        message = "TIF raster and connectivity options are not valid with --source xmdf"
        raise ValueError(message)
    paths: tuple[Path, ...] = tuple(
        path.absolute() for path in (args.xmdf if args.xmdf is not None else DEFAULT_XMDF_FILES)
    )
    if not paths:
        message = "Select one or more XMDF files with --xmdf or DEFAULT_XMDF_FILES"
        raise ValueError(message)
    crs = args.xmdf_crs if args.xmdf_crs is not None else DEFAULT_XMDF_CRS
    if not crs:
        message = "XMDF requires --xmdf-crs because the source does not contain CRS metadata"
        raise ValueError(message)
    mappings = {
        "Nu": args.nu_dataset if args.nu_dataset is not None else DEFAULT_NU_DATASET,
        "Nc": args.nc_dataset if args.nc_dataset is not None else DEFAULT_NC_DATASET,
        "Nd": args.nd_dataset if args.nd_dataset is not None else DEFAULT_ND_DATASET,
        "dt": args.dt_dataset if args.dt_dataset is not None else DEFAULT_DT_DATASET,
    }
    output = _output_path(args, paths[0].parent, DEFAULT_XMDF_OUTPUT)
    config = XmdfHotspotConfig(
        xmdf_files=paths,
        output=output,
        crs=crs,
        mesh_file=(
            args.xmdf_mesh.absolute()
            if args.xmdf_mesh is not None
            else DEFAULT_XMDF_MESH.absolute()
            if DEFAULT_XMDF_MESH is not None
            else None
        ),
        dataset_names={name: value for name, value in mappings.items() if value is not None},
        start_time_hr=args.start_time if args.start_time is not None else DEFAULT_START_TIME_HR,
        end_time_hr=args.end_time if args.end_time is not None else DEFAULT_END_TIME_HR,
        time_stride=args.time_stride if args.time_stride is not None else DEFAULT_TIME_STRIDE,
        low_dt=args.low_dt if args.low_dt is not None else DEFAULT_LOW_DT_SECONDS,
        near_limit_fraction=args.near_limit if args.near_limit is not None else DEFAULT_NEAR_LIMIT_FRACTION,
        control_limits=_control_limits(args),
        max_points_per_type=(
            args.max_points_per_type if args.max_points_per_type is not None else DEFAULT_MAX_POINTS_PER_TYPE
        ),
        overwrite=args.overwrite,
        dry_run=args.dry_run,
    )
    return output, config


def main(args: argparse.Namespace) -> int:
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    try:
        source = args.source if args.source is not None else DEFAULT_SOURCE
        if source == "xmdf":
            output, config = _run_xmdf(args)
        elif source == "tif":
            output, config = _run_tif(args)
        else:
            message = f"Unsupported source: {source}"
            raise ValueError(message)
        with setup_logger(
            console_log_level=args.console_log_level or DEFAULT_CONSOLE_LOG_LEVEL,
            log_file=(str(output.with_suffix(".log")) if not config.dry_run and output.parent.is_dir() else None),
            file_log_level="DEBUG",
        ):
            try:
                if isinstance(config, XmdfHotspotConfig):
                    run_xmdf_hotspot_workflow(config)
                else:
                    run_hourly_dt_hotspot_workflow(config)
            except Exception:  # noqa: BLE001 - Wrapper process boundary captures all workflow failures.
                logger.exception("TUFLOW hotspot workflow failed")
                return 1
    except Exception as error:  # noqa: BLE001 - Validate configuration and log failures at the process boundary.
        print(f"ERROR: {error}")
        return 1
    return 0


if __name__ == "__main__":
    parsed = _parse_arguments()
    code = main(parsed)
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, leading_blank_line=True)
    if not parsed.no_pause:
        pause_console()
    raise SystemExit(code)
