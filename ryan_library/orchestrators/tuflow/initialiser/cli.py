"""Installed TUFLOW initialiser command-line entry point.

Editable values below provide defaults; explicit CLI arguments take precedence,
including --no-* switches overriding values set to True. Paths and list-based
selections remain editable without changing library workflow code.
"""

import argparse
from pathlib import Path

from rasterio.crs import CRS

from .catchment_workflow import CatchmentConfig, build_catchments
from .project_setup import TuflowProjectConfig, initialize_tuflow_project
from .workflow import BuildConfig, build

# Editable process-level settings. Keep incomplete paths unset until configured.
OUTPUT_PATH: Path | None = None
PROJECT_NAME: str | None = None
SCENARIO_NAME: str | None = None
PROJECTION_FILE: Path | None = None
TUFLOW_EXECUTABLE: Path | None = None
UTILITY_SOURCE_ROOT: Path | None = None  # None selects the shared bundled utilities.
COPY_UTILITIES = True
OVERWRITE = False

TERRAIN_RASTERS: list[Path] = []  # Highest-priority terrain first.
EXCLUSION_POLYGONS: list[Path] = []
AOI_FILE: Path | None = None
INTEREST_FILE: Path | None = None
RAINFALL_NAME: str | None = None
ANALYSE_EXCLUSIONS = False
ANALYSIS_RESOLUTION_M: float | None = None
ANALYSIS_BOUNDS: list[float] | None = None  # [west, south, east, north] in ANALYSIS_CRS.
MAX_ANALYSIS_CELLS = 100_000_000
GRASS_EXECUTABLE = "grass"
BUFFER_M = 500.0
STREAM_AREA_M2 = 100_000.0
FILL_DEPRESSIONS = False
MAX_FILL_DEPTH_M = 2.0
GENERATE_CATCHMENTS = False

TGC_FILE: Path | None = None
TGC_SCENARIOS: list[str] = []
TGC_GLOBAL_ONLY = False
ANALYSIS_CRS: str | None = None
SNAP_DISTANCE_M = 0.0
ALLOW_PARTIAL = False


def _configured[T](cli_value: T | None, editable_value: T) -> T:
    """Return an explicit CLI override or the editable entry-point setting."""
    return editable_value if cli_value is None else cli_value


def _required_value[T](value: T | None, option: str) -> T:
    """Narrow required editable/CLI values after argparse processing."""
    if value is None:
        msg = f"Missing required initialiser option: {option}"
        raise ValueError(msg)
    return value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Initialise 2D rainfall-on-grid TUFLOW models")
    commands = parser.add_subparsers(dest="command", required=True)

    init = commands.add_parser("init", help="Create empty TUFLOW project from canonical templates")
    init.add_argument("--output", type=Path, required=OUTPUT_PATH is None)
    init.add_argument("--name", required=PROJECT_NAME is None)
    init.add_argument("--scenario", required=SCENARIO_NAME is None)
    init.add_argument("--prj", type=Path, required=PROJECTION_FILE is None)
    init.add_argument("--tuflow-exe", type=Path, required=TUFLOW_EXECUTABLE is None)
    init.add_argument("--ryan-tools-scripts", type=Path)
    init.add_argument("--copy-utilities", action=argparse.BooleanOptionalAction, default=None)
    init.add_argument("--overwrite", action=argparse.BooleanOptionalAction, default=None)

    make = commands.add_parser("build", help="Build a project from DEM/AOI via GRASS and canonical TUFLOW GIS")
    make.add_argument(
        "--terrain",
        required=not TERRAIN_RASTERS,
        type=Path,
        action="append",
        help="Repeat highest-to-lowest priority (for example LiDAR then SRTM)",
    )
    make.add_argument("--exclude", type=Path, action="append", help="Optional upstream exclusions; repeatable")
    make.add_argument("--analyse-exclusions", action=argparse.BooleanOptionalAction, default=None)
    make.add_argument("--analysis-resolution-m", type=float)
    make.add_argument("--max-analysis-cells", type=int)
    make.add_argument("--aoi", type=Path, required=AOI_FILE is None)
    make.add_argument("--output", type=Path, required=OUTPUT_PATH is None)
    make.add_argument("--name", required=PROJECT_NAME is None)
    make.add_argument("--scenario", required=SCENARIO_NAME is None)
    make.add_argument("--prj", type=Path, required=PROJECTION_FILE is None)
    make.add_argument("--tuflow-exe", type=Path, required=TUFLOW_EXECUTABLE is None)
    make.add_argument("--grass-exe")
    make.add_argument("--buffer-m", type=float)
    make.add_argument("--stream-area-m2", type=float)
    make.add_argument("--rainfall-name", required=RAINFALL_NAME is None)
    make.add_argument("--fill-depressions", action=argparse.BooleanOptionalAction, default=None)
    make.add_argument("--max-fill-depth-m", type=float)
    make.add_argument("--ryan-tools-scripts", type=Path)
    make.add_argument("--copy-utilities", action=argparse.BooleanOptionalAction, default=None)
    make.add_argument("--catchments", action=argparse.BooleanOptionalAction, default=None)

    catchments = commands.add_parser("catchments", help="Write catchment GeoPackage only; no TUFLOW project")
    terrain_source = catchments.add_mutually_exclusive_group(required=not TERRAIN_RASTERS and TGC_FILE is None)
    terrain_source.add_argument("--terrain", type=Path, action="append")
    terrain_source.add_argument("--tgc", type=Path, help="TGC parsed by PyTUFLOW for Read Grid Zpts")
    catchments.add_argument("--tgc-scenario", action="append", help="Active TGC scenario; repeatable")
    catchments.add_argument("--tgc-global-only", action=argparse.BooleanOptionalAction, default=None)
    catchments.add_argument("--interest", type=Path, required=INTEREST_FILE is None)
    catchments.add_argument("--output", type=Path, required=OUTPUT_PATH is None)
    catchments.add_argument("--crs", required=ANALYSIS_CRS is None)
    catchments.add_argument("--analysis-resolution-m", type=float)
    catchments.add_argument("--analysis-bounds", type=float, nargs=4, metavar=("WEST", "SOUTH", "EAST", "NORTH"))
    catchments.add_argument("--max-analysis-cells", type=int)
    catchments.add_argument("--snap-distance-m", type=float)
    catchments.add_argument("--allow-partial", action=argparse.BooleanOptionalAction, default=None)
    catchments.add_argument("--grass-exe")
    catchments.add_argument("--fill-depressions", action=argparse.BooleanOptionalAction, default=None)
    return parser


def main() -> int:
    """Parse CLI inputs and call a workflow; only this process boundary can exit."""
    parser = _parser()
    args = parser.parse_args()
    if args.command == "init":
        initialize_tuflow_project(
            TuflowProjectConfig(
                output_dir=_required_value(_configured(args.output, OUTPUT_PATH), "--output"),
                project_name=_required_value(_configured(args.name, PROJECT_NAME), "--name"),
                scenario_name=_required_value(_configured(args.scenario, SCENARIO_NAME), "--scenario"),
                prj_file=_required_value(_configured(args.prj, PROJECTION_FILE), "--prj"),
                tuflow_executable=_required_value(_configured(args.tuflow_exe, TUFLOW_EXECUTABLE), "--tuflow-exe"),
                copy_utilities=_configured(args.copy_utilities, COPY_UTILITIES),
                utility_source_root=_configured(args.ryan_tools_scripts, UTILITY_SOURCE_ROOT),
                overwrite=_configured(args.overwrite, OVERWRITE),
            )
        )
    elif args.command == "build":
        build(
            BuildConfig(
                terrains=tuple(_configured(args.terrain, TERRAIN_RASTERS)),
                aoi=_required_value(_configured(args.aoi, AOI_FILE), "--aoi"),
                output=_required_value(_configured(args.output, OUTPUT_PATH), "--output"),
                exclusions=tuple(_configured(args.exclude, EXCLUSION_POLYGONS)),
                analyse_exclusions=_configured(args.analyse_exclusions, ANALYSE_EXCLUSIONS),
                analysis_resolution_m=_configured(args.analysis_resolution_m, ANALYSIS_RESOLUTION_M),
                max_analysis_cells=_configured(args.max_analysis_cells, MAX_ANALYSIS_CELLS),
                name=_required_value(_configured(args.name, PROJECT_NAME), "--name"),
                scenario=_required_value(_configured(args.scenario, SCENARIO_NAME), "--scenario"),
                prj=_required_value(_configured(args.prj, PROJECTION_FILE), "--prj"),
                tuflow_executable=_required_value(_configured(args.tuflow_exe, TUFLOW_EXECUTABLE), "--tuflow-exe"),
                rainfall_name=_required_value(_configured(args.rainfall_name, RAINFALL_NAME), "--rainfall-name"),
                grass_executable=_configured(args.grass_exe, GRASS_EXECUTABLE),
                buffer_m=_configured(args.buffer_m, BUFFER_M),
                stream_threshold_m2=_configured(args.stream_area_m2, STREAM_AREA_M2),
                fill_depressions=_configured(args.fill_depressions, FILL_DEPRESSIONS),
                max_fill_depth_m=_configured(args.max_fill_depth_m, MAX_FILL_DEPTH_M),
                utility_source_root=_configured(args.ryan_tools_scripts, UTILITY_SOURCE_ROOT),
                copy_utilities=_configured(args.copy_utilities, COPY_UTILITIES),
                generate_catchments=_configured(args.catchments, GENERATE_CATCHMENTS),
            )
        )
    elif args.command == "catchments":
        if args.terrain is not None:
            source_rasters, source_tgc = args.terrain, None
        elif args.tgc is not None:
            source_rasters, source_tgc = [], args.tgc
        else:
            source_rasters, source_tgc = TERRAIN_RASTERS, TGC_FILE
            if source_rasters and source_tgc is not None:
                parser.error("Set either TERRAIN_RASTERS or TGC_FILE, not both")
        bounds = _configured(args.analysis_bounds, ANALYSIS_BOUNDS)
        if bounds is not None and len(bounds) != 4:
            parser.error("ANALYSIS_BOUNDS must have exactly four coordinates")
        build_catchments(
            CatchmentConfig(
                terrains=tuple(source_rasters),
                interest=_required_value(_configured(args.interest, INTEREST_FILE), "--interest"),
                output=_required_value(_configured(args.output, OUTPUT_PATH), "--output"),
                target_crs=CRS.from_string(_required_value(_configured(args.crs, ANALYSIS_CRS), "--crs")),
                tgc=source_tgc,
                tgc_scenarios=tuple(_configured(args.tgc_scenario, TGC_SCENARIOS)),
                tgc_global_only=_configured(args.tgc_global_only, TGC_GLOBAL_ONLY),
                analysis_bounds=(bounds[0], bounds[1], bounds[2], bounds[3]) if bounds is not None else None,
                analysis_resolution_m=_configured(args.analysis_resolution_m, ANALYSIS_RESOLUTION_M),
                max_analysis_cells=_configured(args.max_analysis_cells, MAX_ANALYSIS_CELLS),
                grass_executable=_configured(args.grass_exe, GRASS_EXECUTABLE),
                fill_depressions=_configured(args.fill_depressions, FILL_DEPRESSIONS),
                snap_distance_m=_configured(args.snap_distance_m, SNAP_DISTANCE_M),
                allow_partial=_configured(args.allow_partial, ALLOW_PARTIAL),
            )
        )
    else:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
