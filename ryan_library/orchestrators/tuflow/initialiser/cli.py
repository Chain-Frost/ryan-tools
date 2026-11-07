"""Standalone command-line interface; hydrology GUI is not required."""

import argparse
from pathlib import Path

from rasterio.crs import CRS

from .catchment_workflow import CatchmentConfig, build_catchments
from .project_setup import TuflowProjectConfig, initialize_tuflow_project
from .workflow import BuildConfig, build


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Initialise standalone 2D ROG TUFLOW models")
    commands = parser.add_subparsers(dest="command", required=True)

    init = commands.add_parser("init", help="Create empty TUFLOW project from canonical templates")
    init.add_argument("--output", required=True, type=Path)
    init.add_argument("--name", required=True)
    init.add_argument("--scenario", required=True)
    init.add_argument("--prj", required=True, type=Path)
    init.add_argument("--tuflow-exe", required=True, type=Path)
    init.add_argument("--ryan-tools-scripts", type=Path, default=None)
    init.add_argument("--overwrite", action="store_true")

    make = commands.add_parser("build", help="Build project from DEM/AOI via GRASS and canonical TUFLOW GIS")
    make.add_argument(
        "--terrain",
        required=True,
        type=Path,
        action="append",
        help="Repeat in highest-to-lowest priority order (e.g. LiDAR then SRTM)",
    )
    make.add_argument(
        "--exclude",
        type=Path,
        action="append",
        default=[],
        help="Optional upstream catchment polygon to remove from model; repeatable",
    )
    make.add_argument(
        "--analyse-exclusions",
        action="store_true",
        help="Include excluded catchments in detailed stream and terrain-characteristic analysis "
        "(default: skip detailed analysis while preserving required flow routing)",
    )
    make.add_argument(
        "--analysis-resolution-m",
        type=float,
        default=None,
        help="Hydrology raster resolution in metres; not the TUFLOW cell size",
    )
    make.add_argument("--aoi", required=True, type=Path, help="Vector AOI, in DEM CRS")
    make.add_argument("--output", required=True, type=Path)
    make.add_argument("--name", required=True)
    make.add_argument("--scenario", required=True)
    make.add_argument("--prj", required=True, type=Path)
    make.add_argument("--tuflow-exe", required=True, type=Path)
    make.add_argument("--grass-exe", default="grass")
    make.add_argument("--buffer-m", type=float, default=500)
    make.add_argument("--stream-area-m2", type=float, default=100000)
    make.add_argument("--rainfall-name", required=True)
    make.add_argument("--fill-depressions", action="store_true")
    make.add_argument("--max-fill-depth-m", type=float, default=2)
    make.add_argument("--ryan-tools-scripts", type=Path, default=None)
    make.add_argument(
        "--catchments",
        action="store_true",
        help="Also generate optional inflow and stream-reach catchment products",
    )

    catchments = commands.add_parser("catchments", help="Generate catchment polygons only; no TUFLOW project files")
    terrain_source = catchments.add_mutually_exclusive_group(required=True)
    terrain_source.add_argument("--terrain", type=Path, action="append")
    terrain_source.add_argument("--tgc", type=Path, help="TGC parsed by PyTUFLOW for Read Grid Zpts terrain")
    catchments.add_argument(
        "--tgc-scenario",
        action="append",
        default=[],
        help="Active TGC scenario; repeat for combined scenarios",
    )
    catchments.add_argument(
        "--tgc-global-only",
        action="store_true",
        help="Use only globally scoped TGC terrain and exclude all scenario branches",
    )
    catchments.add_argument(
        "--interest",
        required=True,
        type=Path,
        help="Point, line or polygon vector in the DEM CRS",
    )
    catchments.add_argument("--output", required=True, type=Path, help="New catchment GeoPackage")
    catchments.add_argument("--crs", required=True, help="Analysis CRS, for example EPSG:28351")
    catchments.add_argument("--analysis-resolution-m", type=float, default=None)
    catchments.add_argument(
        "--analysis-bounds",
        type=float,
        nargs=4,
        metavar=("WEST", "SOUTH", "EAST", "NORTH"),
        help="Crop the analysis raster to explicit coordinates in the analysis CRS",
    )
    catchments.add_argument("--max-analysis-cells", type=int, default=100_000_000)
    catchments.add_argument(
        "--snap-distance-m",
        type=float,
        default=0,
        help="For point interests, snap to the highest-accumulation valid cell within this distance",
    )
    catchments.add_argument(
        "--allow-partial",
        action="store_true",
        help="Write catchments clipped by analysis bounds or NoData and mark their QA status",
    )
    catchments.add_argument("--grass-exe", default="grass")
    catchments.add_argument("--fill-depressions", action="store_true")
    return parser


def main() -> int:
    """Parse command-line inputs and initialise or build a TUFLOW project."""
    args = _parser().parse_args()
    if args.command == "init":
        initialize_tuflow_project(
            TuflowProjectConfig(
                output_dir=args.output,
                project_name=args.name,
                scenario_name=args.scenario,
                prj_file=args.prj,
                tuflow_executable=args.tuflow_exe,
                templates_dir=Path(__file__).absolute().parent / "resources" / "tuflow_templates",
                copy_utilities=args.ryan_tools_scripts is not None,
                utility_source_root=args.ryan_tools_scripts,
                overwrite=args.overwrite,
            )
        )
    elif args.command == "build":
        build(
            BuildConfig(
                terrains=tuple(args.terrain),
                aoi=args.aoi,
                output=args.output,
                exclusions=tuple(args.exclude),
                analyse_exclusions=args.analyse_exclusions,
                analysis_resolution_m=args.analysis_resolution_m,
                name=args.name,
                scenario=args.scenario,
                prj=args.prj,
                tuflow_executable=args.tuflow_exe,
                rainfall_name=args.rainfall_name,
                grass_executable=args.grass_exe,
                buffer_m=args.buffer_m,
                stream_threshold_m2=args.stream_area_m2,
                fill_depressions=args.fill_depressions,
                max_fill_depth_m=args.max_fill_depth_m,
                utility_source_root=args.ryan_tools_scripts,
                generate_catchments=args.catchments,
            )
        )
    elif args.command == "catchments":
        build_catchments(
            CatchmentConfig(
                terrains=tuple(args.terrain or ()),
                interest=args.interest,
                output=args.output,
                target_crs=CRS.from_string(args.crs),
                tgc=args.tgc,
                tgc_scenarios=tuple(args.tgc_scenario),
                tgc_global_only=args.tgc_global_only,
                analysis_bounds=(tuple(args.analysis_bounds) if args.analysis_bounds else None),
                analysis_resolution_m=args.analysis_resolution_m,
                max_analysis_cells=args.max_analysis_cells,
                grass_executable=args.grass_exe,
                fill_depressions=args.fill_depressions,
                snap_distance_m=args.snap_distance_m,
                allow_partial=args.allow_partial,
            )
        )
    else:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
