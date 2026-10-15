#!/usr/bin/env python3
"""Build, clip, compress, and report TUFLOW maximum rasters for v06 and v07.

The pipeline discovers result grids under both version folders, creates a
cell-wise maximum with GDAL's VRT ``max`` pixel function, clips that maximum to
the matching ``Model`` polygon, and writes the final compressed GeoTIFF.

Normal GD model outputs are written to an AEP subdirectory. FG03 and the
combined FG01+FG02 model are clipped with FG Outputzones.gpkg and written to an
AEP subdirectory below ``FG`` with ``-clip`` appended.

Incremental rebuilding is based on fingerprints embedded in every output. A
changed, added, or removed source raster, a changed output-zone GeoPackage, a
different compression profile, or a changed processing algorithm causes the
output to be regenerated. Existing legacy outputs without fingerprints are
rebuilt once so subsequent runs can make reliable incremental decisions.

Examples:
    python tuflow_maximum_pipeline.py --dry-run
    python tuflow_maximum_pipeline.py
    python tuflow_maximum_pipeline.py --aep 01.0p 05.0p 10.0p 20.0p 50.0p
    python tuflow_maximum_pipeline.py --version v06 --model GD01 GD02
    python tuflow_maximum_pipeline.py --grid d_HR_Max V_Max
    python tuflow_maximum_pipeline.py --overwrite
    python tuflow_maximum_pipeline.py --profile efficient
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

from osgeo import gdal, ogr

from ryan_library.orchestrators.tuflow.maximum_rasters import (
    ALGORITHM_ID,
    DEFAULT_LEGACY_LOCAL_CRS,
    MODEL_FIELD,
    OVERVIEW_LEVELS,
    Job,
    SourceGroup,
    ZoneCatalog,
    discover_jobs as _discover_jobs,
    load_zone_catalog,
    output_path as _output_path,
    parse_source_group,
    execute_jobs,
    write_report,
)

# Project defaults retained verbatim for drop-in use.
WRAPPER_VERSION = "2026-10-15.1"
SCRIPT_DIR = Path(__file__).absolute().parent
RESULTS_DIR = SCRIPT_DIR.parent
VERSION_NAMES = ("v06", "v07")
MAIN_ZONES = SCRIPT_DIR / "Model Output Zones.gpkg"
FG_OUTPUT_DIR = SCRIPT_DIR / "FG"
FG_ZONES = FG_OUTPUT_DIR / "FG Outputzones.gpkg"
AEP_NAMES = ("01.0p", "05.0p", "10.0p", "20.0p", "50.0p")
DEFAULT_AEPS = AEP_NAMES
GRID_TYPES = ("d_HR_Max", "h_HR_Max", "V_Max", "Z0_Max")
OUTPUT_TEMPLATE = "RTIO_{version}_{model}_EXG_CCNear_{aep}_MAXIMUM_04M_{grid}.tif"
REPORT_TEMPLATE = "MaxRaster_Summary_{stamp}.xlsx"
FG_COMBINED_MODEL = "FG"
FG_COMBINED_EXTENTS = ("FG01", "FG02")
FG_DIRECT_MODELS = ("FG03",)
DEFAULT_GROUPS: tuple[SourceGroup, ...] = (
    SourceGroup("FG", FG_COMBINED_EXTENTS, "fg"),
    SourceGroup("FG03", ("FG03",), "fg"),
)


def output_path(
    version: str,
    model: str,
    aep: str,
    grid_type: str,
    fg: bool,
    *,
    output_root: Path = SCRIPT_DIR,
    template: str = OUTPUT_TEMPLATE,
) -> Path:
    """Maintain the original copied-wrapper output defaults."""
    return _output_path(
        version, model, aep, grid_type, fg, output_root=output_root, template=template,
    )


def discover_jobs(
    versions: tuple[str, ...],
    aeps: tuple[str, ...],
    grid_types: tuple[str, ...],
    wanted_models: set[str] | None,
    main_zones: ZoneCatalog,
    fg_zones: ZoneCatalog | None,
    *,
    input_root: Path = RESULTS_DIR,
    output_root: Path = SCRIPT_DIR,
    output_template: str = OUTPUT_TEMPLATE,
    groups: tuple[SourceGroup, ...] = DEFAULT_GROUPS,
    legacy_local_crs: str = DEFAULT_LEGACY_LOCAL_CRS,
    strict_alignment: bool = False,
) -> tuple[list[Job], list[str]]:
    """Keep the RTIO folder mapping while delegating discovery to the library."""
    return _discover_jobs(
        versions, aeps, grid_types, wanted_models, main_zones, fg_zones,
        input_root=input_root, output_root=output_root,
        output_template=output_template, groups=groups,
        legacy_local_crs=legacy_local_crs,
        strict_alignment=strict_alignment,
    )


# ---------------------------------------------------------------------------
# Command line
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--version",
        nargs="+",
        help="Only search selected version folders (default: v06 v07).",
    )
    parser.add_argument(
        "--aep",
        nargs="+",
        help="Only process selected AEP folders (default: all configured AEPs).",
    )
    parser.add_argument(
        "--model",
        "--extent",
        dest="models",
        nargs="+",
        metavar="NAME",
        help="Only process selected output models, for example GD01 FG FG03.",
    )
    parser.add_argument(
        "--grid",
        nargs="+",
        help="Only process selected grid types.",
    )
    parser.add_argument(
        "--profile",
        choices=("tuflow", "efficient"),
        default="tuflow",
        help="Compression profile: conservative DEFLATE or tiled ZSTD (default: tuflow).",
    )
    parser.add_argument("--input-root", type=Path, default=RESULTS_DIR, help="Folder containing version subdirectories.")
    parser.add_argument("--output-root", type=Path, default=SCRIPT_DIR, help="Output root (FG results under its FG subfolder).")
    parser.add_argument("--main-zones", type=Path, default=MAIN_ZONES, help="Main model output-zone GeoPackage (one layer).")
    parser.add_argument("--fg-zones", type=Path, default=FG_ZONES, help="Special output-zone GeoPackage (one layer).")
    parser.add_argument("--zone-field", default=MODEL_FIELD, help="Polygon attribute used to match model names.")
    parser.add_argument("--legacy-local-crs", default=DEFAULT_LEGACY_LOCAL_CRS, help="Assumed projected CRS for legacy GDA94_MG rasters (default EPSG:28350).")
    parser.add_argument("--strict-grid-alignment", action="store_true", help="Reject sources whose pixel sizes or origins cannot align without resampling.")
    parser.add_argument("--output-template", default=OUTPUT_TEMPLATE, help="Filename template with version/model/aep/grid.")
    parser.add_argument(
        "--group", action="append", metavar="[main|fg:]MODEL=EXTENT1,EXTENT2",
        help="Source extents contributing to an output zone. When supplied, replaces default FG groups.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Rebuild every discovered output.")
    parser.add_argument("--dry-run", action="store_true", help="Report intended actions without writing rasters.")
    parser.add_argument("--no-overviews", action="store_true", help="Do not create external .ovr pyramids.")
    parser.add_argument("--no-report", action="store_true", help="Do not create the Excel summary.")
    parser.add_argument("--report", type=Path, help="Write the Excel summary to this path.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    gdal.UseExceptions()
    ogr.UseExceptions()

    run_started = datetime.now()
    versions = tuple(args.version or VERSION_NAMES)
    aeps = tuple(dict.fromkeys(args.aep or DEFAULT_AEPS))
    grid_types = tuple(args.grid or GRID_TYPES)
    wanted_models = (
        {value.casefold() for value in args.models}
        if args.models
        else None
    )
    build_overviews = not args.no_overviews

    try:
        groups = tuple(parse_source_group(value) for value in args.group) if args.group is not None else DEFAULT_GROUPS
        main_zones = load_zone_catalog(args.main_zones.absolute(), args.zone_field)
        fg_zones = (
            load_zone_catalog(args.fg_zones.absolute(), args.zone_field)
            if any(group.zone_set == "fg" for group in groups) else None
        )
        jobs, discovery_notes = discover_jobs(
            versions,
            aeps,
            grid_types,
            wanted_models,
            main_zones,
            fg_zones,
            input_root=args.input_root.absolute(),
            output_root=args.output_root.absolute(),
            output_template=args.output_template,
            groups=groups,
            legacy_local_crs=args.legacy_local_crs,
            strict_alignment=args.strict_grid_alignment,
        )
    except Exception as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1

    print(f"Versions : {', '.join(versions)}")
    print(f"AEPs     : {', '.join(aeps)}")
    print(f"Grid types: {', '.join(grid_types)}")
    print(f"Profile  : {args.profile}")
    print(f"Overviews: {'enabled' if build_overviews else 'disabled'}")
    print(f"Main zones: {args.main_zones.absolute()}")
    print(f"FG zones  : {args.fg_zones.absolute() if fg_zones is not None else 'not used'}")
    print(f"Discovered {len(jobs)} output job(s).")
    for note in discovery_notes:
        print(f"  NOTE: {note}")
    if not jobs:
        print("ERROR: No matching maximum-raster jobs were discovered.", file=sys.stderr)
        return 1

    results = execute_jobs(
        jobs,
        profile=args.profile,
        build_overviews=build_overviews,
        overwrite=args.overwrite,
        dry_run=args.dry_run,
        notify=print,
        notify_error=lambda message: print(message, file=sys.stderr),
    )

    counts: dict[str, int] = {}
    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1
    print("\nFinished: " + ", ".join(f"{status}={count}" for status, count in sorted(counts.items())))

    if not args.no_report:
        report_path = args.report or args.output_root.absolute() / REPORT_TEMPLATE.format(
            stamp=run_started.strftime("%Y%m%d_%H%M%S")
        )
        run_info = [
            ("Run started", run_started.strftime("%Y-%m-%d %H:%M:%S")),
            ("Run finished", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            ("Mode", "Dry run" if args.dry_run else "Live"),
            ("Algorithm", ALGORITHM_ID),
            ("Legacy local CRS", args.legacy_local_crs),
            ("Strict grid alignment", str(args.strict_grid_alignment)),
            ("Versions searched", ", ".join(versions)),
            ("AEPs searched", ", ".join(aeps)),
            ("Grid types", ", ".join(grid_types)),
            ("Selected models", ", ".join(args.models) if args.models else "All discovered"),
            ("Compression profile", args.profile),
            ("External overview levels", ", ".join(map(str, OVERVIEW_LEVELS)) if build_overviews else "Disabled"),
            ("Main output root", str(args.output_root.absolute() / "<AEP>")),
            ("FG output root", str(args.output_root.absolute() / "FG" / "<AEP>")),
            ("Main model zones", str(args.main_zones.absolute())),
            ("FG model zones", str(args.fg_zones.absolute()) if fg_zones is not None else "not used"),
            ("Overwrite", "Yes" if args.overwrite else "No"),
            ("Jobs discovered", str(len(jobs))),
            ("Results", ", ".join(f"{key}={value}" for key, value in sorted(counts.items()))),
        ]
        try:
            if write_report(report_path, results, jobs, run_info):
                print(f"Summary written to: {report_path}")
        except PermissionError:
            print(f"! Could not write summary; is it open in Excel? {report_path}")
        except Exception as error:
            print(f"! Could not write summary: {error}")

    return 1 if counts.get("FAILED", 0) else 0


if __name__ == "__main__":
    raise SystemExit(main())
