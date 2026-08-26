r"""Plot terrain and TUFLOW water-level profiles along GeoPackage lines.

Inputs are a line GeoPackage, a terrain raster, and one water-level raster for
each requested AEP. Outputs are 300 dpi PNG profiles in ``OUTPUT_DIR``.

Edit the defaults below for ordinary use, or override them from a terminal::

    py -3.14 plot_water_level_profiles.py --spacing 0.5 --no-pause
"""

from pathlib import Path
from typing import Literal

WRAPPER_VERSION = "2026-08-20.3"

# Project inputs currently used to validate the workflow.
LINES_GPKG = Path(r"E:\Projects\profile-line_01.gpkg")
LINES_LAYER_NAME: str | None = None
NAME_FIELD = "Code"
# Optional fallback used only when line CRS metadata is absent. None infers the
# CRS from any tagged raster, or assumes shared source coordinates if all inputs are untagged.
LINES_CRS_IF_MISSING: str | None = None

TERRAIN_TIF = Path(r"E:\Projects\DTM_1m_GDA94MGAz51_AHD.tif")
TUFLOW_RESULTS_DIR = Path(r"E:\Projects\max_of_means")
TARGET_AEPS = ("01.00p", "20.00p")
TARGET_RESULT_TYPE = "h_HR_Max"

OUTPUT_DIR = Path(r"E:\Projects")
OVERWRITE_EXISTING = True

PLOT_WIDTH_CM = 16.0
PLOT_HEIGHT_CM = 11.0
# Maximum station spacing in projected CRS units. None uses half the terrain cell size.
SPACING: float | None = None
SAMPLING_METHOD: Literal["bilinear", "bilinear_valid", "bilinear_masked", "nearest"] = "bilinear_masked"
DRY_AREA_HANDLING: Literal["no_plot", "ground_level"] = "no_plot"
# Interpolate only complete internal NoData runs at or below this length.
MAX_INTERPOLATION_GAP = 10
DISCONNECTED_LINE_HANDLING: Literal["error", "separate"] = "error"
CONSOLE_LOG_LEVEL = "INFO"
WORKING_DIR: Path = Path(__file__).absolute().parent

import argparse
from dataclasses import dataclass

from loguru import logger

from ryan_library.functions.loguru_helpers import configure_serial_logging
from ryan_library.functions.wrapper_utils import (
    CommonWrapperOptions,
    add_execution_cli_arguments,
    change_working_directory,
    parse_common_cli_arguments,
    pause_console,
    print_wrapper_banner,
)
from ryan_library.orchestrators.tuflow.water_level_profiles import (
    DisconnectedLineHandling,
    DryAreaHandling,
    WaterLevelProfileConfig,
    run_water_level_profile_workflow,
)
from ryan_library.functions.gdal.profiling import RasterSamplingError, SamplingMethod


@dataclass(slots=True, frozen=True)
class CliOptions:
    """Command-line overrides for the editable profile defaults."""

    common: CommonWrapperOptions
    lines_gpkg: Path | None
    lines_layer: str | None
    name_field: str | None
    lines_crs_if_missing: str | None
    terrain_raster: Path | None
    results_directory: Path | None
    output_directory: Path | None
    target_aeps: tuple[str, ...] | None
    target_result_type: str | None
    spacing: float | None
    sampling_method: SamplingMethod | None
    dry_area_handling: DryAreaHandling | None
    max_interpolation_gap: int | None
    disconnected_line_handling: DisconnectedLineHandling | None
    overwrite_existing: bool | None


def main(
    *,
    working_directory: Path | None = None,
    console_log_level: str | None = None,
    lines_gpkg: Path | None = None,
    lines_layer: str | None = None,
    name_field: str | None = None,
    lines_crs_if_missing: str | None = None,
    terrain_raster: Path | None = None,
    results_directory: Path | None = None,
    output_directory: Path | None = None,
    target_aeps: tuple[str, ...] | None = None,
    target_result_type: str | None = None,
    spacing: float | None = None,
    sampling_method: SamplingMethod | None = None,
    dry_area_handling: DryAreaHandling | None = None,
    max_interpolation_gap: int | None = None,
    disconnected_line_handling: DisconnectedLineHandling | None = None,
    overwrite_existing: bool | None = None,
) -> int:
    """Resolve editable defaults and run the profile workflow."""
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    configure_serial_logging(console_log_level=console_log_level or CONSOLE_LOG_LEVEL)
    if not change_working_directory(target_dir=working_directory or WORKING_DIR):
        return 1

    config = WaterLevelProfileConfig(
        lines_gpkg=lines_gpkg or LINES_GPKG,
        lines_layer_name=lines_layer if lines_layer is not None else LINES_LAYER_NAME,
        name_field=name_field or NAME_FIELD,
        lines_crs_if_missing=lines_crs_if_missing if lines_crs_if_missing is not None else LINES_CRS_IF_MISSING,
        terrain_raster=terrain_raster or TERRAIN_TIF,
        tuflow_results_dir=results_directory or TUFLOW_RESULTS_DIR,
        output_dir=output_directory or OUTPUT_DIR,
        target_aeps=target_aeps or TARGET_AEPS,
        target_result_type=target_result_type or TARGET_RESULT_TYPE,
        spacing=spacing if spacing is not None else SPACING,
        sampling_method=sampling_method or SAMPLING_METHOD,
        dry_area_handling=dry_area_handling or DRY_AREA_HANDLING,
        max_interpolation_gap=(max_interpolation_gap if max_interpolation_gap is not None else MAX_INTERPOLATION_GAP),
        disconnected_line_handling=disconnected_line_handling or DISCONNECTED_LINE_HANDLING,
        overwrite_existing=OVERWRITE_EXISTING if overwrite_existing is None else overwrite_existing,
        plot_width_cm=PLOT_WIDTH_CM,
        plot_height_cm=PLOT_HEIGHT_CM,
    )
    try:
        run_water_level_profile_workflow(config)
    except (FileNotFoundError, FileExistsError, RasterSamplingError, ValueError) as exc:
        logger.error("Water-level profile workflow failed: {}", exc)
        return 1
    except Exception:
        logger.exception("Water-level profile workflow failed")
        return 1
    return 0


def _parse_cli_arguments() -> CliOptions:
    parser = argparse.ArgumentParser(
        description="Plot terrain and TUFLOW water levels along configured profile lines. CLI values override defaults."
    )
    add_execution_cli_arguments(parser=parser)
    parser.add_argument("--lines-gpkg", type=Path, help="Profile-line GeoPackage.")
    parser.add_argument("--lines-layer", help="GeoPackage layer name; omitted uses safe automatic selection.")
    parser.add_argument("--name-field", help="Attribute used in plot titles and output filenames.")
    parser.add_argument(
        "--lines-crs-if-missing",
        help="Optional line CRS fallback; otherwise infer from tagged rasters or assume shared source coordinates.",
    )
    parser.add_argument("--terrain-raster", type=Path, help="Terrain elevation GeoTIFF.")
    parser.add_argument(
        "--results-directory", type=Path, help="Directory searched recursively for water-level rasters."
    )
    parser.add_argument("--output-directory", type=Path, help="Destination for profile PNG files.")
    parser.add_argument("--target-aeps", nargs="+", help="AEP tokens to plot, such as 01.00p 20.00p.")
    parser.add_argument("--target-result-type", help="Exact parsed TUFLOW raster type, such as h_HR_Max.")
    parser.add_argument("--spacing", type=float, help="Maximum station spacing in projected CRS units.")
    parser.add_argument(
        "--sampling-method",
        choices=("bilinear", "bilinear_valid", "bilinear_masked", "nearest"),
        help="Raster sampling policy; defaults to containing-cell-masked bilinear interpolation.",
    )
    parser.add_argument(
        "--dry-area-handling",
        choices=("no_plot", "ground_level"),
        help="Leave dry gaps or draw water at ground level.",
    )
    parser.add_argument(
        "--max-interpolation-gap",
        type=int,
        help="Maximum complete internal NoData run to interpolate.",
    )
    parser.add_argument(
        "--disconnected-line-handling",
        choices=("error", "separate"),
        help="Reject disconnected lines or plot every part separately.",
    )
    parser.add_argument(
        "--overwrite-existing",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Allow replacement of existing profile PNG files.",
    )
    args: argparse.Namespace = parser.parse_args()
    common: CommonWrapperOptions = parse_common_cli_arguments(args=args)
    return CliOptions(
        common=common,
        lines_gpkg=args.lines_gpkg,
        lines_layer=args.lines_layer,
        name_field=args.name_field,
        lines_crs_if_missing=args.lines_crs_if_missing,
        terrain_raster=args.terrain_raster,
        results_directory=args.results_directory,
        output_directory=args.output_directory,
        target_aeps=tuple(args.target_aeps) if args.target_aeps else None,
        target_result_type=args.target_result_type,
        spacing=args.spacing,
        sampling_method=args.sampling_method,
        dry_area_handling=args.dry_area_handling,
        max_interpolation_gap=args.max_interpolation_gap,
        disconnected_line_handling=args.disconnected_line_handling,
        overwrite_existing=args.overwrite_existing,
    )


if __name__ == "__main__":
    options: CliOptions = _parse_cli_arguments()
    result: int = main(
        working_directory=options.common.working_directory,
        console_log_level=options.common.console_log_level,
        lines_gpkg=options.lines_gpkg,
        lines_layer=options.lines_layer,
        name_field=options.name_field,
        lines_crs_if_missing=options.lines_crs_if_missing,
        terrain_raster=options.terrain_raster,
        results_directory=options.results_directory,
        output_directory=options.output_directory,
        target_aeps=options.target_aeps,
        target_result_type=options.target_result_type,
        spacing=options.spacing,
        sampling_method=options.sampling_method,
        dry_area_handling=options.dry_area_handling,
        max_interpolation_gap=options.max_interpolation_gap,
        disconnected_line_handling=options.disconnected_line_handling,
        overwrite_existing=options.overwrite_existing,
    )
    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not options.common.no_pause:
        pause_console()
    raise SystemExit(result)
