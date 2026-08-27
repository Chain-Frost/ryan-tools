"""Coordinate terrain and TUFLOW water-level profile plotting."""

from __future__ import annotations

# Rasterio, GeoPandas, PyProj, and Matplotlib expose incomplete third-party typing.
# pyright: reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownVariableType=false

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
import re
from typing import Literal, cast

import geopandas as gpd
from geopandas import GeoDataFrame
from loguru import logger
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator
import numpy as np
from numpy.typing import NDArray
import pandas as pd
from pyproj import CRS
import rasterio  # pyright: ignore[reportMissingTypeStubs]
from shapely.geometry import LineString, MultiLineString
from shapely.geometry.base import BaseGeometry
from shapely.ops import linemerge

from ryan_library.classes.tuflow_string_classes import TuflowStringParser
from ryan_library.functions.gdal.profiling import (
    SamplingMethod,
    interpolate_short_nan_gaps,
    sample_raster_along_line,
)
from ryan_library.functions.path_stuff import sanitize_windows_filename

type DryAreaHandling = Literal["no_plot", "ground_level"]
type DisconnectedLineHandling = Literal["error", "separate"]
type FloatArray = NDArray[np.float64]


_AEP_ARI_PATTERN: re.Pattern[str] = re.compile(
    pattern=r"(?:^|_)AEP(?P<ari>\d+(?:\.\d+)?)(?:_|$)",
    flags=re.IGNORECASE,
)


@dataclass(slots=True, frozen=True)
class WaterLevelProfileConfig:
    """Inputs and presentation settings for water-level profile plots."""

    lines_gpkg: Path
    terrain_raster: Path
    tuflow_results_dir: Path
    output_dir: Path
    target_aeps: tuple[str, ...]

    target_result_type: str = "h_HR_Max"
    lines_layer_name: str | None = None
    name_field: str = "Code"
    lines_crs_if_missing: str | None = None

    spacing: float | None = None
    sampling_method: SamplingMethod = "bilinear_masked"
    dry_area_handling: DryAreaHandling = "no_plot"
    max_interpolation_gap: int = 10
    disconnected_line_handling: DisconnectedLineHandling = "error"

    overwrite_existing: bool = True

    plot_width_cm: float = 16.0
    plot_height_cm: float = 11.0
    chainage_start_km: float = 0.0

    formation_label: str | None = None
    formation_color: str = "black"
    formation_linewidth: float = 1.4
    formation_linestyle: str = "--"

    colors: Mapping[str, str] = field(
        default_factory=lambda: {
            "01.00p": "blue",
            "20.00p": "cyan",
        }
    )

    minor_grid_subdivisions: int = 5


def _normalized_aep(value: str) -> str:
    normalized = value.strip().casefold()
    if not normalized:
        raise ValueError("Target AEP values cannot be empty.")
    return normalized


def _aep_from_filename(path: Path) -> str | None:
    """Return an AEP<n> event token embedded in a filename, when present."""
    match = _AEP_ARI_PATTERN.search(path.stem)
    if match is None:
        return None

    recurrence_interval = float(match.group("ari"))
    if recurrence_interval.is_integer():
        return f"AEP{int(recurrence_interval)}"
    return f"AEP{recurrence_interval:g}"


def discover_tuflow_profile_rasters(
    results_directory: Path,
    *,
    target_aeps: tuple[str, ...],
    target_result_type: str,
) -> dict[str, Path]:
    """Find exactly one matching TUFLOW raster for each requested AEP.

    Normal TUFLOW AEP representations are read with ``TuflowStringParser``.
    Event names such as ``AEP5`` and ``AEP100`` are also supported directly
    from the filename.
    """
    if not results_directory.is_dir():
        raise FileNotFoundError(
            f"TUFLOW results directory not found: {results_directory}"
        )
    if not target_aeps:
        raise ValueError("At least one target AEP is required.")

    requested = tuple(_normalized_aep(aep) for aep in target_aeps)
    if len(set(requested)) != len(requested):
        raise ValueError(f"Target AEP values must be unique: {target_aeps!r}")

    matches: dict[str, list[Path]] = {aep: [] for aep in requested}
    target_suffix = f"_{target_result_type}".casefold()

    logger.info(
        "Searching {} for {} rasters",
        results_directory,
        target_result_type,
    )

    for path in results_directory.rglob("*.tif"):
        if not path.is_file():
            continue

        if not path.stem.casefold().endswith(target_suffix):
            continue

        filename_aep = _aep_from_filename(path)
        if filename_aep is not None:
            aep = _normalized_aep(filename_aep)
        else:
            parser = TuflowStringParser(path)
            if (
                parser.data_type is None
                or parser.data_type.casefold() != target_result_type.casefold()
                or parser.aep is None
            ):
                continue
            aep = _normalized_aep(parser.aep.text_repr)

        if aep in matches:
            logger.debug("Matched {} to {}", aep, path.name)
            matches[aep].append(path)

    problems: list[str] = []
    for aep, paths in matches.items():
        if not paths:
            problems.append(f"{aep}: no matching {target_result_type} GeoTIFF")
        elif len(paths) > 1:
            listed_paths = ", ".join(str(path) for path in sorted(paths))
            problems.append(f"{aep}: multiple matches ({listed_paths})")

    if problems:
        raise ValueError(
            "TUFLOW profile raster discovery failed: " + "; ".join(problems)
        )

    selected = {aep: matches[aep][0] for aep in requested}
    for aep, path in selected.items():
        logger.info("Using {} raster: {}", aep.upper(), path.name)

    return selected


def resolve_profile_layer_name(
    lines_gpkg: Path,
    requested_layer: str | None,
) -> str:
    """Resolve a GeoPackage layer using explicit, sole-layer, or stem matching."""
    if not lines_gpkg.is_file():
        raise FileNotFoundError(
            f"Profile-line GeoPackage not found: {lines_gpkg}"
        )

    layer_table = gpd.list_layers(lines_gpkg)
    available = tuple(str(value) for value in layer_table["name"].tolist())

    if not available:
        raise ValueError(
            f"GeoPackage contains no readable layers: {lines_gpkg}"
        )

    if requested_layer is not None:
        matches = tuple(
            name
            for name in available
            if name.casefold() == requested_layer.casefold()
        )
        if len(matches) == 1:
            return matches[0]
        raise ValueError(
            f"Layer {requested_layer!r} not found in {lines_gpkg}. "
            f"Available layers: {available}"
        )

    if len(available) == 1:
        return available[0]

    stem_matches = tuple(
        name
        for name in available
        if name.casefold() == lines_gpkg.stem.casefold()
    )
    if len(stem_matches) == 1:
        return stem_matches[0]

    raise ValueError(
        f"Could not choose a unique layer from {lines_gpkg}. "
        f"Available layers: {available}. "
        "Set lines_layer_name explicitly."
    )


def load_profile_lines(
    lines_gpkg: Path,
    *,
    requested_layer: str | None,
    name_field: str,
    lines_crs_if_missing: str | None,
    target_crs: CRS | None,
) -> tuple[GeoDataFrame, CRS | None]:
    """Load profile lines and resolve their CRS against raster metadata."""
    layer_name = resolve_profile_layer_name(lines_gpkg, requested_layer)
    logger.info(
        "Reading profile lines from {} layer {}",
        lines_gpkg,
        layer_name,
    )

    lines = gpd.read_file(lines_gpkg, layer=layer_name)

    if lines.empty:
        raise ValueError(
            f"Profile layer is empty: {lines_gpkg}:{layer_name}"
        )
    if name_field not in lines.columns:
        raise ValueError(
            f"Field {name_field!r} not found. "
            f"Available fields: {tuple(lines.columns)}"
        )

    if lines.crs is None:
        if lines_crs_if_missing is not None:
            source_crs: CRS | None = CRS.from_user_input(lines_crs_if_missing)
            logger.warning(
                "Profile layer has no CRS metadata; assigning configured CRS {}",
                source_crs.to_string(),
            )
            logger.warning(
                "Profile layer has no CRS metadata; assigning configured CRS {}",
                source_crs.to_string(),
            )
            lines = lines.set_crs(source_crs)
        elif target_crs is not None:
            source_crs = target_crs
            logger.warning(
                "Profile layer has no CRS metadata; assuming raster CRS {}",
                source_crs.to_string(),
            )
            logger.warning(
                "Profile layer has no CRS metadata; assuming raster CRS {}",
                source_crs.to_string(),
            )
            lines = lines.set_crs(source_crs)
        else:
            source_crs = None
            logger.warning(
                "Profile lines and rasters have no CRS metadata; assuming "
                "their source coordinates are already aligned"
            )
    else:
        source_crs = CRS.from_user_input(lines.crs)

    resolved_crs = target_crs or source_crs

    if (
        source_crs is not None
        and target_crs is not None
        and source_crs != target_crs
    ):
        logger.info(
            "Reprojecting profile lines from {} to {}",
            source_crs.to_string(),
            target_crs.to_string(),
        )
        lines = lines.to_crs(target_crs)

    return lines, resolved_crs


def _validate_known_profile_crs(profile_crs: CRS | None) -> None:
    """Validate horizontal units when CRS metadata is available."""
    if profile_crs is None:
        return

    if not profile_crs.is_projected:
        raise ValueError(
            "Profile sampling requires a projected CRS, received "
            f"{profile_crs.to_string()}."
        )

    horizontal_units = {
        axis.unit_name.casefold()
        for axis in profile_crs.axis_info[:2]
        if axis.unit_name
    }

    if not horizontal_units or not horizontal_units <= {"metre", "meter"}:
        raise ValueError(
            "Profile distance labels require a metre-based CRS, "
            f"received units {sorted(horizontal_units)}."
        )


def split_profile_line(
    geometry: BaseGeometry,
    *,
    line_name: str,
    disconnected_handling: DisconnectedLineHandling,
) -> tuple[LineString, ...]:
    """Return connected LineStrings without silently dropping multipart data."""
    if geometry.is_empty:
        raise ValueError(f"Profile {line_name!r} has empty geometry.")

    if isinstance(geometry, LineString):
        if geometry.length <= 0.0:
            raise ValueError(f"Profile {line_name!r} has zero length.")
        return (geometry,)

    if not isinstance(geometry, MultiLineString):
        raise ValueError(
            f"Profile {line_name!r} has unsupported geometry type "
            f"{geometry.geom_type!r}."
        )

    merged = linemerge(geometry)

    if isinstance(merged, LineString):
        return (merged,)

    if disconnected_handling == "error":
        raise ValueError(
            f"Profile {line_name!r} contains {len(merged.geoms)} "
            "disconnected line parts. Use "
            "disconnected_line_handling='separate' to plot every part."
        )

    return tuple(merged.geoms)


def _aep_label(aep: str) -> str:
    """Return a readable AEP label."""
    ari_match = re.fullmatch(
        r"AEP(?P<ari>\d+(?:\.\d+)?)",
        aep,
        re.IGNORECASE,
    )
    if ari_match is not None:
        ari = float(ari_match.group("ari"))
        return f"1 in {ari:g} AEP"

    value = aep.casefold().removesuffix("p")
    try:
        return f"{float(value):g}% AEP"
    except ValueError:
        return aep


def _raster_crs_and_spacing(
    path: Path,
    spacing: float | None,
) -> tuple[CRS | None, float]:
    if not path.is_file():
        raise FileNotFoundError(f"Raster not found: {path}")

    with rasterio.open(path) as source:
        raster_crs = (
            CRS.from_user_input(source.crs)
            if source.crs is not None
            else None
        )
        effective_spacing = (
            spacing
            if spacing is not None
            else min(
                abs(float(source.res[0])),
                abs(float(source.res[1])),
            )
            / 2.0
        )

    return raster_crs, effective_spacing


def _read_water_raster_crs(
    water_rasters: Mapping[str, Path],
) -> dict[str, CRS | None]:
    water_crs: dict[str, CRS | None] = {}

    for aep, path in water_rasters.items():
        with rasterio.open(path) as source:
            water_crs[aep] = (
                CRS.from_user_input(source.crs)
                if source.crs is not None
                else None
            )

    return water_crs


def _resolve_known_raster_crs(
    terrain_crs: CRS | None,
    water_crs: Mapping[str, CRS | None],
) -> CRS | None:
    known: list[tuple[str, CRS]] = []

    if terrain_crs is not None:
        known.append(("terrain", terrain_crs))

    known.extend(
        (f"water raster {aep}", crs)
        for aep, crs in water_crs.items()
        if crs is not None
    )

    if not known:
        return None

    reference_name, reference_crs = known[0]

    for name, crs in known[1:]:
        if crs != reference_crs:
            raise ValueError(
                f"CRS mismatch: {reference_name} uses "
                f"{reference_crs.to_string()}, but {name} uses "
                f"{crs.to_string()}."
            )

    return reference_crs


def _log_assumed_raster_crs(
    *,
    terrain_path: Path,
    terrain_crs: CRS | None,
    water_rasters: Mapping[str, Path],
    water_crs: Mapping[str, CRS | None],
    resolved_crs: CRS | None,
) -> None:
    assumption = (
        resolved_crs.to_string()
        if resolved_crs is not None
        else "the shared source coordinate system"
    )

    if terrain_crs is None:
        logger.warning(
            "Terrain raster has no CRS metadata; assuming {} for {}",
            assumption,
            terrain_path,
        )

    for aep, crs in water_crs.items():
        if crs is None:
            logger.warning(
                "Water raster {} has no CRS metadata; assuming {} for {}",
                aep,
                assumption,
                water_rasters[aep],
            )


def sample_line_z(
    line: LineString,
    distances: FloatArray,
) -> FloatArray:
    """Interpolate alignment Z values at supplied 2D distances along a line."""
    coordinates = np.asarray(line.coords, dtype=np.float64)

    if coordinates.ndim != 2 or coordinates.shape[1] < 3:
        raise ValueError(
            "Formation plotting was requested but the profile line does "
            "not contain Z coordinates."
        )

    xy = coordinates[:, :2]
    z = coordinates[:, 2]

    segment_lengths = np.hypot(
        np.diff(xy[:, 0]),
        np.diff(xy[:, 1]),
    )
    cumulative_distance = np.concatenate(
        (
            np.array([0.0], dtype=np.float64),
            np.cumsum(segment_lengths, dtype=np.float64),
        )
    )

    if cumulative_distance[-1] <= 0.0:
        raise ValueError(
            "Formation plotting requires a line with positive horizontal length."
        )

    return np.asarray(
        np.interp(distances, cumulative_distance, z),
        dtype=np.float64,
    )


def _configure_profile_grid(
    axes: object,
    *,
    minor_subdivisions: int,
) -> None:
    """Configure major and minor horizontal and vertical plot grids."""
    axes.xaxis.set_minor_locator(AutoMinorLocator(minor_subdivisions))
    axes.yaxis.set_minor_locator(AutoMinorLocator(minor_subdivisions))

    axes.grid(
        True,
        which="major",
        linestyle="--",
        linewidth=0.7,
        alpha=0.6,
    )
    axes.grid(
        True,
        which="minor",
        linestyle=":",
        linewidth=0.4,
        alpha=0.35,
    )


def run_water_level_profile_workflow(
    config: WaterLevelProfileConfig,
) -> tuple[Path, ...]:
    """Create one terrain/water-level profile plot per connected input line."""
    if config.max_interpolation_gap < 0:
        raise ValueError("max_interpolation_gap must be non-negative.")

    if config.plot_width_cm <= 0.0 or config.plot_height_cm <= 0.0:
        raise ValueError("Plot dimensions must be positive.")
    if config.scenario_name is not None and not config.scenario_name.strip():
        raise ValueError("scenario_name cannot be blank.")

    if not np.isfinite(config.chainage_start_km):
        raise ValueError("chainage_start_km must be finite.")

    if config.formation_linewidth <= 0.0:
        raise ValueError("formation_linewidth must be positive.")

    if config.minor_grid_subdivisions < 2:
        raise ValueError("minor_grid_subdivisions must be at least 2.")

    water_rasters = discover_tuflow_profile_rasters(
        config.tuflow_results_dir,
        target_aeps=config.target_aeps,
        target_result_type=config.target_result_type,
    )

    terrain_crs, effective_spacing = _raster_crs_and_spacing(
        config.terrain_raster,
        config.spacing,
    )
    water_crs = _read_water_raster_crs(water_rasters)
    raster_crs = _resolve_known_raster_crs(terrain_crs, water_crs)

    lines, resolved_crs = load_profile_lines(
        config.lines_gpkg,
        requested_layer=config.lines_layer_name,
        name_field=config.name_field,
        lines_crs_if_missing=config.lines_crs_if_missing,
        target_crs=raster_crs,
    )

    _validate_known_profile_crs(resolved_crs)

    _log_assumed_raster_crs(
        terrain_path=config.terrain_raster,
        terrain_crs=terrain_crs,
        water_rasters=water_rasters,
        water_crs=water_crs,
        resolved_crs=resolved_crs,
    )

    logger.info(
        "Sampling {} profile features every {:.3f} CRS units",
        len(lines),
        effective_spacing,
    )

    profile_jobs: list[tuple[str, LineString, Path]] = []
    reserved_names: set[str] = set()

    for _, row in lines.iterrows():
        raw_name = row[config.name_field]

        if raw_name is None or bool(pd.isna(raw_name)):
            raise ValueError(
                f"Profile has an empty {config.name_field!r} value."
            )

        line_name = str(raw_name)

        if not line_name.strip():
            raise ValueError(
                f"Profile has a blank {config.name_field!r} value."
            )

        geometry = cast(BaseGeometry | None, row.geometry)

        if geometry is None:
            raise ValueError(
                f"Profile {line_name!r} has null geometry."
            )

        parts = split_profile_line(
            geometry,
            line_name=line_name,
            disconnected_handling=config.disconnected_line_handling,
        )

        for part_number, line in enumerate(parts, start=1):
            display_name = (
                line_name
                if len(parts) == 1
                else f"{line_name} (part {part_number})"
            )

            filename_name = sanitize_windows_filename(
                line_name,
                fallback="profile",
            )
            part_suffix = (
                ""
                if len(parts) == 1
                else f"_part_{part_number}"
            )
            output_path = (
                config.output_dir
                / f"{filename_name}{part_suffix}_profile.png"
            )

            normalized_output = output_path.name.casefold()

            if normalized_output in reserved_names:
                raise ValueError(
                    f"Duplicate profile output filename: {output_path.name}"
                )

            reserved_names.add(normalized_output)

            if output_path.exists() and not config.overwrite_existing:
                raise FileExistsError(
                    f"Profile output already exists: {output_path}"
                )

            profile_jobs.append((display_name, line, output_path))

    config.output_dir.mkdir(parents=True, exist_ok=True)
    output_paths: list[Path] = []

    for display_name, line, output_path in profile_jobs:
        logger.info("Processing profile {}", display_name)

        figure, axes = plt.subplots(
            figsize=(
                config.plot_width_cm / 2.54,
                config.plot_height_cm / 2.54,
            )
        )
        temporary_output = output_path.with_name(
            f".{output_path.stem}.tmp{output_path.suffix}"
        )

        try:
            distances, terrain = sample_raster_along_line(
                line,
                config.terrain_raster,
                effective_spacing,
                method=config.sampling_method,
            )

            chainages = distances / 1000.0 + config.chainage_start_km

            axes.plot(
                chainages,
                terrain,
                color="saddlebrown",
                label="Ground Level",
                linewidth=1.5,
                zorder=3,
            )

            if config.formation_label is not None:
                formation = sample_line_z(line, distances)
                axes.plot(
                    chainages,
                    formation,
                    color=config.formation_color,
                    label=config.formation_label,
                    linewidth=config.formation_linewidth,
                    linestyle=config.formation_linestyle,
                    zorder=4,
                )

            for aep, raster_path in water_rasters.items():
                water_distances, water = sample_raster_along_line(
                    line,
                    raster_path,
                    effective_spacing,
                    method=config.sampling_method,
                )

                if not np.array_equal(distances, water_distances):
                    raise RuntimeError(
                        "Sampling stations differ between terrain "
                        f"and {aep} raster."
                    )

                water = interpolate_short_nan_gaps(
                    water,
                    max_gap=config.max_interpolation_gap,
                )
                water_plot = water.copy()

                dry = (
                    np.isnan(water)
                    | np.isnan(terrain)
                    | (water <= terrain)
                )

                if config.dry_area_handling == "ground_level":
                    water_plot[dry] = terrain[dry]
                else:
                    water_plot[dry] = np.nan

                color = config.colors.get(aep, "blue")

                axes.plot(
                    chainages,
                    water_plot,
                    color=color,
                    label=f"{_aep_label(aep)} WL",
                    linewidth=1.2,
                    zorder=2,
                )

            axes.set_title(f"Profile: {display_name}")
            axes.set_xlabel("Chainage (km)")
            axes.set_ylabel("Elevation (mAHD)")

            _configure_profile_grid(
                axes,
                minor_subdivisions=config.minor_grid_subdivisions,
            )

            axes.legend()
            figure.tight_layout()
            figure.savefig(temporary_output, dpi=300)
            temporary_output.replace(output_path)

        finally:
            plt.close(figure)
            temporary_output.unlink(missing_ok=True)

        output_paths.append(output_path)
        logger.success("Saved profile plot to {}", output_path)

    logger.success(
        "Created {} water-level profile plots",
        len(output_paths),
    )
    return tuple(output_paths)
