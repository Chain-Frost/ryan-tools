# ryan-scripts\misc-python\dtm_str_converter_to_gpkg.py
# Updated 2026-05-25
"""Convert ASCII Surpac STR and DTM files to GIS-friendly outputs.

What this script does:
- Converts a Surpac STR file to point and linestring GeoPackage layers.
- Converts a matching Surpac DTM file to a triangulation GeoPackage layer.
- Automatically looks for a DTM with the same name as the STR file, for example
  ``file1.str`` -> ``file1.dtm``.
- Rejects binary STR/DTM files because this script only supports ASCII Surpac
  files.

Default behavior:
- GeoPackage output is enabled.
- Excel output is disabled. Add ``--excel`` if tabular XLSX files are needed.
- DTM conversion is automatic when a same-basename DTM exists next to the STR.

Common command-line examples:
    python dtm_str_converter_v3.py --str "path/to/file.str"
    python dtm_str_converter_v3.py --str "path/to/file.str" --excel
    python dtm_str_converter_v3.py --str "path/to/file.str" --dtm "path/to/file.dtm"
    python dtm_str_converter_v3.py --str "path/to/file.str" --no-dtm
    python dtm_str_converter_v3.py --str "path/to/file.str" --output-dir "path/to/outputs"

For frequent local use, edit the DEFAULT_* constants below. Command-line
arguments override those defaults.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Point, Polygon

SCRIPT_DIR: Path = Path(__file__).resolve().parent

# Default settings. CLI arguments can override these values.
DEFAULT_BASE_DIR: Path = SCRIPT_DIR
DEFAULT_DTM_FILE: Path | None = None
DEFAULT_STR_FILE: Path = Path("bhsite2403.str")
DEFAULT_OUTPUT_DIR: Path = Path(".")
DEFAULT_CRS: str = "EPSG:28351"
DEFAULT_EXPORT_GEOPACKAGE: bool = True
DEFAULT_EXPORT_EXCEL: bool = False
DEFAULT_PROGRESS_INTERVAL: int = 10_000
DEFAULT_VERBOSE: bool = True
DEFAULT_PAUSE_ON_COMPLETE: bool = False
DEFAULT_AUTO_DTM: bool = True
DEFAULT_ENCODING: str = "ascii"
ASCII_CONTROL_BYTES: set[int] = {9, 10, 13}

DTM_COLUMNS: list[str] = [
    "triangle_number",
    "vertex1",
    "vertex2",
    "vertex3",
    "neighbour1",
    "neighbour2",
    "neighbour3",
]

GEOPACKAGE_OUTPUT_SUFFIXES: dict[str, str] = {
    "str_linestrings": "str_linestrings.gpkg",
    "str_points": "str_points.gpkg",
    "dtm_triangulation": "dtm_triangulation.gpkg",
}

EXCEL_OUTPUT_SUFFIXES: dict[str, str] = {
    "str_linestrings": "str_linestrings.xlsx",
    "str_points": "str_points.xlsx",
    "dtm_df": "dtm_df.xlsx",
}


@dataclass(frozen=True)
class ConverterConfig:
    base_dir: Path = DEFAULT_BASE_DIR
    dtm_file_path: Path | None = DEFAULT_DTM_FILE
    str_file_path: Path = DEFAULT_STR_FILE
    output_dir: Path = DEFAULT_OUTPUT_DIR
    crs: str = DEFAULT_CRS
    export_geopackage: bool = DEFAULT_EXPORT_GEOPACKAGE
    export_excel: bool = DEFAULT_EXPORT_EXCEL
    progress_interval: int = DEFAULT_PROGRESS_INTERVAL
    verbose: bool = DEFAULT_VERBOSE
    pause_on_complete: bool = DEFAULT_PAUSE_ON_COMPLETE
    auto_dtm: bool = DEFAULT_AUTO_DTM


class ConverterError(Exception):
    """Base exception for expected conversion failures."""


class ConverterInputError(ConverterError):
    """Raised when a configured input cannot be read as supported text."""


class ConverterParseError(ConverterError):
    """Raised when an ASCII input file cannot be parsed."""


def resolve_path(path: Path, base_dir: Path) -> Path:
    """Resolve relative paths against the configured base directory."""
    return path if path.is_absolute() else base_dir / path


def resolve_optional_path(path: Path | None, base_dir: Path) -> Path | None:
    return None if path is None else resolve_path(path, base_dir)


def find_matching_dtm_file(str_file_path: Path) -> Path | None:
    exact_candidate: Path = str_file_path.with_suffix(".dtm")
    if exact_candidate.exists():
        return exact_candidate

    uppercase_candidate: Path = str_file_path.with_suffix(".DTM")
    if uppercase_candidate.exists():
        return uppercase_candidate

    for sibling_path in str_file_path.parent.glob(f"{str_file_path.stem}.*"):
        if sibling_path.suffix.lower() == ".dtm":
            return sibling_path

    return None


def get_dtm_file_path(
    config: ConverterConfig,
    str_file_path: Path,
    base_dir: Path,
) -> Path | None:
    configured_dtm_file_path: Path | None = resolve_optional_path(
        config.dtm_file_path,
        base_dir,
    )
    if configured_dtm_file_path is not None:
        return configured_dtm_file_path

    if not config.auto_dtm:
        return None

    return find_matching_dtm_file(str_file_path)


def build_output_name(source_stem: str, output_suffix: str) -> str:
    return f"{source_stem}_{output_suffix}"


def get_output_source_stem(
    name: str,
    str_file_path: Path,
    dtm_file_path: Path | None,
) -> str:
    if name.startswith("dtm_") and dtm_file_path is not None:
        return dtm_file_path.stem

    return str_file_path.stem


def read_ascii_lines(file_path: Path, file_description: str) -> list[str]:
    try:
        raw_data: bytes = file_path.read_bytes()
    except FileNotFoundError as exc:
        raise ConverterInputError(f"{file_description} file not found: {file_path}") from exc
    except OSError as exc:
        raise ConverterInputError(f"Could not read {file_description} file: {file_path}") from exc

    if not raw_data:
        raise ConverterParseError(f"{file_description} file is empty: {file_path}")

    unsupported_control_bytes: list[int] = [byte for byte in raw_data if byte < 32 and byte not in ASCII_CONTROL_BYTES]
    if unsupported_control_bytes:
        raise ConverterInputError(f"{file_description} file appears to be binary, not ASCII text: {file_path}")

    try:
        text: str = raw_data.decode(DEFAULT_ENCODING)
    except UnicodeDecodeError as exc:
        raise ConverterInputError(f"{file_description} file is not supported ASCII text: {file_path}") from exc

    return text.splitlines()


def read_dtm_file(dtm_file_path: Path) -> pd.DataFrame:
    lines: list[str] = read_ascii_lines(dtm_file_path, "DTM")

    try:
        start_index = next(i for i, line in enumerate(lines) if "TRISOLATION" in line.upper())
    except StopIteration as exc:
        raise ConverterParseError(f"No TRISOLATION section found in DTM file: {dtm_file_path}") from exc

    data: list[list[str]] = []
    for line in lines[start_index + 1 :]:
        split_line: list[str] = [part.rstrip(",") for part in line.strip().split()]
        if len(split_line) >= len(DTM_COLUMNS):
            data.append(split_line[: len(DTM_COLUMNS)])

    if not data:
        raise ConverterParseError(f"No triangle rows found in DTM file: {dtm_file_path}")

    try:
        return pd.DataFrame(data, columns=DTM_COLUMNS).astype(int)
    except (TypeError, ValueError) as exc:
        raise ConverterParseError(f"Could not parse triangle rows in DTM file: {dtm_file_path}") from exc


def read_str_file(str_file_path: Path) -> pd.DataFrame:
    """Read a Surpac STR file into a DataFrame with generated point and group IDs."""
    data: list[list[Any]] = []
    data_line_numbers: list[int] = []
    current_group: int = 0
    point_counter: int = -1
    max_description_columns: int = 0

    lines: list[str] = read_ascii_lines(str_file_path, "STR")
    if len(lines) < 2:
        raise ConverterParseError(f"STR file does not contain point rows: {str_file_path}")

    for line_number, line in enumerate(lines[1:], start=2):
        parts: list[str] = [part.strip() for part in line.strip().split(",")]
        point_counter += 1

        if not parts or parts[0] == "0":
            current_group += 1
            continue

        if len(parts) < 4:
            continue

        data.append([point_counter, current_group, *parts])
        data_line_numbers.append(line_number)
        max_description_columns = max(max_description_columns, len(parts) - 4)

    if not data:
        raise ConverterParseError(f"No valid point rows found in STR file: {str_file_path}")

    column_names: list[str] = ["point_number", "group", "string", "y", "x", "z"] + [
        f"d{i}" for i in range(1, max_description_columns + 1)
    ]

    df_str: pd.DataFrame = pd.DataFrame(data, columns=column_names)
    df_str[["string", "x", "y", "z"]] = df_str[["string", "x", "y", "z"]].apply(
        pd.to_numeric,
        errors="coerce",
    )
    invalid_numeric_rows: pd.Series = df_str[["string", "x", "y", "z"]].isna().any(axis=1)
    if invalid_numeric_rows.any():
        invalid_lines: list[int] = [data_line_numbers[index] for index in df_str.index[invalid_numeric_rows].tolist()]
        raise ConverterParseError(f"Could not parse numeric STR values on line(s) {invalid_lines}: {str_file_path}")

    return df_str[df_str["string"] != 0]


def create_points_gdf(df_str: pd.DataFrame, crs: str) -> gpd.GeoDataFrame:
    geometry: list[Point] = [Point(x, y, z) for x, y, z in zip(df_str["x"], df_str["y"], df_str["z"])]
    return gpd.GeoDataFrame(df_str, geometry=geometry, crs=crs)


def generate_linestrings(gdf_points: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    rows: list[dict[str, Any]] = []

    for group, values in gdf_points.groupby("group"):
        if len(values) > 1:
            rows.append(
                {
                    "geometry": LineString(values.geometry.tolist()),
                    "group": group,
                }
            )

    return gpd.GeoDataFrame(rows, columns=["geometry", "group"], crs=gdf_points.crs)


def create_polygons(
    df_dtm: pd.DataFrame,
    gdf_points: gpd.GeoDataFrame,
    progress_interval: int = DEFAULT_PROGRESS_INTERVAL,
) -> gpd.GeoDataFrame:
    point_geom_map: dict[int, Point] = {
        int(point_number): geometry for point_number, geometry in zip(gdf_points["point_number"], gdf_points.geometry)
    }

    polygons: list[Polygon] = []
    dtm_rows: list[dict[str, Any]] = []

    for progress_counter, (_, row) in enumerate(df_dtm.iterrows(), start=1):
        vertex_ids: list[int] = [
            int(row["vertex1"]),
            int(row["vertex2"]),
            int(row["vertex3"]),
        ]
        vertices: list[Point | None] = [point_geom_map.get(vertex_id) for vertex_id in vertex_ids]

        if not all(vertex is not None and vertex.has_z for vertex in vertices):
            print("Missing 3D point for triangle " f"{row['triangle_number']} with vertices {vertex_ids}")
            continue

        valid_vertices: list[Point] = [vertex for vertex in vertices if vertex is not None]
        polygon = Polygon([(vertex.x, vertex.y, vertex.z) for vertex in valid_vertices])
        polygons.append(polygon)
        dtm_rows.append(row.to_dict())

        if progress_interval > 0 and progress_counter % progress_interval == 0:
            print(f"Processed {progress_counter} triangles")

    return gpd.GeoDataFrame(dtm_rows, geometry=polygons, crs=gdf_points.crs)


def export_to_geopackage(gdf: gpd.GeoDataFrame, output_path: Path) -> None:
    print(f"Exporting {output_path}")
    gdf.to_file(output_path, driver="GPKG")


def export_to_excel(df: pd.DataFrame, output_path: Path) -> None:
    print(f"Exporting {output_path}")
    df.to_excel(output_path, index=False)


def print_preview(title: str, df: pd.DataFrame) -> None:
    print(f"{title}:")
    print(df.head())
    print("")


def run_conversion(config: ConverterConfig) -> None:
    base_dir: Path = config.base_dir.resolve()
    str_file_path: Path = resolve_path(config.str_file_path, base_dir)
    dtm_file_path: Path | None = get_dtm_file_path(config, str_file_path, base_dir)
    output_dir: Path = resolve_path(config.output_dir, base_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if config.verbose:
        print(f"Base directory: {base_dir}")
        print(f"DTM input: {dtm_file_path if dtm_file_path is not None else 'none'}")
        print(f"STR input: {str_file_path}")
        print(f"Output directory: {output_dir}")
        print("")

    df_str: pd.DataFrame = read_str_file(str_file_path)
    if config.verbose:
        print_preview("Processed STR DataFrame", df_str)
        print("STR groups:")
        print(df_str["group"].unique())
        print("")

    gdf_points: gpd.GeoDataFrame = create_points_gdf(df_str, config.crs)
    if config.verbose:
        print_preview("Points GeoDataFrame", gdf_points)

    gdf_linestrings: gpd.GeoDataFrame = generate_linestrings(gdf_points)
    if config.verbose:
        print_preview("LineStrings GeoDataFrame", gdf_linestrings)

    geodataframes: dict[str, gpd.GeoDataFrame] = {
        "str_linestrings": gdf_linestrings,
        "str_points": gdf_points,
    }
    dataframes: dict[str, pd.DataFrame] = {
        "str_linestrings": gdf_linestrings.drop(columns="geometry", errors="ignore"),
        "str_points": gdf_points.drop(columns="geometry", errors="ignore"),
    }

    if dtm_file_path is not None:
        df_dtm: pd.DataFrame = read_dtm_file(dtm_file_path)
        if config.verbose:
            print_preview("DTM DataFrame", df_dtm)

        gdf_triangles: gpd.GeoDataFrame = create_polygons(
            df_dtm,
            gdf_points,
            progress_interval=config.progress_interval,
        )
        if config.verbose:
            print_preview("Triangulation GeoDataFrame", gdf_triangles)

        geodataframes["dtm_triangulation"] = gdf_triangles
        dataframes["dtm_df"] = df_dtm

    if config.export_geopackage:
        for name, output_suffix in GEOPACKAGE_OUTPUT_SUFFIXES.items():
            if name in geodataframes:
                source_stem: str = get_output_source_stem(
                    name,
                    str_file_path,
                    dtm_file_path,
                )
                output_name: str = build_output_name(source_stem, output_suffix)
                export_to_geopackage(geodataframes[name], output_dir / output_name)

    if config.export_excel:
        for name, output_suffix in EXCEL_OUTPUT_SUFFIXES.items():
            if name in dataframes:
                source_stem: str = get_output_source_stem(
                    name,
                    str_file_path,
                    dtm_file_path,
                )
                output_name: str = build_output_name(source_stem, output_suffix)
                export_to_excel(dataframes[name], output_dir / output_name)

    print("Script completed. The data has been processed and saved.")

    if config.pause_on_complete:
        input("Press Enter to exit...")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert Surpac DTM and STR files to GeoPackage and Excel outputs.",
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=DEFAULT_BASE_DIR,
        help="Base directory used to resolve relative input and output paths.",
    )
    parser.add_argument(
        "--dtm",
        dest="dtm_file_path",
        type=Path,
        default=DEFAULT_DTM_FILE,
        help="Optional input DTM file path. Requires a matching STR file.",
    )
    parser.add_argument(
        "--no-dtm",
        dest="disable_dtm",
        action="store_true",
        help="Disable DTM processing and convert only the STR file.",
    )
    parser.add_argument(
        "--auto-dtm",
        action=argparse.BooleanOptionalAction,
        default=DEFAULT_AUTO_DTM,
        help="Automatically convert a same-basename DTM next to the STR file.",
    )
    parser.add_argument(
        "--str",
        dest="str_file_path",
        type=Path,
        default=DEFAULT_STR_FILE,
        help="Input STR file path.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where output files will be written.",
    )
    parser.add_argument(
        "--crs",
        default=DEFAULT_CRS,
        help="Coordinate reference system for outputs, such as EPSG:28351.",
    )
    parser.add_argument(
        "--gpkg",
        dest="export_geopackage",
        action=argparse.BooleanOptionalAction,
        default=DEFAULT_EXPORT_GEOPACKAGE,
        help="Enable or disable GeoPackage exports.",
    )
    parser.add_argument(
        "--excel",
        dest="export_excel",
        action=argparse.BooleanOptionalAction,
        default=DEFAULT_EXPORT_EXCEL,
        help="Enable or disable Excel exports. Disabled by default.",
    )
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=DEFAULT_PROGRESS_INTERVAL,
        help="Triangle progress print interval. Use 0 to disable progress messages.",
    )
    parser.add_argument(
        "--verbose",
        action=argparse.BooleanOptionalAction,
        default=DEFAULT_VERBOSE,
        help="Enable or disable DataFrame preview output.",
    )
    parser.add_argument(
        "--pause",
        action=argparse.BooleanOptionalAction,
        default=DEFAULT_PAUSE_ON_COMPLETE,
        help="Pause for keyboard input before exiting.",
    )
    return parser.parse_args(argv)


def config_from_args(args: argparse.Namespace) -> ConverterConfig:
    return ConverterConfig(
        base_dir=args.base_dir,
        dtm_file_path=None if args.disable_dtm else args.dtm_file_path,
        str_file_path=args.str_file_path,
        output_dir=args.output_dir,
        crs=args.crs,
        export_geopackage=args.export_geopackage,
        export_excel=args.export_excel,
        progress_interval=args.progress_interval,
        verbose=args.verbose,
        pause_on_complete=args.pause,
        auto_dtm=args.auto_dtm and not args.disable_dtm,
    )


def main(argv: Sequence[str] | None = None) -> int:
    config: ConverterConfig = config_from_args(parse_args(argv))
    try:
        run_conversion(config)
    except ConverterError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
