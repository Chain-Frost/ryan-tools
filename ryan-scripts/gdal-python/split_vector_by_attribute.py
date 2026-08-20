"""Split one vector layer into datasets grouped by an attribute value."""

# moved from unsorted, not tested in production yet - 2026-08-20

from __future__ import annotations
from pathlib import Path

WRAPPER_VERSION = "2026-08-20.1"
DEFAULT_INPUT = Path("input.gpkg")
DEFAULT_OUTPUT_DIR = Path("split_output")
DEFAULT_ATTRIBUTE = "Layer"
DEFAULT_FORMAT = "shp"
DEFAULT_LAYER: str | None = None
DEFAULT_PARALLEL = False
DEFAULT_WORKERS = 4

import argparse
import sys
import concurrent.futures

from loguru import logger

from ryan_library.functions.gdal.vector_conversion import (
    translate_vector_dataset,
    resolve_vector_format,
    get_unique_attribute_values,
    get_vector_layer_names,
)
from ryan_library.functions.path_stuff import to_single_path, sanitize_windows_filename
from ryan_library.functions.wrapper_utils import print_wrapper_banner, pause_console


def _parse_cli_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=f"Split a vector file by a specified attribute into multiple files (v{WRAPPER_VERSION})."
    )
    parser.add_argument("input", nargs="?", type=Path, help="Override DEFAULT_INPUT.")
    parser.add_argument("output_dir", nargs="?", type=Path, help="Override DEFAULT_OUTPUT_DIR.")
    parser.add_argument(
        "--attribute",
        "-a",
        type=str,
        default=None,
        help="Override DEFAULT_ATTRIBUTE.",
    )
    parser.add_argument(
        "--format",
        "-f",
        type=str,
        default=None,
        help="Override DEFAULT_FORMAT.",
    )
    parser.add_argument(
        "--layer",
        "-l",
        type=str,
        default=None,
        help="Specific layer name to use if the input dataset has multiple layers.",
    )
    parser.add_argument(
        "--parallel",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override DEFAULT_PARALLEL.",
    )
    parser.add_argument("--workers", type=int, default=None, help="Override DEFAULT_WORKERS.")
    parser.add_argument("--no-pause", action="store_true")
    return parser.parse_args(argv)


def process_single_value(
    val: str,
    input_path: Path,
    out_dir: Path,
    format_name: str,
    ext: str,
    attribute: str,
    layer_name: str | None,
) -> bool:
    safe_name = sanitize_windows_filename(val)
    out_file = out_dir / f"{input_path.stem}_{safe_name}{ext}"
    quoted_attribute = attribute.replace('"', '""')
    quoted_value = val.replace("'", "''")
    where_clause = f"\"{quoted_attribute}\" = '{quoted_value}'"

    try:
        translate_vector_dataset(
            source=input_path,
            output=out_file,
            vector_format=format_name,
            layer_name=layer_name,
            where=where_clause,
        )
        return True
    except Exception as e:
        logger.error("Failed for value '{}': {}", val, e)
        return False


def main(args: argparse.Namespace, *, working_directory: Path | None = None) -> int:
    input_path = to_single_path(args.input if args.input is not None else DEFAULT_INPUT)
    out_dir = to_single_path(args.output_dir if args.output_dir is not None else DEFAULT_OUTPUT_DIR)
    attribute = args.attribute if args.attribute is not None else DEFAULT_ATTRIBUTE
    output_format = args.format if args.format is not None else DEFAULT_FORMAT
    layer_name = args.layer if args.layer is not None else DEFAULT_LAYER
    parallel = args.parallel if args.parallel is not None else DEFAULT_PARALLEL
    workers = args.workers if args.workers is not None else DEFAULT_WORKERS

    if not input_path.exists():
        logger.error("Input file not found: {}", input_path)
        return 1

    try:
        format_name, spec = resolve_vector_format(output_format)
    except ValueError as e:
        logger.error(e)
        return 1

    out_dir.mkdir(parents=True, exist_ok=True)

    if layer_name is None:
        try:
            layers = get_vector_layer_names(input_path)
            if not layers:
                logger.error("No layers found in input dataset.")
                return 1
            if len(layers) > 1:
                logger.error("Input has multiple layers; choose one with --layer: {}", ", ".join(layers))
                return 1
            layer_name = layers[0]
        except Exception as e:
            logger.error("Could not read layer names: {}", e)
            return 1

    logger.info("Extracting unique values for attribute '{}' from layer '{}'...", attribute, layer_name)
    try:
        unique_values = get_unique_attribute_values(input_path, layer_name, attribute)
    except Exception as e:
        logger.error("Failed to query attribute values: {}", e)
        return 1

    if not unique_values:
        logger.warning("No values found for attribute '{}'. Exiting.", attribute)
        return 0

    output_names = [sanitize_windows_filename(value).casefold() for value in unique_values]
    if len(output_names) != len(set(output_names)):
        logger.error("Attribute values would produce duplicate output filenames after sanitizing.")
        return 1

    logger.info("Found {} unique values. Beginning translation...", len(unique_values))

    success_count = 0

    if parallel:
        if workers < 1:
            logger.error("--workers must be at least 1.")
            return 2
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(
                    process_single_value,
                    val,
                    input_path,
                    out_dir,
                    format_name,
                    spec.extension,
                    attribute,
                    layer_name,
                )
                for val in unique_values
            ]
            for future in concurrent.futures.as_completed(futures):
                if future.result():
                    success_count += 1
    else:
        for val in unique_values:
            logger.info("Translating {}...", val)
            if process_single_value(val, input_path, out_dir, format_name, spec.extension, attribute, layer_name):
                success_count += 1

    logger.success("Successfully processed {}/{} values.", success_count, len(unique_values))
    return 0 if success_count == len(unique_values) else 1


if __name__ == "__main__":
    args = _parse_cli_arguments()
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    try:
        result = main(args)
    except Exception as e:
        logger.exception("Wrapper failed: {}", e)
        result = 1
    finally:
        print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, leading_blank_line=True)
    if not args.no_pause:
        pause_console(collect_before_pause=True)
    sys.exit(result)
