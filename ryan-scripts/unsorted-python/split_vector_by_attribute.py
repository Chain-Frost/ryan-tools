from __future__ import annotations
from pathlib import Path

WRAPPER_VERSION = "1.0.0"

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


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=f"Split a vector file by a specified attribute into multiple files (v{WRAPPER_VERSION})."
    )
    parser.add_argument(
        "input",
        type=str,
        help="Input vector file.",
    )
    parser.add_argument(
        "output_dir",
        type=str,
        help="Output directory to place the split files.",
    )
    parser.add_argument(
        "--attribute",
        "-a",
        type=str,
        default="Layer",
        help="Attribute to split by (default: 'Layer').",
    )
    parser.add_argument(
        "--format",
        "-f",
        type=str,
        default="shp",
        help="Output vector format (default: 'shp').",
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
        action="store_true",
        help="Run translations in parallel.",
    )
    return parser.parse_args()


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
    where_clause = f'"{attribute}" = \'{val}\''
    
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
        logger.error(f"Failed for value '{val}': {e}")
        return False


def main() -> None:
    args = _parse_cli_arguments()

    input_path = to_single_path(args.input)
    out_dir = to_single_path(args.output_dir)

    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        sys.exit(1)

    try:
        format_name, spec = resolve_vector_format(args.format)
    except ValueError as e:
        logger.error(e)
        sys.exit(1)

    out_dir.mkdir(parents=True, exist_ok=True)

    layer_name = args.layer
    if layer_name is None:
        try:
            layers = get_vector_layer_names(input_path)
            if not layers:
                logger.error("No layers found in input dataset.")
                sys.exit(1)
            layer_name = layers[0]
            if len(layers) > 1:
                logger.warning(f"Multiple layers found. Defaulting to first layer: '{layer_name}'. Use --layer to specify.")
        except Exception as e:
            logger.error(f"Could not read layer names: {e}")
            sys.exit(1)

    logger.info("Extracting unique values for attribute '{}' from layer '{}'...", args.attribute, layer_name)
    try:
        unique_values = get_unique_attribute_values(input_path, layer_name, args.attribute)
    except Exception as e:
        logger.error(f"Failed to query attribute values: {e}")
        sys.exit(1)

    if not unique_values:
        logger.warning(f"No values found for attribute '{args.attribute}'. Exiting.")
        sys.exit(0)

    logger.info(f"Found {len(unique_values)} unique values. Beginning translation...")
    
    success_count = 0
    
    if args.parallel:
        with concurrent.futures.ProcessPoolExecutor() as executor:
            futures = [
                executor.submit(
                    process_single_value,
                    val,
                    input_path,
                    out_dir,
                    format_name,
                    spec.extension,
                    args.attribute,
                    layer_name,
                )
                for val in unique_values
            ]
            for future in concurrent.futures.as_completed(futures):
                if future.result():
                    success_count += 1
    else:
        for val in unique_values:
            logger.info(f"Translating {val}...")
            if process_single_value(val, input_path, out_dir, format_name, spec.extension, args.attribute, layer_name):
                success_count += 1

    logger.success(f"Successfully processed {success_count}/{len(unique_values)} values.")

if __name__ == "__main__":
    main()
