from __future__ import annotations

WRAPPER_VERSION = "1.0.0"

import argparse
import sys

from loguru import logger

from ryan_library.functions.gdal.vector_conversion import (
    translate_vector_dataset,
    resolve_vector_format,
)
from ryan_library.functions.path_stuff import to_single_path


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=f"Translate a vector dataset to a different format or CRS (v{WRAPPER_VERSION})."
    )
    parser.add_argument(
        "input",
        type=str,
        help="Input vector file (e.g. .dxf, .shp, .gpkg).",
    )
    parser.add_argument(
        "output",
        type=str,
        help="Output vector file. The format is inferred from the extension.",
    )
    parser.add_argument(
        "--source-crs",
        "-s_srs",
        type=str,
        default=None,
        help="Override source CRS (e.g. EPSG:28351).",
    )
    parser.add_argument(
        "--target-crs",
        "-t_srs",
        type=str,
        default=None,
        help="Reproject to this target CRS (e.g. EPSG:28351).",
    )
    parser.add_argument(
        "--layer",
        type=str,
        default=None,
        help="Optional: name of the specific layer to translate.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_cli_arguments()

    input_path = to_single_path(args.input)
    output_path = to_single_path(args.output)

    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        sys.exit(1)

    try:
        format_name, _ = resolve_vector_format(output_path.suffix)
    except ValueError as e:
        logger.error(e)
        sys.exit(1)

    logger.info("Translating {} to {}...", input_path.name, output_path.name)
    try:
        generated = translate_vector_dataset(
            source=input_path,
            output=output_path,
            vector_format=format_name,
            layer_name=args.layer,
            src_srs=args.source_crs,
            dst_srs=args.target_crs,
        )
        logger.success("Created {} files at {}", len(generated), output_path.parent)
        for f in generated:
            logger.debug(f" - {f.name}")
    except Exception as e:
        logger.error(f"Failed to translate dataset: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
