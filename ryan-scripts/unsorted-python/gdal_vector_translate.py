from __future__ import annotations

WRAPPER_VERSION = "2026-08-11.1"

import argparse
import sys
from pathlib import Path

from loguru import logger

from ryan_library.functions.gdal.vector_conversion import (
    translate_vector_dataset,
    resolve_vector_format,
)
from ryan_library.functions.path_stuff import to_single_path
from ryan_library.functions.wrapper_utils import print_wrapper_banner, pause_console


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


def main(*, working_directory: Path | None = None) -> int:
    args = _parse_cli_arguments()

    input_path = to_single_path(args.input)
    output_path = to_single_path(args.output)

    if not input_path.exists():
        logger.error("Input file not found: {}", input_path)
        return 1

    try:
        format_name, _ = resolve_vector_format(output_path.suffix)
    except ValueError as e:
        logger.error(e)
        return 1

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
            logger.debug(" - {}", f.name)
    except Exception as e:
        logger.error("Failed to translate dataset: {}", e)
        return 1
    return 0


if __name__ == "__main__":
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    try:
        result = main()
    except Exception as e:
        logger.exception("Wrapper failed: {}", e)
        result = 1
    finally:
        print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, leading_blank_line=True)
        pause_console(collect_before_pause=True)
    sys.exit(result)
