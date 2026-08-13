"""Translate a vector dataset using editable defaults and optional CLI overrides."""

from __future__ import annotations

from pathlib import Path

WRAPPER_VERSION = "2026-08-13.1"
DEFAULT_INPUT = Path("input.shp")
DEFAULT_OUTPUT = Path("output.gpkg")
DEFAULT_SOURCE_CRS: str | None = None
DEFAULT_TARGET_CRS: str | None = None
DEFAULT_LAYER: str | None = None

import argparse

from loguru import logger

from ryan_library.functions.path_stuff import to_single_path
from ryan_library.functions.wrapper_utils import pause_console, print_wrapper_banner
from vector_conversion_candidate import resolve_vector_format, translate_vector_dataset


def main(args: argparse.Namespace) -> int:
    input_path = to_single_path(args.input if args.input is not None else DEFAULT_INPUT)
    output_path = to_single_path(args.output if args.output is not None else DEFAULT_OUTPUT)
    source_crs = args.source_crs if args.source_crs is not None else DEFAULT_SOURCE_CRS
    target_crs = args.target_crs if args.target_crs is not None else DEFAULT_TARGET_CRS
    layer = args.layer if args.layer is not None else DEFAULT_LAYER
    if not input_path.is_file():
        logger.error("Input file not found: {}", input_path)
        return 1
    try:
        format_name, _ = resolve_vector_format(output_path.suffix)
        generated = translate_vector_dataset(
            source=input_path,
            output=output_path,
            vector_format=format_name,
            layer_name=layer,
            src_srs=source_crs,
            dst_srs=target_crs,
        )
    except Exception:
        logger.exception("Failed to translate {}", input_path)
        return 1
    logger.success("Created {} files at {}", len(generated), output_path.parent)
    return 0


def _parse_cli_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=f"Translate a vector dataset (v{WRAPPER_VERSION}).")
    parser.add_argument("input", nargs="?", type=Path, help="Override DEFAULT_INPUT.")
    parser.add_argument("output", nargs="?", type=Path, help="Override DEFAULT_OUTPUT.")
    parser.add_argument("--source-crs", "-s_srs", default=None, help="Override DEFAULT_SOURCE_CRS.")
    parser.add_argument("--target-crs", "-t_srs", default=None, help="Override DEFAULT_TARGET_CRS.")
    parser.add_argument("--layer", default=None, help="Override DEFAULT_LAYER.")
    parser.add_argument("--no-pause", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    cli_args = _parse_cli_arguments()
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    result = main(cli_args)
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, leading_blank_line=True)
    if not cli_args.no_pause:
        pause_console(collect_before_pause=True)
    raise SystemExit(result)
