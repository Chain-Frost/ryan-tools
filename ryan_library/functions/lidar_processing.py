"""Point-cloud file conversion helpers."""

# laspy does not currently publish complete typing information for its optional
# compression backends. Keep strict checking enabled for this module otherwise.
# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import laspy
from loguru import logger


def convert_laz_to_las(
    source: Path,
    output: Path,
    *,
    chunk_size: int = 1_000_000,
    overwrite: bool = False,
) -> Path:
    """Stream one compressed LAZ file to an uncompressed LAS file.

    Chunked transfer avoids loading the complete point cloud into memory and
    preserves the source header, point format, scales, offsets, and VLRs.
    """
    source = source.resolve()
    output = output.resolve()
    if output.exists() and not overwrite:
        raise FileExistsError(f"Output already exists: {output}")
    if chunk_size < 1:
        raise ValueError("chunk_size must be one or greater.")
    output.parent.mkdir(parents=True, exist_ok=True)
    with laspy.open(source) as reader:
        with laspy.open(output, mode="w", header=reader.header, do_compress=False) as writer:
            for points in reader.chunk_iterator(chunk_size):
                writer.write_points(points)
    logger.info(f"Converted LAZ to LAS: {source} -> {output}")
    return output


def convert_laz_directory(
    source_directory: Path,
    output_directory: Path,
    *,
    recursive: bool = False,
    workers: int | None = None,
    chunk_size: int = 1_000_000,
    overwrite: bool = False,
) -> list[Path]:
    """Convert all LAZ files in a directory to LAS with bounded concurrency."""
    source_directory = source_directory.resolve()
    output_directory = output_directory.resolve()
    pattern = "**/*.laz" if recursive else "*.laz"
    sources = sorted(path for path in source_directory.glob(pattern) if path.is_file())
    if not sources:
        logger.warning(f"No LAZ files found in: {source_directory}")
        return []
    worker_count = min(len(sources), workers or 1)

    def process(source: Path) -> Path:
        relative = source.relative_to(source_directory).with_suffix(".las")
        output = output_directory / relative
        if output.exists() and not overwrite and output.stat().st_mtime >= source.stat().st_mtime:
            logger.info(f"LAS output is current: {output}")
            return output
        return convert_laz_to_las(source, output, chunk_size=chunk_size, overwrite=output.exists())

    if worker_count == 1:
        return [process(source) for source in sources]
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        return list(executor.map(process, sources))
