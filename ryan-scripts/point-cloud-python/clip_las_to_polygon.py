# ryan-scripts\misc-python\clip_las_to_polygon.py
"""
Clip all LAS/LAZ files in the target folder to the polygon(s) stored in clip.shp.

Dependencies (install via pip if needed):
    pip install laspy numpy shapely fiona
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, Optional

import laspy
import numpy as np
import fiona
from shapely.geometry import shape
from shapely.ops import unary_union
from shapely.prepared import prep
from shapely.geometry.base import BaseGeometry
from shapely import vectorized as shapely_vectorized



def parse_args() -> argparse.Namespace:
    default_dir = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description="Clip LAS/LAZ files to the polygon(s) defined in clip.shp.",
    )
    parser.add_argument(
        "-i",
        "--input-dir",
        type=Path,
        default=default_dir,
        help="Folder that holds clip.shp and the .las/.laz files (default: script directory).",
    )
    parser.add_argument(
        "-s",
        "--shapefile",
        type=Path,
        default=None,
        help="Path to the clipping polygon shapefile (default: <input-dir>/clip.shp).",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=None,
        help="Where clipped LAS files are written (default: <input-dir>/clipped).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=2_000_000,
        help="How many points to read at once from each LAS file (default: 2,000,000).",
    )
    parser.add_argument(
        "--glob",
        default="*.las",
        help="Glob used to discover input files relative to input-dir (default: *.las).",
    )
    parser.add_argument(
        "--also-laz",
        action="store_true",
        help="Include *.laz files automatically (keeps --glob for *.las as well).",
    )
    return parser.parse_args()


class PolygonClipper:
    """Loads and evaluates the polygon(s) used to clip point clouds."""

    def __init__(self, shapefile_path: Path) -> None:
        self.geometry = self._load_union(shapefile_path)
        if self.geometry.is_empty:
            raise ValueError(f"Clip geometry in {shapefile_path} is empty.")
        self.prepared = prep(self.geometry)
        self.bounds = self.geometry.bounds  # (minx, miny, maxx, maxy)
        self._vectorized_func = self._pick_vectorized_func()

    @staticmethod
    def _load_union(shapefile_path: Path) -> BaseGeometry:
        if not shapefile_path.exists():
            raise FileNotFoundError(f"Shapefile not found: {shapefile_path}")

        geometries = []
        with fiona.open(shapefile_path) as src:
            for feature in src:
                geom = feature.get("geometry")
                if geom:
                    g = shape(geom)
                    if not g.is_empty:
                        geometries.append(g)

        if not geometries:
            raise ValueError(f"No polygon geometries found in {shapefile_path}")

        return unary_union(geometries)

    @staticmethod
    def _pick_vectorized_func():
        if shapely_vectorized is None:
            return None
        if hasattr(shapely_vectorized, "covers"):
            return shapely_vectorized.covers
        if hasattr(shapely_vectorized, "contains"):
            return shapely_vectorized.contains
        if hasattr(shapely_vectorized, "intersects"):
            return shapely_vectorized.intersects
        return None

    def mask(self, xs: np.ndarray, ys: np.ndarray) -> np.ndarray:
        """Return boolean mask of points that fall within the polygon (bounds-inclusive)."""
        xs = np.asarray(xs)
        ys = np.asarray(ys)
        if xs.size == 0:
            return np.array([], dtype=bool)

        minx, miny, maxx, maxy = self.bounds
        mask = (xs >= minx) & (xs <= maxx) & (ys >= miny) & (ys <= maxy)
        if not mask.any():
            return mask

        idx = np.nonzero(mask)[0]
        if self._vectorized_func is not None:  # fast path using shapely.vectorized
            hits = self._vectorized_func(self.geometry, xs[idx], ys[idx])
            mask[idx] = hits
            return mask

        # Fallback to prepared geometry point tests (slower but robust).
        from shapely.geometry import Point  # imported lazily to avoid overhead

        for i in idx:
            pt = Point(xs[i], ys[i])
            mask[i] = self.prepared.contains(pt) or self.prepared.touches(pt)
        return mask


def discover_files(input_dir: Path, glob_pattern: str, include_laz: bool) -> Iterable[Path]:
    files = sorted(input_dir.glob(glob_pattern))
    if include_laz:
        files.extend(sorted(input_dir.glob("*.laz")))
    seen = set()
    for file_path in files:
        if file_path.is_file():
            if file_path not in seen:
                seen.add(file_path)
                yield file_path


def clip_las_file(
    las_path: Path,
    clipper: PolygonClipper,
    output_dir: Path,
    chunk_size: int,
) -> Optional[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / f"{las_path.stem}-CLIP{las_path.suffix}"
    temp = target.with_suffix(target.suffix + ".tmp")

    if temp.exists():
        temp.unlink()

    total_points = 0
    kept_points = 0

    with laspy.open(las_path) as reader:
        header = reader.header
        with laspy.open(temp, mode="w", header=header) as writer:
            for chunk in reader.chunk_iterator(chunk_size):
                total_points += len(chunk.x)
                mask = clipper.mask(chunk.x, chunk.y)
                if not mask.any():
                    continue
                writer.write_points(chunk[mask])
                kept_points += int(mask.sum())

    if kept_points == 0:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass
        print(f"{las_path.name}: no points fell inside the clip polygon.")
        return None

    temp.replace(target)
    print(
        f"{las_path.name}: kept {kept_points:,} of "
        f"{total_points:,} points ({kept_points / total_points:.1%})."
    )
    return target


def main() -> None:
    args = parse_args()
    input_dir = args.input_dir.resolve()
    if args.shapefile:
        shapefile = args.shapefile
    else:
        shapefile = input_dir / "clip.shp"
    output_dir = (args.output_dir or (input_dir / "clipped")).resolve()

    clipper = PolygonClipper(shapefile.resolve())
    files = list(discover_files(input_dir, args.glob, args.also_laz))
    if not files:
        print(f"No files found in {input_dir} with pattern '{args.glob}'.", file=sys.stderr)
        sys.exit(1)

    print(f"Clipping {len(files)} file(s) using {shapefile}.")
    print(f"Outputs will be written to: {output_dir}")

    processed = 0
    for las_file in files:
        try:
            result = clip_las_file(las_file, clipper, output_dir, args.chunk_size)
            if result:
                processed += 1
        except Exception as exc:
            print(f"Failed to clip {las_file.name}: {exc}", file=sys.stderr)

    if processed == 0:
        print("No clipped files produced.")
    else:
        print(f"Finished. Created {processed} clipped file(s) in {output_dir}.")


if __name__ == "__main__":
    main()
