# used to convert heic to jpeg to allow qgis geotagger to load the data.
from __future__ import annotations

import argparse
import os
import shutil
import sys
import uuid
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from pathlib import Path

try:
    from PIL import Image, ImageOps
    from pillow_heif import register_heif_opener
except ImportError as exc:
    raise SystemExit("The selected Python environment needs Pillow and pillow-heif.") from exc


DEFAULT_SOURCE = Path(r"P:\path\path")
DEFAULT_DESTINATION = Path(str(DEFAULT_SOURCE) + "_jpeg")
GPS_IFD_TAG = 0x8825


@dataclass(frozen=True)
class PhotoResult:
    action: str
    relative: Path
    gps: tuple[float, float] | None = None
    error: str | None = None


def initialize_worker() -> None:
    register_heif_opener()


def gps_position(path: Path) -> tuple[float, float] | None:
    with Image.open(path) as image:
        gps = image.getexif().get_ifd(GPS_IFD_TAG)

    try:
        latitude_values = gps[2]
        longitude_values = gps[4]
        latitude_ref = str(gps[1]).upper()
        longitude_ref = str(gps[3]).upper()
    except (KeyError, TypeError):
        return None

    latitude = sum(float(value) / (60**index) for index, value in enumerate(latitude_values))
    longitude = sum(float(value) / (60**index) for index, value in enumerate(longitude_values))
    if latitude_ref == "S":
        latitude = -latitude
    if longitude_ref == "W":
        longitude = -longitude
    return latitude, longitude


def convert_heic(source: Path, destination: Path, quality: int) -> None:
    temporary = destination.with_name(f".{destination.stem}.{uuid.uuid4().hex}.tmp.jpg")
    try:
        with Image.open(source) as opened:
            opened.load()
            image = ImageOps.exif_transpose(opened)
            if image.mode not in ("RGB", "L", "CMYK"):
                image = image.convert("RGB")

            save_options: dict[str, object] = {
                "format": "JPEG",
                "quality": quality,
                "optimize": True,
                "exif": image.getexif().tobytes(),
            }
            if opened.info.get("icc_profile"):
                save_options["icc_profile"] = opened.info["icc_profile"]
            if opened.info.get("xmp"):
                save_options["xmp"] = opened.info["xmp"]
            image.save(temporary, **save_options)

        source_gps = gps_position(source)
        output_gps = gps_position(temporary)
        if source_gps is not None:
            if output_gps is None:
                raise RuntimeError("source GPS metadata was not written to the JPEG")
            if any(abs(before - after) > 0.0000001 for before, after in zip(source_gps, output_gps)):
                raise RuntimeError(f"GPS changed from {source_gps} to {output_gps}")

        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)


def process_photo(job: tuple[Path, Path, Path, int]) -> PhotoResult:
    photo, output, relative, quality = job
    try:
        source_gps = gps_position(photo)
        if photo.suffix.lower() in {".heic", ".heif"}:
            convert_heic(photo, output, quality)
            action = "CONVERT"
        else:
            shutil.copy2(photo, output)
            action = "COPY"

        if source_gps is not None:
            output_gps = gps_position(output)
            if output_gps is None or any(
                abs(before - after) > 0.0000001 for before, after in zip(source_gps, output_gps)
            ):
                raise RuntimeError("output GPS verification failed")

        return PhotoResult(action=action, relative=relative, gps=source_gps)
    except Exception as exc:
        return PhotoResult(action="FAIL", relative=relative, error=str(exc))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert HEIC site photos to geotagged JPEGs and copy existing JPEGs.")
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--destination", type=Path, default=DEFAULT_DESTINATION)
    parser.add_argument("--quality", type=int, choices=range(1, 101), default=95)
    parser.add_argument("--force", action="store_true", help="Replace existing destination files.")
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of worker processes (default: number of available CPU cores).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.workers is not None and args.workers < 1:
        raise SystemExit("--workers must be at least 1.")

    source = args.source.resolve(strict=True)
    destination = args.destination.resolve(strict=False)
    if source == destination:
        raise SystemExit("Source and destination directories must be different.")
    destination.mkdir(parents=True, exist_ok=True)

    photos = sorted(
        (
            path
            for path in source.rglob("*")
            if path.is_file() and path.suffix.lower() in {".heic", ".heif", ".jpg", ".jpeg"}
        ),
        key=lambda path: str(path).lower(),
    )
    if not photos:
        print(f"No HEIC, HEIF, JPG, or JPEG photos found in {source}")
        return 0

    converted = copied = skipped = failed = gps_verified = 0
    claimed_outputs: dict[Path, Path] = {}
    jobs: list[tuple[Path, Path, Path, int]] = []

    for photo in photos:
        relative = photo.relative_to(source)
        output_relative = relative.with_suffix(".jpg") if photo.suffix.lower() in {".heic", ".heif"} else relative
        output = destination / output_relative
        collision_key = Path(str(output).lower())
        previous = claimed_outputs.get(collision_key)
        if previous is not None:
            failed += 1
            print(f"FAIL    {relative}: output name conflicts with {previous.relative_to(source)}")
            continue
        claimed_outputs[collision_key] = photo

        output.parent.mkdir(parents=True, exist_ok=True)
        if output.exists() and not args.force:
            skipped += 1
            print(f"SKIP    {relative} (output already exists)")
            continue

        jobs.append((photo, output, relative, args.quality))

    worker_count = args.workers if args.workers is not None else (os.process_cpu_count() or 1)
    if jobs:
        print(f"Processing {len(jobs)} photos with {worker_count} worker processes...")
        with ProcessPoolExecutor(max_workers=worker_count, initializer=initialize_worker) as executor:
            for result in executor.map(process_photo, jobs):
                if result.error is not None:
                    failed += 1
                    print(f"FAIL    {result.relative}: {result.error}")
                    continue

                if result.action == "CONVERT":
                    converted += 1
                else:
                    copied += 1

                if result.gps is None:
                    print(f"WARNING {result.action} {result.relative} (source has no GPS coordinates)")
                else:
                    gps_verified += 1
                    print(
                        f"{result.action:<7} {result.relative} "
                        f"(GPS verified: {result.gps[0]:.8f}, {result.gps[1]:.8f})"
                    )

    print()
    print(f"Destination:  {destination}")
    print(f"Converted:    {converted}")
    print(f"JPEGs copied: {copied}")
    print(f"GPS verified: {gps_verified}")
    print(f"Skipped:      {skipped}")
    print(f"Failed:       {failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
