"""Reproduce the shared real-DEM integration run using installed GRASS/TUFLOW."""

import argparse
import hashlib
import json
import time
from pathlib import Path

import rasterio
from ryan_library.orchestrators.tuflow.initialiser.verification import ProjectQA, verify_project

from ryan_library.orchestrators.tuflow.initialiser.workflow import BuildConfig, build


class RunQA(ProjectQA):
    """Project evidence plus execution settings and fixture checksums."""

    build_and_verification_seconds: float
    fixture_sha256: dict[str, str]
    tools: dict[str, str]


def main() -> None:
    """Verify shared inputs, run real tools and save reopened-output QA."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--grass-exe", required=True)
    parser.add_argument("--tuflow-exe", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--fixture",
        required=True,
        type=Path,
        help="Directory with manifest.json, dem_epsg28350.tif and region.gpkg from the original initialiser",
    )
    args = parser.parse_args()
    fixture = args.fixture.absolute()
    manifest = json.loads((fixture / "manifest.json").read_text(encoding="utf-8"))
    for name, expected in manifest["sha256"].items():
        with (fixture / name).open("rb") as stream:
            actual = hashlib.file_digest(stream, "sha256").hexdigest()
        if actual != expected:
            msg = f"Shared fixture checksum mismatch: {name}"
            raise ValueError(msg)
    output = args.output.resolve()
    if output.exists():
        msg = f"Existing output is protected: {output}"
        raise FileExistsError(msg)
    output.parent.mkdir(parents=True, exist_ok=True)
    # Derive the projection from the tracked GeoTIFF; no local .prj is required.
    with rasterio.open(fixture / "dem_epsg28350.tif") as dem:
        if dem.crs is None or dem.crs.to_epsg() != 28350:
            msg = "Shared DEM must declare EPSG:28350"
            raise ValueError(msg)
        wkt = dem.crs.to_wkt()
    projection = output.parent / f"{output.name}_projection.prj"
    with projection.open("x", encoding="utf-8") as stream:
        stream.write(wkt)
    started = time.perf_counter()
    build(
        BuildConfig(
            terrains=(fixture / "dem_epsg28350.tif",),
            aoi=fixture / "region.gpkg",
            output=output,
            name="RealDEM",
            scenario="EXG",
            prj=projection,
            tuflow_executable=args.tuflow_exe.resolve(),
            rainfall_name="RF01",
            grass_executable=args.grass_exe,
        )
    )
    report: RunQA = {
        **verify_project(output),
        "build_and_verification_seconds": time.perf_counter() - started,
        "fixture_sha256": manifest["sha256"],
        "tools": {"grass_executable": args.grass_exe, "tuflow_executable": str(args.tuflow_exe.resolve())},
    }
    (output / "qa.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
