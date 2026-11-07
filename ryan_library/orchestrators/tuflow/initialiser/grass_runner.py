"""Noninteractive GRASS processing; neither a QGIS nor ryan-tools dependency."""

import subprocess
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class GrassResults:
    """GRASS-derived rasters on original DEM grid."""

    flow_direction: Path
    accumulation: Path
    conditioned_dem: Path


def _run(args: list[str], *, directory: Path) -> None:
    result = subprocess.run(  # noqa: S603 - explicit executable/argv; no shell or command interpolation
        args, cwd=directory, capture_output=True, text=True, encoding="utf-8", errors="replace", check=False
    )
    if result.returncode:
        tail = "\n".join((result.stdout + "\n" + result.stderr).splitlines()[-35:])
        msg = f"GRASS command failed ({result.returncode}): {args!r}\n{tail}"
        raise RuntimeError(msg)


def process_terrain(
    dem: Path,
    *,
    work_dir: Path,
    grass_executable: str = "grass",
    fill_depressions: bool = False,
    memory_mb: int = 2048,
) -> GrassResults:
    """Create a temporary GRASS project and export direction/accumulation.

    Work directory must be unique per run. The original DEM is never edited.
    GRASS r.watershed accumulation is a count of contributing cells without
    a flow/weight input, and drainage uses GRASS 1..8 directions.
    """
    if not dem.is_file():
        raise FileNotFoundError(dem)
    if work_dir.exists() and any(work_dir.iterdir()):
        msg = f"GRASS staging directory must be empty: {work_dir}"
        raise FileExistsError(msg)
    work_dir.mkdir(parents=True, exist_ok=True)
    project = work_dir / "grass_project"
    grass = str(grass_executable)
    _run([grass, "-c", str(dem.absolute()), "-e", str(project.absolute())], directory=work_dir)
    mapset = project / "PERMANENT"

    def run_grass(*args: str) -> None:
        _run([grass, str(mapset), "--exec", *args], directory=work_dir)

    run_grass("r.in.gdal", f"input={dem.absolute()}", "output=raw_dem", "--overwrite")
    run_grass("g.region", "raster=raw_dem")
    elevation_name = "raw_dem"
    if fill_depressions:
        run_grass("r.fill.dir", "input=raw_dem", "output=hydro_dem", "direction=fill_direction", "--overwrite")
        elevation_name = "hydro_dem"
    run_grass(
        "r.watershed",
        "-s",
        f"elevation={elevation_name}",
        "accumulation=upstream_cells",
        "drainage=d8_direction",
        f"memory={memory_mb}",
        "--overwrite",
    )

    direction = work_dir / "d8_direction.tif"
    accumulation = work_dir / "upstream_cells.tif"
    conditioned = work_dir / "hydro_dem.tif"
    run_grass("r.out.gdal", "input=d8_direction", f"output={direction}", "format=GTiff", "type=Int16", "--overwrite")
    run_grass(
        "r.out.gdal", "input=upstream_cells", f"output={accumulation}", "format=GTiff", "type=Float64", "--overwrite"
    )
    run_grass(
        "r.out.gdal",
        f"input={elevation_name}",
        f"output={conditioned}",
        "format=GTiff",
        "type=Float32",
        "--overwrite",
    )
    for raster in (direction, accumulation, conditioned):
        if not raster.is_file():
            msg = f"GRASS output missing: {raster}"
            raise FileNotFoundError(msg)
    return GrassResults(direction, accumulation, conditioned)
