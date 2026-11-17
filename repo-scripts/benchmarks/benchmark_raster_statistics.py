"""Compare source-enabled mean/max raster processing against a trusted Git revision.

Run from a checkout with ryan_library importable. All rasters are synthetic and
created in a temporary directory. Output parity is checked outside timed work.
The baseline module is executed Python code from the explicitly selected Git
revision; use only a revision you trust. No original project files are read.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.util
import json
import platform
import shutil
import statistics
import subprocess
import sys
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from time import perf_counter
from types import ModuleType
from typing import Any, cast

import numpy as np
import rasterio
from rasterio.transform import from_origin

from ryan_library.functions.tuflow import asc_to_asc_raster_operations as current
from ryan_library.functions.tuflow.raster_performance import RasterExecutionOptions, RasterOperationTimings

BASELINE_REF = "fc33c109d1fbf62b37b0142576355b94966b1d7d"
WIDTH = 1024
HEIGHT = 512
TEMPORAL_PATTERNS = 10
DURATIONS = 2
REPEATS = 3
INPUT_LAYOUT = "strips"
GDAL_THREADS = 1
GDAL_CACHE_MB = 128
CHUNK_MEMORY_MB = 256
SEED = 20261118
MODULE_PATH = "ryan_library/functions/tuflow/asc_to_asc_raster_operations.py"


@dataclass(frozen=True, slots=True)
class BenchmarkConfig:
    baseline_ref: str
    width: int
    height: int
    temporal_patterns: int
    durations: int
    repeats: int
    layout: str
    threads: int
    cache_mb: int
    chunk_mb: int


def _baseline_module(root: Path, revision: str) -> tuple[ModuleType, str]:
    repository = Path(__file__).absolute().parents[2]
    git = shutil.which("git")
    if git is None:
        msg = "Git must be available to read the selected baseline revision"
        raise RuntimeError(msg)
    commit = subprocess.check_output(
        [git, "rev-parse", "--verify", "--end-of-options", f"{revision}^{{commit}}"], cwd=repository, text=True
    ).strip()
    content = subprocess.check_output([git, "show", f"{commit}:{MODULE_PATH}"], cwd=repository)
    source = root / "trusted_baseline.py"
    source.write_bytes(content)
    name = "raster_statistics_benchmark_baseline"
    spec = importlib.util.spec_from_file_location(name, source)
    if spec is None or spec.loader is None:
        msg = "Cannot load the selected baseline raster module"
        raise RuntimeError(msg)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module, commit


def _make_inputs(root: Path, config: BenchmarkConfig) -> list[list[str]]:
    rng = np.random.default_rng(SEED)
    groups: list[list[str]] = []
    profile: dict[str, Any] = {
        "driver": "GTiff",
        "width": config.width,
        "height": config.height,
        "count": 1,
        "dtype": "float32",
        "nodata": -9999.0,
        "crs": "EPSG:28350",
        "transform": from_origin(300000.0, 6000000.0, 1.0, 1.0),
        "compress": "DEFLATE",
        "predictor": 2,
        "zlevel": 6,
        "num_threads": "1",
    }
    if config.layout == "tiles":
        profile.update(tiled=True, blockxsize=256, blockysize=256)
    else:
        profile.update(tiled=False, blockysize=1)
    for duration in range(config.durations):
        group: list[str] = []
        for tp in range(config.temporal_patterns):
            values = rng.random((config.height, config.width), dtype=np.float32) * (4 + duration)
            values[:, : config.width // 4] = -9999.0
            values[config.height // 3 : config.height // 2, config.width // 2 :] = -9999.0
            values[::7, ::11] = 0.0
            path = root / f"duration_{duration}_tp_{tp}.tif"
            with rasterio.open(path, "w", **profile) as dataset:
                dataset.write(values, 1)
            group.append(str(path))
        groups.append(group)
    return groups


def _run_pipeline(
    module: ModuleType, groups: list[list[str]], destination: Path, options: RasterExecutionOptions | None
) -> dict[str, object]:
    destination.mkdir()
    timings = RasterOperationTimings()
    kwargs: dict[str, Any] = {} if options is None else {"execution_options": options, "timings": timings}
    started = perf_counter()
    means: list[str] = []
    mean_started = perf_counter()
    for index, group in enumerate(groups):
        output = str(destination / f"mean_{index}.tif")
        module.compute_stat(
            "mean", group, output, nodata_policy="zero", mean_value_method="asc_to_asc", write_source=True, **kwargs
        )
        means.append(output)
    mean_seconds = perf_counter() - mean_started
    maximum = str(destination / "maximum.tif")
    nested = [str(current.source_output_paths(path)[0]) for path in means]
    max_started = perf_counter()
    if options is None:
        module.compute_stat("max", means, maximum, nodata_policy="zero", write_source=True)
        module.flatten_nested_source_provenance(
            output_file=maximum, nested_source_files=nested, original_input_groups=groups
        )
    else:
        module.compute_stat(
            "max",
            means,
            maximum,
            nodata_policy="zero",
            write_source=True,
            nested_source_files=nested,
            original_input_groups=groups,
            **kwargs,
        )
    max_seconds = perf_counter() - max_started
    return {
        "seconds": perf_counter() - started,
        "mean_seconds": mean_seconds,
        "max_seconds": max_seconds,
        "phases": asdict(timings) if options is not None else None,
    }


def _verify_outputs(baseline: Path, candidate: Path) -> str:
    if {path.name for path in baseline.iterdir()} != {path.name for path in candidate.iterdir()}:
        msg = "The baseline and candidate produced different output file sets"
        raise AssertionError(msg)
    digest = hashlib.sha256()
    for source in sorted(baseline.iterdir()):
        target = candidate / source.name
        if source.suffix == ".tif":
            with rasterio.open(source) as first, rasterio.open(target) as second:
                if first.meta != second.meta or first.compression != second.compression:
                    msg = f"Raster metadata differs: {source.name}"
                    raise AssertionError(msg)
                for _, window in first.block_windows(1):
                    a, b = first.read(1, window=window), second.read(1, window=window)
                    if a.tobytes() != b.tobytes():
                        msg = f"Raster values differ: {source.name} at {window}"
                        raise AssertionError(msg)
                    np.testing.assert_array_equal(
                        first.read_masks(1, window=window), second.read_masks(1, window=window)
                    )
                    digest.update(a.tobytes())
        elif source.suffix == ".csv":
            with (
                source.open(encoding="utf-8", newline="") as first,
                target.open(encoding="utf-8", newline="") as second,
            ):
                first_rows, second_rows = list(csv.DictReader(first)), list(csv.DictReader(second))
            if first_rows != second_rows:
                msg = f"Source legend differs: {source.name}"
                raise AssertionError(msg)
    return digest.hexdigest()


def main() -> int:
    """Measure a bounded synthetic source-enabled pipeline and print JSON."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-ref", default=BASELINE_REF)
    for flag, default in (
        ("width", WIDTH),
        ("height", HEIGHT),
        ("temporal-patterns", TEMPORAL_PATTERNS),
        ("durations", DURATIONS),
        ("repeats", REPEATS),
        ("threads", GDAL_THREADS),
        ("cache-mb", GDAL_CACHE_MB),
        ("chunk-mb", CHUNK_MEMORY_MB),
    ):
        parser.add_argument(f"--{flag}", type=int, default=default)
    parser.add_argument("--layout", choices=("strips", "tiles"), default=INPUT_LAYOUT)
    args = parser.parse_args()
    config = BenchmarkConfig(
        args.baseline_ref,
        args.width,
        args.height,
        args.temporal_patterns,
        args.durations,
        args.repeats,
        args.layout,
        args.threads,
        args.cache_mb,
        args.chunk_mb,
    )
    if min(config.width, config.height, config.temporal_patterns, config.durations, config.repeats) < 1:
        parser.error("Raster dimensions, input counts and repeats must be positive")
    options = RasterExecutionOptions(cache_mb=config.cache_mb, chunk_mb=config.chunk_mb, threads=config.threads)
    observations: dict[str, list[dict[str, object]]] = {"baseline": [], "candidate": []}
    with tempfile.TemporaryDirectory(prefix="ryan-raster-benchmark-") as directory:
        root = Path(directory)
        baseline, commit = _baseline_module(root, config.baseline_ref)
        groups = _make_inputs(root, config)
        input_bytes = sum(Path(path).stat().st_size for group in groups for path in group)
        parity_hashes: list[str] = []
        for iteration in range(config.repeats):
            order = ("baseline", "candidate") if iteration % 2 == 0 else ("candidate", "baseline")
            destinations = {name: root / f"{name}_{iteration}" for name in order}
            for name in order:
                observation = _run_pipeline(
                    baseline if name == "baseline" else current,
                    groups,
                    destinations[name],
                    None if name == "baseline" else options,
                )
                observations[name].append(observation)
                seconds = cast("float", observation["seconds"])
                print(f"{name} run {iteration + 1}: {seconds:.3f}s", file=sys.stderr)
            parity_hashes.append(_verify_outputs(destinations["baseline"], destinations["candidate"]))
        medians = {
            name: {
                key: statistics.median(cast("float", observation[key]) for observation in runs)
                for key in ("seconds", "mean_seconds", "max_seconds")
            }
            for name, runs in observations.items()
        }
    print(
        json.dumps(
            {
                "description": "Synthetic local source-enabled means and maximum; sequential groups; warm-cache effects uncontrolled",
                "baseline_commit": commit,
                "candidate_module": current.__file__,
                "candidate_module_sha256": hashlib.sha256(Path(current.__file__).read_bytes()).hexdigest(),
                "config": asdict(config),
                "input_bytes": input_bytes,
                "python": platform.python_version(),
                "platform": platform.platform(),
                "numpy": np.__version__,
                "rasterio": rasterio.__version__,
                "gdal": rasterio.__gdal_version__,
                "medians": medians,
                "observations": observations,
                "exact_output_parity_sha256": parity_hashes,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
