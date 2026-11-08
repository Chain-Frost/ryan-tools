"""Run the bounded, scratch-only Windows Policy experiment from issue #299.

The production profiles and source tree are immutable inputs. This is a serial
artifact-replay pilot, not a production installer or a general benchmark service.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import tarfile
import time
import tomllib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from urllib.parse import urldefrag, urlsplit

SOURCE_SHA = "92351854625dae81001c033306969f73eff4a7f7"
PYTHON_VERSION = "3.14.10"
PIP_VERSION = "26.3"
COMMAND_TIMEOUT_SECONDS = 600
EXPERIMENT_TIMEOUT_SECONDS = 35 * 60
OMITTED = frozenset({"build", "ruff", "pytest-cov"})
ALLOWED_CHANGES = frozenset(
    {
        ".github/workflows/policy-dependency-benchmark.yml",
        "repo-scripts/benchmarks/benchmark_policy_dependencies.py",
        "repo-scripts/benchmarks/README.md",
        "docs/work/2026-11-08-ci-dependency-groups.md",
        "docs/work/README.md",
    }
)
EXCLUDED_TREES = ("unsorted", "tests/test_data", "qgis-resources", "excel-resources")
MANIFEST = (
    "repo-scripts/ci_scope.py",
    "tests/repo_scripts/test_ci_scope.py",
    "ryan_library/functions/pandas/median_calc.py",
    "ryan_library/functions/tuflow/gpu_occupancy.py",
    "ryan_library/functions/gdal/profiling.py",
    "ryan_library/functions/gdal/terrain_mosaic.py",
    "ryan_library/functions/geometry/raster_coordinates.py",
    "ryan_library/functions/tuflow/tgc_terrain.py",
    "ryan_library/functions/tuflow/pytuflow_compatibility.py",
    "ryan_library/orchestrators/tuflow/project_setup.py",
    "ryan_library/orchestrators/tuflow/initialiser/gis_writer.py",
    "ryan_library/orchestrators/tuflow/initialiser/workflow.py",
    "ryan_library/orchestrators/tuflow/initialiser/cli.py",
    "repo-scripts/tuflow-initialiser/validate_test_project.py",
    "ryan_library/mcp/server.py",
    "ryan_library/functions/lidar_processing.py",
    "ryan_library/processors/tuflow/maximums_1d/ccAProcessor.py",
)
SUPPLEMENTAL_MANIFEST = (
    "ryan-scripts/AutoCAD-python/group-by-entity-colour-and-dissolve-to-hatch_dask3.py",
    "ryan-scripts/misc-python/heic_to_jpeg.py",
    "ryan-scripts/unsorted-python/export_water_level_profile_tables_50m.py",
)
CORE_FILES = (
    "pyproject.toml",
    "repo-scripts/ci-policy-requirements.txt",
    "repo-scripts/pytuflow-requirement.txt",
    "repo-scripts/install_latest_wheel.py",
    "repo-scripts/ci_gis_bootstrap.py",
    "repo-scripts/vendor_pytuflow_wheel.py",
    *MANIFEST,
    *SUPPLEMENTAL_MANIFEST,
)
LIMITATIONS = (
    "One serial Windows runner, two repeats; no account-wide cost projection.",
    "Cold/warm refers only to pip's download cache, not OS, network or CDN caches.",
    "Named URL/hash constraints change resolution work relative to unconstrained routine Policy.",
    "Fresh source snapshots and environments are used for every sample, including warm-cache samples.",
    "The fixed maintained-file manifest is representative, not the entire cumulative PR selection.",
    "Locked remote URLs may prevent the GIS helper's local-file network recovery; such a run fails closed.",
    "Full Hosted tests, QGIS, solver execution and legacy-script campaigns are outside this experiment.",
)


def _name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).lower()


def _requirement_name(value: str) -> str:
    match = re.match(r"\s*([A-Za-z0-9][A-Za-z0-9_.-]*)", value)
    if match is None:
        msg = f"Unsupported requirement: {value!r}"
        raise ValueError(msg)
    return _name(match[1])


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    value: Any = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        msg = f"Expected a JSON object: {path}"
        raise ValueError(msg)
    return cast(dict[str, Any], value)


def _sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _executable(name: str) -> str:
    executable = shutil.which(name)
    if executable is None:
        message = f"Required executable is unavailable: {name}"
        raise RuntimeError(message)
    return executable


def _git(repository: Path, *arguments: str) -> bytes:
    return subprocess.run(
        [_executable("git"), *arguments], cwd=repository, check=True, capture_output=True, timeout=30
    ).stdout


def _under(path: Path, parent: Path) -> bool:
    return path == parent or parent in path.parents


def _cache_state(path: Path) -> dict[str, int]:
    files = [file for file in path.rglob("*") if file.is_file()]
    return {"files": len(files), "bytes": sum(file.stat().st_size for file in files)}


def _source_plan(repository: Path) -> dict[str, Any]:
    requested = os.environ.get("POLICY_SOURCE_SHA", SOURCE_SHA)
    if requested != SOURCE_SHA:
        msg = f"This reviewed experiment is fixed to {SOURCE_SHA}, got {requested}"
        raise ValueError(msg)
    source_tree = _git(repository, "rev-parse", f"{SOURCE_SHA}^{{tree}}").decode().strip()
    head = _git(repository, "rev-parse", "HEAD").decode().strip()
    changed = set(
        _git(repository, "diff", "--no-renames", "--name-only", "-z", SOURCE_SHA, "HEAD")
        .decode("utf-8")
        .strip("\0")
        .split("\0")
    ) - {""}
    dirty = set(_git(repository, "diff", "--name-only", "-z", "HEAD").decode("utf-8").strip("\0").split("\0")) - {""}
    if changed - ALLOWED_CHANGES or dirty - ALLOWED_CHANGES:
        msg = f"Source drift outside the experiment allowlist: {sorted((changed | dirty) - ALLOWED_CHANGES)}"
        raise ValueError(msg)
    metadata = tomllib.loads(_git(repository, "show", f"{SOURCE_SHA}:pyproject.toml").decode("utf-8"))
    project = metadata["project"]
    development: list[str] = project["optional-dependencies"]["dev"]
    if frozenset(name for entry in development if (name := _requirement_name(entry)) in OMITTED) != OMITTED:
        msg = "The reviewed three development dependencies are not present"
        raise ValueError(msg)
    candidate = [entry for entry in development if _requirement_name(entry) not in OMITTED]
    tracked = _git(repository, "ls-tree", "-r", "--name-only", "-z", SOURCE_SHA).decode("utf-8").strip("\0").split("\0")
    identity_files = sorted(
        set(CORE_FILES)
        | {path for path in tracked if path.endswith(".pyi") or path.startswith("repo-scripts/wheels/pytuflow/")}
    )
    hashes = {
        path: hashlib.sha256(_git(repository, "show", f"{SOURCE_SHA}:{path}")).hexdigest() for path in identity_files
    }
    return {
        "source_sha": SOURCE_SHA,
        "source_tree": source_tree,
        "experiment_head": head,
        "allowed_head_changes": sorted(changed),
        "tracked_local_changes": sorted(dirty),
        "runtime_requirements": project["dependencies"],
        "mcp_requirements": project["optional-dependencies"]["mcp"],
        "canonical_dev_requirements": development,
        "candidate_dev_requirements": candidate,
        "build_requirements": metadata["build-system"]["requires"],
        "manifest": list(MANIFEST),
        "supplemental_manifest": {
            "classification": "Repository-only legacy consumers; strict checking only, no execution or lifecycle promotion",
            "files": list(SUPPLEMENTAL_MANIFEST),
        },
        "core_sha256": hashes,
        "schedule": [
            {"repeat": repeat, "variant": variant, "cache": condition}
            for repeat, variants in ((1, ("canonical", "candidate")), (2, ("candidate", "canonical")))
            for condition in ("cold", "warm")
            for variant in variants
        ],
        "python_version": PYTHON_VERSION,
        "pip_version": PIP_VERSION,
        "command_timeout_seconds": COMMAND_TIMEOUT_SECONDS,
        "experiment_timeout_seconds": EXPERIMENT_TIMEOUT_SECONDS,
        "limitations": list(LIMITATIONS),
    }


@dataclass(frozen=True)
class Artifact:
    """A pip-selected remote wheel identity, with no inference from a version alone."""

    name: str
    version: str
    url: str
    sha256: str

    def constraint(self) -> str:
        return f"{self.name} @ {self.url}#sha256={self.sha256}"

    def as_json(self) -> dict[str, str]:
        return {"name": self.name, "version": self.version, "url": self.url, "sha256": self.sha256}


def _artifacts(report_path: Path) -> tuple[dict[str, Artifact], dict[str, str]]:
    report = _read_json(report_path)
    if report.get("version") != "1" or report.get("pip_version") != PIP_VERSION:
        msg = f"Unexpected pip report version: {report_path}"
        raise ValueError(msg)
    remote: dict[str, Artifact] = {}
    versions: dict[str, str] = {}
    for entry in report["install"]:
        name = _name(entry["metadata"]["name"])
        version: str = entry["metadata"]["version"]
        if name in versions:
            msg = f"Duplicate distribution in {report_path}: {name}"
            raise ValueError(msg)
        versions[name] = version
        info = entry["download_info"]
        raw_url: str = info["url"]
        if name in {"ryan-tools", "pytuflow"}:
            if urlsplit(raw_url).scheme != "file":
                msg = f"Local accepted source was replaced: {name}"
                raise ValueError(msg)
            continue
        url, _ = urldefrag(raw_url)
        parsed = urlsplit(url)
        digest: str = info.get("archive_info", {}).get("hashes", {}).get("sha256", "")
        if (
            parsed.scheme != "https"
            or parsed.username is not None
            or parsed.password is not None
            or not parsed.path.lower().endswith(".whl")
            or re.fullmatch(r"[0-9a-f]{64}", digest) is None
        ):
            msg = f"Cannot freeze a remote wheel identity for {name}: {report_path}"
            raise ValueError(msg)
        remote[name] = Artifact(name, version, url, digest)
    return remote, versions


def _constraints(path: Path, artifacts: dict[str, Artifact]) -> None:
    path.write_text("\n".join(artifacts[name].constraint() for name in sorted(artifacts)) + "\n", encoding="utf-8")


INVENTORY_CODE = r"""
import importlib.metadata as metadata
import json
import pathlib
import re
import site
import sys
import sysconfig

def name(value):
    return re.sub(r"[-_.]+", "-", value).lower()

prefix = pathlib.Path(sys.prefix).resolve()
assert sys.prefix != sys.base_prefix and not site.ENABLE_USER_SITE
locations = sorted({str(pathlib.Path(sysconfig.get_path(key)).resolve()) for key in ("purelib", "platlib")})
assert all(pathlib.Path(path).is_relative_to(prefix) for path in locations)
distributions = {}
for dist in metadata.distributions(path=locations):
    key = name(dist.metadata["Name"])
    origin = pathlib.Path(dist.locate_file("")).resolve()
    if not origin.is_relative_to(prefix):
        raise RuntimeError("Distribution outside this environment: " + str(origin))
    if key in distributions:
        raise RuntimeError("Duplicate installed distribution: " + key)
    distributions[key] = {
        "version": dist.version,
        "location": str(origin),
        "direct_url": json.loads(dist.read_text("direct_url.json") or "null"),
    }
plugins = sorted(
    [{"name": entry.name, "value": entry.value,
      "distribution": name(entry.dist.metadata["Name"]), "version": entry.dist.version}
     for entry in metadata.entry_points(group="pytest11")], key=lambda value: value["name"])
result = {"executable": sys.executable, "prefix": sys.prefix, "base_prefix": sys.base_prefix,
          "sys_path": sys.path, "site_locations": locations, "user_site_enabled": site.ENABLE_USER_SITE,
          "python_version": sys.version, "distributions": distributions, "pytest_plugins": plugins}
pathlib.Path(sys.argv[1]).write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
"""


PROBE_CODE = r"""
import importlib
import io
import json
import pathlib
import sys
import tempfile

import numpy as np
import pandas as pd
from osgeo import gdal
import rasterio
from rasterio.io import MemoryFile
from rasterio.transform import from_origin
import fiona
from scipy.ndimage import binary_dilation
import distributed
from dask.distributed import Client
import pillow_heif
from PIL import Image
import docx
from ryan_library.functions.tuflow.pytuflow_compatibility import require_validation_api
from ryan_library.mcp import server

source = pathlib.Path.cwd().resolve()
prefix = pathlib.Path(sys.prefix).resolve()
origins = {}
for module_name in ("numpy", "pandas", "pyarrow", "osgeo.gdal", "rasterio", "fiona", "scipy",
                    "distributed", "dask", "pillow_heif", "PIL", "docx", "mcp", "pytuflow", "ryan_library"):
    module = importlib.import_module(module_name)
    origin = pathlib.Path(module.__file__).resolve()
    expected = source if module_name == "ryan_library" else prefix
    if not origin.is_relative_to(expected):
        raise RuntimeError(f"Foreign import for {module_name}: {origin}")
    origins[module_name] = str(origin)

dataset = gdal.GetDriverByName("MEM").Create("", 2, 2, 1, gdal.GDT_Float32)
dataset.GetRasterBand(1).Fill(7)
assert np.array_equal(dataset.ReadAsArray(), np.full((2, 2), 7, dtype="float32"))
dataset = None
values = np.array([[1, 2], [3, 4]], dtype="float32")
with MemoryFile() as memory:
    with memory.open(driver="GTiff", height=2, width=2, count=1, dtype="float32",
                     crs="EPSG:28356", transform=from_origin(100, 100, 1, 1)) as raster:
        raster.write(values, 1)
    with memory.open() as raster:
        assert np.array_equal(raster.read(1), values)
        assert raster.crs.to_epsg() == 28356
with tempfile.TemporaryDirectory(dir=sys.argv[2]) as temporary:
    path = pathlib.Path(temporary) / "probe.gpkg"
    schema = {"geometry": "Point", "properties": {"probe_id": "int"}}
    with fiona.open(path, "w", driver="GPKG", schema=schema, crs="EPSG:28356") as collection:
        collection.write({"geometry": {"type": "Point", "coordinates": (100.0, 100.0)},
                          "properties": {"probe_id": 7}})
    with fiona.open(path) as collection:
        features = list(collection)
        assert len(features) == 1 and features[0]["properties"]["probe_id"] == 7
frame = pd.DataFrame({"value": [1, 2]})
buffer = io.BytesIO()
frame.to_parquet(buffer, engine="pyarrow", index=False)
buffer.seek(0)
assert pd.read_parquet(buffer, engine="pyarrow").equals(frame)
mask = np.zeros((3, 3), dtype=bool)
mask[1, 1] = True
assert int(binary_dilation(mask).sum()) == 5
assert distributed.Client is Client and callable(Client)
pillow_heif.register_heif_opener()
assert "HEIF" in Image.OPEN
document = docx.Document()
document.add_paragraph("Policy dependency probe")
buffer = io.BytesIO()
document.save(buffer)
buffer.seek(0)
assert docx.Document(buffer).paragraphs[0].text == "Policy dependency probe"
assert server.mcp is not None
exports = ("parse_tuflow_filename", "inspect_tuflow_result", "inspect_tuflow_collection",
           "inspect_raster_metadata", "check_repo_health", "list_workflows", "get_workflow")
assert all(callable(getattr(server, name)) for name in exports)
parsed = server.parse_tuflow_filename("benchmark_1d_H.csv")
assert parsed["file_name"] == "benchmark_1d_H.csv"
diagnostic = require_validation_api().diagnostic
result = {"passed": ["gdal_mem", "rasterio_memory_gtiff", "fiona_gpkg", "pandas_pyarrow",
                     "scipy_binary_dilation", "distributed_client", "heif_registration", "docx_roundtrip",
                     "mcp_exports", "tuflow_filename", "accepted_parser_api"],
          "parser_diagnostic": diagnostic.message,
          "parser_status": {"available": diagnostic.available, "version": diagnostic.version, "problem": diagnostic.problem},
          "mcp_exports": list(exports), "module_origins": origins}
pathlib.Path(sys.argv[1]).write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
"""


class Experiment:
    """Own bounded temporary files, subprocesses and evidence for one execution."""

    def __init__(self, repository: Path, output: Path, scratch: Path, plan: dict[str, Any]) -> None:
        self.repository = repository
        self.output = output
        self.scratch = scratch
        self.plan = plan
        self.started = time.monotonic()
        self.commands: list[dict[str, Any]] = []
        self.samples: list[dict[str, Any]] = []
        self.archive = scratch / "source.tar"
        self.pip_wheel: Path | None = None
        self.build_constraints = output / "build-constraints.txt"
        self.gis_constraints = output / "gis-constraints.txt"
        self.profile_constraints = output / "profile-constraints.txt"
        self.gis_artifacts: dict[str, Artifact] = {}
        self.profile_artifacts: dict[str, Artifact] = {}
        self.expected_versions: dict[str, dict[str, str]] = {}
        self.baseline_diagnostics: dict[str, Any] | None = None
        self.baseline_capabilities: dict[str, Any] | None = None
        self.canonical_plugins: list[dict[str, str]] | None = None

    def checkpoint(self, status: str, error: str | None = None) -> None:
        elapsed = time.monotonic() - self.started
        _write_json(
            self.output / "results.json",
            {
                "status": status,
                "error": error,
                "source_sha": SOURCE_SHA,
                "experiment_head": self.plan["experiment_head"],
                "elapsed_seconds": elapsed,
                "samples": self.samples,
                "commands": self.commands,
                "limitations": list(LIMITATIONS),
            },
        )

    def run(
        self,
        label: str,
        command: list[str],
        *,
        cwd: Path,
        env: dict[str, str],
        accepted: tuple[int, ...] = (0,),
    ) -> dict[str, Any]:
        remaining = EXPERIMENT_TIMEOUT_SECONDS - (time.monotonic() - self.started)
        if remaining <= 0:
            msg = "The experiment's 35-minute internal deadline expired"
            raise TimeoutError(msg)
        log = self.output / "logs" / f"{len(self.commands):03d}-{label}.log"
        log.parent.mkdir(parents=True, exist_ok=True)
        started = time.monotonic()
        record: dict[str, Any] = {
            "label": label,
            "command": command,
            "cwd": str(cwd),
            "started_at": datetime.now(UTC).isoformat(),
            "log": log.relative_to(self.output).as_posix(),
        }
        self.commands.append(record)
        print(f"Starting {label}", flush=True)
        try:
            with log.open("wb") as stream:
                process = subprocess.Popen(
                    command,
                    cwd=cwd,
                    env=env,
                    stdout=stream,
                    stderr=subprocess.STDOUT,
                    start_new_session=os.name != "nt",
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
                )
                try:
                    record["returncode"] = process.wait(timeout=min(COMMAND_TIMEOUT_SECONDS, remaining))
                except subprocess.TimeoutExpired:
                    if os.name == "nt":
                        subprocess.run(
                            [_executable("taskkill"), "/PID", str(process.pid), "/T", "/F"],
                            capture_output=True,
                            check=False,
                            timeout=30,
                        )
                    else:
                        os.killpg(process.pid, signal.SIGKILL)
                    process.kill()
                    process.wait(timeout=30)
                    record["timed_out"] = True
                    msg = f"Command deadline expired: {label}"
                    raise TimeoutError(msg) from None
            if record["returncode"] not in accepted:
                msg = f"{label} failed with exit {record['returncode']}; see {log}"
                raise RuntimeError(msg)
            return record
        finally:
            record["elapsed_seconds"] = time.monotonic() - started
            self.checkpoint("running")

    def environment(self, python: Path | None, cache: Path, source: Path) -> dict[str, str]:
        env = {
            key: value
            for key, value in os.environ.items()
            if not key.upper().startswith(("PIP_", "PYTHON", "PYRIGHT_"))
            and key.upper() not in {"VIRTUAL_ENV", "CONDA_PREFIX", "CONDA_DEFAULT_ENV"}
        }
        env.update(
            {
                "PIP_CONFIG_FILE": os.devnull,
                "PIP_INDEX_URL": "https://pypi.org/simple",
                "PIP_CACHE_DIR": str(cache),
                "PIP_DISABLE_PIP_VERSION_CHECK": "1",
                "PIP_NO_INPUT": "1",
                "PIP_PROGRESS_BAR": "off",
                "PIP_TIMEOUT": "30",
                "PIP_RETRIES": "1",
                "PYTHONNOUSERSITE": "1",
                "PYTHONUNBUFFERED": "1",
                "PYTHONUTF8": "1",
                "PYRIGHT_PYTHON_CACHE_DIR": str(source.parent / "pyright-cache"),
                "PYRIGHT_PYTHON_ENV_DIR": str(source.parent / "pyright-env"),
                "RYAN_MCP_PROFILE": "create",
                "RYAN_TOOLS_REPOSITORY_ROOT": str(source),
                "MPLBACKEND": "Agg",
            }
        )
        if python is not None:
            env["PATH"] = str(python.parent) + os.pathsep + env.get("PATH", "")
            env["VIRTUAL_ENV"] = str(python.parent.parent)
        return env

    def snapshot(self, destination: Path) -> None:
        destination.mkdir(parents=True)
        with tarfile.open(self.archive) as archive:
            members = [
                member
                for member in archive.getmembers()
                if not any(member.name == tree or member.name.startswith(tree + "/") for tree in EXCLUDED_TREES)
            ]
            archive.extractall(destination, members=members, filter="data")
        actual = {path: _sha256(destination / path) for path in self.plan["core_sha256"]}
        if actual != self.plan["core_sha256"]:
            msg = "Fresh source snapshot differs from the reviewed source"
            raise ValueError(msg)

    def fresh(self, name: str, cache: Path) -> tuple[Path, Path, dict[str, str], dict[str, float]]:
        started = time.monotonic()
        root = self.scratch / name
        source = root / "source"
        self.snapshot(source)
        timing = {"source_copy_seconds": time.monotonic() - started}
        env = self.environment(None, cache, source)
        timing["environment_creation_seconds"] = self.run(
            f"{name}-venv", [sys.executable, "-m", "venv", str(root / "venv")], cwd=source, env=env
        )["elapsed_seconds"]
        python = root / "venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        env = self.environment(python, cache, source)
        if self.pip_wheel is None:
            msg = "The fixed pip wheel has not been prepared"
            raise RuntimeError(msg)
        timing["pip_pin_seconds"] = self.run(
            f"{name}-pip",
            [str(python), "-m", "pip", "install", "--no-index", "--no-deps", "--no-cache-dir", str(self.pip_wheel)],
            cwd=source,
            env=env,
        )["elapsed_seconds"]
        timing["source_and_environment_seconds"] = time.monotonic() - started
        return source, python, env, timing

    def profile(self, source: Path, variant: str, destination: Path) -> Path:
        if variant == "canonical":
            return source / "repo-scripts/ci-policy-requirements.txt"
        metadata = tomllib.loads((source / "pyproject.toml").read_text(encoding="utf-8"))
        development: list[str] = metadata["project"]["optional-dependencies"]["dev"]
        retained = [entry for entry in development if _requirement_name(entry) not in OMITTED]
        if (
            development != self.plan["canonical_dev_requirements"]
            or retained != self.plan["candidate_dev_requirements"]
        ):
            message = "The per-sample generated candidate differs from the frozen source metadata"
            raise ValueError(message)
        contents = [
            f"-r {source.joinpath('repo-scripts/pytuflow-requirement.txt').as_posix()}",
            "-e .[mcp]",
            *retained,
        ]
        destination.write_text("\n".join(contents) + "\n", encoding="utf-8")
        return destination

    def inventory(self, name: str, python: Path, source: Path, env: dict[str, str]) -> dict[str, Any]:
        path = self.output / f"{name}-inventory.json"
        self.run(name + "-inventory", [str(python), "-c", INVENTORY_CODE, str(path)], cwd=source, env=env)
        return _read_json(path)

    def prepare(self) -> None:
        started = time.monotonic()
        with self.archive.open("wb") as stream:
            subprocess.run(
                [_executable("git"), "archive", "--format=tar", SOURCE_SHA],
                cwd=self.repository,
                stdout=stream,
                check=True,
                timeout=60,
            )
        cache = self.scratch / "preparation-cache"
        env = self.environment(None, cache, self.repository)
        wheel_dir = self.scratch / "bootstrap-wheels"
        wheel_dir.mkdir()
        self.run(
            "prepare-pip-wheel",
            [
                sys.executable,
                "-m",
                "pip",
                "download",
                "--only-binary=:all:",
                "--no-deps",
                "-d",
                str(wheel_dir),
                f"pip=={PIP_VERSION}",
            ],
            cwd=self.repository,
            env=env,
        )
        wheels = list(wheel_dir.glob("pip-*.whl"))
        if len(wheels) != 1:
            msg = "Expected exactly one fixed pip wheel"
            raise ValueError(msg)
        self.pip_wheel = wheels[0]
        source, python, env, _ = self.fresh("preparation", cache)
        build_report = self.output / "prepare-build-report.json"
        self.run(
            "prepare-build-resolution",
            [
                str(python),
                "-m",
                "pip",
                "install",
                "--dry-run",
                "--ignore-installed",
                "--only-binary=:all:",
                "--report",
                str(build_report),
                *self.plan["build_requirements"],
            ],
            cwd=source,
            env=env,
        )
        build_artifacts, _ = _artifacts(build_report)
        _constraints(self.build_constraints, build_artifacts)
        env["PIP_BUILD_CONSTRAINT"] = str(self.build_constraints)
        gis_report = self.output / "prepare-gis-report.json"
        env["PIP_REPORT"] = str(gis_report)
        self.run("prepare-gis", [str(python), "repo-scripts/ci_gis_bootstrap.py"], cwd=source, env=env)
        env.pop("PIP_REPORT")
        self.gis_artifacts, gis_versions = _artifacts(gis_report)
        if not {"fiona", "rasterio", "gdal"}.issubset(self.gis_artifacts):
            msg = "GIS preparation did not retain all three wheel identities; recovery may have replaced its report"
            raise ValueError(msg)
        _constraints(self.gis_constraints, self.gis_artifacts)
        canonical_report = self.output / "prepare-canonical-report.json"
        self.run(
            "prepare-canonical-profile",
            [
                str(python),
                "-m",
                "pip",
                "install",
                "--report",
                str(canonical_report),
                "-r",
                str(self.profile(source, "canonical", self.output / "unused.txt")),
            ],
            cwd=source,
            env=env,
        )
        canonical_artifacts, canonical_versions = _artifacts(canonical_report)
        self.profile_artifacts = self.gis_artifacts | canonical_artifacts
        _constraints(self.profile_constraints, self.profile_artifacts)
        canonical = self.inventory("prepare-canonical", python, source, env)
        self.expected_versions["canonical"] = {
            name: entry["version"] for name, entry in canonical["distributions"].items()
        }
        expected = gis_versions | canonical_versions | {"pip": PIP_VERSION}
        if expected != self.expected_versions["canonical"]:
            msg = "Preparation reports do not account for every installed canonical distribution"
            raise ValueError(msg)
        self.canonical_plugins = canonical["pytest_plugins"]
        candidate_profile = self.profile(source, "candidate", self.output / "prepare-candidate-requirements.txt")
        candidate_report = self.output / "prepare-candidate-report.json"
        env["PIP_CONSTRAINT"] = str(self.profile_constraints)
        self.run(
            "prepare-candidate-resolution",
            [
                str(python),
                "-m",
                "pip",
                "install",
                "--dry-run",
                "--ignore-installed",
                "--report",
                str(candidate_report),
                "-r",
                str(candidate_profile),
            ],
            cwd=source,
            env=env,
        )
        candidate_artifacts, candidate_versions = _artifacts(candidate_report)
        for name, artifact in candidate_artifacts.items():
            if artifact != self.profile_artifacts.get(name):
                msg = f"Candidate selected a different shared artifact: {name}"
                raise ValueError(msg)
        self.expected_versions["candidate"] = gis_versions | candidate_versions | {"pip": PIP_VERSION}
        if set(self.expected_versions["candidate"]) - set(self.expected_versions["canonical"]):
            msg = "Candidate introduced an unexpected distribution"
            raise ValueError(msg)
        for name, version in self.expected_versions["candidate"].items():
            if version != self.expected_versions["canonical"][name]:
                msg = f"Candidate changed a retained package version: {name}"
                raise ValueError(msg)
        _write_json(
            self.output / "preparation.json",
            {
                "elapsed_seconds": time.monotonic() - started,
                "source_archive_sha256": _sha256(self.archive),
                "pip_wheel_sha256": _sha256(self.pip_wheel),
                "build_artifacts": {name: value.as_json() for name, value in build_artifacts.items()},
                "gis_artifacts": {name: value.as_json() for name, value in self.gis_artifacts.items()},
                "profile_artifacts": {name: value.as_json() for name, value in self.profile_artifacts.items()},
                "expected_versions": self.expected_versions,
                "candidate_distributions_removed": sorted(
                    set(self.expected_versions["canonical"]) - set(self.expected_versions["candidate"])
                ),
            },
        )
        shutil.rmtree(self.scratch / "preparation")
        shutil.rmtree(cache)

    def sample(self, repeat: int, variant: str, condition: str) -> None:
        name = f"r{repeat}-{variant}-{condition}"
        cache = self.scratch / f"cache-r{repeat}-{variant}"
        if condition == "cold":
            if cache.exists():
                msg = f"Cold cache already exists: {cache}"
                raise ValueError(msg)
            cache.mkdir()
        elif not cache.is_dir() or not any(cache.iterdir()):
            msg = f"Warm cache was not populated by its paired cold sample: {cache}"
            raise ValueError(msg)
        started = time.monotonic()
        record: dict[str, Any] = {
            "name": name,
            "repeat": repeat,
            "variant": variant,
            "cache": condition,
            "free_disk_bytes_before": shutil.disk_usage(self.scratch).free,
            "status": "running",
            "cache_before": _cache_state(cache),
            "cache_seed_seconds": 0.0,
            "cache_seed_method": "paired cold sample" if condition == "warm" else "new empty directory",
        }
        self.samples.append(record)
        if record["free_disk_bytes_before"] < 8 * 1024**3:
            msg = "Less than 8 GiB free before a fresh environment; stopping the bounded experiment"
            raise RuntimeError(msg)
        source, python, env, setup_timing = self.fresh(name, cache)
        record.update(setup_timing)
        if condition == "cold" and any(cache.iterdir()):
            msg = "Environment creation or pip pinning unexpectedly populated the cold pip cache"
            raise ValueError(msg)
        env["PIP_BUILD_CONSTRAINT"] = str(self.build_constraints)
        env["PIP_CONSTRAINT"] = str(self.gis_constraints)
        gis_report = self.output / f"{name}-gis-report.json"
        env["PIP_REPORT"] = str(gis_report)
        record["gis_seconds"] = self.run(
            name + "-gis", [str(python), "repo-scripts/ci_gis_bootstrap.py"], cwd=source, env=env
        )["elapsed_seconds"]
        env.pop("PIP_REPORT")
        env["PIP_CONSTRAINT"] = str(self.profile_constraints)
        generation_started = time.monotonic()
        profile = self.profile(source, variant, self.output / f"{name}-requirements.txt")
        record["requirement_generation_seconds"] = time.monotonic() - generation_started
        profile_report = self.output / f"{name}-profile-report.json"
        record["profile_locked_resolve_install_seconds"] = self.run(
            name + "-profile",
            [str(python), "-m", "pip", "install", "--report", str(profile_report), "-r", str(profile)],
            cwd=source,
            env=env,
        )["elapsed_seconds"]
        verification = self.run(
            name + "-parser-origin",
            [str(python), "repo-scripts/vendor_pytuflow_wheel.py", "--installed"],
            cwd=source,
            env=env,
        )
        record["parser_verification_seconds"] = verification["elapsed_seconds"]
        self.validate_artifacts(name, gis_report, profile_report, variant)
        self.validate_inventory(name, python, source, env, variant)
        record["strict_pyright_seconds"] = self.strict_check(name, python, source, env)
        record["policy_seconds"] = sum(
            record[key]
            for key in (
                "gis_seconds",
                "requirement_generation_seconds",
                "profile_locked_resolve_install_seconds",
                "parser_verification_seconds",
                "strict_pyright_seconds",
            )
        )
        record["extra_capability_seconds"] = self.capabilities(name, python, source, env)
        record["status"] = "passed"
        record["elapsed_seconds"] = time.monotonic() - started
        record["free_disk_bytes_after"] = shutil.disk_usage(self.scratch).free
        record["cache_after"] = _cache_state(cache)
        cleanup_started = time.monotonic()
        shutil.rmtree(source.parent)
        if condition == "warm":
            shutil.rmtree(cache)
        record["cleanup_seconds"] = time.monotonic() - cleanup_started
        self.checkpoint("running")

    def validate_inventory(self, name: str, python: Path, source: Path, env: dict[str, str], variant: str) -> None:
        inventory = self.inventory(name, python, source, env)
        versions = {key: value["version"] for key, value in inventory["distributions"].items()}
        if versions != self.expected_versions[variant]:
            msg = f"Installed distribution versions differ from the resolved plan: {name}"
            raise ValueError(msg)
        plugins = inventory["pytest_plugins"]
        expected_plugins = self.canonical_plugins
        if variant == "candidate" and "pytest-cov" not in versions:
            expected_plugins = [entry for entry in (expected_plugins or []) if entry["distribution"] != "pytest-cov"]
        if plugins != expected_plugins:
            msg = f"Unexpected pytest plugin difference: {name}"
            raise ValueError(msg)

    def strict_check(self, name: str, python: Path, source: Path, env: dict[str, str]) -> float:
        node = shutil.which("node", path=env["PATH"])
        if node != self.plan["host"]["node_executable"]:
            msg = f"The sample would use a different Node executable: {name}: {node}"
            raise ValueError(msg)
        pyright = self.output / f"{name}-pyright.json"
        typing = self.run(
            name + "-pyright",
            [
                str(python),
                "-m",
                "pyright",
                "--outputjson",
                "--pythonpath",
                str(python),
                *MANIFEST,
                *SUPPLEMENTAL_MANIFEST,
            ],
            cwd=source,
            env=env,
            accepted=(0, 1),
        )
        pyright.write_text((self.output / typing["log"]).read_text(encoding="utf-8"), encoding="utf-8")
        diagnostics = _read_json(pyright)
        if diagnostics["summary"]["errorCount"] != 0 or typing["returncode"] != 0:
            msg = f"The fixed maintained and supplemental manifests did not pass strict checking: {name}"
            raise ValueError(msg)
        normalized: dict[str, Any] = {
            "version": diagnostics["version"],
            "filesAnalyzed": diagnostics["summary"]["filesAnalyzed"],
            "errorCount": diagnostics["summary"]["errorCount"],
            "warningCount": diagnostics["summary"]["warningCount"],
            "informationCount": diagnostics["summary"]["informationCount"],
            "generalDiagnostics": json.loads(
                json.dumps(diagnostics["generalDiagnostics"]).replace(str(source).replace("\\", "\\\\"), "<SOURCE>")
            ),
        }
        if self.baseline_diagnostics is None:
            self.baseline_diagnostics = normalized
        elif self.baseline_diagnostics != normalized:
            msg = f"Strict diagnostics changed between samples: {name}"
            raise ValueError(msg)
        return typing["elapsed_seconds"]

    def capabilities(self, name: str, python: Path, source: Path, env: dict[str, str]) -> float:
        probe_started = time.monotonic()
        self.run(name + "-pip-check", [str(python), "-m", "pip", "check"], cwd=source, env=env)
        capability_path = self.output / f"{name}-capabilities.json"
        self.run(
            name + "-capabilities",
            [str(python), "-c", PROBE_CODE, str(capability_path), str(source.parent)],
            cwd=source,
            env=env,
        )
        capabilities = _read_json(capability_path)
        capabilities.pop("module_origins")
        capabilities.pop("parser_diagnostic")
        if self.baseline_capabilities is None:
            self.baseline_capabilities = capabilities
        elif self.baseline_capabilities != capabilities:
            msg = f"Capability results changed between samples: {name}"
            raise ValueError(msg)
        return time.monotonic() - probe_started

    def validate_artifacts(self, name: str, gis_report: Path, profile_report: Path, variant: str) -> None:
        gis, _ = _artifacts(gis_report)
        profile, _ = _artifacts(profile_report)
        if gis != self.gis_artifacts:
            msg = f"GIS artifact identity changed: {name}"
            raise ValueError(msg)
        actual = gis | profile
        expected = {
            key: value for key, value in self.profile_artifacts.items() if key in self.expected_versions[variant]
        }
        if actual != expected:
            msg = f"Profile artifact identity changed: {name}"
            raise ValueError(msg)

    def summary(self, status: str, error: str | None) -> None:
        lines = ["## Windows Policy dependency pilot", "", f"Status: **{status}**", ""]
        if error:
            lines += [f"Stopped: `{error}`", ""]
        lines += [
            "| Sample | GIS (s) | Profile (s) | Strict (s) | Policy total (s) | Result |",
            "| --- | ---: | ---: | ---: | ---: | --- |",
        ]
        for sample in self.samples:
            values = [
                f"{sample[key]:.2f}" if key in sample else "—"
                for key in (
                    "gis_seconds",
                    "profile_locked_resolve_install_seconds",
                    "strict_pyright_seconds",
                    "policy_seconds",
                )
            ]
            lines.append(f"| {sample['name']} | {' | '.join(values)} | {sample['status']} |")
        lines += ["", *[f"- {limitation}" for limitation in LIMITATIONS], ""]
        content = "\n".join(lines)
        (self.output / "summary.md").write_text(content, encoding="utf-8")
        step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
        if step_summary:
            with Path(step_summary).open("a", encoding="utf-8") as stream:
                stream.write(content)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--plan-only", action="store_true")
    arguments = parser.parse_args()
    repository = Path(__file__).resolve().parents[2]
    output = arguments.output.resolve()
    scratch = arguments.scratch.resolve()
    if _under(output, repository) or _under(scratch, repository) or _under(output, scratch) or _under(scratch, output):
        parser.error("Output and scratch must be separate directories outside the repository")
    if (output.exists() and any(output.iterdir())) or (scratch.exists() and any(scratch.iterdir())):
        parser.error("Output and scratch must be new or empty experiment-owned directories")
    output.mkdir(parents=True, exist_ok=True)
    plan = _source_plan(repository)
    _write_json(output / "plan.json", plan)
    if arguments.plan_only:
        print(
            json.dumps({"status": "plan-only", "samples": plan["schedule"], "manifest_files": len(MANIFEST)}, indent=2)
        )
        return 0
    if platform.system() != "Windows" or platform.python_version() != PYTHON_VERSION:
        parser.error(f"Execution requires Windows with Python {PYTHON_VERSION}; use --plan-only elsewhere")
    node = shutil.which("node")
    if node is None:
        parser.error("A preinstalled Node executable is required; nodeenv downloads are outside this experiment")
    scratch.mkdir(parents=True, exist_ok=True)
    plan["host"] = {
        "platform": platform.platform(),
        "python": sys.version,
        "executable": sys.executable,
        "node_executable": node,
        "node_version": subprocess.run(
            [node, "--version"], check=True, capture_output=True, text=True, timeout=30
        ).stdout.strip(),
        "node_sha256": _sha256(Path(node)),
        "runner_image_os": os.environ.get("ImageOS"),
        "runner_image_version": os.environ.get("ImageVersion"),
        "runner_arch": os.environ.get("RUNNER_ARCH"),
        "runner_os": os.environ.get("RUNNER_OS"),
        "runner_name": os.environ.get("RUNNER_NAME"),
    }
    _write_json(output / "plan.json", plan)
    experiment = Experiment(repository, output, scratch, plan)
    status = "failed"
    error: str | None = None
    try:
        experiment.prepare()
        for sample in plan["schedule"]:
            experiment.sample(sample["repeat"], sample["variant"], sample["cache"])
        status = "completed"
        return 0
    except Exception as exception:  # noqa: BLE001 -- Save bounded experiment evidence for every failure.
        error = f"{type(exception).__name__}: {exception}"
        print(error, file=sys.stderr, flush=True)
        if experiment.samples and experiment.samples[-1]["status"] == "running":
            experiment.samples[-1]["status"] = "failed"
        return 1
    finally:
        experiment.checkpoint(status, error)
        experiment.summary(status, error)


if __name__ == "__main__":
    raise SystemExit(main())
