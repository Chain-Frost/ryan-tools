"""Verify the current ryan-functions wheel and its bundled package contents."""

from __future__ import annotations

import argparse
import hashlib
import tomllib
import zipfile
from pathlib import Path
from typing import cast

PROJECT_ROOT: Path = Path(__file__).resolve().parents[1]
DIST_DIR: Path = PROJECT_ROOT / "dist"
DISTRIBUTION_PREFIX: str = "ryan_functions-"
RESOURCE_ROOT: str = "ryan_library/resources/"


def _project_metadata() -> tuple[str, str, str, str]:
    """Read authoritative name, version, licence and Python requirement metadata."""
    with (PROJECT_ROOT / "pyproject.toml").open("rb") as pyproject_file:
        data: dict[str, object] = tomllib.load(pyproject_file)
    project_data = data.get("project")
    if not isinstance(project_data, dict):
        msg = "pyproject.toml has no [project] table."
        raise ValueError(msg)
    project = cast("dict[str, object]", project_data)
    fields: list[str] = []
    for key in ("name", "version", "license", "requires-python"):
        value = project.get(key)
        if not isinstance(value, str) or not value:
            msg = f"pyproject.toml has no nonempty project {key!r}."
            raise ValueError(msg)
        fields.append(value)
    return fields[0], fields[1], fields[2], fields[3]


def _normalized(content: bytes) -> bytes:
    """Normalize UTF-8 text newlines for archive comparisons."""
    return content.decode("utf-8").replace("\r\n", "\n").encode("utf-8")


def _single_name(names: set[str], suffix: str) -> str:
    """Return one archive member ending in suffix or fail explicitly."""
    matches = sorted(name for name in names if name.endswith(suffix))
    if len(matches) != 1:
        msg = f"Expected one *{suffix} member, found {len(matches)}."
        raise ValueError(msg)
    return matches[0]


def _metadata_field(metadata: str, field: str) -> tuple[str, ...]:
    """Return all values for one core-metadata field."""
    prefix = f"{field}: "
    return tuple(line.removeprefix(prefix) for line in metadata.splitlines() if line.startswith(prefix))


def _verify_source_files(
    archive: zipfile.ZipFile,
    names: set[str],
    *,
    source_directory: Path,
    archive_directory: str,
    suffixes: tuple[str, ...],
) -> None:
    """Require selected repository resources to be present and byte-identical."""
    source_files = sorted(
        path
        for path in source_directory.rglob("*")
        if path.is_file() and (path.name == ".gitignore" or path.suffix.casefold() in suffixes)
    )
    if not source_files:
        msg = f"No required source resources found under {source_directory}."
        raise ValueError(msg)
    expected_names = {f"{archive_directory}/{path.relative_to(source_directory).as_posix()}" for path in source_files}
    actual_names = {
        name
        for name in names
        if name.startswith(f"{archive_directory}/")
        and (Path(name).name == ".gitignore" or Path(name).suffix.casefold() in suffixes)
    }
    if actual_names != expected_names:
        missing = sorted(expected_names - actual_names)
        unexpected = sorted(actual_names - expected_names)
        msg = f"Packaged resource set differs for {archive_directory}; missing={missing}, unexpected={unexpected}"
        raise ValueError(msg)
    for source_file in source_files:
        archive_name = f"{archive_directory}/{source_file.relative_to(source_directory).as_posix()}"
        if archive.read(archive_name) != source_file.read_bytes():
            msg = f"Packaged resource differs from repository source: {archive_name}"
            raise ValueError(msg)


def _verify_wheel(
    wheel: Path,
    *,
    project_name: str,
    version: str,
    licence: str,
    requires_python: str,
    repository_license: bytes,
) -> None:
    """Verify metadata, licensing, bundled packages, typed markers and resources."""
    with zipfile.ZipFile(wheel) as archive:
        names = set(archive.namelist())
        metadata_name = _single_name(names, ".dist-info/METADATA")
        license_name = _single_name(names, ".dist-info/licenses/LICENSE")
        metadata = archive.read(metadata_name).decode("utf-8")
        expected_fields = {
            "Name": project_name,
            "Version": version,
            "License-Expression": licence,
            "License-File": "LICENSE",
            "Requires-Python": requires_python,
        }
        for field, expected in expected_fields.items():
            if _metadata_field(metadata, field) != (expected,):
                msg = f"Wheel {field} metadata does not match pyproject.toml."
                raise ValueError(msg)
        if _normalized(archive.read(license_name)) != repository_license:
            msg = "Wheel LICENSE differs from the repository LICENSE."
            raise ValueError(msg)

        required_members = {
            "culvert_solver/__init__.py",
            "culvert_solver/py.typed",
            "run_hy8/__init__.py",
            "run_hy8/py.typed",
            "ryan_library/__init__.py",
            "ryan_library/py.typed",
            "ryan_library/resources/mcp/gdal_cli_tools.json",
            "ryan_library/resources/mcp/workflows.json",
        }
        missing_members = sorted(required_members - names)
        if missing_members:
            msg = f"Wheel is missing required package members: {missing_members}"
            raise ValueError(msg)

        forbidden_prefixes = (
            "docs/",
            "excel-resources/",
            "qgis-resources/",
            "repo-scripts/",
            "tests/",
        )
        forbidden_members = sorted(
            name
            for name in names
            if name.startswith(forbidden_prefixes) or "/__pycache__/" in name or name.endswith((".pyc", ".pyo"))
        )
        if forbidden_members:
            msg = f"Wheel contains development or generated inputs: {forbidden_members[:10]}"
            raise ValueError(msg)

        _verify_source_files(
            archive,
            names,
            source_directory=PROJECT_ROOT / "ryan_library" / "classes",
            archive_directory="ryan_library/classes",
            suffixes=(".json",),
        )
        _verify_source_files(
            archive,
            names,
            source_directory=PROJECT_ROOT / "ryan_library" / "resources" / "mcp",
            archive_directory=f"{RESOURCE_ROOT}mcp".rstrip("/"),
            suffixes=(".json",),
        )
        _verify_source_files(
            archive,
            names,
            source_directory=PROJECT_ROOT / "ryan_library" / "resources" / "tuflow_templates",
            archive_directory=f"{RESOURCE_ROOT}tuflow_templates".rstrip("/"),
            suffixes=(".bat", ".csv", ".tbc", ".tef", ".tgc", ".trd", ".tsoilf"),
        )
        _verify_source_files(
            archive,
            names,
            source_directory=PROJECT_ROOT / "qgis-resources" / "styles" / "TUFLOW",
            archive_directory=f"{RESOURCE_ROOT}qgis/tuflow".rstrip("/"),
            suffixes=(".qml",),
        )
        _verify_source_files(
            archive,
            names,
            source_directory=PROJECT_ROOT / "vendor" / "run_hy8" / "src" / "run_hy8",
            archive_directory="run_hy8",
            suffixes=(".py", ".typed"),
        )
        _verify_source_files(
            archive,
            names,
            source_directory=PROJECT_ROOT / "vendor" / "ryan_culverts" / "src" / "culvert_solver",
            archive_directory="culvert_solver",
            suffixes=(".py", ".typed"),
        )


def _sha256(path: Path) -> str:
    """Return a lowercase SHA-256 artifact digest."""
    digest = hashlib.sha256()
    with path.open("rb") as artifact_file:
        for block in iter(lambda: artifact_file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def retained_wheel(version: str) -> Path:
    """Return the sole retained project wheel when it matches the project version."""
    wheels = sorted(DIST_DIR.glob(f"{DISTRIBUTION_PREFIX}*.whl"))
    if len(wheels) != 1:
        msg = f"Expected exactly one retained project wheel under dist/, found {len(wheels)}."
        raise ValueError(msg)
    wheel = wheels[0]
    expected_name = f"{DISTRIBUTION_PREFIX}{version}-py3-none-any.whl"
    if wheel.name != expected_name:
        msg = f"Retained wheel {wheel.name} does not match project version {version}."
        raise ValueError(msg)
    return wheel


def main(argv: list[str] | None = None) -> int:
    """Verify a selected wheel and print its reproducibility identifier."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "wheel",
        nargs="?",
        type=Path,
        help="Specific staged wheel to verify; defaults to the current wheel under dist/.",
    )
    args = parser.parse_args(argv)
    project_name, version, licence, requires_python = _project_metadata()
    expected_name = f"{DISTRIBUTION_PREFIX}{version}-py3-none-any.whl"
    wheel = args.wheel.resolve() if args.wheel is not None else retained_wheel(version)
    if not wheel.is_file():
        msg = "Build the current wheel first."
        raise FileNotFoundError(msg)
    if wheel.name != expected_name:
        msg = f"Expected wheel named {expected_name}, found {wheel.name}."
        raise ValueError(msg)
    repository_license = _normalized((PROJECT_ROOT / "LICENSE").read_bytes())
    _verify_wheel(
        wheel,
        project_name=project_name,
        version=version,
        licence=licence,
        requires_python=requires_python,
        repository_license=repository_license,
    )
    print(f"Verified: {wheel.name} ({wheel.stat().st_size} bytes, sha256={_sha256(wheel)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
