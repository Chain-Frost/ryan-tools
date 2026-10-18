"""Prepare a Perth-dated calendar release using the repository's verified builder."""

from __future__ import annotations

import argparse
import datetime as dt
import re
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import cast

PERTH = dt.timezone(dt.timedelta(hours=8))


def next_version(current: str, today: dt.date) -> str:
    """Advance the calendar date/revision, preserving the existing year width."""
    match = re.fullmatch(r"(\d{2}|\d{4})\.(\d{1,2})\.(\d{1,2})\.([1-9]\d*)", current)
    if match is None:
        msg = f"Unsupported calendar release version: {current!r}"
        raise ValueError(msg)
    year_text, month, day, revision = match.groups()
    year = int(year_text) + (2000 if len(year_text) == 2 else 0)
    previous = dt.date(year, int(month), int(day))
    if today < previous:
        msg = f"Release date {today} precedes current version {current}."
        raise ValueError(msg)
    release_year = today.year if len(year_text) == 4 else today.year % 100
    release_revision = int(revision) + 1 if today == previous else 1
    return f"{release_year}.{today.month}.{today.day}.{release_revision}"


def project_version(path: Path) -> str:
    """Read the authoritative project version without importing the package."""
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    project = cast("dict[str, object]", data["project"])
    version = project["version"]
    if not isinstance(version, str):
        msg = "Project version must be a string."
        raise TypeError(msg)
    return version


def replace_version(content: bytes, field: str, previous: str, version: str) -> bytes:
    """Replace exactly one matching assignment while preserving other bytes."""
    pattern = rf'(?m)^({re.escape(field)}[\t ]*=[\t ]*"){re.escape(previous)}("[\t ]*\r?$)'
    updated, count = re.subn(pattern, lambda match: f"{match[1]}{version}{match[2]}", content.decode("utf-8"))
    if count != 1:
        msg = f"Expected exactly one {field} assignment for {previous!r}; found {count}."
        raise ValueError(msg)
    return updated.encode("utf-8")


def prepare(root: Path, builder: list[str], marker: Path | None, today: dt.date) -> int:
    """Bump metadata and build, restoring metadata if the verified builder fails."""
    root = root.resolve()
    if not builder or not (root / builder[0]).resolve().is_relative_to(root):
        msg = "Builder must be a repository-local script."
        raise ValueError(msg)
    project = root / "pyproject.toml"
    current = project_version(project)
    version = next_version(current, today)
    originals = {project: project.read_bytes()}
    updates = {project: replace_version(originals[project], "version", current, version)}
    if marker is not None:
        marker = (root / marker).resolve()
        if not marker.is_relative_to(root):
            msg = "Version marker must remain inside the repository."
            raise ValueError(msg)
        originals[marker] = marker.read_bytes()
        updates[marker] = replace_version(originals[marker], "__version__", current, version)
    status = 1
    try:
        for path, content in updates.items():
            path.write_bytes(content)
        print(f"Preparing Perth calendar release {current} -> {version}", flush=True)
        status = subprocess.run([sys.executable, *builder], cwd=root, check=False).returncode
    finally:
        if status != 0:
            for path, content in originals.items():
                path.write_bytes(content)
    return status


def main() -> int:
    """Run release preparation without changing ordinary builder defaults."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--builder", required=True, help="Repository-relative verified build script.")
    parser.add_argument("--builder-arg", action="append", default=[], help="Argument passed to the builder.")
    parser.add_argument("--version-marker", type=Path, help="Optional package-local __version__ marker.")
    parser.add_argument("--dry-run", action="store_true", help="Print the next version without writing or building.")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    today = dt.datetime.now(PERTH).date()
    if args.dry_run:
        print(next_version(project_version(root / "pyproject.toml"), today))
        return 0
    builder_args = cast("list[str]", args.builder_arg)
    return prepare(root, [str(args.builder), *builder_args], args.version_marker, today)


if __name__ == "__main__":
    raise SystemExit(main())
