"""Select a calendar version and transactionally build one verified wheel.

The default release path increments the normalized ``yy.m.d.vv`` version.
``--version`` selects an explicit newer version, while ``--no-bump`` rebuilds
the version already declared in ``pyproject.toml`` for verification or CI.

The candidate wheel is built and verified in temporary storage. The retained
wheel under ``dist`` is replaced only after those steps succeed.
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path
from typing import cast

PROJECT_ROOT: Path = Path(__file__).resolve().parents[1]
PROJECT_PATH: Path = PROJECT_ROOT / "pyproject.toml"
DIST_DIR: Path = PROJECT_ROOT / "dist"
DISTRIBUTION_PREFIX: str = "ryan_functions-"
CALENDAR_VERSION: re.Pattern[str] = re.compile(
    r"^(?P<year>\d{2})\.(?P<month>\d{1,2})\.(?P<day>\d{1,2})\.(?P<revision>[1-9]\d*)$"
)
SECTION_HEADER: re.Pattern[str] = re.compile(r"(?m)^\[(?P<name>[^]]+)]\s*$")
VERSION_LINE: re.Pattern[str] = re.compile(
    r'(?m)^(?P<prefix>version[\t ]*=[\t ]*")(?P<version>[^"]+)(?P<suffix>")[\t ]*$'
)


def project_version() -> str:
    """Read the authoritative package version from ``pyproject.toml``."""
    with PROJECT_PATH.open("rb") as pyproject_file:
        data: dict[str, object] = tomllib.load(pyproject_file)
    project_data = data.get("project")
    if not isinstance(project_data, dict):
        msg = "pyproject.toml has no [project] table."
        raise ValueError(msg)
    project = cast("dict[str, object]", project_data)
    version = project.get("version")
    if not isinstance(version, str) or not version:
        msg = "pyproject.toml has no nonempty project version."
        raise ValueError(msg)
    return version


def parse_calendar_version(version: str) -> tuple[dt.date, int]:
    """Parse one normalized ``yy.m.d.vv`` version or fail explicitly."""
    match = CALENDAR_VERSION.fullmatch(version)
    if match is None:
        msg = f"Version must use normalized yy.m.d.vv form: {version!r}"
        raise ValueError(msg)
    release_date = dt.date(
        2000 + int(match.group("year")),
        int(match.group("month")),
        int(match.group("day")),
    )
    revision = int(match.group("revision"))
    normalized = f"{release_date.year % 100}.{release_date.month}.{release_date.day}.{revision}"
    if version != normalized:
        msg = f"Version must be normalized as {normalized!r}, not {version!r}"
        raise ValueError(msg)
    return release_date, revision


def next_calendar_version(current_version: str, today: dt.date) -> str:
    """Return today's next calendar version from the current project version."""
    current_date, current_revision = parse_calendar_version(current_version)
    if today < current_date:
        msg = (
            f"Local date {today.isoformat()} precedes current release date "
            f"{current_date.isoformat()}; refusing a version regression"
        )
        raise ValueError(msg)
    revision = current_revision + 1 if current_date == today else 1
    return f"{today.year % 100}.{today.month}.{today.day}.{revision}"


def validate_explicit_version(current_version: str, requested_version: str) -> str:
    """Validate that an explicit calendar version moves the project forward."""
    current = parse_calendar_version(current_version)
    requested = parse_calendar_version(requested_version)
    if requested <= current:
        msg = f"Explicit version {requested_version!r} must be newer than {current_version!r}"
        raise ValueError(msg)
    return requested_version


def replace_project_version(project_path: Path, new_version: str) -> str:
    """Replace only the version in the TOML ``[project]`` section."""
    content = project_path.read_text(encoding="utf-8")
    headers = list(SECTION_HEADER.finditer(content))
    for index, header in enumerate(headers):
        if header.group("name") != "project":
            continue
        section_end = headers[index + 1].start() if index + 1 < len(headers) else len(content)
        section = content[header.end() : section_end]
        match = VERSION_LINE.search(section)
        if match is None:
            break
        updated_section = VERSION_LINE.sub(rf"\g<prefix>{new_version}\g<suffix>", section, count=1)
        project_path.write_text(
            f"{content[: header.end()]}{updated_section}{content[section_end:]}",
            encoding="utf-8",
            newline="\n",
        )
        return match.group("version")
    msg = "pyproject.toml [project] has no version field"
    raise ValueError(msg)


def _ensure_build_installed() -> None:
    """Install or update the build frontend for the selected interpreter."""
    subprocess.run([sys.executable, "-m", "pip", "install", "--upgrade", "build"], check=True)


def _run_build(output_dir: Path) -> int:
    """Build a wheel into temporary storage and return the frontend exit status."""
    command = [sys.executable, "-m", "build", "--wheel", "--outdir", str(output_dir)]
    return subprocess.run(command, cwd=PROJECT_ROOT, check=False).returncode


def _run_verification(wheel: Path) -> int:
    """Verify a staged wheel without importing from the source checkout."""
    command = [sys.executable, str(PROJECT_ROOT / "repo-scripts" / "verify_wheel.py"), str(wheel)]
    return subprocess.run(command, cwd=PROJECT_ROOT, check=False).returncode


def clean_generated_build_state() -> None:
    """Remove only setuptools' known generated directories before a build."""
    project_root = PROJECT_ROOT.resolve()
    for directory in (PROJECT_ROOT / "build", PROJECT_ROOT / "ryan_functions.egg-info"):
        resolved = directory.resolve()
        if resolved.parent != project_root:
            msg = f"Refusing to remove generated path outside the project root: {resolved}"
            raise ValueError(msg)
        if resolved.is_dir():
            shutil.rmtree(resolved)


def promote_wheel(wheel: Path) -> Path:
    """Atomically promote a verified wheel, then remove older project artifacts."""
    DIST_DIR.mkdir(exist_ok=True)
    destination = DIST_DIR / wheel.name
    incoming = DIST_DIR / f".{wheel.name}.incoming"
    incoming.unlink(missing_ok=True)
    try:
        shutil.copy2(wheel, incoming)
        incoming.replace(destination)
    finally:
        incoming.unlink(missing_ok=True)

    for pattern in (f"{DISTRIBUTION_PREFIX}*.whl", f"{DISTRIBUTION_PREFIX}*.tar.gz"):
        for artifact in DIST_DIR.glob(pattern):
            if artifact != destination and artifact.is_file() and artifact.parent.resolve() == DIST_DIR.resolve():
                artifact.unlink()
    return destination


def select_version(
    current_version: str,
    *,
    requested_version: str | None,
    no_bump: bool,
    today: dt.date,
) -> str:
    """Select and validate the version requested by the command line."""
    if no_bump:
        parse_calendar_version(current_version)
        return current_version
    if requested_version is not None:
        return validate_explicit_version(current_version, requested_version)
    return next_calendar_version(current_version, today)


def build_and_promote(version: str, current_version: str) -> int:
    """Build and verify a candidate, preserving metadata on any failure."""
    previous_content = PROJECT_PATH.read_text(encoding="utf-8")
    try:
        if version != current_version:
            replace_project_version(PROJECT_PATH, version)
        clean_generated_build_state()
        with tempfile.TemporaryDirectory(prefix="ryan-tools-build-") as build_directory:
            build_dir = Path(build_directory)
            build_status = _run_build(build_dir)
            if build_status != 0:
                PROJECT_PATH.write_text(previous_content, encoding="utf-8", newline="\n")
                return build_status
            expected = build_dir / f"{DISTRIBUTION_PREFIX}{version}-py3-none-any.whl"
            if not expected.is_file():
                PROJECT_PATH.write_text(previous_content, encoding="utf-8", newline="\n")
                print(f"ERROR: expected wheel is missing: {expected.name}", file=sys.stderr)
                return 1
            verification_status = _run_verification(expected)
            if verification_status != 0:
                PROJECT_PATH.write_text(previous_content, encoding="utf-8", newline="\n")
                return verification_status
            promoted = promote_wheel(expected)
    except Exception:
        PROJECT_PATH.write_text(previous_content, encoding="utf-8", newline="\n")
        raise

    print(f"Built and verified: {promoted}")
    return 0


def main(argv: list[str] | None = None, *, today: dt.date | None = None) -> int:
    """Select a version, stage and verify its wheel, then promote it."""
    parser = argparse.ArgumentParser(description=__doc__)
    version_group = parser.add_mutually_exclusive_group()
    version_group.add_argument(
        "--version",
        help="Use this explicit newer yy.m.d.vv version instead of auto-incrementing.",
    )
    version_group.add_argument(
        "--no-bump",
        action="store_true",
        help="Build the version already declared in pyproject.toml.",
    )
    parser.add_argument(
        "--skip-pip",
        action="store_true",
        help="Do not install or update the build frontend before building.",
    )
    parser.add_argument(
        "--skip-artifacts",
        action="store_true",
        help="Update the selected version without building or replacing wheel artifacts.",
    )
    args = parser.parse_args(argv)
    if args.no_bump and args.skip_artifacts:
        parser.error("--no-bump cannot be combined with --skip-artifacts")

    current_version = project_version()
    try:
        version = select_version(
            current_version,
            requested_version=args.version,
            no_bump=args.no_bump,
            today=today or dt.date.today(),
        )
    except ValueError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2

    print(f"Using Python: {sys.executable}")
    print(f"Current version: {current_version}")
    print(f"Selected version: {version}")

    if args.skip_artifacts:
        if version != current_version:
            replace_project_version(PROJECT_PATH, version)
        print("Artifact build skipped.")
        return 0

    if not args.skip_pip:
        _ensure_build_installed()
    return build_and_promote(version, current_version)


if __name__ == "__main__":
    raise SystemExit(main())
