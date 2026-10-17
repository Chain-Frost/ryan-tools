"""Allocate strictly increasing calendar versions and stamp release checkouts.

A release tag must refer to a successful main CI commit. The source checkout
is stamped for the verified wheel build; branch protection is never bypassed.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
import tomllib
from pathlib import Path

_VERSION = re.compile(r"^(?P<year>\d{2}|\d{4})\.(?P<month>\d{1,2})\.(?P<day>\d{1,2})\.(?P<revision>[1-9]\d*)$")
_PROJECT = re.compile(r"(?ms)^(\[project\]\s*\n)(.*?)(?=^\[|\Z)")
_ASSIGNMENT = re.compile(r'(?m)^version[ \t]*=[ \t]*"([^"]+)"[ \t]*$')
_MARKER = re.compile(r'(?m)^__version__[ \t]*=[ \t]*"([^"]+)"[ \t]*$')


def calendar_version(value: str, *, year_width: int | None = None) -> tuple[int, int, int, int]:
    """Reject noncalendar, mixed-width, and invalid historical versions."""
    match = _VERSION.fullmatch(value)
    if match is None:
        message = f"Unsupported published version {value!r}; expected calendar YYYY.M.D.N or YY.M.D.N."
        raise ValueError(message)
    year = match["year"]
    if year_width is not None and len(year) != year_width:
        message = f"Historical version {value!r} has a different year width; resolve before publishing."
        raise ValueError(message)
    year_number = int(year) + (2000 if len(year) == 2 else 0)
    month, day, revision = (int(match[key]) for key in ("month", "day", "revision"))
    dt.date(year_number, month, day)
    return (year_number, month, day, revision)


def next_version(current: str, previous: list[str], today: dt.date) -> str:
    """Return a strictly greater version than source, Git tags and ALL PyPI releases."""
    match = _VERSION.fullmatch(current)
    if match is None:
        message = f"Invalid source calendar version: {current!r}"
        raise ValueError(message)
    width = len(match["year"])
    baseline = max(calendar_version(item, year_width=width) for item in [current, *previous])
    today_key = (today.year, today.month, today.day)
    if today_key > baseline[:3]:
        year, month, day, revision = (*today_key, 1)
    else:
        year, month, day, revision = (*baseline[:3], baseline[3] + 1)
    year_text = str(year if width == 4 else year - 2000)
    version = f"{year_text}.{month}.{day}.{revision}"
    if any(
        calendar_version(version, year_width=width) <= calendar_version(item, year_width=width)
        for item in [current, *previous]
    ):
        message = "The proposed release version is not strictly newer than historical versions."
        raise ValueError(message)
    return version


def _change_line(contents: str, pattern: re.Pattern[str], new: str, expected: str) -> str:
    found = list(pattern.finditer(contents))
    if len(found) != 1 or found[0][1] != expected:
        message = f"Expected exactly one existing version assignment matching {expected!r}."
        raise ValueError(message)
    start, end = found[0].span(1)
    return contents[:start] + new + contents[end:]


def stamp(root: Path, version: str, marker: str | None) -> None:
    """Stamp the checked-out source only; do not push changes to protected main."""
    project = root / "pyproject.toml"
    content = project.read_text(encoding="utf-8")
    parsed = tomllib.loads(content)
    source = parsed["project"]["version"]
    if not isinstance(source, str):
        message = "pyproject project.version must be text."
        raise TypeError(message)
    if calendar_version(
        version, year_width=len(_VERSION.fullmatch(source)["year"]) if _VERSION.fullmatch(source) else 0
    ) < calendar_version(source):
        message = "Release version must not go backwards from the source checkout."
        raise ValueError(message)
    section = _PROJECT.search(content)
    if section is None:
        message = "Missing TOML [project] section."
        raise ValueError(message)
    changed = _change_line(section[2], _ASSIGNMENT, version, source)
    updated = content[: section.start(2)] + changed + content[section.end(2) :]
    marker_path = root / marker if marker else None
    marker_updated: str | None = None
    if marker_path is not None:
        marker_content = marker_path.read_text(encoding="utf-8")
        marker_updated = _change_line(marker_content, _MARKER, version, source)
    # Validate both inputs before writing either.
    project.write_text(updated, encoding="utf-8")
    if marker_path is not None and marker_updated is not None:
        marker_path.write_text(marker_updated, encoding="utf-8")
    if tomllib.loads(project.read_text(encoding="utf-8"))["project"]["version"] != version:
        message = "Version stamping did not take effect."
        raise ValueError(message)


def main() -> int:
    """Handle version planning and local release-checkout stamping."""
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("next", help="Read {current, versions, today} JSON on stdin and emit the new version.")
    stamping = subcommands.add_parser("stamp", help="Set local project and optional package version from a v-tag.")
    stamping.add_argument("tag")
    stamping.add_argument("--marker", help="Optional package _version.py to stamp alongside project metadata.")
    args = parser.parse_args()
    if args.command == "next":
        record = json.load(sys.stdin)
        print(next_version(record["current"], record["versions"], dt.date.fromisoformat(record["today"])))
    else:
        if not args.tag.startswith("v"):
            message = "Stable release tag must begin with v."
            raise ValueError(message)
        stamp(Path.cwd(), args.tag[1:], args.marker)
        print(f"Stamped local publication checkout for {args.tag}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
