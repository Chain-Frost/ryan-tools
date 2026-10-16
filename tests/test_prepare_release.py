"""Exercise calendar progression and release metadata rollback around a real builder process."""

from __future__ import annotations

import datetime as dt
import importlib.util
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "repo-scripts" / "prepare_release.py"
SPEC = importlib.util.spec_from_file_location("release_preparation", SCRIPT)
if SPEC is None or SPEC.loader is None:
    message = "Cannot load release preparation helper"
    raise RuntimeError(message)
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)


@pytest.mark.parametrize(
    ("current", "today", "expected"),
    [
        ("26.10.15.4", dt.date(2026, 10, 15), "26.10.15.5"),
        ("26.10.15.4", dt.date(2026, 10, 16), "26.10.16.1"),
        ("2026.10.05.1", dt.date(2026, 10, 5), "2026.10.5.2"),
        ("2026.10.05.1", dt.date(2026, 10, 16), "2026.10.16.1"),
        ("26.12.31.9", dt.date(2027, 1, 1), "27.1.1.1"),
        ("2026.12.31.9", dt.date(2027, 1, 1), "2027.1.1.1"),
    ],
)
def test_calendar_progression(current: str, today: dt.date, expected: str) -> None:
    """Preserve year width and advance revisions across date boundaries."""
    assert release.next_version(current, today) == expected


@pytest.mark.parametrize("current", ["26.10.17.1", "26.2.30.1", "1.2.3", "26.10.16.1rc1", "26.10.16.0"])
def test_invalid_or_future_versions_fail(current: str) -> None:
    """Reject malformed, non-calendar and future release versions."""
    with pytest.raises(ValueError, match=r"Unsupported|precedes|day"):
        release.next_version(current, dt.date(2026, 10, 16))


@pytest.mark.parametrize("exit_code", [0, 7])
@pytest.mark.parametrize("with_marker", [False, True])
def test_build_result_controls_metadata_commit(tmp_path: Path, exit_code: int, *, with_marker: bool) -> None:
    """A real subprocess sees synchronized versions; failed builds restore exact bytes."""
    project = tmp_path / "pyproject.toml"
    original = b'[project]\r\nname = "fixture"\r\nversion = "26.10.15.1"\r\n'
    project.write_bytes(original)
    marker = tmp_path / "_version.py"
    marker_original = b'__version__ = "26.10.15.1"\r\n'
    marker.write_bytes(marker_original)
    (tmp_path / "builder.py").write_text(
        "import pathlib, sys, tomllib\n"
        "version = tomllib.loads(pathlib.Path('pyproject.toml').read_text())['project']['version']\n"
        "if version != '26.10.16.1': sys.exit(99)\n"
        + ("if '26.10.16.1' not in pathlib.Path('_version.py').read_text(): sys.exit(98)\n" if with_marker else "")
        + f"sys.exit({exit_code})\n",
        encoding="utf-8",
    )
    result = release.prepare(
        tmp_path, ["builder.py"], Path("_version.py") if with_marker else None, dt.date(2026, 10, 16)
    )
    assert result == exit_code
    if exit_code:
        assert project.read_bytes() == original
        assert marker.read_bytes() == marker_original
    else:
        assert release.project_version(project) == "26.10.16.1"
        assert marker.read_bytes() == (
            marker_original.replace(b"26.10.15.1", b"26.10.16.1") if with_marker else marker_original
        )


def test_marker_mismatch_leaves_metadata_untouched(tmp_path: Path) -> None:
    """Do not start a release from inconsistent package and project versions."""
    project = tmp_path / "pyproject.toml"
    project.write_bytes(b'[project]\nversion = "26.10.15.1"\n')
    marker = tmp_path / "_version.py"
    marker.write_bytes(b'__version__ = "26.10.14.1"\n')
    original = project.read_bytes()
    with pytest.raises(ValueError, match="Expected exactly one"):
        release.prepare(tmp_path, ["builder.py"], Path("_version.py"), dt.date(2026, 10, 16))
    assert project.read_bytes() == original
