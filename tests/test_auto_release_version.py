"""Checks for automatic release version allocation and source metadata stamping."""

from __future__ import annotations

import datetime as dt
import importlib.util
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = _ROOT / "repo-scripts" / "auto_release_version.py"
_SPEC = importlib.util.spec_from_file_location("auto_release_version", _SCRIPT)
assert _SPEC is not None
assert _SPEC.loader is not None
_release = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_release)


@pytest.mark.parametrize(
    ("current", "history", "today", "expected"),
    [
        ("26.10.16.1", ["26.10.16.2", "26.10.15.9"], dt.date(2026, 10, 17), "26.10.17.1"),
        ("26.10.17.1", ["26.10.17.2"], dt.date(2026, 10, 17), "26.10.17.3"),
        ("2026.10.16.1", ["2026.10.17.1"], dt.date(2026, 10, 17), "2026.10.17.2"),
        ("26.10.20.3", ["26.10.20.4"], dt.date(2026, 10, 17), "26.10.20.5"),
        ("26.12.31.4", [], dt.date(2027, 1, 1), "27.1.1.1"),
    ],
)
def test_next_is_strictly_newer(current: str, history: list[str], today: dt.date, expected: str) -> None:
    assert _release.next_version(current, history, today) == expected


@pytest.mark.parametrize("historical", ["not-a-version", "2026.10.17.5", "26.13.1.1", "26.2.30.1"])
def test_invalid_historical_versions_fail_closed(historical: str) -> None:
    with pytest.raises(ValueError, match=r"Unsupported|width|day|month"):
        _release.next_version("26.10.17.1", [historical], dt.date(2026, 10, 17))


def test_local_stamp_preserves_other_project_metadata(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text(
        '[build-system]\nrequires = ["setuptools"]\n\n[project]\nname = "test-package"\nversion = "26.10.17.1"\ndescription = "example"\n',
        encoding="utf-8",
    )
    marker = tmp_path / "_version.py"
    marker.write_text('__version__ = "26.10.17.1"\n', encoding="utf-8")
    _release.stamp(tmp_path, "26.10.17.2", "_version.py")
    assert 'version = "26.10.17.2"' in (tmp_path / "pyproject.toml").read_text(encoding="utf-8")
    assert 'description = "example"' in (tmp_path / "pyproject.toml").read_text(encoding="utf-8")
    assert marker.read_text(encoding="utf-8") == '__version__ = "26.10.17.2"\n'
    with pytest.raises(ValueError, match="backwards"):
        _release.stamp(tmp_path, "26.10.17.1", "_version.py")


def test_mismatched_marker_leaves_project_untouched(tmp_path: Path) -> None:
    project = tmp_path / "pyproject.toml"
    project.write_text('[project]\nversion = "26.10.17.1"\n', encoding="utf-8")
    (tmp_path / "_version.py").write_text('__version__ = "26.10.16.1"\n', encoding="utf-8")
    with pytest.raises(ValueError, match="matching"):
        _release.stamp(tmp_path, "26.10.17.2", "_version.py")
    assert "26.10.17.1" in project.read_text(encoding="utf-8")
