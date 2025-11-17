"""Tests for ryan_library.functions.path_stuff."""

from __future__ import annotations

from pathlib import Path
import sys

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ryan_library.functions import path_stuff


@pytest.fixture
def cwd(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Set the working directory to a temporary location."""
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_convert_to_relative_path_returns_relative_when_under_cwd(cwd: Path) -> None:
    target = cwd / "nested" / "file.txt"
    target.parent.mkdir()
    target.touch()

    result = path_stuff.convert_to_relative_path(target)

    assert result == Path("nested") / "file.txt"


def test_convert_to_relative_path_returns_absolute_when_outside_cwd(cwd: Path) -> None:
    outside_dir = cwd.parent / "external"
    outside_dir.mkdir(exist_ok=True)
    outside_file = outside_dir / "other.txt"
    outside_file.touch()

    result = path_stuff.convert_to_relative_path(outside_file)

    assert result == outside_file.resolve()


def test_convert_network_path_to_drive_letter(monkeypatch: pytest.MonkeyPatch) -> None:
    mapping = {r"\\server\share": "Z:"}
    monkeypatch.setattr(path_stuff, "network_drive_mapping", mapping, raising=False)
    unc_path = Path(r"\\server\share\folder\file.txt")

    result = path_stuff.convert_network_path_to_drive_letter(unc_path)

    assert result == Path(r"Z:\folder\file.txt")


def test_is_relative_to_current_directory(cwd: Path) -> None:
    inside_path = cwd / "inside"
    outside_path = cwd.parent / "outside"

    assert path_stuff.is_relative_to_current_directory(inside_path) is True
    assert path_stuff.is_relative_to_current_directory(outside_path) is False
