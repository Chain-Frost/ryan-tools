"""Synthetic DXF smoke and no-clobber checks for the standalone CAD converter."""

from __future__ import annotations

import importlib.util
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock

import pytest

SCRIPT = Path(__file__).absolute().parents[1] / "ryan-scripts" / "cad-python" / "cad_to_gpkg.py"


def _run_converter(source: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), str(source), *args],
        check=False,
        capture_output=True,
        text=True,
        timeout=45,
    )


def test_dxf_to_gpkg_keeps_geometry_layers_and_refuses_existing_output(tmp_path: Path) -> None:
    """Validate actual GDAL DXF conversion and non-destructive default reruns."""
    import ezdxf

    source = tmp_path / "sample.dxf"
    drawing = ezdxf.new("R2010")  # pyright: ignore[reportPrivateImportUsage]
    space = drawing.modelspace()
    space.add_point((5.0, 6.0, 0.0))  # pyright: ignore[reportUnknownMemberType]
    space.add_line((0.0, 0.0, 0.0), (10.0, 0.0, 0.0))  # pyright: ignore[reportUnknownMemberType]
    drawing.saveas(source)  # pyright: ignore[reportUnknownMemberType]

    result = _run_converter(source, "--crs", "EPSG:28350")
    assert result.returncode == 0, result.stdout + result.stderr

    gpkg = source.with_suffix(".gpkg")
    assert gpkg.is_file()
    with sqlite3.connect(gpkg) as connection:
        layers = {
            str(row[0]): int(row[1])
            for row in connection.execute("SELECT table_name, srs_id FROM gpkg_contents WHERE data_type = 'features'")
        }
        assert layers == {"areas": 28350, "lines": 28350, "points": 28350}
        assert connection.execute('SELECT COUNT(*) FROM "lines"').fetchone()[0] >= 1
        assert connection.execute('SELECT COUNT(*) FROM "points"').fetchone()[0] >= 1

    before = gpkg.read_bytes()
    repeat = _run_converter(source, "--crs", "EPSG:28350")
    assert repeat.returncode != 0
    assert "--overwrite" in repeat.stderr
    assert gpkg.read_bytes() == before
    replacement = _run_converter(source, "--crs", "EPSG:28350", "--overwrite")
    assert replacement.returncode == 0, replacement.stdout + replacement.stderr
    with sqlite3.connect(gpkg) as reopened:
        assert reopened.execute('SELECT COUNT(*) FROM "lines"').fetchone()[0] >= 1


@pytest.fixture
def converter(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    if importlib.util.find_spec("osgeo") is None:
        # Publication uses only stdlib SQLite. Stub unrelated CAD bindings so
        # these regressions also run in the minimal Linux security job.
        monkeypatch.setitem(sys.modules, "osgeo", MagicMock())
    specification = importlib.util.spec_from_file_location("cad_publication_tests", SCRIPT)
    assert specification is not None
    assert specification.loader is not None
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    return module


def _database(path: Path, value: str) -> None:
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("CREATE TABLE payload (value TEXT)")
        connection.execute("INSERT INTO payload VALUES (?)", (value,))
        connection.commit()


@pytest.mark.parametrize("journal_mode", ["WAL", "DELETE"])
def test_overwrite_refuses_active_sqlite_journals(tmp_path: Path, converter: ModuleType, journal_mode: str) -> None:
    output, staged = tmp_path / "output.gpkg", tmp_path / "staged.gpkg"
    _database(output, "original")
    _database(staged, "replacement")
    with sqlite3.connect(output) as active:
        active.execute(f"PRAGMA journal_mode={journal_mode}")
        active.execute("UPDATE payload SET value='uncommitted'")
        with pytest.raises(converter.ConversionError, match="journaled"):
            converter._publish_temporary_output(staged, output, overwrite=True)
        active.rollback()
        assert active.execute("SELECT value FROM payload").fetchone() == ("original",)
    assert staged.is_file()
    with sqlite3.connect(output) as reopened:
        assert reopened.execute("SELECT value FROM payload").fetchone() == ("original",)


def test_overwrite_refuses_active_reader_without_journal(tmp_path: Path, converter: ModuleType) -> None:
    output, staged = tmp_path / "output.gpkg", tmp_path / "staged.gpkg"
    _database(output, "original")
    _database(staged, "replacement")
    with sqlite3.connect(output) as reader:
        reader.execute("BEGIN")
        reader.execute("SELECT * FROM payload").fetchall()
        with pytest.raises(converter.ConversionError, match="in use"):
            converter._publish_temporary_output(staged, output, overwrite=True)
    with sqlite3.connect(output) as reopened:
        assert reopened.execute("SELECT value FROM payload").fetchone() == ("original",)
    assert staged.is_file()


def test_overwrite_keeps_database_identity_and_publishes_replacement(tmp_path: Path, converter: ModuleType) -> None:
    output, staged = tmp_path / "output.gpkg", tmp_path / "staged.gpkg"
    _database(output, "original")
    _database(staged, "replacement")
    before = output.stat()
    converter._publish_temporary_output(staged, output, overwrite=True)
    assert output.stat().st_ino == before.st_ino
    assert not staged.exists()
    with sqlite3.connect(output) as reopened:
        assert reopened.execute("SELECT value FROM payload").fetchone() == ("replacement",)
