"""Disposable SQLite safety checks for the opt-in GeoPackage VACUUM wrapper."""

from __future__ import annotations

import argparse
import runpy
import sqlite3
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Protocol, cast

import pytest

SCRIPT = Path(__file__).absolute().parents[2] / "ryan-scripts" / "gdal-python" / "vacuum_geopackages.py"
GPKG_APPLICATION_ID = 0x47504B47


class VacuumMain(Protocol):
    def __call__(
        self,
        *,
        input_directories: Path,
        execute: bool | None = None,
        backup: bool | None = None,
    ) -> int: ...


@pytest.fixture
def vacuum_main() -> VacuumMain:
    namespace = runpy.run_path(str(SCRIPT))
    assert namespace["DEFAULT_EXECUTE"] is False
    assert namespace["DEFAULT_BACKUP"] is True
    return cast(VacuumMain, namespace["main"])


def _create_gpkg(path: Path) -> None:
    """Create enough native GeoPackage metadata to test SQLite protection."""
    with sqlite3.connect(path) as connection:
        connection.execute(f"PRAGMA application_id = {GPKG_APPLICATION_ID}")
        connection.execute("CREATE TABLE gpkg_spatial_ref_sys (srs_id INTEGER PRIMARY KEY)")
        connection.execute("CREATE TABLE gpkg_contents (table_name TEXT PRIMARY KEY)")
        connection.execute("CREATE TABLE features (id INTEGER PRIMARY KEY, description TEXT NOT NULL)")
        connection.execute("INSERT INTO gpkg_spatial_ref_sys VALUES (28350)")
        connection.execute("INSERT INTO gpkg_contents VALUES ('features')")
        connection.executemany(
            "INSERT INTO features (description) VALUES (?)",
            [(f"test_{i:04d}_" + "x" * 200,) for i in range(300)],
        )
        connection.execute("DELETE FROM features WHERE id > 15")


def _record_state(path: Path) -> tuple[list[tuple[int, str]], list[tuple[str, str | None]]]:
    with sqlite3.connect(path) as connection:
        data = cast(list[tuple[int, str]], connection.execute("SELECT * FROM features ORDER BY id").fetchall())
        schema = cast(
            list[tuple[str, str | None]],
            connection.execute("SELECT name, sql FROM sqlite_master WHERE type='table' ORDER BY name").fetchall(),
        )
    return data, schema


def test_default_is_read_only_and_does_not_create_backup(tmp_path: Path, vacuum_main: VacuumMain) -> None:
    gpkg = tmp_path / "native.gpkg"
    _create_gpkg(gpkg)
    original_bytes = gpkg.read_bytes()
    assert vacuum_main(input_directories=tmp_path) == 0
    assert gpkg.read_bytes() == original_bytes
    assert not (tmp_path / "native.gpkg.pre-vacuum.bak").exists()


def test_execute_preserves_consistent_backup_and_gpkg_tables(tmp_path: Path, vacuum_main: VacuumMain) -> None:
    gpkg = tmp_path / "native.gpkg"
    _create_gpkg(gpkg)
    before_data, before_schema = _record_state(gpkg)

    assert vacuum_main(input_directories=tmp_path, execute=True) == 0
    backup_path = tmp_path / "native.gpkg.pre-vacuum.bak"
    assert backup_path.is_file()
    assert _record_state(backup_path) == (before_data, before_schema)
    assert _record_state(gpkg) == (before_data, before_schema)
    with sqlite3.connect(gpkg) as connection:
        assert connection.execute("PRAGMA quick_check").fetchone() == ("ok",)
        assert connection.execute("PRAGMA application_id").fetchone() == (GPKG_APPLICATION_ID,)


def test_existing_backup_blocks_mutation_without_overwrite(tmp_path: Path, vacuum_main: VacuumMain) -> None:
    gpkg = tmp_path / "native.gpkg"
    _create_gpkg(gpkg)
    before = gpkg.read_bytes()
    backup = tmp_path / "native.gpkg.pre-vacuum.bak"
    backup.write_bytes(b"important preexisting backup")
    assert vacuum_main(input_directories=tmp_path, execute=True) == 1
    assert gpkg.read_bytes() == before
    assert backup.read_bytes() == b"important preexisting backup"


def test_invalid_sqlite_file_is_not_changed(tmp_path: Path, vacuum_main: VacuumMain) -> None:
    gpkg = tmp_path / "corrupt.gpkg"
    gpkg.write_bytes(b"not a database")
    assert vacuum_main(input_directories=tmp_path, execute=True) == 1
    assert gpkg.read_bytes() == b"not a database"
    assert not list(tmp_path.glob("*.pre-vacuum.bak"))


def test_sqlite_without_geopackage_identity_is_rejected(tmp_path: Path, vacuum_main: VacuumMain) -> None:
    gpkg = tmp_path / "wrong.gpkg"
    with sqlite3.connect(gpkg) as connection:
        connection.execute("CREATE TABLE ordinary (id INTEGER)")
    original = gpkg.read_bytes()
    assert vacuum_main(input_directories=tmp_path, execute=True) == 1
    assert gpkg.read_bytes() == original
    assert not list(tmp_path.glob("*.pre-vacuum.bak"))


def test_sqlite_lock_causes_failure_without_backup(tmp_path: Path, vacuum_main: VacuumMain) -> None:
    gpkg = tmp_path / "locked.gpkg"
    _create_gpkg(gpkg)
    with sqlite3.connect(gpkg, timeout=1.0) as exclusive:
        exclusive.execute("BEGIN EXCLUSIVE")
        try:
            assert vacuum_main(input_directories=tmp_path, execute=True) == 1
            assert not list(tmp_path.glob("*.pre-vacuum.bak"))
        finally:
            exclusive.rollback()


def test_symlink_is_rejected_and_target_is_untouched(tmp_path: Path, vacuum_main: VacuumMain) -> None:
    target = tmp_path / "real.db"
    _create_gpkg(target)
    link = tmp_path / "linked.gpkg"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("Symlink creation is unavailable on this platform")
    original = target.read_bytes()
    assert vacuum_main(input_directories=tmp_path, execute=True) == 1
    assert target.read_bytes() == original
    assert not list(tmp_path.glob("*.pre-vacuum.bak"))


def test_no_backup_requires_explicit_opt_out(tmp_path: Path, vacuum_main: VacuumMain) -> None:
    gpkg = tmp_path / "native.gpkg"
    _create_gpkg(gpkg)
    before_state = _record_state(gpkg)
    assert vacuum_main(input_directories=tmp_path, execute=True, backup=False) == 0
    assert _record_state(gpkg) == before_state
    assert not list(tmp_path.glob("*.pre-vacuum.bak"))


def test_cli_exposes_execution_and_backup_switches(monkeypatch: pytest.MonkeyPatch) -> None:
    namespace = runpy.run_path(str(SCRIPT))
    parser = cast("Callable[[], argparse.Namespace]", namespace["_parse_cli_arguments"])
    monkeypatch.setattr(sys, "argv", ["vacuum_geopackages.py", "--execute", "--no-backup", "--no-pause"])
    arguments = parser()
    assert arguments.execute is True
    assert arguments.no_backup is True
    assert arguments.no_pause is True
