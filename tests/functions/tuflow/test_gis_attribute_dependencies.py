"""Read-only, once-per-database GIS attribute dependency materialisation tests."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from ryan_library.functions.tuflow.gis_attribute_dependencies import inspect_gis_attribute_sources


def make_network_gpkg(path: Path, *, geometry: str = "LINESTRING", attributes: int = 11) -> None:
    """Create the minimum SQLite/GPKG schema for field-position discovery."""
    columns = ", ".join(f"attr_{index} TEXT" for index in range(attributes))
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE gpkg_geometry_columns(table_name TEXT, column_name TEXT, geometry_type_name TEXT)")
        db.execute("CREATE TABLE gpkg_contents(table_name TEXT)")
        db.execute(
            "INSERT INTO gpkg_geometry_columns VALUES (?, ?, ?)",
            ("1d_nwk_test", "geom", geometry),
        )
        db.execute("INSERT INTO gpkg_contents VALUES (?)", ("1d_nwk_test",))
        db.execute(f'CREATE TABLE "1d_nwk_test" (fid INTEGER PRIMARY KEY, geom BLOB, {columns})')
        if attributes > 10:
            for channel, file_values in (
                ("M", "operational.csv | section.csv"),
                ("Q", "rating.csv"),
                ("C", "ignored.csv"),
                ("M", ""),
            ):
                db.execute(
                    'INSERT INTO "1d_nwk_test" (attr_1, attr_10) VALUES (?, ?)',
                    (channel, file_values),
                )


def test_network_m_and_q_paths_are_discovered_without_geometries(tmp_path: Path) -> None:
    gpkg = tmp_path / "network.gpkg"
    make_network_gpkg(gpkg)
    result, opens = inspect_gis_attribute_sources({(gpkg, "1d_nwk_test"), (gpkg, "1d_nwk_*")})
    expected = {
        tmp_path / "operational.csv",
        tmp_path / "section.csv",
        tmp_path / "rating.csv",
    }
    assert opens == 1
    assert result[(gpkg, "1d_nwk_test")].status == "present"
    assert set(result[(gpkg, "1d_nwk_test")].sources) == expected
    assert set(result[(gpkg, "1d_nwk_*")].sources) == expected


def test_unknown_or_insufficient_gis_schema_fails_closed(tmp_path: Path) -> None:
    bad = tmp_path / "short.gpkg"
    make_network_gpkg(bad, attributes=5)
    result, opens = inspect_gis_attribute_sources({(bad, "1d_nwk_test")})
    assert opens == 1
    assert result[(bad, "1d_nwk_test")].status == "unresolved"
    assert "index" in result[(bad, "1d_nwk_test")].detail


def test_unknown_layer_type_and_mixed_network_geometry_fail_closed(tmp_path: Path) -> None:
    gpkg = tmp_path / "network.gpkg"
    make_network_gpkg(gpkg, geometry="GEOMETRY")
    result, _ = inspect_gis_attribute_sources({(gpkg, "1d_nwk_test")})
    assert result[(gpkg, "1d_nwk_test")].status == "unresolved"
    assert "geometry" in result[(gpkg, "1d_nwk_test")].detail.lower()
    result, _ = inspect_gis_attribute_sources({(gpkg, "1d_unsupported_test")})
    assert result[(gpkg, "1d_unsupported_test")].status == "unresolved"


def test_unexpanded_attribute_variables_fail_closed(tmp_path: Path) -> None:
    gpkg = tmp_path / "network.gpkg"
    make_network_gpkg(gpkg)
    with sqlite3.connect(gpkg) as db:
        db.execute(
            'UPDATE "1d_nwk_test" SET attr_10 = ? WHERE attr_1 = ?',
            ("<<SCENARIO>>/file.csv", "Q"),
        )
    result, _ = inspect_gis_attribute_sources({(gpkg, "1d_nwk_test")})
    assert result[(gpkg, "1d_nwk_test")].status == "unresolved"
    assert "Unexpanded" in result[(gpkg, "1d_nwk_test")].detail


@pytest.mark.parametrize("source", ["M", "Q"])
def test_missing_or_blank_attribute_values_do_not_invent_files(tmp_path: Path, source: str) -> None:
    gpkg = tmp_path / "network.gpkg"
    make_network_gpkg(gpkg)
    with sqlite3.connect(gpkg) as db:
        db.execute('DELETE FROM "1d_nwk_test" WHERE attr_1 <> ?', (source,))
        db.execute('UPDATE "1d_nwk_test" SET attr_10 = NULL')
    result, _ = inspect_gis_attribute_sources({(gpkg, "1d_nwk_test")})
    assert result[(gpkg, "1d_nwk_test")].status == "present"
    assert not result[(gpkg, "1d_nwk_test")].sources
