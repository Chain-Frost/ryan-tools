"""Read-only, once-per-database GIS attribute dependency materialisation tests."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from ryan_library.functions.tuflow.gis_attribute_dependencies import (
    inspect_gis_attribute_sources,
    read_pit_inlet_database,
)


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
                ("QO", "Gate_Control | operated.csv"),
                ("Q", "Curve_A"),
                ("C", "ignored.csv"),
                ("C", ""),
            ):
                db.execute(
                    'INSERT INTO "1d_nwk_test" (attr_1, attr_10) VALUES (?, ?)',
                    (channel, file_values),
                )


def test_network_m_qo_files_and_q_curve_names_are_distinguished(tmp_path: Path) -> None:
    gpkg = tmp_path / "network.gpkg"
    make_network_gpkg(gpkg)
    result, opens = inspect_gis_attribute_sources({(gpkg, "1d_nwk_test"), (gpkg, "1d_nwk_*")})
    expected = {
        tmp_path / "operational.csv",
        tmp_path / "section.csv",
        tmp_path / "operated.csv",
    }
    assert opens == 1
    assert result[(gpkg, "1d_nwk_test")].status == "present"
    assert set(result[(gpkg, "1d_nwk_test")].sources) == expected
    assert set(result[(gpkg, "1d_nwk_*")].sources) == expected
    assert result[(gpkg, "1d_nwk_test")].identifiers == ("Curve_A",)


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
            ("Gate | <<SCENARIO>>/file.csv", "QO"),
        )
    result, _ = inspect_gis_attribute_sources({(gpkg, "1d_nwk_test")})
    assert result[(gpkg, "1d_nwk_test")].status == "unresolved"
    assert "Unexpanded" in result[(gpkg, "1d_nwk_test")].detail


@pytest.mark.parametrize("source", ["M", "Q", "QO"])
def test_blank_active_matrix_or_curve_values_fail_closed(tmp_path: Path, source: str) -> None:
    gpkg = tmp_path / "network.gpkg"
    make_network_gpkg(gpkg)
    with sqlite3.connect(gpkg) as db:
        db.execute('DELETE FROM "1d_nwk_test" WHERE attr_1 <> ?', (source,))
        db.execute('UPDATE "1d_nwk_test" SET attr_10 = NULL')
    result, _ = inspect_gis_attribute_sources({(gpkg, "1d_nwk_test")})
    assert result[(gpkg, "1d_nwk_test")].status == "unresolved"
    assert "blank" in result[(gpkg, "1d_nwk_test")].detail


def test_network_ignore_y_t_skips_stale_m_and_qo_sources(tmp_path: Path) -> None:
    gpkg = tmp_path / "network.gpkg"
    make_network_gpkg(gpkg)
    with sqlite3.connect(gpkg) as db:
        db.execute(
            'UPDATE "1d_nwk_test" SET attr_2 = ? WHERE attr_1 = ?',
            ("Y", "M"),
        )
        db.execute(
            'UPDATE "1d_nwk_test" SET attr_2 = ? WHERE attr_1 = ?',
            ("t", "QO"),
        )
    result, opens = inspect_gis_attribute_sources({(gpkg, "1d_nwk_test")})
    assert opens == 1
    assert result[(gpkg, "1d_nwk_test")].status == "present"
    assert not result[(gpkg, "1d_nwk_test")].sources
    assert result[(gpkg, "1d_nwk_test")].identifiers == ("Curve_A",)


def test_pit_inlet_identifiers_are_not_treated_as_paths(tmp_path: Path) -> None:
    gpkg = tmp_path / "pit.gpkg"
    with sqlite3.connect(gpkg) as db:
        db.execute("CREATE TABLE gpkg_geometry_columns(table_name TEXT, column_name TEXT, geometry_type_name TEXT)")
        db.execute("INSERT INTO gpkg_geometry_columns VALUES ('1d_pit_test', 'geom', 'POINT')")
        db.execute('CREATE TABLE "1d_pit_test" (fid INTEGER PRIMARY KEY, geom BLOB, id TEXT, type TEXT, network TEXT, inlet TEXT)')
        for kind, inlet in (("Q", "Kerb_Inlet_A"), ("VPI", "Grated"), ("R", "Unused")):
            db.execute('INSERT INTO "1d_pit_test" (type, inlet) VALUES (?, ?)', (kind, inlet))
    results, opens = inspect_gis_attribute_sources({(gpkg, "1d_pit_test")})
    assert opens == 1
    assert results[(gpkg, "1d_pit_test")].status == "present"
    assert results[(gpkg, "1d_pit_test")].sources == ()
    assert results[(gpkg, "1d_pit_test")].identifiers == ("Grated", "Kerb_Inlet_A")


def test_pit_inlet_database_maps_curve_ids_to_source_files(tmp_path: Path) -> None:
    database = tmp_path / "pit_database.csv"
    database.write_text(
        "Name,Source,Column 1,Column 2,Area,Width\n"
        "Kerb_Inlet_A,pit_curves.csv,Depth,Kerb,0.3,1\n"
        "Grated,grated.csv,Depth,Flow,0.4,1\n",
        encoding="utf-8",
    )
    mapping = read_pit_inlet_database(database)
    assert mapping == {
        "kerb_inlet_a": tmp_path / "pit_curves.csv",
        "grated": tmp_path / "grated.csv",
    }
    with database.open("a", encoding="utf-8") as stream:
        stream.write("KERB_INLET_A,another.csv,Depth,Other,0.1,1\n")
    with pytest.raises(ValueError, match="Duplicate"):
        read_pit_inlet_database(database)


def test_qo_without_control_separator_fails_closed(tmp_path: Path) -> None:
    gpkg = tmp_path / "network.gpkg"
    make_network_gpkg(gpkg)
    with sqlite3.connect(gpkg) as db:
        db.execute(
            'UPDATE "1d_nwk_test" SET attr_10 = ? WHERE attr_1 = ?',
            ("unqualified_curve.csv", "QO"),
        )
    result, _ = inspect_gis_attribute_sources({(gpkg, "1d_nwk_test")})
    assert result[(gpkg, "1d_nwk_test")].status == "unresolved"
    assert "QO" in result[(gpkg, "1d_nwk_test")].detail
