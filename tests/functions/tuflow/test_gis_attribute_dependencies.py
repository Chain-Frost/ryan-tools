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
    assert result[(gpkg, "1d_nwk_test")].network_identifiers == ("Curve_A",)
    assert not result[(gpkg, "1d_nwk_test")].pit_identifiers


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
        db.execute(
            'CREATE TABLE "1d_pit_test" (fid INTEGER PRIMARY KEY, geom BLOB, id TEXT, type TEXT, network TEXT, inlet TEXT)'
        )
        for kind, inlet in (("Q", "Kerb_Inlet_A"), ("VPI", "Grated"), ("R", "Unused")):
            db.execute('INSERT INTO "1d_pit_test" (type, inlet) VALUES (?, ?)', (kind, inlet))
    results, opens = inspect_gis_attribute_sources({(gpkg, "1d_pit_test")})
    assert opens == 1
    assert results[(gpkg, "1d_pit_test")].status == "present"
    assert results[(gpkg, "1d_pit_test")].sources == ()
    assert results[(gpkg, "1d_pit_test")].identifiers == ("Grated", "Kerb_Inlet_A")
    assert results[(gpkg, "1d_pit_test")].pit_identifiers == ("Grated", "Kerb_Inlet_A")
    assert not results[(gpkg, "1d_pit_test")].network_identifiers


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


def make_network_shapefile(path: Path) -> None:
    """Build a real Shapefile with attributes but no stored feature geometry."""
    import shapefile

    with shapefile.Writer(str(path), shapeType=shapefile.POLYLINE) as writer:
        for index in range(11):
            writer.field(f"attr_{index}", "C", size=120)
        for channel, ignore, value in (
            ("M", "", "operational.csv | section.csv"),
            ("QO", "", "Gate_Control | operated.csv"),
            ("Q", "", "Curve_A"),
            ("M", "Y", "not_required.csv"),
            ("QO", "t", "Gate | stale.csv"),
            ("C", "", "ignored.csv"),
        ):
            attrs = [""] * 11
            attrs[1], attrs[2], attrs[10] = channel, ignore, value
            writer.null()
            writer.record(*attrs)


def test_shapefile_and_geopackage_use_identical_network_attribute_rules(tmp_path: Path) -> None:
    """Same 1d_nwk semantics must not depend on the GIS container format."""
    gpkg = tmp_path / "network.gpkg"
    shp = tmp_path / "1d_nwk_test.shp"
    make_network_gpkg(gpkg)
    make_network_shapefile(shp)
    results, opens = inspect_gis_attribute_sources({(gpkg, "1d_nwk_test"), (shp, "")})
    assert opens == 2
    eager = results[(gpkg, "1d_nwk_test")]
    deferred = results[(shp, "")]
    assert eager.status == deferred.status == "present"
    assert eager.sources == deferred.sources
    assert eager.identifiers == deferred.identifiers == ("Curve_A",)
    assert eager.network_identifiers == deferred.network_identifiers == ("Curve_A",)
    assert set(deferred.sources) == {
        tmp_path / "operational.csv",
        tmp_path / "section.csv",
        tmp_path / "operated.csv",
    }


def test_shapefile_is_read_once_for_multiple_layer_selectors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The DBF is opened once and geometry features are never loaded."""
    import shapefile

    shp = tmp_path / "1d_nwk_example.shp"
    make_network_shapefile(shp)

    def disallow_shapes(*_args: object, **_kwargs: object) -> None:
        msg = "Geometry must not be loaded when only attributes are required"
        raise AssertionError(msg)

    monkeypatch.setattr(shapefile.Reader, "iterShapes", disallow_shapes)
    monkeypatch.setattr(shapefile.Reader, "iterShapeRecords", disallow_shapes)
    results, opens = inspect_gis_attribute_sources(
        {(shp, ""), (shp, "1d_nwk_*"), (shp, "all"), (shp, "1d_xs_not_present")}
    )
    assert opens == 1
    for selector in ("", "all", "1d_nwk_*"):
        assert results[(shp, selector)].status == "present"
        assert results[(shp, selector)].identifiers == ("Curve_A",)
    assert results[(shp, "1d_xs_not_present")].status == "unresolved"


def test_shapefile_attribute_schema_fail_closed(tmp_path: Path) -> None:
    """Missing DBF, unknown field layout and unexpected geometry never pass."""
    import shapefile

    shp = tmp_path / "1d_nwk_incomplete.shp"
    with shapefile.Writer(str(shp), shapeType=shapefile.POLYLINE) as writer:
        writer.field("attr_0", "C")
        writer.null()
        writer.record("ABC")
    results, _ = inspect_gis_attribute_sources({(shp, "")})
    assert results[(shp, "")].status == "unresolved"
    assert "index" in results[(shp, "")].detail

    shp.with_suffix(".dbf").unlink()
    results, _ = inspect_gis_attribute_sources({(shp, "")})
    assert results[(shp, "")].status == "unresolved"


def test_shapefile_pit_identifiers_use_same_rule_as_geopackage(tmp_path: Path) -> None:
    """Shapefile Q/VPI pits produce database identifiers, not guessed paths."""
    import shapefile

    shp = tmp_path / "1d_pit_test.shp"
    with shapefile.Writer(str(shp), shapeType=shapefile.POINT) as writer:
        for index in range(4):
            writer.field(f"attr_{index}", "C", size=80)
        for kind, value in (("Q", "Kerb_A"), ("VPI", "Grate_B"), ("R", "Ignore")):
            writer.null()
            writer.record("id", kind, "", value)
    result, opens = inspect_gis_attribute_sources({(shp, "")})
    assert opens == 1
    assert result[(shp, "")].status == "present"
    assert result[(shp, "")].sources == ()
    assert result[(shp, "")].identifiers == ("Grate_B", "Kerb_A")


def test_shapefile_xs_file_attribute_matches_geopackage(tmp_path: Path) -> None:
    """Non-network file-bearing TUFLOW elements get the same first-field rule."""
    import shapefile

    shp = tmp_path / "1d_xs_example.shp"
    with shapefile.Writer(str(shp), shapeType=shapefile.POLYLINE) as writer:
        writer.field("source", "C", size=100)
        writer.null()
        writer.record("section.csv")
    gpkg = tmp_path / "xs.gpkg"
    with sqlite3.connect(gpkg) as db:
        db.execute("CREATE TABLE gpkg_geometry_columns(table_name TEXT, column_name TEXT, geometry_type_name TEXT)")
        db.execute("INSERT INTO gpkg_geometry_columns VALUES ('1d_xs_example','geom','LINESTRING')")
        db.execute('CREATE TABLE "1d_xs_example" (fid INTEGER PRIMARY KEY, geom BLOB, source TEXT)')
        db.execute('INSERT INTO "1d_xs_example" (source) VALUES ("section.csv")')
    result, opens = inspect_gis_attribute_sources({(shp, ""), (gpkg, "1d_xs_example")})
    assert opens == 2
    assert result[(shp, "")].sources == result[(gpkg, "1d_xs_example")].sources
    assert result[(shp, "")].sources == (tmp_path / "section.csv",)
