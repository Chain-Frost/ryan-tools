"""Regression coverage for ccA GeoPackage handling."""

from __future__ import annotations

import sqlite3
import pytest
from pathlib import Path

from ryan_library.processors.tuflow.maximums_1d.ccAProcessor import ccAProcessor


def _create_minimal_cca_gpkg(path: Path) -> None:
    """Create a tiny GeoPackage with a single ccA layer for read-only tests."""

    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE gpkg_contents (
                table_name TEXT NOT NULL PRIMARY KEY,
                data_type TEXT NOT NULL,
                identifier TEXT UNIQUE,
                description TEXT DEFAULT '',
                last_change DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                min_x DOUBLE,
                min_y DOUBLE,
                max_x DOUBLE,
                max_y DOUBLE,
                srs_id INTEGER
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE cc_results_1d_ccA_L (
                fid INTEGER PRIMARY KEY,
                geom BLOB,
                "Chan ID" TEXT,
                pFull_Max REAL,
                pTime_Full REAL,
                Area_Max REAL,
                Area_Culv REAL,
                Dur_Full REAL,
                Dur_10pFull REAL,
                Sur_CD REAL,
                Dur_Sur REAL,
                pTime_Sur REAL,
                TFirst_Sur REAL
            );
            """
        )

        cur.execute(
            """
            INSERT INTO gpkg_contents (table_name, data_type, identifier, srs_id)
            VALUES ('cc_results_1d_ccA_L', 'features', 'cc_results_1d_ccA_L', 4326);
            """
        )
        cur.execute(
            """
            INSERT INTO cc_results_1d_ccA_L (
                fid, geom, "Chan ID", pFull_Max, pTime_Full, Area_Max, Area_Culv,
                Dur_Full, Dur_10pFull, Sur_CD, Dur_Sur, pTime_Sur, TFirst_Sur
            ) VALUES (1, NULL, 'CULV1', 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0);
            """
        )
        conn.commit()
    finally:
        conn.close()


def test_cca_gpkg_read_is_read_only(tmp_path: Path, change_cwd) -> None:
    """Processing ccA GeoPackages should not create WAL/SHM sidecar files."""

    with change_cwd(tmp_path):
        gpkg_path = tmp_path / "EG12_001_Results1D.gpkg"
        wal_path = gpkg_path.with_name(gpkg_path.name + "-wal")
        shm_path = gpkg_path.with_name(gpkg_path.name + "-shm")

        _create_minimal_cca_gpkg(gpkg_path)
        wal_path.unlink(missing_ok=True)
        shm_path.unlink(missing_ok=True)

        processor = ccAProcessor(file_path=gpkg_path)
        processor.process()
        df = processor.df

    assert not df.empty
    assert df.loc[0, "Chan ID"] == "CULV1"
    assert not wal_path.exists()
    assert not shm_path.exists()


def test_cca_dbf_processing(tmp_path: Path, change_cwd) -> None:
    """Test processing of a DBF file."""
    import shapefile

    with change_cwd(tmp_path):
        dbf_path = tmp_path / "Test_Run_1d_ccA_L.dbf"
        shp_path = tmp_path / "Test_Run_1d_ccA_L.shp"
        shx_path = tmp_path / "Test_Run_1d_ccA_L.shx"

        # Create a mock DBF file
        # To avoid ShapefileException, we must create .shp and .shx too or use a specific way.
        # But simplest is to just let it create them or ignore.
        # Using shapeType=shapefile.NULL might work?
        # Or just create a writer without extension and let it create all 3?
        base_path = tmp_path / "Test_Run_1d_ccA_L"
        with shapefile.Writer(str(base_path)) as w:
            w.field("Channel", "C")
            w.field("pFull_Max", "N", 10, 2)
            w.null()  # Add a null shape
            w.record("Chan1", 1.23)

        # Now we have .dbf, .shp, .shx

        processor = ccAProcessor(file_path=dbf_path)
        processor.process()

    assert processor.processed
    assert not processor.df.empty
    assert "Chan ID" in processor.df.columns  # Renamed from Channel
    assert processor.df.iloc[0]["Chan ID"] == "Chan1"
    assert processor.df.iloc[0]["pFull_Max"] == 1.23


def test_cca_processor_robustness(tmp_path: Path, change_cwd) -> None:
    """Test robustness against invalid files."""
    with change_cwd(tmp_path):
        # Empty file
        empty_path = tmp_path / "Empty_Run_1d_ccA_L.dbf"
        empty_path.touch()
        proc_empty = ccAProcessor(file_path=empty_path)
        proc_empty.process()
        assert not proc_empty.processed

        # Invalid extension
        txt_path = tmp_path / "Invalid_Run_1d_ccA_L.txt"
        txt_path.touch()
        # BaseProcessor raises ValueError if suffix doesn't match known types
        with pytest.raises(ValueError, match="data_type was not set"):
            ccAProcessor(file_path=txt_path)


def test_cca_processor_real_file(find_test_file):
    """Test ccAProcessor with a real file if available."""
    # Try to find a .dbf or .gpkg
    file_path = find_test_file("_1d_ccA_L.dbf")
    if not file_path:
        file_path = find_test_file("_Results1D.gpkg")

    if not file_path:
        pytest.skip("No _1d_ccA_L.dbf or _Results1D.gpkg file found in test data")

    processor = ccAProcessor(file_path=file_path)
    processor.process()

    # It might fail if the real file is missing columns or malformed,
    # but we expect it to handle it gracefully (processed=False) or succeed.
    # If it raises an exception, the test will fail, which is good.
    if processor.processed:
        assert not processor.df.empty
        assert "internalName" in processor.df.columns
