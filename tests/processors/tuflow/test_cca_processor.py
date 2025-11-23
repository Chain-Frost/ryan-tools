"""Regression coverage for ccA GeoPackage handling."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from ryan_library.processors.tuflow.maximums_1d.ccAProcessor import ccAProcessor


def _create_minimal_cca_gpkg(path: Path) -> None:
    """Create a tiny GeoPackage with a single ccA layer for read-only tests."""

    with sqlite3.connect(path) as conn:
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


def test_cca_gpkg_read_is_read_only(tmp_path: Path) -> None:
    """Processing ccA GeoPackages should not create WAL/SHM sidecar files."""

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
