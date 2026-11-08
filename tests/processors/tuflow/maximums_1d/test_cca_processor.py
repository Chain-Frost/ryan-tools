"""Regression coverage for ccA GeoPackage handling."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from contextlib import AbstractContextManager
from pathlib import Path
from unittest.mock import patch

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
            CREATE TABLE gpkg_geometry_columns (
                table_name TEXT NOT NULL PRIMARY KEY,
                column_name TEXT NOT NULL,
                geometry_type_name TEXT NOT NULL,
                srs_id INTEGER NOT NULL,
                z TINYINT NOT NULL DEFAULT 0,
                m TINYINT NOT NULL DEFAULT 0
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
            INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id)
            VALUES ('cc_results_1d_ccA_L', 'geom', 'POINT', 4326);
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


def test_cca_gpkg_read_is_read_only(tmp_path: Path, change_cwd: Callable[[Path], AbstractContextManager[None]]) -> None:
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


def test_cca_gpkg_closed_wal_mode_does_not_create_sidecars(tmp_path: Path) -> None:
    """Read a closed WAL database from owned staging without creating source sidecars."""
    path = tmp_path / "ClosedWAL_Results1D.gpkg"
    _create_minimal_cca_gpkg(path)
    with sqlite3.connect(path) as writer:
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"

    wal = path.with_name(path.name + "-wal")
    shm = path.with_name(path.name + "-shm")
    assert path.read_bytes()[18:20] == b"\x02\x02"
    assert not wal.exists()
    assert not shm.exists()
    original = path.read_bytes()

    processor = ccAProcessor(file_path=path)
    data = processor.process_gpkg()
    assert data.loc[0, "Chan ID"] == "CULV1"
    assert data.loc[0, "pFull_Max"] == 1.0
    assert path.read_bytes() == original
    assert not wal.exists()
    assert not shm.exists()


def test_cca_gpkg_writer_shutdown_between_preflight_and_read(tmp_path: Path) -> None:
    """Closing the writer during a WAL read must not recreate source sidecars."""
    path = tmp_path / "WriterShutdown_Results1D.gpkg"
    _create_minimal_cca_gpkg(path)
    writer = sqlite3.connect(path)
    assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
    writer.execute("UPDATE cc_results_1d_ccA_L SET pFull_Max = 99")
    writer.commit()
    wal = path.with_name(path.name + "-wal")
    shm = path.with_name(path.name + "-shm")
    assert wal.is_file() and shm.is_file()

    opening = ccAProcessor._open_sqlite_connection
    bytes_after_writer_close: bytes | None = None

    def shutdown_before_open(processor: ccAProcessor, target: Path) -> sqlite3.Connection | None:
        nonlocal bytes_after_writer_close
        writer.close()
        bytes_after_writer_close = path.read_bytes()
        assert not wal.exists() and not shm.exists()
        return opening(processor, target)

    try:
        with patch.object(ccAProcessor, "_open_sqlite_connection", shutdown_before_open):
            data = ccAProcessor(file_path=path).process_gpkg()
    finally:
        writer.close()

    assert data.loc[0, "pFull_Max"] == 99
    assert bytes_after_writer_close is not None
    assert path.read_bytes() == bytes_after_writer_close
    assert not wal.exists() and not shm.exists()


def test_cca_dbf_processing(tmp_path: Path, change_cwd: Callable[[Path], AbstractContextManager[None]]) -> None:
    """Test processing of a DBF file."""
    import shapefile

    with change_cwd(tmp_path):
        dbf_path = tmp_path / "Test_Run_1d_ccA_L.dbf"

        # Create a mock DBF file
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


def test_cca_processor_robustness(tmp_path: Path, change_cwd: Callable[[Path], AbstractContextManager[None]]) -> None:
    """Test robustness against invalid files."""
    with change_cwd(tmp_path):
        # Empty file
        empty_path = tmp_path / "Empty_Run_1d_ccA_L.dbf"
        empty_path.touch()
        proc_empty = ccAProcessor(file_path=empty_path)
        proc_empty.process()
        assert not proc_empty.processed

        # Invalid extension (but valid suffix for BaseProcessor init)
        # If we pass a file with .txt extension but it's not in suffixes config, it raises ValueError.
        # If we pass a file with valid suffix but wrong content, it should fail gracefully.

        # Unsupported extension logic in process()
        # We need to bypass BaseProcessor checks or use a valid suffix but force it to be unsupported in process?
        # process() checks extension.

        # Let's mock file_path.suffix
        p = tmp_path / "Test_Run_1d_ccA_L.dbf"
        p.touch()
        _processor = ccAProcessor(file_path=p)
        with patch.object(
            Path, "suffix", ".xyz"
        ):  # Mock property? No, can't mock property on instance easily like this.
            # Instead, subclass or mock the whole path object.
            pass


def test_process_gpkg_missing_file(tmp_path: Path) -> None:
    """Test process_gpkg with missing file."""
    p = tmp_path / "Missing_Results1D.gpkg"
    processor = ccAProcessor(file_path=p)
    df = processor.process_gpkg()
    assert df.empty


def test_process_gpkg_no_layer(tmp_path: Path) -> None:
    """Test process_gpkg with no matching layer."""
    p = tmp_path / "NoLayer_Results1D.gpkg"
    conn = sqlite3.connect(p)
    conn.execute("CREATE TABLE gpkg_contents (table_name TEXT, data_type TEXT)")
    conn.commit()
    conn.close()

    processor = ccAProcessor(file_path=p)
    df = processor.process_gpkg()
    assert df.empty


def test_process_gpkg_missing_columns(tmp_path: Path) -> None:
    """Test process_gpkg with missing required columns."""
    p = tmp_path / "MissingCols_Results1D.gpkg"
    conn = sqlite3.connect(p)
    conn.execute("CREATE TABLE gpkg_contents (table_name TEXT, data_type TEXT)")
    conn.execute("INSERT INTO gpkg_contents VALUES ('layer_1d_ccA_L', 'features')")
    conn.execute("CREATE TABLE layer_1d_ccA_L (fid INTEGER PRIMARY KEY, geom BLOB, WrongCol TEXT)")
    conn.commit()
    conn.close()

    processor = ccAProcessor(file_path=p)
    # Mock output_columns to require "Chan ID"
    processor.output_columns = {"Chan ID": "string"}

    df = processor.process_gpkg()
    assert df.empty


def test_process_gpkg_sqlite_error(tmp_path: Path) -> None:
    """Test process_gpkg handling sqlite error."""
    p = tmp_path / "Error_Results1D.gpkg"
    p.touch()  # Not a valid sqlite file

    processor = ccAProcessor(file_path=p)
    df = processor.process_gpkg()
    assert df.empty


def test_process_dbf_error(tmp_path: Path) -> None:
    """Test process_dbf handling error."""
    p = tmp_path / "Error_1d_ccA_L.dbf"
    p.touch()  # Not a valid DBF

    processor = ccAProcessor(file_path=p)
    df = processor.process_dbf()
    assert df.empty


def test_process_validation_failure(tmp_path: Path) -> None:
    """Test process validation failure."""
    p = tmp_path / "Valid_1d_ccA_L.dbf"
    # Create valid DBF
    import shapefile

    base_path = tmp_path / "Valid_1d_ccA_L"
    with shapefile.Writer(str(base_path)) as w:
        w.field("Channel", "C")
        w.null()
        w.record("C1")

    processor = ccAProcessor(file_path=p)

    with patch.object(ccAProcessor, "validate_data", return_value=False):
        processor.process()
        assert not processor.processed
        assert processor.df.empty


def test_process_exception(tmp_path: Path) -> None:
    """Test process exception handling."""
    p = tmp_path / "Test_1d_ccA_L.dbf"
    p.touch()
    processor = ccAProcessor(file_path=p)

    with patch.object(ccAProcessor, "process_dbf", side_effect=Exception("Boom")):
        processor.process()
        assert not processor.processed
        assert processor.df.empty


def test_cca_gpkg_reads_committed_wal_with_active_writer(
    tmp_path: Path, change_cwd: Callable[[Path], AbstractContextManager[None]]
) -> None:
    """Uncheckpointed committed WAL values must take precedence over the base database."""
    gpkg_path = tmp_path / "Café #1_Results1D.gpkg"
    _create_minimal_cca_gpkg(gpkg_path)
    with sqlite3.connect(gpkg_path) as writer:
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        writer.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        writer.execute("UPDATE cc_results_1d_ccA_L SET pFull_Max = 99")
        writer.commit()

        original_main = gpkg_path.read_bytes()
        original_wal = gpkg_path.with_name(gpkg_path.name + "-wal").read_bytes()
        files_before = set(tmp_path.iterdir())
        with change_cwd(tmp_path):
            processor = ccAProcessor(file_path=gpkg_path)
            processor.process()

        assert processor.processed
        assert processor.df.loc[0, "pFull_Max"] == 99
        assert gpkg_path.read_bytes() == original_main
        assert gpkg_path.with_name(gpkg_path.name + "-wal").read_bytes() == original_wal
        assert set(tmp_path.iterdir()) == files_before

        writer.execute("BEGIN IMMEDIATE")
        writer.execute("UPDATE cc_results_1d_ccA_L SET pFull_Max = 150")
        with change_cwd(tmp_path):
            next_processor = ccAProcessor(file_path=gpkg_path)
            next_processor.process()
        assert next_processor.processed
        assert next_processor.df.loc[0, "pFull_Max"] == 99
        writer.rollback()


def test_cca_gpkg_locked_writer_fails_with_bounded_wait(tmp_path: Path) -> None:
    """An exclusive writer must not hang processing or cause a stale fallback."""
    import time

    path = tmp_path / "Locked_Results1D.gpkg"
    _create_minimal_cca_gpkg(path)
    with sqlite3.connect(path) as writer:
        writer.execute("PRAGMA locking_mode=EXCLUSIVE")
        writer.execute("BEGIN EXCLUSIVE")
        writer.execute("UPDATE cc_results_1d_ccA_L SET pFull_Max=77")
        start = time.monotonic()
        processor = ccAProcessor(file_path=path)
        assert processor.process_gpkg().empty
        assert time.monotonic() - start < 2.5
        writer.rollback()


def test_cca_gpkg_ambiguous_layers_are_rejected(tmp_path: Path) -> None:
    path = tmp_path / "Ambiguous_Results1D.gpkg"
    _create_minimal_cca_gpkg(path)
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE other_1d_ccA_L (fid INTEGER PRIMARY KEY, shape BLOB, Channel TEXT)")
        conn.execute(
            "INSERT INTO gpkg_contents (table_name, data_type, identifier, srs_id) "
            "VALUES ('other_1d_ccA_L', 'features', 'other_1d_ccA_L', 4326)"
        )
        conn.execute(
            "INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id) "
            "VALUES ('other_1d_ccA_L', 'shape', 'POINT', 4326)"
        )
    processor = ccAProcessor(file_path=path)
    assert processor.process_gpkg().empty


def test_cca_gpkg_reads_geometry_identity_from_metadata(tmp_path: Path) -> None:
    """An attribute named geom is retained when the actual geometry column is shape."""
    path = tmp_path / "Geometry_Results1D.gpkg"
    _create_minimal_cca_gpkg(path)
    with sqlite3.connect(path) as conn:
        conn.execute("ALTER TABLE cc_results_1d_ccA_L RENAME COLUMN geom TO shape")
        conn.execute("ALTER TABLE cc_results_1d_ccA_L ADD COLUMN geom TEXT")
        conn.execute("UPDATE gpkg_geometry_columns SET column_name = 'shape'")
        conn.execute("UPDATE cc_results_1d_ccA_L SET geom = 'ordinary attribute'")
    processor = ccAProcessor(file_path=path)
    result = processor.process_gpkg()
    assert result.loc[0, "geom"] == "ordinary attribute"
    assert "shape" not in result.columns
    assert "fid" not in result.columns


def test_cca_gpkg_partial_optional_fields_match_dbf_contract(tmp_path: Path) -> None:
    """Missing optional metrics must not invalidate an otherwise useful ccA layer."""
    path = tmp_path / "Partial_Results1D.gpkg"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE gpkg_contents (table_name TEXT, data_type TEXT)")
        conn.execute("CREATE TABLE gpkg_geometry_columns (table_name TEXT, column_name TEXT)")
        conn.execute(
            'CREATE TABLE "partial_1d_ccA_L" (fid INTEGER PRIMARY KEY, geometry BLOB, Channel TEXT, pFull_Max REAL)'
        )
        conn.execute("INSERT INTO gpkg_contents VALUES ('partial_1d_ccA_L', 'features')")
        conn.execute("INSERT INTO gpkg_geometry_columns VALUES ('partial_1d_ccA_L', 'geometry')")
        conn.execute("INSERT INTO partial_1d_ccA_L VALUES (1, NULL, 'C1', 2.5)")
    proc = ccAProcessor(file_path=path)
    proc.process()
    assert proc.processed
    assert proc.df.loc[0, "Chan ID"] == "C1"
    assert proc.df.loc[0, "pFull_Max"] == 2.5
    assert "Dur_Sur" not in proc.df.columns


def test_cca_gpkg_missing_shm_is_not_created(tmp_path: Path) -> None:
    """Fail a WAL-only view instead of creating a -shm sidecar."""
    path = tmp_path / "Sidecar_Results1D.gpkg"
    _create_minimal_cca_gpkg(path)
    wal = path.with_name(path.name + "-wal")
    shm = path.with_name(path.name + "-shm")
    wal.write_bytes(b"synthetic WAL awaiting writer recovery")
    assert not shm.exists()
    processor = ccAProcessor(file_path=path)
    assert processor.process_gpkg().empty
    assert wal.read_bytes() == b"synthetic WAL awaiting writer recovery"
    assert not shm.exists()


def test_cca_gpkg_missing_or_replaced_input_is_never_created(tmp_path: Path) -> None:
    path = tmp_path / "Missing_Results1D.gpkg"
    processor = ccAProcessor(file_path=path)
    assert processor.process_gpkg().empty
    assert not path.exists()
    _create_minimal_cca_gpkg(path)
    path.unlink()
    path.write_bytes(b"replacement which is not SQLite")
    assert processor.process_gpkg().empty
    assert path.read_bytes() == b"replacement which is not SQLite"
