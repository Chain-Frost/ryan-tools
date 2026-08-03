# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
"""Convert extracted DXF polyface Parquet tables to a filtered 3D GeoPackage.

Run ``python polyface_parquet_to_gpkg.py --help``. Supply the vertices and faces
Parquet files produced by ``dwg-to-points.py`` plus an output path, for example
``--vertices model.vertices.parquet --faces model.triangles.parquet --output model.gpkg``.

The output contains 3D vertex and polygon layers in an undefined Cartesian CRS.
An existing output GeoPackage is deleted and rebuilt, so retain any file that
must be preserved.
"""

from __future__ import annotations

import argparse
import sqlite3
import struct
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable

import numpy as np
import numpy.typing as npt
import pyarrow.parquet as pq

SQL_BATCH_SIZE = 50_000
PARQUET_BATCH_SIZE = 250_000
GPKG_APPLICATION_ID = 0x47504B47
GPKG_USER_VERSION = 10400
UNDEFINED_CARTESIAN_SRS_ID = -1


def gpkg_point_z(x: float, y: float, z: float) -> bytes:
    return struct.pack("<2sBBiBIddd", b"GP", 0, 1, UNDEFINED_CARTESIAN_SRS_ID, 1, 1001, x, y, z)


def gpkg_polygon_z(points: list[tuple[float, float, float]]) -> bytes:
    closed_points = [*points, points[0]]
    geometry = bytearray()
    geometry.extend(struct.pack("<2sBBiBI", b"GP", 0, 1, UNDEFINED_CARTESIAN_SRS_ID, 1, 1003))
    geometry.extend(struct.pack("<II", 1, len(closed_points)))
    for x, y, z in closed_points:
        geometry.extend(struct.pack("<ddd", x, y, z))
    return bytes(geometry)


def chunked(rows: list[tuple[object, ...]], size: int = SQL_BATCH_SIZE) -> Iterable[list[tuple[object, ...]]]:
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


def create_gpkg(output_path: Path) -> sqlite3.Connection:
    if output_path.exists():
        output_path.unlink()

    conn = sqlite3.connect(output_path)
    conn.execute("PRAGMA journal_mode = OFF")
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA temp_store = MEMORY")
    conn.execute("PRAGMA locking_mode = EXCLUSIVE")
    conn.execute(f"PRAGMA application_id = {GPKG_APPLICATION_ID}")
    conn.execute(f"PRAGMA user_version = {GPKG_USER_VERSION}")

    conn.executescript("""
        CREATE TABLE gpkg_spatial_ref_sys (
            srs_name TEXT NOT NULL,
            srs_id INTEGER NOT NULL PRIMARY KEY,
            organization TEXT NOT NULL,
            organization_coordsys_id INTEGER NOT NULL,
            definition TEXT NOT NULL,
            description TEXT
        );

        INSERT INTO gpkg_spatial_ref_sys VALUES
            ('Undefined Cartesian SRS', -1, 'NONE', -1, 'undefined', 'undefined Cartesian coordinate reference system'),
            ('Undefined Geographic SRS', 0, 'NONE', 0, 'undefined', 'undefined geographic coordinate reference system'),
            ('WGS 84 geodetic', 4326, 'EPSG', 4326, 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]', 'longitude/latitude coordinates in decimal degrees on the WGS 84 spheroid');

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
            srs_id INTEGER,
            CONSTRAINT fk_gc_r_srs_id FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys(srs_id)
        );

        CREATE TABLE gpkg_geometry_columns (
            table_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            geometry_type_name TEXT NOT NULL,
            srs_id INTEGER NOT NULL,
            z TINYINT NOT NULL,
            m TINYINT NOT NULL,
            CONSTRAINT pk_geom_cols PRIMARY KEY (table_name, column_name),
            CONSTRAINT uk_gc_table_name UNIQUE (table_name),
            CONSTRAINT fk_gc_tn FOREIGN KEY (table_name) REFERENCES gpkg_contents(table_name),
            CONSTRAINT fk_gc_srs FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys(srs_id)
        );

        CREATE TABLE vertices_z_gt_0 (
            fid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            geom BLOB NOT NULL,
            mesh_id INTEGER NOT NULL,
            mesh_handle TEXT,
            layer TEXT,
            vertex_index INTEGER NOT NULL,
            elevation DOUBLE NOT NULL
        );

        CREATE TABLE faces_z_gt_0 (
            fid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            geom BLOB NOT NULL,
            mesh_id INTEGER NOT NULL,
            mesh_handle TEXT,
            layer TEXT,
            face_index INTEGER NOT NULL,
            v1 INTEGER NOT NULL,
            v2 INTEGER NOT NULL,
            v3 INTEGER NOT NULL,
            v4 INTEGER NOT NULL
        );
        """)

    timestamp = datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    conn.executemany(
        """
        INSERT INTO gpkg_contents
            (table_name, data_type, identifier, description, last_change, srs_id)
        VALUES
            (?, 'features', ?, ?, ?, ?)
        """,
        [
            (
                "vertices_z_gt_0",
                "vertices_z_gt_0",
                "Polyface mesh vertices where z is greater than 0.",
                timestamp,
                UNDEFINED_CARTESIAN_SRS_ID,
            ),
            (
                "faces_z_gt_0",
                "faces_z_gt_0",
                "Polyface mesh faces where every referenced vertex z is greater than 0.",
                timestamp,
                UNDEFINED_CARTESIAN_SRS_ID,
            ),
        ],
    )
    conn.executemany(
        """
        INSERT INTO gpkg_geometry_columns
            (table_name, column_name, geometry_type_name, srs_id, z, m)
        VALUES
            (?, 'geom', ?, ?, 1, 0)
        """,
        [
            ("vertices_z_gt_0", "POINT", UNDEFINED_CARTESIAN_SRS_ID),
            ("faces_z_gt_0", "POLYGON", UNDEFINED_CARTESIAN_SRS_ID),
        ],
    )
    conn.commit()
    return conn


def load_vertex_lookup(
    vertices_path: Path,
) -> tuple[
    npt.NDArray[np.float64],
    npt.NDArray[np.float64],
    npt.NDArray[np.float64],
    npt.NDArray[np.bool_],
    dict[int, int],
    dict[int, int],
]:
    table = pq.read_table(vertices_path, columns=["mesh_id", "vertex_index", "x", "y", "z"])
    mesh_ids = table["mesh_id"].combine_chunks().to_numpy()
    vertex_indices = table["vertex_index"].combine_chunks().to_numpy()
    x = table["x"].combine_chunks().to_numpy()
    y = table["y"].combine_chunks().to_numpy()
    z = table["z"].combine_chunks().to_numpy()

    if len(mesh_ids) == 0:
        return x, y, z, z > 0, {}, {}

    change_positions = np.flatnonzero(np.diff(mesh_ids) != 0) + 1
    starts = np.r_[0, change_positions]
    ends = np.r_[change_positions, len(mesh_ids)]

    mesh_start_by_id: dict[int, int] = {}
    mesh_count_by_id: dict[int, int] = {}
    for start, end in zip(starts, ends, strict=True):
        mesh_id = int(mesh_ids[start])
        mesh_start_by_id[mesh_id] = int(start)
        mesh_count_by_id[mesh_id] = int(end - start)
        expected = np.arange(1, end - start + 1, dtype=vertex_indices.dtype)
        actual = vertex_indices[start:end]
        if not np.array_equal(actual, expected):
            msg = f"Vertex indices for mesh_id {mesh_id} are not contiguous from 1."
            raise ValueError(msg)

    return x, y, z, z > 0, mesh_start_by_id, mesh_count_by_id


def update_extent(
    conn: sqlite3.Connection,
    table_name: str,
    min_x: float,
    min_y: float,
    max_x: float,
    max_y: float,
) -> None:
    conn.execute(
        """
        UPDATE gpkg_contents
        SET min_x = ?, min_y = ?, max_x = ?, max_y = ?
        WHERE table_name = ?
        """,
        (min_x, min_y, max_x, max_y, table_name),
    )


def write_vertices(conn: sqlite3.Connection, vertices_path: Path) -> tuple[int, tuple[float, float, float, float]]:
    parquet_file = pq.ParquetFile(vertices_path)
    insert_sql = """
        INSERT INTO vertices_z_gt_0
            (geom, mesh_id, mesh_handle, layer, vertex_index, elevation)
        VALUES
            (?, ?, ?, ?, ?, ?)
    """

    written = 0
    min_x = np.inf
    min_y = np.inf
    max_x = -np.inf
    max_y = -np.inf

    for batch in parquet_file.iter_batches(batch_size=PARQUET_BATCH_SIZE):
        data = batch.to_pydict()
        rows: list[tuple[object, ...]] = []
        for mesh_id, mesh_handle, layer, vertex_index, x, y, z in zip(
            data["mesh_id"],
            data["mesh_handle"],
            data["layer"],
            data["vertex_index"],
            data["x"],
            data["y"],
            data["z"],
            strict=True,
        ):
            if z <= 0:
                continue
            rows.append((gpkg_point_z(x, y, z), mesh_id, mesh_handle, layer, vertex_index, z))
            min_x = min(min_x, x)
            min_y = min(min_y, y)
            max_x = max(max_x, x)
            max_y = max(max_y, y)

        for row_batch in chunked(rows):
            conn.executemany(insert_sql, row_batch)
        written += len(rows)
        conn.commit()

    if written == 0:
        return written, (0.0, 0.0, 0.0, 0.0)
    return written, (float(min_x), float(min_y), float(max_x), float(max_y))


def write_faces(
    conn: sqlite3.Connection,
    faces_path: Path,
    x: npt.NDArray[np.float64],
    y: npt.NDArray[np.float64],
    z: npt.NDArray[np.float64],
    valid_vertex: npt.NDArray[np.bool_],
    mesh_start_by_id: dict[int, int],
    mesh_count_by_id: dict[int, int],
) -> tuple[int, tuple[float, float, float, float]]:
    parquet_file = pq.ParquetFile(faces_path)
    insert_sql = """
        INSERT INTO faces_z_gt_0
            (geom, mesh_id, mesh_handle, layer, face_index, v1, v2, v3, v4)
        VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    written = 0
    skipped = 0
    min_x = np.inf
    min_y = np.inf
    max_x = -np.inf
    max_y = -np.inf

    for batch in parquet_file.iter_batches(batch_size=PARQUET_BATCH_SIZE):
        data = batch.to_pydict()
        rows: list[tuple[object, ...]] = []
        for mesh_id, mesh_handle, layer, face_index, v1, v2, v3, v4 in zip(
            data["mesh_id"],
            data["mesh_handle"],
            data["layer"],
            data["face_index"],
            data["v1"],
            data["v2"],
            data["v3"],
            data["v4"],
            strict=True,
        ):
            vertex_numbers = [abs(v1), abs(v2), abs(v3)]
            if v4 != 0:
                vertex_numbers.append(abs(v4))

            mesh_start = mesh_start_by_id.get(mesh_id)
            mesh_count = mesh_count_by_id.get(mesh_id)
            if mesh_start is None or mesh_count is None or any(v <= 0 or v > mesh_count for v in vertex_numbers):
                skipped += 1
                continue

            absolute_indices = [mesh_start + vertex_number - 1 for vertex_number in vertex_numbers]
            if not all(bool(valid_vertex[index]) for index in absolute_indices):
                skipped += 1
                continue

            points = [(float(x[index]), float(y[index]), float(z[index])) for index in absolute_indices]
            rows.append((gpkg_polygon_z(points), mesh_id, mesh_handle, layer, face_index, v1, v2, v3, v4))
            point_x = [point[0] for point in points]
            point_y = [point[1] for point in points]
            min_x = min(min_x, *point_x)
            min_y = min(min_y, *point_y)
            max_x = max(max_x, *point_x)
            max_y = max(max_y, *point_y)

        for row_batch in chunked(rows):
            conn.executemany(insert_sql, row_batch)
        written += len(rows)
        conn.commit()

    conn.execute("CREATE TABLE conversion_notes (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    conn.executemany(
        "INSERT INTO conversion_notes (key, value) VALUES (?, ?)",
        [
            ("skipped_faces", str(skipped)),
            ("face_filter", "Faces are included only when every referenced vertex has z > 0."),
        ],
    )

    if written == 0:
        return written, (0.0, 0.0, 0.0, 0.0)
    return written, (float(min_x), float(min_y), float(max_x), float(max_y))


def convert(vertices_path: Path, faces_path: Path, output_path: Path) -> dict[str, object]:
    started_at = time.perf_counter()
    conn = create_gpkg(output_path)
    try:
        vertex_count, vertex_extent = write_vertices(conn, vertices_path)
        update_extent(conn, "vertices_z_gt_0", *vertex_extent)

        x, y, z, valid_vertex, mesh_start_by_id, mesh_count_by_id = load_vertex_lookup(vertices_path)
        face_count, face_extent = write_faces(
            conn=conn,
            faces_path=faces_path,
            x=x,
            y=y,
            z=z,
            valid_vertex=valid_vertex,
            mesh_start_by_id=mesh_start_by_id,
            mesh_count_by_id=mesh_count_by_id,
        )
        update_extent(conn, "faces_z_gt_0", *face_extent)
        conn.commit()
    finally:
        conn.close()

    return {
        "output": str(output_path),
        "output_bytes": output_path.stat().st_size,
        "vertices_written": vertex_count,
        "faces_written": face_count,
        "elapsed_seconds": round(time.perf_counter() - started_at, 1),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vertices", type=Path, required=True, help="Input polyface vertices Parquet path.")
    parser.add_argument("--faces", type=Path, required=True, help="Input polyface faces Parquet path.")
    parser.add_argument("--output", type=Path, required=True, help="Output GeoPackage path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = convert(args.vertices.resolve(), args.faces.resolve(), args.output.resolve())
    for key, value in summary.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
