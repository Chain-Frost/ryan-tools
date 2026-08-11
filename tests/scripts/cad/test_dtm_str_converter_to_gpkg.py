from __future__ import annotations

import importlib.util
import math
import struct
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point


@pytest.fixture(scope="module")
def converter() -> ModuleType:
    script_path = Path(__file__).parents[3] / "ryan-scripts" / "cad-python" / "dtm_str_converter_to_gpkg.py"
    spec = importlib.util.spec_from_file_location("dtm_str_converter_to_gpkg_for_tests", script_path)
    if spec is None or spec.loader is None:
        pytest.fail(f"Could not load converter script: {script_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def binary_record(
    string_number: int,
    y: float,
    x: float,
    z: float,
    description: str = "",
) -> bytes:
    return struct.pack(">iddd", string_number, y, x, z) + description.encode("ascii") + b"\0"


def binary_str(*records: bytes, prefix: bytes = b"\0" * 5, end_marker: bytes = b"END\0") -> bytes:
    headers = b"synthetic.str,08-Aug-26,test,styles\r\n0, 0, 0, 0, 0, 0, 0\r\n"
    return headers + prefix + b"".join(records) + end_marker


def binary_dtm_triangle(
    triangle_number: int,
    vertex1: int,
    vertex2: int,
    vertex3: int,
    neighbour1: int,
    neighbour2: int,
    neighbour3: int,
    *,
    vertex_count: int = 3,
    terminator: int = 0,
) -> bytes:
    return struct.pack(
        ">8iB",
        vertex_count,
        triangle_number,
        vertex1,
        vertex2,
        vertex3,
        neighbour1,
        neighbour2,
        neighbour3,
        terminator,
    )


def binary_dtm_block(
    string_number: int,
    *triangles: bytes,
    metadata: bytes = b"neighbours=yes,validated=true,closed=no,direction=surface,algorithm=legacy",
    block_header: tuple[int, int, int, int] = (1, 0, 2, 1),
) -> bytes:
    block_flag, reserved, record_type, version = block_header
    return (
        struct.pack(">iiiBi", block_flag, string_number, reserved, record_type, version)
        + metadata
        + b"\0"
        + b"".join(triangles)
    )


def binary_dtm_submesh(
    mesh_number: int,
    *triangles: bytes,
    metadata: bytes = b"neighbours=yes,validated=true,closed=yes,direction=solid,algorithm=legacy",
) -> bytes:
    return struct.pack(">iBi", 0, 2, mesh_number) + metadata + b"\0" + b"".join(triangles)


def binary_dtm(
    *blocks: bytes,
    prefix: bytes = b"\0" * 33 + b"END\0",
    final_marker: bytes = b"\xff" * 8,
) -> bytes:
    return b"synthetic.str,1;algorithm=standard;fields=x,y\r\n" + prefix + b"".join(blocks) + final_marker


def test_ascii_and_binary_str_produce_equivalent_dataframes(
    converter: ModuleType,
    tmp_path: Path,
) -> None:
    precise_z = 1.2345678901234567
    ascii_path = tmp_path / "equivalent_ascii.str"
    ascii_path.write_text(
        "synthetic.str,08-Aug-26,test,styles\n"
        "0, 0, 0, 0, 0, 0, 0\n"
        f"1,-2.5,3.125,{precise_z:.17g},BW (4),27\n"
        "0,0,0,0\n"
        "2,4.5,-6.25,0\n"
        "0,0,0,0\n",
        encoding="ascii",
    )
    binary_path = tmp_path / "equivalent_binary.str"
    binary_path.write_bytes(
        binary_str(
            binary_record(1, -2.5, 3.125, precise_z, "BW (4),27"),
            binary_record(0, 0.0, 0.0, 0.0),
            binary_record(2, 4.5, -6.25, 0.0),
            binary_record(0, 0.0, 0.0, 0.0),
        )
    )

    ascii_df, ascii_format = converter.read_str_file_with_format(ascii_path)
    binary_df, binary_format = converter.read_str_file_with_format(binary_path)

    pd.testing.assert_frame_equal(binary_df, ascii_df)
    assert ascii_format == "ASCII STR"
    assert binary_format == "binary STR"
    assert binary_df["point_number"].tolist() == [1, 3]
    assert binary_df["group"].tolist() == [1, 2]
    assert binary_df.loc[0, ["d1", "d2"]].tolist() == ["BW (4)", "27"]
    assert binary_df.loc[0, "z"] == precise_z


def test_binary_str_supports_multiple_points_per_segment_and_3d_geometry(
    converter: ModuleType,
    tmp_path: Path,
) -> None:
    path = tmp_path / "segments.str"
    path.write_bytes(
        binary_str(
            binary_record(7, -1.0, -2.0, -3.0, "first"),
            binary_record(7, 1.0, 2.0, 3.0),
            binary_record(0, 0.0, 0.0, 0.0),
            binary_record(9999, 10.0, 20.0, 30.0),
        )
    )

    dataframe = converter.read_str_file(path)
    points = converter.create_points_gdf(dataframe, "EPSG:28351")
    lines = converter.generate_linestrings(points)

    assert dataframe["string"].tolist() == [7, 7, 9999]
    assert dataframe["point_number"].tolist() == [1, 2, 4]
    assert len(lines) == 1
    assert lines.geometry.iloc[0].has_z
    assert list(lines.geometry.iloc[0].coords) == [(-2.0, -1.0, -3.0), (2.0, 1.0, 3.0)]


def test_binary_str_accepts_terminal_break_without_description_terminator(
    converter: ModuleType,
    tmp_path: Path,
) -> None:
    path = tmp_path / "terminal_break.str"
    terminal_break = struct.pack(">iddd", 0, 0.0, 0.0, 0.0)
    path.write_bytes(binary_str(binary_record(1, 2.0, 3.0, 4.0), terminal_break))

    dataframe = converter.read_str_file(path)

    assert dataframe["point_number"].tolist() == [1]
    assert dataframe["group"].tolist() == [1]


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (b"one header only\n\0\0\0\0\0END\0", "expected two ASCII header lines"),
        (
            b"bad\xff\n0,0,0,0\n" + b"\0" * 5 + b"END\0",
            "header is not ASCII",
        ),
        (binary_str(prefix=b"\0" * 4 + b"X"), "Unexpected binary STR prefix"),
        (binary_str(binary_record(1, 2.0, 3.0, 4.0), end_marker=b"STOP"), "missing the final END marker"),
        (binary_str(b"\0" * 12), "Truncated binary STR record"),
        (
            binary_str(struct.pack(">iddd", 1, 2.0, 3.0, 4.0) + b"unterminated"),
            "Unterminated binary STR description",
        ),
        (binary_str(struct.pack(">iddd", 1, 2.0, 3.0, 4.0) + b"\xff\0"), "description is not ASCII"),
        (binary_str(binary_record(-1, 2.0, 3.0, 4.0)), "Negative binary STR string number"),
        (binary_str(binary_record(1, math.nan, 3.0, 4.0)), "Non-finite binary STR coordinate"),
        (binary_str(binary_record(0, 1.0, 0.0, 0.0)), "Malformed binary STR segment break"),
    ],
)
def test_invalid_binary_str_is_rejected_with_path_and_offset(
    converter: ModuleType,
    tmp_path: Path,
    payload: bytes,
    message: str,
) -> None:
    path = tmp_path / "invalid.str"
    path.write_bytes(payload)

    with pytest.raises(converter.ConverterParseError) as exc_info:
        converter.read_str_file(path)

    error = str(exc_info.value)
    assert message in error
    assert "byte offset" in error
    assert str(path) in error


def test_ascii_and_binary_dtm_produce_equivalent_triangles(
    converter: ModuleType,
    tmp_path: Path,
) -> None:
    ascii_path = tmp_path / "equivalent_ascii.dtm"
    ascii_path.write_text(
        "synthetic.str,1;algorithm=standard;fields=x,y\n" "TRISOLATION\n" "1 1 2 3 -1 2 -1\n" "2 2 4 3 -1 -1 1\n",
        encoding="ascii",
    )
    binary_path = tmp_path / "equivalent_binary.dtm"
    binary_path.write_bytes(
        binary_dtm(
            binary_dtm_block(
                301,
                binary_dtm_triangle(1, 1, 2, 3, -1, 2, -1),
                binary_dtm_triangle(2, 2, 4, 3, -1, -1, 1),
            )
        )
    )

    ascii_df, ascii_format = converter.read_dtm_file_with_format(ascii_path)
    binary_df, binary_format = converter.read_dtm_file_with_format(binary_path)

    pd.testing.assert_frame_equal(binary_df[converter.DTM_COLUMNS], ascii_df)
    assert ascii_format == "ASCII DTM"
    assert binary_format == "binary DTM"
    assert binary_df["string"].tolist() == [301, 301]
    assert binary_df["mesh"].tolist() == [1, 1]


def test_binary_dtm_supports_submeshes_and_embedded_coordinate_prefix(
    converter: ModuleType,
    tmp_path: Path,
) -> None:
    embedded_point = struct.pack(">iBddd", 0, 1, 10.0, 20.0, 30.0)
    embedded_prefix = b"\0" * 4 + embedded_point + b"\0" * 58 + b"END\0"
    open_metadata = b"neighbours=yes,validated=false,algorithm=retriangulation"
    path = tmp_path / "variants.dtm"
    path.write_bytes(
        binary_dtm(
            binary_dtm_block(
                7,
                binary_dtm_triangle(1, 1, 2, 3, 0, 0, 0)[:-1],
                metadata=open_metadata,
            ),
            binary_dtm_submesh(
                3,
                binary_dtm_triangle(1, 4, 5, 6, -1, -1, -1),
                metadata=open_metadata,
            ),
            binary_dtm_block(
                9,
                binary_dtm_triangle(1, 7, 8, 9, 0, 0, 0)[:-1],
                metadata=open_metadata,
                block_header=(1, 0, 2, 4),
            ),
            binary_dtm_submesh(
                6,
                binary_dtm_triangle(1, 10, 11, 12, 0, 0, 0),
                metadata=open_metadata,
            ),
            prefix=embedded_prefix,
        )
    )

    dataframe = converter.read_dtm_file(path)

    assert dataframe[["string", "mesh", "triangle_number"]].values.tolist() == [
        [7, 1, 1],
        [7, 3, 1],
        [9, 4, 1],
        [9, 6, 1],
    ]


def test_binary_dtm_builds_expected_3d_triangles(converter: ModuleType, tmp_path: Path) -> None:
    path = tmp_path / "triangles.dtm"
    path.write_bytes(
        binary_dtm(
            binary_dtm_block(
                301,
                binary_dtm_triangle(1, 1, 2, 3, -1, 2, -1),
                binary_dtm_triangle(2, 2, 4, 3, -1, -1, 1),
            )
        )
    )
    points_df = pd.DataFrame(
        {
            "point_number": [1, 2, 3, 4],
            "group": [1, 1, 1, 1],
            "string": [301, 301, 301, 301],
            "y": [0.0, 0.0, 1.0, 1.0],
            "x": [0.0, 1.0, 0.0, 1.0],
            "z": [10.0, 11.0, 12.0, 13.0],
        }
    )

    triangles_df = converter.read_dtm_file(path)
    points = converter.create_points_gdf(points_df, "EPSG:28351")
    triangles = converter.create_polygons(triangles_df, points)

    assert len(triangles) == 2
    assert triangles["string"].tolist() == [301, 301]
    assert triangles.geometry.has_z.all()
    assert list(triangles.geometry.iloc[0].exterior.coords) == [
        (0.0, 0.0, 10.0),
        (1.0, 0.0, 11.0),
        (0.0, 1.0, 12.0),
        (0.0, 0.0, 10.0),
    ]


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (binary_dtm(prefix=b"\0" * 32 + b"XEND\0"), "Unexpected binary DTM prefix"),
        (binary_dtm(final_marker=b"BADFINAL"), "missing the final marker"),
        (
            binary_dtm(binary_dtm_block(301, binary_dtm_triangle(1, 1, 2, 3, -1, -1, -1), block_header=(2, 0, 2, 1))),
            "Unsupported binary DTM block header",
        ),
        (
            binary_dtm(
                binary_dtm_block(
                    301,
                    binary_dtm_triangle(1, 1, 2, 3, -1, -1, -1),
                    metadata=b"neighbours=no",
                )
            ),
            "Unsupported binary DTM metadata",
        ),
        (
            binary_dtm(binary_dtm_block(301, binary_dtm_triangle(2, 1, 2, 3, -1, -1, -1))),
            "Malformed binary DTM triangle",
        ),
        (
            binary_dtm(binary_dtm_block(301, binary_dtm_triangle(1, 1, 2, 3, -1, -1, -1, terminator=1))),
            "Malformed binary DTM triangle",
        ),
        (
            binary_dtm(binary_dtm_block(301, binary_dtm_triangle(1, 1, 2, 3, 2, -1, -1))),
            "neighbour is out of range",
        ),
        (
            binary_dtm(
                binary_dtm_block(
                    301,
                    binary_dtm_triangle(1, 1, 2, 3, -1, 2, -1),
                    binary_dtm_triangle(2, 4, 5, 6, -1, -1, 1),
                )
            ),
            "Inconsistent binary DTM neighbour topology",
        ),
        (
            binary_dtm(
                binary_dtm_block(301, binary_dtm_triangle(1, 1, 2, 3, -1, -1, -1)),
                binary_dtm_submesh(1, binary_dtm_triangle(1, 4, 5, 6, -1, -1, -1)),
            ),
            "Unsupported binary DTM submesh header",
        ),
    ],
)
def test_invalid_binary_dtm_is_rejected_with_path_and_offset(
    converter: ModuleType,
    tmp_path: Path,
    payload: bytes,
    message: str,
) -> None:
    path = tmp_path / "invalid.dtm"
    path.write_bytes(payload)

    with pytest.raises(converter.ConverterParseError) as exc_info:
        converter.read_dtm_file(path)

    error = str(exc_info.value)
    assert message in error
    assert "byte offset" in error
    assert str(path) in error


def test_crs_definition_can_be_loaded_from_prj_file(converter: ModuleType, tmp_path: Path) -> None:
    crs_path = tmp_path / "local_grid.prj"
    crs_path.write_text('DERIVEDPROJCRS["Local Grid"]', encoding="ascii")

    assert converter.resolve_crs_definition(crs_path.name, tmp_path) == 'DERIVEDPROJCRS["Local Grid"]'


def test_geopackage_export_uses_fiona_for_wkt2_crs(
    converter: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    geodataframe = gpd.GeoDataFrame({"id": [1]}, geometry=[Point(1.0, 2.0, 3.0)], crs="EPSG:28351")
    observed: dict[str, Any] = {}

    def fake_to_file(self: gpd.GeoDataFrame, path: Path, **kwargs: Any) -> None:
        observed["self"] = self
        observed["path"] = path
        observed.update(kwargs)

    monkeypatch.setattr(gpd.GeoDataFrame, "to_file", fake_to_file)
    output_path = tmp_path / "output.gpkg"

    converter.export_to_geopackage(geodataframe, output_path)

    assert observed["self"] is geodataframe
    assert observed["path"] == output_path
    assert observed["driver"] == "GPKG"
    assert observed["engine"] == "fiona"
