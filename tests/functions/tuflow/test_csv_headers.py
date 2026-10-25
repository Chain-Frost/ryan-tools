"""Header-only reads, bounded failure and named-column matching."""

from io import BytesIO
from pathlib import Path

import pytest

from ryan_library.functions.tuflow.csv_headers import CsvHeader, read_csv_header


@pytest.mark.parametrize(
    "prefix",
    [b"Time,Rain\n", b"! export\nEvent ID,123\nTime,Rain\n", b"Event ID,123\n\n! units\nTime,Rain\n"],
)
def test_header_read_stops_before_data_row(monkeypatch: pytest.MonkeyPatch, prefix: bytes) -> None:
    class HeaderOnly(BytesIO):
        def readline(self, size: int | None = -1, /) -> bytes:
            assert self.tell() < len(prefix), "Read a rainfall data row"
            return super().readline(size)

    stream = HeaderOnly(prefix + b"\xff\xff invalid data\n")

    def open_header(*_args: object, **_kwargs: object) -> HeaderOnly:
        return stream

    monkeypatch.setattr(Path, "open", open_header)
    assert read_csv_header(Path("rain.csv"), required_columns=("Time", "Rain")) == CsvHeader(("Time", "Rain"))


def test_bom_comments_quoted_and_multiline_header(tmp_path: Path) -> None:
    path = tmp_path / "rain.csv"
    path.write_bytes(b'\xef\xbb\xbf! exported\n\nTime,"Rain, A","Rain\nB"\n0,1,2\n')
    header = read_csv_header(path)
    assert header.columns == ("Time", "Rain, A", "Rain\nB")
    assert not header.problem(("time", "rain, a"))


@pytest.mark.parametrize("content", [b"", b"! comment only\n", b'"unterminated\n', b"\xff\n"])
def test_invalid_headers_fail_closed(tmp_path: Path, content: bytes) -> None:
    path = tmp_path / "rain.csv"
    path.write_bytes(content)
    assert read_csv_header(path).error


def test_size_limit_and_read_error(tmp_path: Path) -> None:
    path = tmp_path / "rain.csv"
    path.write_bytes(b"Time,Rain\n")
    assert "limit" in read_csv_header(path, maximum_bytes=4).error
    assert read_csv_header(tmp_path / "absent.csv").error


def test_missing_duplicate_and_unsupported_columns() -> None:
    header = CsvHeader(("Time", "Rain", "rain"))
    assert "missing" in header.problem(("Time", "Absent"))
    assert "duplicate" in header.problem(("Time", "Rain"))
    assert "unsupported" in header.problem(("", "Rain"))
    assert "matching PyTUFLOW" in header.problem(None)


def test_preamble_header_missing_columns_does_not_search_values(tmp_path: Path) -> None:
    path = tmp_path / "rain.csv"
    path.write_bytes(b"! export\nEvent ID,123,456\nWrongTime,Good,Other\n\xff\xff\n")
    header = read_csv_header(path, required_columns=("Time", "Good", "Absent"))
    assert header.columns == ("WrongTime", "Good", "Other")
    assert "missing" in header.problem(("Time", "Absent"))


@pytest.mark.parametrize(
    "content",
    [b"Event ID,123\n", b"Event ID,123,456\nUnknown,Other,Header\n"],
)
def test_incomplete_preamble_fails_closed(tmp_path: Path, content: bytes) -> None:
    path = tmp_path / "rain.csv"
    path.write_bytes(content)
    assert read_csv_header(path, required_columns=("Time", "Rain")).error


def test_event_id_label_alone_does_not_skip_an_actual_header(tmp_path: Path) -> None:
    path = tmp_path / "rain.csv"
    path.write_bytes(b"Event ID,Rain\n\xff\xff\n")
    assert read_csv_header(path).columns == ("Event ID", "Rain")


def test_byte_limit_includes_event_metadata_and_header(tmp_path: Path) -> None:
    path = tmp_path / "rain.csv"
    metadata = b"Event ID,123\n"
    path.write_bytes(metadata + b"Time,Rain\n")
    assert "limit" in read_csv_header(path, required_columns=("Time", "Rain"), maximum_bytes=len(metadata) + 2).error


@pytest.mark.parametrize("header_line", [3, 5, 6])
def test_five_physical_line_boundary(tmp_path: Path, header_line: int) -> None:
    path = tmp_path / "rain.csv"
    path.write_text("! comment\n" + "metadata,information\n" * (header_line - 2) + "Time,Rain\n", encoding="utf-8")
    header = read_csv_header(path, required_columns=("Time", "Rain"))
    if header_line <= 5:
        assert header.columns == ("Time", "Rain")
    else:
        assert "5 physical lines" in header.error


def test_does_not_read_a_sixth_line(monkeypatch: pytest.MonkeyPatch) -> None:
    prefix = b"metadata,information\n" * 5

    class FiveLines(BytesIO):
        def readline(self, size: int | None = -1, /) -> bytes:
            assert self.tell() < len(prefix), "Attempted to read line six"
            return super().readline(size)

    stream = FiveLines(prefix + b"Time,Rain\n")

    def open_header(*_args: object, **_kwargs: object) -> FiveLines:
        return stream

    monkeypatch.setattr(Path, "open", open_header)
    assert "5 physical lines" in read_csv_header(Path("rain.csv"), required_columns=("Time", "Rain")).error
