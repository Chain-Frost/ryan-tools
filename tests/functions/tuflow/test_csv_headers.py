"""Header-only reads, bounded failure and named-column matching."""

from io import BytesIO
from pathlib import Path

import pytest

from ryan_library.functions.tuflow.csv_headers import CsvHeader, read_csv_header


def test_header_read_stops_before_data_row(monkeypatch: pytest.MonkeyPatch) -> None:
    class HeaderOnly(BytesIO):
        def readline(self, size: int | None = -1, /) -> bytes:
            assert self.tell() < len(b"Time,Rain\n"), "Read a rainfall data row"
            return super().readline(size)

    stream = HeaderOnly(b"Time,Rain\n\xff\xff invalid data\n")

    def open_header(*_args: object, **_kwargs: object) -> HeaderOnly:
        return stream

    monkeypatch.setattr(Path, "open", open_header)
    assert read_csv_header(Path("rain.csv")) == CsvHeader(("Time", "Rain"))


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
