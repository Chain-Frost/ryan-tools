"""Report publication must preserve earlier data and propagate incomplete exports."""

import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import BinaryIO, Literal, TextIO
from unittest.mock import patch

import pandas as pd
import pytest

from ryan_library.functions import excel_export as exports


@pytest.mark.parametrize("mode", ["excel", "parquet", "both"])
def test_existing_destination_preserved(tmp_path: Path, mode: Literal["excel", "parquet", "both"]) -> None:
    exporter = exports.ExcelExporter()
    frame = pd.DataFrame({"value": [1]})
    exporter.save_to_excel(frame, file_name="report", output_directory=tmp_path, export_mode=mode)
    before = {path.name: path.read_bytes() for path in tmp_path.iterdir()}
    with pytest.raises(FileExistsError):
        exporter.save_to_excel(frame * 2, file_name="report", output_directory=tmp_path, export_mode=mode)
    assert {path.name: path.read_bytes() for path in tmp_path.iterdir()} == before


@pytest.mark.parametrize("fallback", [False, True])
@pytest.mark.parametrize("sheets", [("A/B", "A B"), ("a", "A")])
def test_sheet_destination_collision_precedes_any_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fallback: bool, sheets: tuple[str, str]
) -> None:
    if fallback:
        monkeypatch.setattr(exports.ExcelExporter, "MAX_EXCEL_ROWS", 1)
    with pytest.raises(ValueError, match=r"collide|Duplicate sheet"):
        exports.ExcelExporter().export_dataframes(
            {"report": {"dataframes": [pd.DataFrame({"v": [1]}), pd.DataFrame({"v": [2]})], "sheets": list(sheets)}},
            output_directory=tmp_path / "new",
            export_mode="excel" if fallback else "parquet",
        )
    assert not (tmp_path / "new").exists()


@pytest.mark.parametrize("failure", [ImportError, ValueError, PermissionError])
@pytest.mark.parametrize("mode", ["parquet", "both", "fallback"])
def test_parquet_failure_leaves_no_reports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: type[Exception], mode: str
) -> None:
    def fail(*_args: object, **_kwargs: object) -> None:
        msg = "synthetic parquet write failure"
        raise failure(msg)

    monkeypatch.setattr(pd.DataFrame, "to_parquet", fail)
    if mode == "fallback":
        monkeypatch.setattr(exports.ExcelExporter, "MAX_EXCEL_ROWS", 1)
    with pytest.raises(failure, match="synthetic parquet"):
        exports.ExcelExporter().save_to_excel(
            pd.DataFrame({"value": [1]}),
            file_name="report",
            output_directory=tmp_path,
            export_mode="parquet" if mode == "parquet" else "excel" if mode == "fallback" else "both",
        )
    outputs = list(tmp_path.iterdir())
    if mode == "both":
        assert len(outputs) == 1
        assert outputs[0].name == "report.xlsx"
    else:
        assert outputs == []


def test_excel_writer_failure_removes_partial_output(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(*_args: object, **_kwargs: object) -> None:
        msg = "synthetic Excel failure"
        raise OSError(msg)

    monkeypatch.setattr(pd.DataFrame, "to_excel", fail)
    with pytest.raises(OSError, match="synthetic Excel failure"):
        exports.ExcelExporter().save_to_excel(pd.DataFrame({"v": [1]}), output_directory=tmp_path)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("failed_link", [1, 2])
def test_concurrent_destination_is_retained_and_completed_reports_identified(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failed_link: int
) -> None:
    original_open = Path.open
    count = 0

    def race(
        path: Path,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> TextIO | BinaryIO:
        nonlocal count
        if mode == "xb" and path.parent == tmp_path:
            count += 1
            if count == failed_link:
                path.write_bytes(b"concurrent owner")
        return original_open(path, mode, buffering, encoding, errors, newline)

    monkeypatch.setattr(Path, "open", race)
    with pytest.raises(FileExistsError) as caught:
        exports.ExcelExporter().save_to_excel(
            pd.DataFrame({"value": [1]}),
            file_name="report",
            output_directory=tmp_path,
            export_mode="both",
        )
    outputs = list(tmp_path.iterdir())
    assert len(outputs) == failed_link
    assert any(path.read_bytes() == b"concurrent owner" for path in outputs)
    if failed_link == 2:
        assert "report.xlsx" in " ".join(caught.value.__notes__)
        assert "already published" in " ".join(caught.value.__notes__)
    assert not any(path.is_dir() for path in outputs)


def test_batch_validation_precedes_first_workbook(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="number of dataframes"):
        exports.ExcelExporter().export_dataframes(
            {
                "first": {"dataframes": [pd.DataFrame({"v": [1]})], "sheets": ["Data"]},
                "second": {"dataframes": [], "sheets": ["Missing"]},
            },
            output_directory=tmp_path,
        )
    assert list(tmp_path.iterdir()) == []


def test_raw_and_report_destinations_share_collision_preflight(tmp_path: Path) -> None:
    frame = pd.DataFrame({"value": [1]})
    output_directory = tmp_path / "new"
    with pytest.raises(ValueError, match="destinations collide"):
        exports.ExcelExporter().export_dataframes(
            {"report": {"dataframes": [frame], "sheets": ["Summary"]}},
            file_name="report",
            output_directory=output_directory,
            export_mode="parquet",
            raw_exports=[exports.RawTableExport("report_Summary.parquet.gzip", frame, "parquet")],
        )
    assert not output_directory.exists()


def test_case_variant_existing_output_is_preserved(tmp_path: Path) -> None:
    output = tmp_path / "REPORT.XLSX"
    output.write_bytes(b"existing")
    with pytest.raises(FileExistsError):
        exports.ExcelExporter().save_to_excel(pd.DataFrame({"v": [1]}), file_name="report", output_directory=tmp_path)
    assert output.read_bytes() == b"existing"


def test_generated_names_refuse_same_minute_collision(tmp_path: Path) -> None:
    exporter = exports.ExcelExporter()
    frame = pd.DataFrame({"v": [1]})
    with patch.object(exports, "datetime") as clock:
        clock.now.return_value = datetime(2026, 11, 4, 12, 0)
        exporter.save_to_excel(frame, output_directory=tmp_path)
        with pytest.raises(FileExistsError):
            exporter.save_to_excel(frame, output_directory=tmp_path)
    assert len(list(tmp_path.iterdir())) == 1


@pytest.mark.parametrize("interrupted", [False, True])
def test_csv_failure_retains_completed_parquet(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, interrupted: bool
) -> None:
    def fail(*_args: object, **_kwargs: object) -> None:
        message = "synthetic CSV failure"
        if interrupted:
            raise KeyboardInterrupt(message)
        raise PermissionError(message)

    monkeypatch.setattr(exports.ExcelExporter, "MAX_EXCEL_ROWS", 1)
    monkeypatch.setattr(pd.DataFrame, "to_csv", fail)
    with pytest.raises(KeyboardInterrupt if interrupted else PermissionError):
        exports.ExcelExporter().save_to_excel(pd.DataFrame({"v": [1]}), output_directory=tmp_path)
    outputs = list(tmp_path.iterdir())
    assert len(outputs) == 1
    assert outputs[0].suffix == ".gzip"


def test_direct_export_does_not_require_hard_links(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(*_args: object, **_kwargs: object) -> None:
        message = "hard links unsupported"
        raise OSError(message)

    monkeypatch.setattr(exports.os, "link", fail)
    exports.ExcelExporter().save_to_excel(pd.DataFrame({"v": [1]}), file_name="report", output_directory=tmp_path)
    assert [path.name for path in tmp_path.iterdir()] == ["report.xlsx"]
    assert pd.read_excel(tmp_path / "report.xlsx")["v"].tolist() == [1]


def test_explicit_extension_spelling_preserved(tmp_path: Path) -> None:
    exports.ExcelExporter().save_to_excel(pd.DataFrame({"v": [1]}), file_name="REPORT.XLSX", output_directory=tmp_path)
    assert [path.name for path in tmp_path.iterdir()] == ["REPORT.XLSX"]


def test_linked_parent_aliases_are_detected_before_writing(tmp_path: Path) -> None:
    shared = tmp_path / "shared"
    shared.mkdir()
    aliases = [tmp_path / "20261104-1200_left", tmp_path / "20261104-1200_right"]
    try:
        for alias in aliases:
            if os.name == "nt":
                subprocess.run(
                    [os.environ["COMSPEC"], "/c", "mklink", "/J", str(alias), str(shared)],
                    check=True,
                    capture_output=True,
                )
            else:
                alias.symlink_to(shared, target_is_directory=True)
        with patch.object(exports, "datetime") as clock:
            clock.now.return_value = datetime(2026, 11, 4, 12, 0)
            with pytest.raises(ValueError, match="destinations collide"):
                exports.ExcelExporter().export_dataframes(
                    {
                        key: {"dataframes": [pd.DataFrame({"v": [1]})], "sheets": ["Data"]}
                        for key in ("left/report", "right/report")
                    },
                    output_directory=tmp_path,
                )
        assert list(shared.iterdir()) == []
    finally:
        # Remove only the disposable links, never their shared target recursively.
        for alias in aliases:
            if alias.exists():
                if os.name == "nt":
                    alias.rmdir()
                else:
                    alias.unlink()
