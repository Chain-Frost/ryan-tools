"""Regression tests for issue #196 master-log append outcome and save safety."""

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from openpyxl import Workbook, load_workbook

from ryan_library.functions.tuflow import logsummary_excel_append as excel_append
from ryan_library.orchestrators.tuflow import tuflow_logsummary_append as app
from ryan_library.orchestrators.tuflow.tuflow_logsummary import LogFileProcessingResult, LogSummaryStatus


def _workbook(path: Path) -> None:
    book = Workbook()
    sheet = book.active
    sheet.title = "Log Summary"
    sheet.append(["Runcode", "orig_log_path"])
    sheet.append(["existing", "existing.tlf"])
    book.save(path)
    book.close()


def _result(path: Path, status: LogSummaryStatus) -> LogFileProcessingResult:
    # The status literal is exercised by the parametrized tests below.
    df = pd.DataFrame({"Runcode": [path.stem], "orig_log_path": [str(path)]}) if status == "OK" else pd.DataFrame()
    return LogFileProcessingResult(logfile=path, data_frame=df, status=status, detail=status)


@pytest.mark.parametrize(
    ("statuses", "expected_ok", "expected_append"),
    [
        ([], True, False),
        (["SKIP"], True, False),
        (["FAIL"], False, False),
        (["OK"], True, True),
        (["OK", "FAIL"], False, True),
        (["OK", "SKIP"], False, True),
    ],
)
def test_append_reports_truthful_outcome(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    statuses: list[LogSummaryStatus],
    expected_ok: bool,
    expected_append: bool,
) -> None:
    master = tmp_path / "master.xlsx"
    _workbook(master)
    files = [tmp_path / f"run_{i}.tlf" for i in range(len(statuses))]
    monkeypatch.setattr(app, "discover_log_files", lambda **_: files)
    monkeypatch.setattr(app, "filter_new_log_files", lambda **_: files)
    monkeypatch.setattr(
        app, "process_log_files", lambda **_: [_result(p, s) for p, s in zip(files, statuses, strict=True)]
    )
    with patch.object(
        app, "append_dataframe_to_workbook_table", wraps=excel_append.append_dataframe_to_workbook_table
    ) as save:
        assert (
            app.append_to_master_log_summary(
                master_workbook_path=master, sheet_name="Log Summary", table_name=None, use_live_dashboard=False
            )
            is expected_ok
        )
    assert save.called is expected_append
    result = load_workbook(master)
    assert result["Log Summary"].max_row == (2 + statuses.count("OK") if expected_append else 2)
    result.close()


def test_missing_process_outcome_fails_without_modifying_workbook(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    master = tmp_path / "master.xlsx"
    _workbook(master)
    original = master.read_bytes()
    monkeypatch.setattr(app, "discover_log_files", lambda **_: [tmp_path / "new.tlf"])
    monkeypatch.setattr(app, "filter_new_log_files", lambda **_: [tmp_path / "new.tlf"])
    monkeypatch.setattr(app, "process_log_files", lambda **_: [])
    with pytest.raises(RuntimeError, match="outcomes returned"):
        app.append_to_master_log_summary(master_workbook_path=master, use_live_dashboard=False)
    assert master.read_bytes() == original


@pytest.mark.parametrize("failure", ["save", "replace"])
def test_failed_master_publication_preserves_workbook(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    master = tmp_path / "master.xlsx"
    _workbook(master)
    original = master.read_bytes()
    dataframe = pd.DataFrame({"Runcode": ["new"], "orig_log_path": ["new.tlf"]})
    if failure == "save":
        monkeypatch.setattr(Workbook, "save", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("disk full")))
    else:
        monkeypatch.setattr(Path, "replace", lambda *_args: (_ for _ in ()).throw(PermissionError("locked")))
    with pytest.raises(OSError):
        excel_append.append_dataframe_to_workbook_table(
            data_frame=dataframe, workbook_path=master, sheet_name="Log Summary", table_name=None
        )
    assert master.read_bytes() == original
    assert sorted(path.name for path in tmp_path.iterdir()) == ["master.xlsx"]


def test_concurrent_source_change_is_not_replaced(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    master = tmp_path / "master.xlsx"
    _workbook(master)
    original_save = Workbook.save

    def external_change_then_save(book: Workbook, filename: str | Path) -> None:
        master.write_bytes(master.read_bytes() + b"external change")
        original_save(book, filename)

    monkeypatch.setattr(Workbook, "save", external_change_then_save)
    dataframe = pd.DataFrame({"Runcode": ["new"]})
    with pytest.raises(RuntimeError, match="changed during"):
        excel_append.append_dataframe_to_workbook_table(
            data_frame=dataframe, workbook_path=master, sheet_name="Log Summary", table_name=None
        )
    assert master.read_bytes().endswith(b"external change")
    assert sorted(path.name for path in tmp_path.iterdir()) == ["master.xlsx"]


def test_macro_workbook_rejected_before_modification(tmp_path: Path) -> None:
    workbook = tmp_path / "master.xlsm"
    workbook.write_bytes(b"sentinel")
    with pytest.raises(ValueError, match="xlsx"):
        excel_append.append_dataframe_to_workbook_table(
            data_frame=pd.DataFrame({"Runcode": ["new"]}),
            workbook_path=workbook,
            sheet_name="Log Summary",
            table_name=None,
        )
    assert workbook.read_bytes() == b"sentinel"
