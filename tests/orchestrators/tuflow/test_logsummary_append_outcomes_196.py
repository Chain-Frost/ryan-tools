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
    assert sheet is not None
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
def test_master_claim_is_held_before_workbook_discovery(tmp_path: Path) -> None:
    master = tmp_path / "master.xlsx"
    _workbook(master)
    claim = master.with_name(f".{master.name}.writing")
    original = excel_append.load_existing_log_summary_rows

    def probe(**kwargs: object) -> excel_append.ExistingLogSummaryRows:
        assert claim.exists()
        with pytest.raises(FileExistsError, match="already being updated"):
            with excel_append.master_workbook_claim(workbook_path=master):
                pass
        return original(**kwargs)  # pyright: ignore[reportArgumentType]

    with (
        patch.object(app, "load_existing_log_summary_rows", side_effect=probe),
        patch.object(app, "discover_log_files", return_value=[]),
    ):
        assert app.append_to_master_log_summary(master_workbook_path=master, use_live_dashboard=False)

    assert not claim.exists()


def test_append_reports_truthful_outcome(
    tmp_path: Path,
    statuses: list[LogSummaryStatus],
    expected_ok: bool,
    expected_append: bool,
) -> None:
    master = tmp_path / "master.xlsx"
    _workbook(master)
    files = [tmp_path / f"run_{i}.tlf" for i in range(len(statuses))]
    with (
        patch.object(app, "discover_log_files", return_value=files),
        patch.object(app, "filter_new_log_files", return_value=files),
        patch.object(
            app,
            "process_log_files",
            return_value=[_result(p, s) for p, s in zip(files, statuses, strict=True)],
        ),
        patch.object(
            app, "append_dataframe_to_workbook_table", wraps=excel_append.append_dataframe_to_workbook_table
        ) as save,
    ):
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


def test_missing_process_outcome_fails_without_modifying_workbook(tmp_path: Path) -> None:
    master = tmp_path / "master.xlsx"
    _workbook(master)
    original = master.read_bytes()
    with (
        patch.object(app, "discover_log_files", return_value=[tmp_path / "new.tlf"]),
        patch.object(app, "filter_new_log_files", return_value=[tmp_path / "new.tlf"]),
        patch.object(app, "process_log_files", return_value=[]),
        pytest.raises(RuntimeError, match="outcomes returned"),
    ):
        app.append_to_master_log_summary(master_workbook_path=master, use_live_dashboard=False)
    assert master.read_bytes() == original


@pytest.mark.parametrize("failure", ["save", "replace"])
def test_failed_master_publication_preserves_workbook(tmp_path: Path, failure: str) -> None:
    master = tmp_path / "master.xlsx"
    _workbook(master)
    original = master.read_bytes()
    dataframe = pd.DataFrame({"Runcode": ["new"], "orig_log_path": ["new.tlf"]})
    if failure == "save":
        with patch.object(Workbook, "save", side_effect=OSError("disk full")), pytest.raises(OSError):
            excel_append.append_dataframe_to_workbook_table(
                data_frame=dataframe, workbook_path=master, sheet_name="Log Summary", table_name=None
            )
    else:
        with patch.object(Path, "replace", side_effect=PermissionError("locked")), pytest.raises(OSError):
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


def test_master_symlink_is_rejected_without_replacing_target(tmp_path: Path) -> None:
    master = tmp_path / "original.xlsx"
    alias = tmp_path / "alias.xlsx"
    _workbook(master)
    original = master.read_bytes()
    try:
        alias.symlink_to(master.name)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"File symlink creation unavailable: {exc}")
    with pytest.raises(ValueError, match="symbolic link"):
        excel_append.load_existing_log_summary_rows(workbook_path=alias, sheet_name="Log Summary", table_name=None)
    with pytest.raises(ValueError, match="symbolic link"):
        excel_append.append_dataframe_to_workbook_table(
            data_frame=pd.DataFrame({"Runcode": ["new"]}),
            workbook_path=alias,
            sheet_name="Log Summary",
            table_name=None,
        )
    assert alias.is_symlink()
    assert master.read_bytes() == original
    assert sorted(p.name for p in tmp_path.iterdir()) == ["alias.xlsx", "original.xlsx"]


def test_master_hardlink_is_rejected_without_severing_alias(tmp_path: Path) -> None:
    master = tmp_path / "original.xlsx"
    alias = tmp_path / "alias.xlsx"
    _workbook(master)
    original = master.read_bytes()
    try:
        alias.hardlink_to(master)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"File hardlink creation unavailable: {exc}")
    with pytest.raises(ValueError, match="Hard-linked"):
        excel_append.load_existing_log_summary_rows(workbook_path=master, sheet_name="Log Summary", table_name=None)
    with pytest.raises(ValueError, match="Hard-linked"):
        excel_append.append_dataframe_to_workbook_table(
            data_frame=pd.DataFrame({"Runcode": ["new"]}),
            workbook_path=master,
            sheet_name="Log Summary",
            table_name=None,
        )
    assert master.read_bytes() == original
    assert alias.read_bytes() == original


def test_hardlink_created_during_staging_prevents_publication(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    master = tmp_path / "original.xlsx"
    alias = tmp_path / "alias.xlsx"
    _workbook(master)
    original = master.read_bytes()
    original_save = Workbook.save

    def create_alias_and_save(workbook: Workbook, filename: str | Path) -> None:
        alias.hardlink_to(master)
        original_save(workbook, filename)

    monkeypatch.setattr(Workbook, "save", create_alias_and_save)
    with pytest.raises(ValueError, match="Hard-linked"):
        excel_append.append_dataframe_to_workbook_table(
            data_frame=pd.DataFrame({"Runcode": ["new"]}),
            workbook_path=master,
            sheet_name="Log Summary",
            table_name=None,
        )
    assert master.read_bytes() == original
    assert alias.read_bytes() == original
    assert sorted(p.name for p in tmp_path.iterdir()) == ["alias.xlsx", "original.xlsx"]
