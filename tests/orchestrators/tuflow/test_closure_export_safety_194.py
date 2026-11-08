# pyright: reportPrivateUsage=false
"""Closure raw tables and formatted reports form one protected export batch."""

import os
import shutil
import subprocess
import sys
from contextlib import closing, nullcontext
from datetime import datetime, tzinfo
from pathlib import Path
from typing import Literal
from unittest.mock import Mock

import pandas as pd
import pytest
from openpyxl import load_workbook

from ryan_library.functions import excel_export as exports
from ryan_library.orchestrators.tuflow import closure_durations as workflow
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection

STAMP = "20261107-1234"
RAW_PARQUET = f"{STAMP}_durex.parquet.gzip"
RAW_CSV = f"{STAMP}_durex.csv"
# Preserve the existing formatted-report names, including their two timestamp prefixes.
REPORT_STEM = f"{STAMP}_{STAMP}_closure_durations"
WORKBOOK = f"{REPORT_STEM}.xlsx"
DURATION_REPORT = f"{REPORT_STEM}_Duration_Exceedance.parquet.gzip"
SUMMARY_REPORT = f"{REPORT_STEM}_Summary.parquet.gzip"


class _FixedDateTime(datetime):
    @classmethod
    def now(cls, tz: tzinfo | None = None) -> _FixedDateTime:
        return cls(2026, 11, 7, 12, 34, tzinfo=tz)


@pytest.fixture(autouse=True)
def export_directory_and_time(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(exports, "datetime", _FixedDateTime)
    monkeypatch.setattr(workflow, "datetime", _FixedDateTime)


@pytest.fixture
def durations() -> pd.DataFrame:
    return pd.DataFrame(
        {"Location": ["L1", "L2"], "Duration_Exceeding": [2.0, 3.0]},
        index=pd.Index([11, 22], name="event_index"),
    )


@pytest.fixture
def summary() -> pd.DataFrame:
    return pd.DataFrame({"Location": ["L1", "L2"], "Central_Value": [2.0, 3.0]})


@pytest.mark.parametrize("mode", ["excel", "parquet", "both"])
def test_raw_tables_retained_in_every_mode_with_selected_report_formats(
    tmp_path: Path, durations: pd.DataFrame, summary: pd.DataFrame, mode: Literal["excel", "parquet", "both"]
) -> None:
    workflow._export_closure_duration_artifacts(
        durations_df=durations, summary_df=summary, timestamp=STAMP, export_mode=mode
    )

    expected = {RAW_PARQUET, RAW_CSV}
    if mode != "parquet":
        expected.add(WORKBOOK)
        with closing(load_workbook(tmp_path / WORKBOOK, read_only=True, data_only=True)) as workbook:
            assert list(workbook["Summary"].values) == [
                tuple(summary.columns),
                *summary.itertuples(index=False, name=None),
            ]
    if mode != "excel":
        expected.update((DURATION_REPORT, SUMMARY_REPORT))
        pd.testing.assert_frame_equal(pd.read_parquet(tmp_path / DURATION_REPORT), durations.reset_index(drop=True))
        pd.testing.assert_frame_equal(pd.read_parquet(tmp_path / SUMMARY_REPORT), summary)
    assert {path.name for path in tmp_path.iterdir()} == expected
    pd.testing.assert_frame_equal(pd.read_parquet(tmp_path / RAW_PARQUET), durations)
    pd.testing.assert_frame_equal(pd.read_csv(tmp_path / RAW_CSV), durations.reset_index(drop=True))


@pytest.mark.parametrize("existing_name", [RAW_PARQUET, RAW_CSV, WORKBOOK, DURATION_REPORT, SUMMARY_REPORT])
def test_any_existing_raw_or_report_destination_prevents_all_writes(
    tmp_path: Path, durations: pd.DataFrame, summary: pd.DataFrame, existing_name: str
) -> None:
    previous = tmp_path / existing_name
    previous.write_bytes(b"previous report")
    with pytest.raises(FileExistsError):
        workflow._export_closure_duration_artifacts(
            durations_df=durations, summary_df=summary, timestamp=STAMP, export_mode="both"
        )
    assert {path.name: path.read_bytes() for path in tmp_path.iterdir()} == {existing_name: b"previous report"}


@pytest.mark.parametrize("writer", ["to_parquet", "to_csv", "to_excel"])
def test_any_raw_or_report_writer_failure_discards_entire_staged_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, durations: pd.DataFrame, summary: pd.DataFrame, writer: str
) -> None:
    def fail(*_args: object, **_kwargs: object) -> None:
        message = "synthetic closure writer failure"
        raise PermissionError(message)

    monkeypatch.setattr(pd.DataFrame, writer, fail)
    with pytest.raises(PermissionError, match="synthetic closure writer failure"):
        workflow._export_closure_duration_artifacts(
            durations_df=durations, summary_df=summary, timestamp=STAMP, export_mode="both"
        )
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("failed_link", [2, 3])
def test_partial_publication_identifies_raw_tables_and_preserves_racing_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, durations: pd.DataFrame, summary: pd.DataFrame, failed_link: int
) -> None:
    original_link = os.link
    destinations: list[Path] = []

    def race(source: Path, destination: Path) -> None:
        destinations.append(destination)
        if len(destinations) == failed_link:
            destination.write_bytes(b"concurrent report")
        original_link(source, destination)

    monkeypatch.setattr(exports.os, "link", race)
    with pytest.raises(FileExistsError) as caught:
        workflow._export_closure_duration_artifacts(
            durations_df=durations, summary_df=summary, timestamp=STAMP, export_mode="both"
        )
    assert destinations[-1].read_bytes() == b"concurrent report"
    assert {path.name for path in tmp_path.iterdir()} == {path.name for path in destinations}
    notes = " ".join(caught.value.__notes__)
    assert "already published" in notes
    for published in destinations[:-1]:
        assert str(published) in notes
    pd.testing.assert_frame_equal(pd.read_parquet(tmp_path / RAW_PARQUET), durations)


@pytest.fixture
def synthetic_pipeline(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub discovery only; retain real duration calculation, summary and file writers."""
    collection = ProcessorCollection()
    collection.processors = [
        Mock(
            data_type="PO",
            df=pd.DataFrame(
                {
                    "Type": ["Flow"] * 3,
                    "Time": [0.0, 1.0, 2.0],
                    "Value": [0.0, 2.0, 4.0],
                    "Location": ["L1"] * 3,
                    "aep_text": ["1%"] * 3,
                    "duration_text": ["0060m"] * 3,
                    "tp_text": ["01"] * 3,
                    "directory_path": ["inputs"] * 3,
                    "trim_runcode": ["synthetic"] * 3,
                }
            ),
        )
    ]
    monkeypatch.setattr(workflow, "bulk_read_and_merge_tuflow_csv", Mock(return_value=collection))
    monkeypatch.setattr(workflow, "setup_logger", Mock(return_value=nullcontext(None)))


@pytest.mark.usefixtures("synthetic_pipeline")
def test_real_closure_calculation_exports_matching_raw_and_summary_values(tmp_path: Path) -> None:
    workflow.run_closure_durations(paths=[tmp_path], thresholds=[1.0])
    raw = pd.read_parquet(tmp_path / RAW_PARQUET)
    assert raw["Duration_Exceeding"].tolist() == [2.0]
    with closing(load_workbook(tmp_path / WORKBOOK, read_only=True, data_only=True)) as workbook:
        rows = list(workbook["Summary"].values)
        assert [row[rows[0].index("Central_Value")] for row in rows[1:]] == [2.0]
    assert pd.read_csv(tmp_path / RAW_CSV)["Duration_Exceeding"].tolist() == [2.0]


@pytest.mark.usefixtures("synthetic_pipeline")
def test_failed_summary_does_not_publish_raw_tables(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(workflow, "summarise_results", Mock(side_effect=ValueError("synthetic summary failure")))
    with pytest.raises(ValueError, match="synthetic summary failure"):
        workflow.run_closure_durations(paths=[tmp_path], thresholds=[1.0])
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("failure", ["raw_collision", "report_collision", "csv_writer"])
def test_copied_wrapper_exits_one_without_partial_outputs(tmp_path: Path, failure: str) -> None:
    """Run the real PO reader and copied CLI from a disposable source-tree test directory."""
    repository = Path(__file__).absolute().parents[3]
    script = tmp_path / "calculate_po_closure_durations.py"
    shutil.copy2(repository / "ryan-scripts/TUFLOW-python/po_and_timeseries" / script.name, script)
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    (inputs / "Synthetic_01.00p_00120m_TP01_PO.csv").write_text(
        "Descriptor,Time,Flow\nLocation,Time,C01\nrow,0,2\nrow,1,4\n", encoding="utf-8"
    )
    existing_name = RAW_CSV if failure == "raw_collision" else WORKBOOK
    if failure != "csv_writer":
        (tmp_path / existing_name).write_bytes(b"previous report")
    launcher = tmp_path / "export_probe.py"
    launcher.write_text(
        "from datetime import datetime\nfrom unittest.mock import Mock\nimport runpy\n"
        "import pandas as pd\n"
        "from ryan_library.functions import excel_export\n"
        "from ryan_library.orchestrators.tuflow import closure_durations\n"
        "def fail(*args, **kwargs):\n    raise PermissionError('synthetic closure CSV denial')\n"
        "if __name__ == '__main__':\n"
        "    clock = Mock(wraps=datetime)\n"
        "    clock.now.return_value = datetime(2026, 11, 7, 12, 34)\n"
        "    excel_export.datetime = clock\n    closure_durations.datetime = clock\n"
        + ("    pd.DataFrame.to_csv = fail\n" if failure == "csv_writer" else "")
        + f"    runpy.run_path({str(script)!r}, run_name='__main__')\n",
        encoding="utf-8",
    )
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(filter(None, (str(repository), environment.get("PYTHONPATH", ""))))
    result = subprocess.run(
        [sys.executable, str(launcher), "--working-directory", str(tmp_path), "--export-mode", "excel", "--no-pause"],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdin=subprocess.DEVNULL,
        timeout=60,
        check=False,
    )
    output = result.stdout + result.stderr
    assert result.returncode == 1, output[-3000:]
    assert ("synthetic closure CSV denial" if failure == "csv_writer" else "Report already exists") in output
    assert "Closure duration export complete" not in output
    reports = {path.name: path.read_bytes() for path in tmp_path.glob(f"{STAMP}*")}
    assert reports == ({} if failure == "csv_writer" else {existing_name: b"previous report"})
    assert not list(tmp_path.glob(".ryan-export-*"))
