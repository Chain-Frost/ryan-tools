"""A malformed completed TLF is a failure, not a normal unfinished-run skip."""

from pathlib import Path
from unittest.mock import patch

import pytest

from ryan_library.orchestrators.tuflow import tuflow_logsummary as summary

COMPLETE = """Build: 2023-03-AF
Computer Name: SyntheticHost
Simulation Started: 2026-Nov-01 14:00
No Specified Events.
No Specified Scenarios.
Initialisation Times
Clock Time: [0.01 h]
Final Times
Clock Time: [0.03 h]
Simulation FINISHED
Final Cumulative ME: 0.0%
"""
INCOMPLETE = """Build: 2023-03-AF
Computer Name: SyntheticHost
Simulation Started: 2026-Nov-01 15:00
No Specified Events.
No Specified Scenarios.
"""
MALFORMED_COMPLETED = """Simulation FINISHED
Final Cumulative ME: 0.0%
"""


def test_unreadable_nonempty_tlf_is_failed_with_path_context(tmp_path: Path) -> None:
    logfile = tmp_path / "run.tlf"
    logfile.write_text(COMPLETE, encoding="utf-8")
    with patch.object(Path, "read_bytes", side_effect=PermissionError("synthetic read denial")):
        outcome = summary.process_log_file_for_dashboard(logfile)
    assert outcome.status == "FAIL"
    assert "run.tlf" in outcome.detail
    assert "Could not read" in outcome.detail


def test_completed_footer_with_missing_header_is_failed(tmp_path: Path) -> None:
    logfile = tmp_path / "broken.tlf"
    logfile.write_text(MALFORMED_COMPLETED, encoding="utf-8")
    outcome = summary.process_log_file_for_dashboard(logfile)
    assert outcome.status == "FAIL"
    assert "broken.tlf" in outcome.detail
    assert "header metadata" in outcome.detail


def test_incomplete_tlf_is_still_an_intentional_skip(tmp_path: Path) -> None:
    logfile = tmp_path / "still-running.tlf"
    logfile.write_text(INCOMPLETE, encoding="utf-8")
    outcome = summary.process_log_file_for_dashboard(logfile)
    assert outcome.status == "SKIP"
    assert outcome.detail == "no completed run data"


def test_log_discovery_rejects_empty_matched_tlf(tmp_path: Path) -> None:
    (tmp_path / "bad.tlf").touch()
    with pytest.raises(OSError, match="bad.tlf"):
        summary.discover_log_files(root_dir=tmp_path)


@pytest.mark.parametrize("enabled", [False, True])
def test_malformed_log_sets_failed_final_outcome_on_both_display_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, enabled: bool
) -> None:
    (tmp_path / "broken.tlf").write_text(MALFORMED_COMPLETED, encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    with patch.object(summary, "calculate_pool_size", return_value=1):
        assert summary.main_processing(use_live_dashboard=enabled) is False


@pytest.mark.parametrize("enabled", [False, True])
def test_incomplete_log_skips_without_report_on_both_display_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, enabled: bool
) -> None:
    (tmp_path / "running.tlf").write_text(INCOMPLETE, encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    with patch.object(summary, "calculate_pool_size", return_value=1):
        assert summary.main_processing(use_live_dashboard=enabled) is True
    assert list(tmp_path.glob("*_ModellingLog.xlsx")) == []
