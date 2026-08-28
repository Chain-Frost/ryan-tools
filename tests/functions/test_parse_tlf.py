"""Unit tests for ryan_library.functions.parse_tlf."""

from typing import Any
from unittest.mock import MagicMock, patch
from pathlib import Path
from datetime import datetime
from ryan_library.classes.tuflow_string_classes import TuflowStringParser
from ryan_library.functions.parse_tlf import (
    get_log_lines,
    is_complete_tlf,
    process_top_lines,
    search_from_top,
    search_for_completion,
    finalise_data,
)


class TestParseTlf:
    """Tests for parse_tlf module."""

    def test_search_from_top_build(self) -> None:
        """Test parsing build version."""
        line = "Build: 2023-03-AA"
        data_dict: dict[str, Any] = {}
        data_dict, _, _, _, _ = search_from_top(line, data_dict, 0, False, False, False)
        assert data_dict["TUFLOW_version"] == "2023-03-AA"

    def test_search_from_top_start_date(self) -> None:
        """Test parsing start date."""
        line = "Simulation Started : 2023-Jan-01 12:00"
        data_dict: dict[str, Any] = {}
        data_dict, success, _, _, _ = search_from_top(line, data_dict, 0, False, False, False)
        assert data_dict["StartDate"] == datetime(2023, 1, 1, 12, 0)
        assert success == 1

    def test_search_from_top_extracts_only_a_username_below_log_folder(self) -> None:
        """Do not report the generic legacy log folder itself as a username."""
        modern_data: dict[str, Any] = {}
        modern_line = (
            r"Simulations Log Folder == C:\ProgramData\TUFLOW\log\Boon Eow\  "
            r"! Path or URL to global simulations log folder."
        )
        modern_data, _, _, _, _ = search_from_top(modern_line, modern_data, 0, False, False, False)

        legacy_data: dict[str, Any] = {}
        legacy_line = (
            r"Simulations Log Folder == C:\BMT_WBM\log  "
            r'! Path or URL to global simulations log folder. Default is "C:\BMT_WBM\log"'
        )
        legacy_data, _, _, _, _ = search_from_top(legacy_line, legacy_data, 0, False, False, False)

        assert modern_data["username"] == "Boon Eow"
        assert "username" not in legacy_data

    def test_search_for_completion_finished(self) -> None:
        """Test parsing simulation finished status."""
        line = "Simulation FINISHED"
        data_dict: dict[str, str | float] = {}
        data_dict, sim_complete, _ = search_for_completion(line, data_dict, 0, None)
        assert sim_complete == 1
        assert data_dict["EndStatus"] == "Simulation FINISHED"

    def test_search_for_completion_times(self) -> None:
        """Test parsing model times."""
        line = "End Time (h): 24.0"
        data_dict: dict[str, str | float] = {}
        data_dict, _, _ = search_for_completion(line, data_dict, 0, None)
        assert data_dict["Model_End_Time"] == 24.0

    def test_search_for_completion_legacy_2016_summary(self) -> None:
        """Treat the 2016 SIMULATION SUMMARY footer as completed run data."""
        lines = [
            "SIMULATION SUMMARY",
            "CPU Time:        1:00:43  [1.012 h]",
            "Clock Time:      1:01:01  [1.017 h]",
            "Simulation FINISHED",
        ]
        data_dict: dict[str, str | float] = {}
        sim_complete: int = 0
        current_section: str | None = None

        for line in lines:
            data_dict, sim_complete, current_section = search_for_completion(
                line, data_dict, sim_complete, current_section
            )

        assert sim_complete == 1
        assert data_dict["Final_CPU_Time"] == 1.012
        assert data_dict["Final_RunTime"] == 1.017
        assert is_complete_tlf(data_dict, sim_complete)

    def test_search_for_completion_modern_summary(self) -> None:
        """Keep the current Final Times footer as the primary supported format."""
        lines = [
            "Final Times",
            "Processor Time:  [0.875 h]",
            "Clock Time:      [1.250 h]",
            "Simulation FINISHED",
        ]
        data_dict: dict[str, str | float] = {}
        sim_complete: int = 0
        current_section: str | None = None

        for line in lines:
            data_dict, sim_complete, current_section = search_for_completion(
                line, data_dict, sim_complete, current_section
            )

        assert sim_complete == 1
        assert data_dict["Final_CPU_Time"] == 0.875
        assert data_dict["Final_RunTime"] == 1.25
        assert is_complete_tlf(data_dict, sim_complete)

    def test_get_log_lines_preserves_current_utf8(self, tmp_path: Path) -> None:
        """Prefer UTF-8 and preserve current log text without fallback decoding."""
        log_path = tmp_path / "current.tlf"
        log_path.write_text("Model path: 流域\nSimulation FINISHED\n", encoding="utf-8")

        lines, last_lines = get_log_lines(log_path, is_large_file=False)

        assert lines == ["Model path: 流域", "Simulation FINISHED"]
        assert last_lines == lines

    def test_get_log_lines_reads_legacy_windows_1252(self, tmp_path: Path) -> None:
        """Read Windows-1252 units emitted by older TUFLOW builds."""
        log_path = tmp_path / "legacy.tlf"
        log_path.write_bytes("Flow rate (m\N{SUPERSCRIPT THREE})\nSimulation FINISHED\n".encode("cp1252"))

        lines, last_lines = get_log_lines(log_path, is_large_file=False)

        assert lines == ["Flow rate (m\N{SUPERSCRIPT THREE})", "Simulation FINISHED"]
        assert last_lines == lines

    def test_process_top_lines_recovers_legacy_initialisation_times(self, tmp_path: Path) -> None:
        """Use the timing pair before the first 2016 GPU solver output as initialisation time."""
        log_path = tmp_path / "legacy.tlf"
        lines = [
            "CPU Time:        0:01:44  [0.02914 h]",
            "Clock Time:      0:01:47  [0.02972 h]",
            "Writing GPU Output at: 0:00:00  Clock Time: 0:01:47  CPU Time: 0:01:45",
        ]
        data_dict: dict[str, Any] = {"TUFLOW_version": "2016-03-AE-iSP-w64"}

        data_dict, _, _, _, _ = process_top_lines(
            logfile_path=log_path,
            lines=lines,
            data_dict=data_dict,
            success=0,
            spec_events=False,
            spec_scen=False,
            spec_var=False,
            is_large_file=False,
            runcode="legacy",
            relative_logfile_path=log_path,
        )

        assert data_dict["Initialise_CPU_Time"] == 0.02914
        assert data_dict["Initialise_RunTime"] == 0.02972

    def test_embedded_run_code_number_is_not_a_duration(self) -> None:
        """Do not interpret Q1006H as a 1006-hour duration token."""
        embedded = TuflowStringParser(file_path=Path("SIM2_Q1006H_ROG.tlf"))
        explicit = TuflowStringParser(file_path=Path("M01_12hr.tlf"))

        assert embedded.duration is None
        assert explicit.duration is not None
        assert explicit.duration.numeric_value == 720

    @patch("ryan_library.functions.parse_tlf.TuflowStringParser")
    def test_finalise_data(self, mock_parser_cls: MagicMock) -> None:
        """Test data finalisation."""
        mock_parser = MagicMock()
        mock_parser.clean_run_code = "Run_001"
        mock_parser.trim_run_code = "Run"
        mock_parser.run_code_parts = {"Part": "1"}
        mock_parser.tp.numeric_value = 1.0
        mock_parser.duration.numeric_value = 2.0
        mock_parser.aep.numeric_value = 1.0
        mock_parser_cls.return_value = mock_parser

        data_dict: dict[str, Any] = {"SomeKey": "Value"}
        df = finalise_data("Run_001", data_dict)

        assert not df.empty
        assert df.iloc[0]["Runcode"] == "Run_001"
        assert df.iloc[0]["SomeKey"] == "Value"
