"""Unit tests for ryan_library.functions.parse_tlf."""

import pytest
from unittest.mock import MagicMock, patch, mock_open
from pathlib import Path
from datetime import datetime
from ryan_library.functions.parse_tlf import (
    search_from_top,
    search_for_completion,
    process_top_lines,
    finalise_data,
)


class TestParseTlf:
    """Tests for parse_tlf module."""

    def test_search_from_top_build(self):
        """Test parsing build version."""
        line = "Build: 2023-03-AA"
        data_dict = {}
        data_dict, success, _, _, _ = search_from_top(line, data_dict, 0, False, False, False)
        assert data_dict["TUFLOW_version"] == "2023-03-AA"

    def test_search_from_top_start_date(self):
        """Test parsing start date."""
        line = "Simulation Started : 2023-Jan-01 12:00"
        data_dict = {}
        data_dict, success, _, _, _ = search_from_top(line, data_dict, 0, False, False, False)
        assert data_dict["StartDate"] == datetime(2023, 1, 1, 12, 0)
        assert success == 1

    def test_search_for_completion_finished(self):
        """Test parsing simulation finished status."""
        line = "Simulation FINISHED"
        data_dict = {}
        data_dict, sim_complete, _ = search_for_completion(line, data_dict, 0, None)
        assert sim_complete == 1
        assert data_dict["EndStatus"] == "Simulation FINISHED"

    def test_search_for_completion_times(self):
        """Test parsing model times."""
        line = "End Time (h): 24.0"
        data_dict = {}
        data_dict, _, _ = search_for_completion(line, data_dict, 0, None)
        assert data_dict["Model_End_Time"] == 24.0

    @patch("ryan_library.functions.parse_tlf.TuflowStringParser")
    def test_finalise_data(self, mock_parser_cls):
        """Test data finalisation."""
        mock_parser = MagicMock()
        mock_parser.clean_run_code = "Run_001"
        mock_parser.trim_run_code = "Run"
        mock_parser.run_code_parts = {"Part": "1"}
        mock_parser.tp.numeric_value = 1.0
        mock_parser.duration.numeric_value = 2.0
        mock_parser.aep.numeric_value = 1.0
        mock_parser_cls.return_value = mock_parser
        
        data_dict = {"SomeKey": "Value"}
        df = finalise_data("Run_001", data_dict)
        
        assert not df.empty
        assert df.iloc[0]["Runcode"] == "Run_001"
        assert df.iloc[0]["SomeKey"] == "Value"
