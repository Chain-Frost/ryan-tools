"""Tests for ryan_library.scripts.pomm_max_items."""

import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
import pandas as pd
from ryan_library.scripts import pomm_max_items


class TestPommMaxItems:
    @patch("ryan_library.scripts.tuflow.pomm_max_items.aggregated_from_paths")
    @patch("ryan_library.scripts.tuflow.pomm_max_items.setup_logger")
    def test_run_peak_report_workflow_success(self, mock_logger, mock_agg):
        """Test successful workflow execution."""
        mock_df = pd.DataFrame({"A": [1]})
        mock_agg.return_value = mock_df

        mock_exporter = MagicMock()

        pomm_max_items.run_peak_report_workflow(script_directory=Path("."), exporter=mock_exporter)

        mock_agg.assert_called_once()
        mock_exporter.assert_called_once()

        # Check exporter args
        _, kwargs = mock_exporter.call_args
        assert kwargs["aggregated_df"].equals(mock_df)
        assert kwargs["include_pomm"] is True

    @patch("ryan_library.scripts.tuflow.pomm_max_items.aggregated_from_paths")
    @patch("ryan_library.scripts.tuflow.pomm_max_items.logger")
    def test_run_peak_report_workflow_no_data(self, mock_logger, mock_agg):
        """Test workflow with no data found."""
        mock_agg.return_value = pd.DataFrame()

        mock_exporter = MagicMock()

        pomm_max_items.run_peak_report_workflow(script_directory=Path("."), exporter=mock_exporter)

        mock_exporter.assert_not_called()
        mock_logger.warning.assert_called()

    @patch("ryan_library.scripts.tuflow.pomm_max_items.aggregated_from_paths")
    @patch("ryan_library.scripts.tuflow.pomm_max_items.logger")
    def test_run_peak_report_workflow_filtered_empty(self, mock_logger, mock_agg):
        """Test workflow where location filter results in empty data."""
        mock_agg.return_value = pd.DataFrame()

        mock_exporter = MagicMock()

        pomm_max_items.run_peak_report_workflow(
            script_directory=Path("."), locations_to_include=["Loc1"], exporter=mock_exporter
        )

        mock_exporter.assert_not_called()
        # Should verify that it logs about location filter specifically
        args, _ = mock_logger.warning.call_args
        assert "Location filter" in args[0]

    @patch("ryan_library.scripts.tuflow.pomm_max_items.run_peak_report_workflow")
    def test_export_median_peak_report(self, mock_workflow):
        """Test export_median_peak_report wrapper."""
        pomm_max_items.export_median_peak_report(script_directory=Path("."), log_level="DEBUG")

        mock_workflow.assert_called_once()
        _, kwargs = mock_workflow.call_args
        assert kwargs["exporter"] == pomm_max_items.save_peak_report_median
        assert kwargs["log_level"] == "DEBUG"

    @patch("ryan_library.scripts.tuflow.pomm_max_items.run_peak_report_workflow")
    def test_export_mean_peak_report(self, mock_workflow):
        """Test export_mean_peak_report wrapper."""
        pomm_max_items.export_mean_peak_report(script_directory=Path("."), log_level="DEBUG")

        mock_workflow.assert_called_once()
        _, kwargs = mock_workflow.call_args
        assert kwargs["exporter"] == pomm_max_items.save_peak_report_mean

    @patch("ryan_library.scripts.tuflow.pomm_max_items.export_median_peak_report")
    def test_run_median_peak_report_deprecated(self, mock_export):
        """Test deprecated run_median_peak_report."""
        with pytest.warns(DeprecationWarning):
            pomm_max_items.run_median_peak_report(script_directory=Path("."))

        mock_export.assert_called_once()

    @patch("ryan_library.scripts.tuflow.pomm_max_items.export_mean_peak_report")
    def test_run_mean_peak_report_deprecated(self, mock_export):
        """Test deprecated run_mean_peak_report."""
        with pytest.warns(DeprecationWarning):
            pomm_max_items.run_mean_peak_report(script_directory=Path("."))

        mock_export.assert_called_once()

    @patch("ryan_library.scripts.tuflow.pomm_max_items.export_median_peak_report")
    def test_run_peak_report_legacy(self, mock_export):
        """Test legacy run_peak_report."""
        # Capture stdout to verify print statements if needed, but mainly check call
        pomm_max_items.run_peak_report(script_directory=Path("."))
        mock_export.assert_called_once()
