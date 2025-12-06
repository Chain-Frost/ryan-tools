"""Tests for ryan_library.scripts.tuflow.tuflow_culverts_merge."""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import MagicMock, patch
from ryan_library.scripts.tuflow import tuflow_culverts_merge


def test_main_processing_success():
    with patch("ryan_library.scripts.tuflow.tuflow_culverts_merge.setup_logger"):
        with patch("ryan_library.scripts.tuflow.tuflow_culverts_merge.bulk_read_and_merge_tuflow_csv") as mock_bulk:
            with patch("ryan_library.scripts.tuflow.tuflow_culverts_merge.ExcelExporter") as mock_exporter:

                # Mock collection
                mock_collection = MagicMock()
                mock_collection.combine_1d_maximums.return_value = pd.DataFrame({"A": [1]})
                mock_collection.combine_raw.return_value = pd.DataFrame({"B": [2]})
                mock_bulk.return_value = mock_collection

                tuflow_culverts_merge.main_processing(
                    paths_to_process=[Path(".")], include_data_types=["type"], output_dir=Path("out")
                )

                mock_bulk.assert_called_once()
                mock_collection.combine_1d_maximums.assert_called_once()
                mock_collection.combine_raw.assert_called_once()
                mock_exporter.return_value.export_dataframes.assert_called_once()


def test_main_processing_parquet():
    with patch("ryan_library.scripts.tuflow.tuflow_culverts_merge.setup_logger"):
        with patch("ryan_library.scripts.tuflow.tuflow_culverts_merge.bulk_read_and_merge_tuflow_csv") as mock_bulk:
            with patch("ryan_library.scripts.tuflow.tuflow_culverts_merge.ExcelExporter") as mock_exporter:

                mock_collection = MagicMock()
                mock_collection.combine_1d_maximums.return_value = pd.DataFrame({"A": [1]})
                mock_collection.combine_raw.return_value = pd.DataFrame({"B": [2]})
                mock_bulk.return_value = mock_collection

                tuflow_culverts_merge.main_processing(
                    paths_to_process=[Path(".")], include_data_types=["type"], output_parquet=True
                )

                _, kwargs = mock_exporter.return_value.export_dataframes.call_args
                assert kwargs["export_mode"] == "both"
