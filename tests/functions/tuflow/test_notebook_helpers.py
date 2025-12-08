import shutil
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ryan_library.functions.tuflow.notebook_helpers import load_tuflow_data
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection


class TestLoadTuflowData:
    @pytest.fixture
    def mock_data_setup(self):
        """Create a temporary directory structure with some dummy files."""
        temp_dir = Path(tempfile.mkdtemp())

        # Create dummy files that would match our patterns (mocking the behavior)
        # We won't actually parse them in unit tests if we mock collect_files/process_files
        # But having a valid path is good.
        (temp_dir / "sim_1_Q.csv").touch()
        (temp_dir / "sim_1_V.csv").touch()

        yield temp_dir
        shutil.rmtree(temp_dir)

    @patch("ryan_library.functions.tuflow.notebook_helpers.collect_files")
    @patch("ryan_library.functions.tuflow.notebook_helpers.process_files_in_parallel")
    def test_load_tuflow_data_parallel(self, mock_process_parallel, mock_collect, mock_data_setup):
        """Test loading data in parallel mode."""

        # Setup mocks
        mock_collect.return_value = [mock_data_setup / "sim_1_Q.csv"]

        mock_collection = MagicMock(spec=ProcessorCollection)
        mock_process_parallel.return_value = mock_collection

        # Execute
        result = load_tuflow_data(paths=[str(mock_data_setup)], data_types=["Q"], parallel=True)

        # Verify
        mock_collect.assert_called_once()
        mock_process_parallel.assert_called_once()
        assert result == mock_collection

    @patch("ryan_library.functions.tuflow.notebook_helpers.collect_files")
    @patch("ryan_library.functions.tuflow.notebook_helpers.process_file")
    def test_load_tuflow_data_serial(self, mock_process_file, mock_collect, mock_data_setup):
        """Test loading data in serial mode."""

        # Setup mocks
        files = [mock_data_setup / "sim_1_Q.csv", mock_data_setup / "sim_1_V.csv"]
        mock_collect.return_value = files

        # Create a real ProcessorCollection for the result logic, but mock the processor/add_processor inputs
        # Actually easier to just verify process_file is called for each file

        mock_proc = MagicMock()
        mock_proc.processed = True
        mock_proc.df = pd.DataFrame({"A": [1]})
        mock_proc.file_name = "test.csv"

        mock_process_file.return_value = mock_proc

        # Execute
        result = load_tuflow_data(paths=[mock_data_setup], data_types=["Q"], parallel=False)

        # Verify
        assert mock_collect.call_count == 1
        assert mock_process_file.call_count == 2
        assert isinstance(result, ProcessorCollection)
        # We mocked the processors, so the collection should contain them
        assert len(result.processors) == 2

    @patch("ryan_library.functions.tuflow.notebook_helpers.collect_files")
    def test_load_tuflow_data_no_files(self, mock_collect, mock_data_setup):
        """Test handling when no files are found."""
        mock_collect.return_value = []

        result = load_tuflow_data(paths=[mock_data_setup], data_types=["Q"])

        assert isinstance(result, ProcessorCollection)
        assert len(result.processors) == 0

    @patch("ryan_library.functions.tuflow.notebook_helpers.collect_files")
    @patch("ryan_library.functions.tuflow.notebook_helpers.process_files_in_parallel")
    def test_load_tuflow_data_with_location_filter(self, mock_process_parallel, mock_collect, mock_data_setup):
        """Test that location filtering is applied."""
        mock_collect.return_value = [mock_data_setup / "sim_1_Q.csv"]

        # Mock the returned collection
        mock_collection = MagicMock(spec=ProcessorCollection)
        mock_process_parallel.return_value = mock_collection

        # Execute
        load_tuflow_data(paths=[mock_data_setup], data_types=["Q"], locations=["LocA"])

        # Verify filter_locations was called
        mock_collection.filter_locations.assert_called_with(["LocA"])
