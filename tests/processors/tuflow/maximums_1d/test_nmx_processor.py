"""Unit tests for ryan_library.processors.tuflow.maximums_1d.NmxProcessor."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ryan_library.processors.tuflow.base_processor import ProcessorStatus
from ryan_library.processors.tuflow.maximums_1d.NmxProcessor import NmxProcessor


def _process_with_mocked_read(processor: NmxProcessor, read_mock: MagicMock) -> None:
    """A successful mocked read must repopulate df after invocation state resets."""
    if read_mock.return_value is ProcessorStatus.SUCCESS:
        source = processor.df.copy(deep=True)

        def fresh_read() -> ProcessorStatus:
            processor.df = source.copy(deep=True)
            return ProcessorStatus.SUCCESS

        read_mock.side_effect = fresh_read
    processor.process()


class TestNmxProcessor:
    """Tests for NmxProcessor class."""

    @pytest.fixture
    def mock_processor(self) -> NmxProcessor:
        """Create a mock NmxProcessor with minimal setup."""
        with patch("ryan_library.processors.tuflow.base_processor.BaseProcessor.__post_init__"):
            processor = NmxProcessor(Path("test_1d_Nmx.csv"))
            processor.file_name = "test_1d_Nmx.csv"
            processor.resolved_file_path = processor.file_path.resolve()
            processor.df = pd.DataFrame()
            processor.columns_to_use = {}
            processor.expected_in_header = []
            processor.data_type = "Nmx"
            return processor

    def test_extract_and_transform_nmx_data_success(self, mock_processor: NmxProcessor) -> None:
        """Test successful extraction and transformation of NMX data."""
        mock_processor.df = pd.DataFrame(
            {
                "Node ID": ["C1.1", "C1.2", "C2.1", "C2.2"],
                "Time Hmax": [1.0, 1.0, 2.0, 2.0],
                "Hmax": [10.0, 9.0, 20.0, 19.0],
            }
        )

        mock_processor._extract_and_transform_nmx_data()

        assert len(mock_processor.df) == 2  # 2 channels
        assert "US_h" in mock_processor.df.columns
        assert "DS_h" in mock_processor.df.columns

        # Check C1
        c1 = mock_processor.df[mock_processor.df["Chan ID"] == "C1"].iloc[0]
        assert c1["US_h"] == 10.0
        assert c1["DS_h"] == 9.0

    def test_extract_and_transform_nmx_data_invalid_suffix(self, mock_processor: NmxProcessor) -> None:
        """Test filtering of invalid suffixes."""
        mock_processor.df = pd.DataFrame(
            {
                "Node ID": ["C1.1", "C1.2", "C1.3"],  # .3 is invalid
                "Time Hmax": [1.0, 1.0, 1.0],
                "Hmax": [10.0, 9.0, 10.0],
            }
        )

        mock_processor._extract_and_transform_nmx_data()

        # Should have C1 with US_h and DS_h. C1.3 is filtered out.

        assert len(mock_processor.df) == 1
        c1 = mock_processor.df.iloc[0]
        assert c1["US_h"] == 10.0
        assert c1["DS_h"] == 9.0

    def test_extract_and_transform_nmx_data_missing_columns(self, mock_processor: NmxProcessor) -> None:
        """Test failure when columns are missing."""
        mock_processor.df = pd.DataFrame(
            {
                "Node ID": ["C1.1"],
                # Missing Time Hmax, Hmax
            }
        )

        mock_processor._extract_and_transform_nmx_data()
        assert mock_processor.df.empty

    def test_extract_and_transform_nmx_data_missing_pivot_columns(self, mock_processor: NmxProcessor) -> None:
        """Test failure when pivot results in missing columns (e.g. only suffix .1 present)."""
        mock_processor.df = pd.DataFrame(
            {
                "Node ID": ["C1.1"],  # Only .1, so .2 (DS_h) will be missing
                "Time Hmax": [1.0],
                "Hmax": [10.0],
            }
        )

        mock_processor._extract_and_transform_nmx_data()
        assert mock_processor.df.empty

    @patch("ryan_library.processors.tuflow.maximums_1d.NmxProcessor.NmxProcessor.read_maximums_csv")
    @patch("ryan_library.processors.tuflow.maximums_1d.NmxProcessor.NmxProcessor.add_common_columns")
    @patch("ryan_library.processors.tuflow.maximums_1d.NmxProcessor.NmxProcessor.apply_output_transformations")
    @patch("ryan_library.processors.tuflow.maximums_1d.NmxProcessor.NmxProcessor.validate_data")
    def test_process_success(
        self,
        mock_validate: MagicMock,
        mock_apply: MagicMock,
        mock_add: MagicMock,
        mock_read: MagicMock,
        mock_processor: NmxProcessor,
    ) -> None:
        """Test full process flow success."""
        mock_read.return_value = ProcessorStatus.SUCCESS
        mock_validate.return_value = True

        mock_processor.df = pd.DataFrame({"Node ID": ["C1.1", "C1.2"], "Time Hmax": [1.0, 1.0], "Hmax": [10.0, 9.0]})

        _process_with_mocked_read(mock_processor, mock_read)

        assert mock_processor.processed is True
        mock_read.assert_called_once()
        mock_add.assert_called_once()

    @patch("ryan_library.processors.tuflow.maximums_1d.NmxProcessor.NmxProcessor.read_maximums_csv")
    def test_process_read_failure(self, mock_read: MagicMock, mock_processor: NmxProcessor) -> None:
        """Test process aborts when read fails."""
        mock_read.return_value = ProcessorStatus.FAILURE

        _process_with_mocked_read(mock_processor, mock_read)

        assert mock_processor.processed is False
        assert mock_processor.df.empty

    @patch("ryan_library.processors.tuflow.maximums_1d.NmxProcessor.NmxProcessor.read_maximums_csv")
    @patch("ryan_library.processors.tuflow.maximums_1d.NmxProcessor.NmxProcessor.add_common_columns")
    @patch("ryan_library.processors.tuflow.maximums_1d.NmxProcessor.NmxProcessor.apply_output_transformations")
    @patch("ryan_library.processors.tuflow.maximums_1d.NmxProcessor.NmxProcessor.validate_data")
    def test_process_validation_failure(
        self,
        mock_validate: MagicMock,
        mock_apply: MagicMock,
        mock_add: MagicMock,
        mock_read: MagicMock,
        mock_processor: NmxProcessor,
    ) -> None:
        """Test process aborts when validation fails."""
        mock_read.return_value = ProcessorStatus.SUCCESS
        mock_validate.return_value = False

        mock_processor.df = pd.DataFrame({"Node ID": ["C1.1", "C1.2"], "Time Hmax": [1.0, 1.0], "Hmax": [10.0, 9.0]})

        _process_with_mocked_read(mock_processor, mock_read)

        assert mock_processor.processed is False
        assert mock_processor.df.empty

    @patch("ryan_library.processors.tuflow.maximums_1d.NmxProcessor.NmxProcessor.read_maximums_csv")
    def test_process_exception(self, mock_read: MagicMock, mock_processor: NmxProcessor) -> None:
        """Test process handles exceptions."""
        mock_read.side_effect = Exception("Test Error")

        _process_with_mocked_read(mock_processor, mock_read)

        assert mock_processor.processed is False
        assert mock_processor.df.empty
