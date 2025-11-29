"""Unit tests for ryan_library.processors.tuflow.maximums_1d.CmxProcessor."""

import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from pathlib import Path
from ryan_library.processors.tuflow.maximums_1d.CmxProcessor import CmxProcessor
from ryan_library.processors.tuflow.base_processor import ProcessorStatus


class TestCmxProcessor:
    """Tests for CmxProcessor class."""

    @pytest.fixture
    def mock_processor(self):
        """Create a mock CmxProcessor with minimal setup."""
        # We need to mock the __post_init__ to avoid config loading during instantiation in tests
        with patch("ryan_library.processors.tuflow.base_processor.BaseProcessor.__post_init__"):
            processor = CmxProcessor(Path("test_1d_Cmx.csv"))
            processor.file_name = "test_1d_Cmx.csv"
            processor.df = pd.DataFrame()
            processor.columns_to_use = {}
            processor.expected_in_header = []
            processor.data_type = "Cmx"
            return processor

    def test_reshape_cmx_data_success(self, mock_processor):
        """Test successful reshaping of CMX data."""
        mock_processor.df = pd.DataFrame({
            "Chan ID": ["C1", "C2"],
            "Time Qmax": [1.0, 2.0],
            "Qmax": [10.0, 20.0],
            "Time Vmax": [1.1, 2.1],
            "Vmax": [1.0, 2.0]
        })

        mock_processor._reshape_cmx_data()

        assert len(mock_processor.df) == 4  # 2 rows * 2 (Q and V)
        assert "Q" in mock_processor.df.columns
        assert "V" in mock_processor.df.columns
        
        # Check Q rows
        q_rows = mock_processor.df[mock_processor.df["Q"].notna()]
        assert len(q_rows) == 2
        assert q_rows.iloc[0]["Q"] == 10.0
        assert pd.isna(q_rows.iloc[0]["V"])

        # Check V rows
        v_rows = mock_processor.df[mock_processor.df["V"].notna()]
        assert len(v_rows) == 2
        assert v_rows.iloc[0]["V"] == 1.0
        assert pd.isna(v_rows.iloc[0]["Q"])

    def test_reshape_cmx_data_missing_columns(self, mock_processor):
        """Test reshaping fails with missing columns."""
        mock_processor.df = pd.DataFrame({
            "Chan ID": ["C1"],
            "Time Qmax": [1.0]
            # Missing Qmax, Vmax, etc.
        })

        mock_processor._reshape_cmx_data()
        assert mock_processor.df.empty

    def test_handle_malformed_data(self, mock_processor):
        """Test handling of malformed data."""
        mock_processor.df = pd.DataFrame({
            "Chan ID": ["C1", None],
            "Time": [1.0, None],
            "Q": [10.0, None],
            "V": [None, None]
        })
        
        mock_processor._handle_malformed_data()
        
        assert len(mock_processor.df) == 1
        assert mock_processor.df.iloc[0]["Chan ID"] == "C1"

    @patch("ryan_library.processors.tuflow.maximums_1d.CmxProcessor.CmxProcessor.read_maximums_csv")
    @patch("ryan_library.processors.tuflow.maximums_1d.CmxProcessor.CmxProcessor.add_common_columns")
    @patch("ryan_library.processors.tuflow.maximums_1d.CmxProcessor.CmxProcessor.apply_output_transformations")
    @patch("ryan_library.processors.tuflow.maximums_1d.CmxProcessor.CmxProcessor.validate_data")
    def test_process_success(self, mock_validate, mock_apply, mock_add, mock_read, mock_processor):
        """Test full process flow success."""
        mock_read.return_value = ProcessorStatus.SUCCESS
        mock_validate.return_value = True
        
        # Setup df for reshaping
        mock_processor.df = pd.DataFrame({
            "Chan ID": ["C1"],
            "Time Qmax": [1.0],
            "Qmax": [10.0],
            "Time Vmax": [1.1],
            "Vmax": [1.0]
        })

        # We need to bind the methods to the mock_processor instance because we are calling them on it
        # But since we patched the class methods, we can just call process on the instance
        
        # However, since we created mock_processor with a patched __post_init__, it's an instance of CmxProcessor.
        # The patches above are on the class, so they should apply.
        
        mock_processor.process()
        
        assert mock_processor.processed is True
        mock_read.assert_called_once()
        mock_add.assert_called_once()
        mock_apply.assert_called_once()
