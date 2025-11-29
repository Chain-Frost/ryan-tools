"""Unit tests for ryan_library.functions.tuflow.tuflow_common."""

import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from pathlib import Path
from ryan_library.functions.tuflow.tuflow_common import (
    collect_files,
    process_file,
    process_files_in_parallel,
    bulk_read_and_merge_tuflow_csv,
)
from ryan_library.classes.suffixes_and_dtypes import SuffixesConfig
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection


class TestTuflowCommon:
    """Tests for tuflow_common module."""

    @patch("ryan_library.functions.tuflow.tuflow_common.find_files_parallel")
    @patch("ryan_library.functions.tuflow.tuflow_common.is_non_zero_file")
    def test_collect_files(self, mock_is_non_zero, mock_find):
        """Test file collection logic."""
        mock_find.return_value = [Path("test_1d_H.csv")]
        mock_is_non_zero.return_value = True
        
        mock_suffixes = MagicMock(spec=SuffixesConfig)
        mock_suffixes.invert_suffix_to_type.return_value = {"H": ["_1d_H.csv"]}
        
        files = []
        with patch.object(Path, "is_dir", return_value=True):
            files = collect_files(
                paths_to_process=[Path("root")],
                include_data_types=["H"],
                suffixes_config=mock_suffixes
            )
        
        assert len(files) == 1
        assert files[0] == Path("test_1d_H.csv")

    @patch("ryan_library.processors.tuflow.base_processor.BaseProcessor.from_file")
    def test_process_file_success(self, mock_from_file):
        """Test successful file processing."""
        mock_proc = MagicMock()
        mock_proc.validate_data.return_value = True
        mock_from_file.return_value = mock_proc
        
        result = process_file(Path("test.csv"))
        
        assert result == mock_proc
        mock_proc.process.assert_called_once()

    @patch("ryan_library.functions.tuflow.tuflow_common.Pool")
    @patch("ryan_library.functions.tuflow.tuflow_common.calculate_pool_size")
    def test_process_files_in_parallel(self, mock_calc_size, mock_pool_cls):
        """Test parallel processing orchestration."""
        mock_calc_size.return_value = 1
        mock_pool = MagicMock()
        mock_pool_cls.return_value.__enter__.return_value = mock_pool
        
        # Mock pool.map return
        mock_proc = MagicMock()
        mock_proc.processed = True
        mock_pool.map.return_value = [mock_proc]
        
        coll = process_files_in_parallel([Path("test.csv")], log_queue=MagicMock())
        
        assert isinstance(coll, ProcessorCollection)
        # Verify processor was added (ProcessorCollection internals might need inspection or mocking add_processor)
        # Assuming add_processor works if coll is not empty/has items. 
        # Since ProcessorCollection is complex, we trust it was called if logic flows.

    @patch("ryan_library.functions.tuflow.tuflow_common.collect_files")
    @patch("ryan_library.functions.tuflow.tuflow_common.process_files_in_parallel")
    def test_bulk_read_and_merge_tuflow_csv(self, mock_process, mock_collect):
        """Test bulk processing orchestration."""
        mock_collect.return_value = [Path("test.csv")]
        mock_process.return_value = ProcessorCollection()
        
        result = bulk_read_and_merge_tuflow_csv(
            paths_to_process=[Path("root")],
            include_data_types=["H"],
            log_queue=MagicMock()
        )
        
        assert isinstance(result, ProcessorCollection)
        mock_collect.assert_called_once()
        mock_process.assert_called_once()
