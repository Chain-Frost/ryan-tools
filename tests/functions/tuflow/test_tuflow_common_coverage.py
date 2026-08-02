"""Coverage tests for tuflow_common.py."""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
import pandas as pd

from ryan_library.functions.tuflow import tuflow_common
from ryan_library.classes.suffixes_and_dtypes import SuffixesConfig


class TestCollectFiles:
    def test_collect_files_no_types(self):
        res = tuflow_common.collect_files([Path(".")], [], SuffixesConfig.get_instance())
        assert res == []

    def test_collect_files_no_suffixes(self, tmp_path):
        mock_config = MagicMock()
        mock_config.invert_suffix_to_type.return_value = {}
        res = tuflow_common.collect_files([tmp_path], ["UNKNOWN"], mock_config)
        assert res == []

    def test_collect_files_invalid_root(self):
        # A file instead of a directory
        res = tuflow_common.collect_files([Path(__file__)], ["POMM"], SuffixesConfig.get_instance())
        assert res == []


class TestFormatBytes:
    def test_format_bytes_large(self):
        assert "1.0 KB" in tuflow_common._format_bytes(1024)
        assert "1.0 MB" in tuflow_common._format_bytes(1024 * 1024)
        assert "1.0 GB" in tuflow_common._format_bytes(1024 * 1024 * 1024)


class TestResolveEntityFilter:
    @patch("ryan_library.functions.tuflow.tuflow_common.TuflowStringParser")
    def test_resolve_mapping(self, mock_parser):
        mock_parser.return_value.data_type = "POMM"
        mapping = {"POMM": ["L1", "L2"]}
        res = tuflow_common._resolve_entity_filter_for_file(Path("test.csv"), mapping)
        assert res == ["L1", "L2"]

    @patch("ryan_library.functions.tuflow.tuflow_common.TuflowStringParser")
    def test_resolve_mapping_no_datatype(self, mock_parser):
        mock_parser.return_value.data_type = None
        mapping = {"POMM": ["L1", "L2"]}
        res = tuflow_common._resolve_entity_filter_for_file(Path("test.csv"), mapping)
        assert res is None

    def test_resolve_string(self):
        res = tuflow_common._resolve_entity_filter_for_file(Path("test.csv"), "L1")
        assert res == ["L1"]


class TestProcessFileExceptions:
    @patch("ryan_library.functions.tuflow.tuflow_common.BaseProcessor.from_file")
    def test_process_file_validation_fails(self, mock_from_file):
        mock_proc = MagicMock()
        mock_proc.validate_data.return_value = False
        mock_from_file.return_value = mock_proc

        proc = tuflow_common.process_file(Path("test.csv"))
        assert proc is mock_proc
        assert mock_proc.discard_raw_dataframe.called

    @patch("ryan_library.functions.tuflow.tuflow_common.BaseProcessor.from_file")
    def test_process_file_exception(self, mock_from_file):
        mock_from_file.side_effect = ValueError("Test Error")
        proc = tuflow_common.process_file(Path("test.csv"))
        assert proc is None


class TestProcessFilesInParallel:
    @patch("ryan_library.functions.tuflow.tuflow_common.calculate_pool_size")
    @patch("ryan_library.functions.tuflow.tuflow_common.Pool")
    def test_parallel_execution(self, mock_pool, mock_calc):
        mock_calc.return_value = 2
        mock_proc = MagicMock()
        mock_proc.processed = True
        mock_proc.df = pd.DataFrame({"A": [1]})

        mock_pool_instance = MagicMock()
        mock_pool_instance.starmap.return_value = [mock_proc]
        mock_pool.return_value.__enter__.return_value = mock_pool_instance

        res = tuflow_common.process_files_in_parallel([Path("test.csv")], MagicMock())
        assert res is not None
        # Verify processor was added
        assert len(res.processors) == 1


class TestBulkMerge:
    @patch("ryan_library.functions.tuflow.tuflow_common.collect_files")
    def test_bulk_merge_no_files(self, mock_collect):
        mock_collect.return_value = []
        res = tuflow_common.bulk_read_and_merge_tuflow_csv([Path(".")], ["POMM"], MagicMock())
        assert len(res.processors) == 0
