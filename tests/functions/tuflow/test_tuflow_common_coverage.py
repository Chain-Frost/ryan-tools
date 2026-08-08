"""Coverage tests for tuflow_common.py."""

from pathlib import Path
from typing import Collection
from unittest.mock import MagicMock, patch
import pandas as pd

from ryan_library.functions.tuflow import tuflow_common
from ryan_library.processors.tuflow.processor_collection import BaseProcessor, ProcessorCollection
from ryan_library.classes.suffixes_and_dtypes import SuffixesConfig


class TestCollectFiles:
    def test_collect_files_no_types(self) -> None:
        res: list[Path] = tuflow_common.collect_files(
            paths_to_process=[Path(".")], include_data_types=[], suffixes_config=SuffixesConfig.get_instance()
        )
        assert res == []

    def test_collect_files_no_suffixes(self, tmp_path: Path) -> None:
        mock_config = MagicMock()
        mock_config.invert_suffix_to_type.return_value = {}
        res: list[Path] = tuflow_common.collect_files(
            paths_to_process=[tmp_path], include_data_types=["UNKNOWN"], suffixes_config=mock_config
        )
        assert res == []

    def test_collect_files_invalid_root(self) -> None:
        # A file instead of a directory
        res: list[Path] = tuflow_common.collect_files(
            paths_to_process=[Path(__file__)],
            include_data_types=["POMM"],
            suffixes_config=SuffixesConfig.get_instance(),
        )
        assert res == []


class TestFormatBytes:
    def test_format_bytes_large(self) -> None:
        assert "1.0 KB" in tuflow_common._format_bytes(size=1024)
        assert "1.0 MB" in tuflow_common._format_bytes(size=1024 * 1024)
        assert "1.0 GB" in tuflow_common._format_bytes(size=1024 * 1024 * 1024)


class TestResolveEntityFilter:
    @patch("ryan_library.functions.tuflow.tuflow_common.TuflowStringParser")
    def test_resolve_mapping(self, mock_parser) -> None:
        mock_parser.return_value.data_type = "POMM"
        mapping: dict[str, list[str]] = {"POMM": ["L1", "L2"]}
        res: Collection[str] | None = tuflow_common._resolve_entity_filter_for_file(
            file_path=Path("test.csv"), entity_filters=mapping
        )
        assert res == ["L1", "L2"]

    @patch("ryan_library.functions.tuflow.tuflow_common.TuflowStringParser")
    def test_resolve_mapping_no_datatype(self, mock_parser) -> None:
        mock_parser.return_value.data_type = None
        mapping: dict[str, list[str]] = {"POMM": ["L1", "L2"]}
        res: Collection[str] | None = tuflow_common._resolve_entity_filter_for_file(Path("test.csv"), mapping)
        assert res is None

    def test_resolve_string(self) -> None:
        res: Collection[str] | None = tuflow_common._resolve_entity_filter_for_file(
            file_path=Path("test.csv"), entity_filters="L1"
        )
        assert res == ["L1"]


class TestProcessFileExceptions:
    @patch("ryan_library.functions.tuflow.tuflow_common.BaseProcessor.from_file")
    def test_process_file_validation_fails(self, mock_from_file) -> None:
        mock_proc = MagicMock()
        mock_proc.validate_data.return_value = False
        mock_from_file.return_value = mock_proc

        proc: BaseProcessor | None = tuflow_common.process_file(file_path=Path("test.csv"))
        assert proc is mock_proc
        assert mock_proc.discard_raw_dataframe.called

    @patch("ryan_library.functions.tuflow.tuflow_common.BaseProcessor.from_file")
    def test_process_file_exception(self, mock_from_file) -> None:
        mock_from_file.side_effect = ValueError("Test Error")
        proc: BaseProcessor | None = tuflow_common.process_file(file_path=Path("test.csv"))
        assert proc is None


class TestProcessFilesInParallel:
    @patch("ryan_library.functions.tuflow.tuflow_common.calculate_pool_size")
    @patch("ryan_library.functions.tuflow.tuflow_common.Pool")
    def test_parallel_execution(self, mock_pool, mock_calc) -> None:
        mock_calc.return_value = 2
        mock_proc = MagicMock()
        mock_proc.processed = True
        mock_proc.df = pd.DataFrame(data={"A": [1]})

        mock_pool_instance = MagicMock()
        mock_pool_instance.starmap.return_value = [mock_proc]
        mock_pool.return_value.__enter__.return_value = mock_pool_instance

        log_queue = MagicMock()
        res: ProcessorCollection = tuflow_common.process_files_in_parallel([Path("test.csv")], log_queue)
        assert res is not None
        assert mock_pool.call_args.kwargs["initargs"] == (log_queue,)
        # Verify processor was added
        assert len(res.processors) == 1


class TestBulkMerge:
    @patch("ryan_library.functions.tuflow.tuflow_common.collect_files")
    def test_bulk_merge_no_files(self, mock_collect) -> None:
        mock_collect.return_value = []
        res: ProcessorCollection = tuflow_common.bulk_read_and_merge_tuflow_csv([Path(".")], ["POMM"], MagicMock())
        assert len(res.processors) == 0
