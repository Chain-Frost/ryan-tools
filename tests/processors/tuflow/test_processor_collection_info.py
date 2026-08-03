"""Tests for basic info operations in ProcessorCollection."""

import pandas as pd
from unittest.mock import MagicMock

from ryan_library.processors.tuflow.processor_collection import ProcessorCollection


class TestProcessorCollectionInfo:
    def test_build_basic_info_lookup(self):
        pc = ProcessorCollection()

        proc1 = MagicMock()
        proc1.build_basic_info_payload.return_value = {
            "file": "file1.csv",
            "path": "/path/to/file1.csv",
            "other": "ignored",
        }

        proc2 = MagicMock()
        proc2.build_basic_info_payload.return_value = {"file": "file2.csv", "path": "/path/to/file2.csv"}

        pc.processors = [proc1, proc2]

        lookup_df = pc.build_basic_info_lookup(columns=["file", "path"])

        assert not lookup_df.empty
        assert "processor_id" in lookup_df.columns
        assert list(lookup_df["processor_id"]) == [0, 1]
        assert list(lookup_df["file"]) == ["file1.csv", "file2.csv"]
        assert "other" not in lookup_df.columns

    def test_compact_basic_info_columns(self):
        pc = ProcessorCollection()

        proc1 = MagicMock()
        proc1.df = pd.DataFrame({"file": ["file1.csv"], "path": ["/path/1"], "data": [10]})
        proc1.build_basic_info_payload.return_value = {"file": "file1.csv", "path": "/path/1"}

        proc2 = MagicMock()
        proc2.df = pd.DataFrame({"processor_id": [1], "data": [20]})  # Already has id
        proc2.build_basic_info_payload.return_value = {"file": "file2.csv", "path": "/path/2"}

        proc3 = MagicMock()
        proc3.df = pd.DataFrame()
        proc3.build_basic_info_payload.return_value = {"file": "file3.csv", "path": "/path/3"}

        pc.processors = [proc1, proc2, proc3]

        lookup_df = pc.compact_basic_info_columns(columns=["file", "path"])

        assert pc.basic_info_lookup is not None
        assert len(lookup_df) == 3

        # Proc 1 should have id and data, but file and path dropped
        assert "processor_id" in proc1.df.columns
        assert "file" not in proc1.df.columns

        # Proc 2 was untouched
        assert list(proc2.df["data"]) == [20]

        # Proc 3 is empty
        assert proc3.df.empty

    def test_attach_basic_info(self):
        pc = ProcessorCollection()

        lookup_df = pd.DataFrame({"processor_id": [0, 1], "file": ["file1.csv", "file2.csv"]})
        pc.basic_info_lookup = lookup_df

        # Merge success
        df = pd.DataFrame({"processor_id": [0, 1], "data": [10, 20]})
        merged = pc.attach_basic_info(df, drop_id=False)
        assert "file" in merged.columns
        assert list(merged["file"]) == ["file1.csv", "file2.csv"]
        assert "processor_id" in merged.columns

        # Merge and drop
        merged_dropped = pc.attach_basic_info(df, drop_id=True)
        assert "processor_id" not in merged_dropped.columns
        assert "file" in merged_dropped.columns

        # No lookup
        pc.basic_info_lookup = None
        unmerged = pc.attach_basic_info(df)
        assert "file" not in unmerged.columns

        # Empty df
        empty = pc.attach_basic_info(pd.DataFrame())
        assert empty.empty

        # Missing id column
        pc.basic_info_lookup = lookup_df
        df_no_id = pd.DataFrame({"data": [10]})
        unmerged_no_id = pc.attach_basic_info(df_no_id)
        assert "file" not in unmerged_no_id.columns

    def test_discard_raw_dataframes(self):
        pc = ProcessorCollection()
        proc1 = MagicMock()
        proc2 = MagicMock()
        pc.processors = [proc1, proc2]

        count = pc.discard_raw_dataframes()
        assert count == 2
        assert proc1.discard_raw_dataframe.called
        assert proc2.discard_raw_dataframe.called
