"""Unit tests for ryan_library.processors.tuflow.processor_collection."""

import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection
from ryan_library.processors.tuflow.base_processor import BaseProcessor


class TestProcessorCollection:
    """Tests for ProcessorCollection class."""

    @pytest.fixture
    def mock_processor(self):
        """Create a mock BaseProcessor."""
        processor = MagicMock()
        processor.processed = True
        processor.df = pd.DataFrame({"A": [1]})
        processor.file_name = "test_file.csv"
        processor.data_type = "Timeseries"
        processor.dataformat = "Timeseries"
        processor.name_parser.raw_run_code = "Run1"
        processor.applied_location_filter = None
        return processor

    def test_add_processor_success(self, mock_processor):
        """Test adding a valid processor."""
        collection = ProcessorCollection()
        collection.add_processor(mock_processor)
        assert len(collection.processors) == 1

    def test_add_processor_empty(self, mock_processor):
        """Test adding a processor with empty dataframe."""
        mock_processor.df = pd.DataFrame()
        collection = ProcessorCollection()
        collection.add_processor(mock_processor)
        assert len(collection.processors) == 0

    def test_add_processor_unprocessed(self, mock_processor):
        """Test adding an unprocessed processor."""
        mock_processor.processed = False
        collection = ProcessorCollection()
        collection.add_processor(mock_processor)
        assert len(collection.processors) == 0

    def test_filter_locations(self, mock_processor):
        """Test filtering locations across processors."""
        collection = ProcessorCollection()
        collection.add_processor(mock_processor)
        
        # Mock filter_locations on the processor to return a set and modify df (conceptually)
        mock_processor.filter_locations.return_value = frozenset(["Loc1"])
        # We don't need to actually modify df here because the collection checks processor.df.empty
        # Let's assume it remains non-empty
        
        collection.filter_locations(["Loc1"])
        mock_processor.filter_locations.assert_called_with(frozenset(["Loc1"]))
        assert len(collection.processors) == 1

    def test_combine_1d_timeseries(self):
        """Test combining timeseries data."""
        p1 = MagicMock()
        p1.processed = True
        p1.dataformat = "Timeseries"
        p1.df = pd.DataFrame({
            "internalName": ["Run1"], "Chan ID": ["C1"], "Time": [0.0], "Val": [10],
            "file": ["f1"], "rel_path": ["p1"], "path": ["P1"], "directory_path": ["D1"]
        })
        
        p2 = MagicMock()
        p2.processed = True
        p2.dataformat = "Timeseries"
        p2.df = pd.DataFrame({
            "internalName": ["Run1"], "Chan ID": ["C1"], "Time": [1.0], "Val": [20],
            "file": ["f2"], "rel_path": ["p2"], "path": ["P2"], "directory_path": ["D2"]
        })

        collection = ProcessorCollection()
        collection.add_processor(p1)
        collection.add_processor(p2)

        combined = collection.combine_1d_timeseries()
        assert len(combined) == 2
        assert "file" not in combined.columns
        assert "Val" in combined.columns

    def test_combine_1d_maximums_with_eof(self):
        """Test combining maximums with EOF merge."""
        # Max processor
        p_max = MagicMock()
        p_max.processed = True
        p_max.data_type = "Cmx"
        p_max.dataformat = "Maximums"
        p_max.name_parser.raw_run_code = "Run1"
        p_max.df = pd.DataFrame({
            "internalName": ["Run1"], "Chan ID": ["C1"], "Q": [100],
            "file": ["f1"], "rel_path": ["p1"], "path": ["P1"], "Time": [0]
        })

        # EOF processor
        p_eof = MagicMock()
        p_eof.processed = True
        p_eof.data_type = "EOF"
        p_eof.dataformat = "EOF"
        p_eof.name_parser.raw_run_code = "Run1"
        p_eof.df = pd.DataFrame({
            "Chan ID": ["C1"], "Area_Culv": [5.0]
        })

        collection = ProcessorCollection()
        collection.add_processor(p_max)
        collection.add_processor(p_eof)

        combined = collection.combine_1d_maximums()
        
        assert len(combined) == 1
        assert combined.iloc[0]["Q"] == 100
        assert combined.iloc[0]["Area_Culv"] == 5.0

    def test_combine_raw(self, mock_processor):
        """Test raw combination."""
        p2 = MagicMock()
        p2.processed = True
        p2.df = pd.DataFrame({"A": [2]})
        p2.data_type = "Other"
        
        collection = ProcessorCollection()
        collection.add_processor(mock_processor)
        collection.add_processor(p2)
        
        combined = collection.combine_raw()
        assert len(combined) == 2
        assert combined["A"].tolist() == [1, 2]

    def test_get_processors_by_data_type(self, mock_processor):
        """Test filtering by data type."""
        collection = ProcessorCollection()
        collection.add_processor(mock_processor) # Type "Timeseries"
        
        filtered = collection.get_processors_by_data_type("Timeseries")
        assert len(filtered.processors) == 1
        
        filtered_empty = collection.get_processors_by_data_type("Other")
        assert len(filtered_empty.processors) == 0

    def test_check_duplicates(self):
        """Test duplicate detection."""
        p1 = MagicMock()
        p1.processed = True
        p1.df = pd.DataFrame({"A": [1]})
        p1.data_type = "TypeA"
        p1.name_parser.raw_run_code = "Run1"
        p1.file_name = "f1"

        p2 = MagicMock()
        p2.processed = True
        p2.df = pd.DataFrame({"A": [1]})
        p2.data_type = "TypeA"
        p2.name_parser.raw_run_code = "Run1"
        p2.file_name = "f2"

        collection = ProcessorCollection()
        collection.add_processor(p1)
        collection.add_processor(p2)

        duplicates = collection.check_duplicates()
        assert len(duplicates) == 1
        assert ("Run1", "TypeA") in duplicates
        assert len(duplicates[("Run1", "TypeA")]) == 2

    def test_merge_chan_and_eof_static(self):
        """Test static merge method directly."""
        chan_df = pd.DataFrame({"Chan ID": ["C1", "C2"], "Val": [1, 2]})
        eof_df = pd.DataFrame({"Chan ID": ["C1"], "Val": [10], "Extra": [99]})
        
        # EOF should overwrite Val for C1, and add Extra
        merged = ProcessorCollection._merge_chan_and_eof(chan_df, eof_df)
        
        # Sort by Chan ID to ensure order for assertion
        merged = merged.sort_values("Chan ID").reset_index(drop=True)
        
        row_c1 = merged[merged["Chan ID"] == "C1"].iloc[0]
        row_c2 = merged[merged["Chan ID"] == "C2"].iloc[0]
        
        assert row_c1["Val"] == 10  # EOF wins
        assert row_c1["Extra"] == 99
        assert row_c2["Val"] == 2   # Original kept
        assert pd.isna(row_c2["Extra"])
