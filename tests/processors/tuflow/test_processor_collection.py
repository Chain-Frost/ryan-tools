
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

    def test_combine_1d_timeseries_empty(self):
        """Test combine_1d_timeseries with no processors."""
        collection = ProcessorCollection()
        assert collection.combine_1d_timeseries().empty

    def test_combine_1d_timeseries_missing_keys(self):
        """Test combine_1d_timeseries with missing group keys."""
        p1 = MagicMock()
        p1.processed = True
        p1.dataformat = "Timeseries"
        # Missing 'Time'
        p1.df = pd.DataFrame({
            "internalName": ["Run1"], "Chan ID": ["C1"], "Val": [10]
        })
        
        collection = ProcessorCollection()
        collection.add_processor(p1)
        
        assert collection.combine_1d_timeseries().empty

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

    def test_combine_1d_maximums_empty(self):
        """Test combine_1d_maximums with no processors."""
        collection = ProcessorCollection()
        assert collection.combine_1d_maximums().empty

    def test_combine_1d_maximums_no_data(self):
        """Test combine_1d_maximums with processors but empty dataframes (after filter)."""
        p1 = MagicMock()
        p1.processed = True
        p1.dataformat = "Maximums"
        p1.df = pd.DataFrame() # Empty
        
        collection = ProcessorCollection()
        collection.add_processor(p1) # Won't be added if empty, wait. add_processor checks empty.
        # So we need to manually insert it or mock add_processor logic?
        # add_processor checks: if processor.processed and not processor.df.empty:
        # So we can't add empty processor via add_processor.
        # But combine_1d_maximums iterates self.processors.
        # So if we manually append to self.processors...
        collection.processors.append(p1)
        
        assert collection.combine_1d_maximums().empty

    def test_combine_1d_maximums_missing_keys(self):
        """Test combine_1d_maximums with missing group keys."""
        p1 = MagicMock()
        p1.processed = True
        p1.dataformat = "Maximums"
        p1.name_parser.raw_run_code = "Run1"
        # Missing 'internalName'
        p1.df = pd.DataFrame({
            "Chan ID": ["C1"], "Q": [100]
        })
        
        collection = ProcessorCollection()
        # Manually append to bypass add_processor check if needed, but here df is not empty
        collection.processors.append(p1)
        
        assert collection.combine_1d_maximums().empty

    def test_combine_1d_maximums_eof_only(self):
        """Test combining with only EOF data (requires at least one Max processor to trigger)."""
        # Dummy Max processor to bypass early exit
        p_max = MagicMock()
        p_max.processed = True
        p_max.dataformat = "Maximums"
        p_max.name_parser.raw_run_code = "Run2" # Different run code
        p_max.df = pd.DataFrame({"internalName": ["Run2"], "Chan ID": ["C2"], "Q": [10]})

        p_eof = MagicMock()
        p_eof.processed = True
        p_eof.data_type = "EOF"
        p_eof.dataformat = "EOF"
        p_eof.name_parser.raw_run_code = "Run1"
        p_eof.df = pd.DataFrame({
            "internalName": ["Run1"], "Chan ID": ["C1"], "Area_Culv": [5.0]
        })

        collection = ProcessorCollection()
        collection.add_processor(p_max)
        collection.add_processor(p_eof)

        combined = collection.combine_1d_maximums()
        assert len(combined) == 2 # 1 Max + 1 EOF-only
        
        # Check EOF data is present
        row_eof = combined[combined["internalName"] == "Run1"].iloc[0]
        assert row_eof["Area_Culv"] == 5.0

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

    def test_pomm_combine(self):
        """Test POMM combine."""
        p1 = MagicMock()
        p1.processed = True
        p1.dataformat = "POMM"
        p1.df = pd.DataFrame({"A": [1]})
        
        collection = ProcessorCollection()
        collection.add_processor(p1)
        
        combined = collection.pomm_combine()
        assert len(combined) == 1
        assert combined.iloc[0]["A"] == 1

    def test_pomm_combine_empty(self):
        """Test POMM combine with no processors."""
        collection = ProcessorCollection()
        assert collection.pomm_combine().empty

    def test_po_combine(self):
        """Test PO combine."""
        p1 = MagicMock()
        p1.processed = True
        p1.dataformat = "PO"
        p1.df = pd.DataFrame({"A": [1]})
        
        collection = ProcessorCollection()
        collection.add_processor(p1)
        
        combined = collection.po_combine()
        assert len(combined) == 1

    def test_po_combine_empty(self):
        """Test PO combine with no processors."""
        collection = ProcessorCollection()
        assert collection.po_combine().empty

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

    def test_check_duplicates_none(self, mock_processor):
        """Test duplicate detection with no duplicates."""
        collection = ProcessorCollection()
        collection.add_processor(mock_processor)
        assert not collection.check_duplicates()

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

    def test_merge_chan_and_eof_edge_cases(self):
        """Test edge cases for merge."""
        df = pd.DataFrame({"Chan ID": ["C1"]})
        empty = pd.DataFrame()
        
        # Empty inputs
        assert ProcessorCollection._merge_chan_and_eof(empty, df).equals(df)
        assert ProcessorCollection._merge_chan_and_eof(df, empty).equals(df)
        
        # Missing Chan ID
        no_id = pd.DataFrame({"A": [1]})
        assert ProcessorCollection._merge_chan_and_eof(no_id, df).equals(no_id)

    def test_calculate_hw_d_ratio_edge_cases(self):
        """Test HW_D calculation edge cases."""
        collection = ProcessorCollection()
        
        # Missing columns
        df_missing = pd.DataFrame({"A": [1]})
        assert "HW_D" not in collection._calculate_hw_d_ratio(df_missing).columns
        
        # Empty DF
        df_empty = pd.DataFrame(columns=["US_h", "US Invert", "Height"])
        res_empty = collection._calculate_hw_d_ratio(df_empty)
        assert "HW_D" in res_empty.columns
        assert res_empty.empty
        
        # No valid data (e.g. Height=0)
        df_invalid = pd.DataFrame({
            "US_h": [10], "US Invert": [5], "Height": [0], "Chan ID": ["C1"]
        })
        res_invalid = collection._calculate_hw_d_ratio(df_invalid)
        assert "HW_D" in res_invalid.columns
        assert pd.isna(res_invalid.iloc[0]["HW_D"])
