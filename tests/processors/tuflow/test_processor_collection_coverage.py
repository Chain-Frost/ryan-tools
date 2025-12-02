"""Additional coverage tests for ProcessorCollection."""

import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection

class TestProcessorCollectionCoverage:
    @patch("ryan_library.processors.tuflow.processor_collection.logger")
    def test_filter_locations_logging(self, mock_logger):
        """Test logging in filter_locations."""
        p1 = MagicMock()
        p1.df = pd.DataFrame({"Location": ["Loc1", "Loc2"]})
        p1.applied_location_filter = None
        # Simulate filtering reducing rows
        def side_effect(locs):
            p1.df = pd.DataFrame({"Location": ["Loc1"]})
            p1.applied_location_filter = locs
        p1.filter_locations.side_effect = side_effect
        
        collection = ProcessorCollection()
        collection.add_processor(p1)
        
        collection.filter_locations(["Loc1"])
        
        # Should log info because rows were reduced
        assert mock_logger.info.called
        args, kwargs = mock_logger.info.call_args
        assert "Applied location filter" in args[0]
        assert kwargs["before"] == 2
        assert kwargs["after"] == 1

    @patch("ryan_library.processors.tuflow.processor_collection.logger")
    def test_filter_locations_removed_processor(self, mock_logger):
        """Test logging when a processor is removed due to empty df."""
        p1 = MagicMock()
        p1.df = pd.DataFrame({"Location": ["Loc2"]})
        p1.applied_location_filter = None
        
        def side_effect(locs):
            p1.df = pd.DataFrame() # Empty
            p1.applied_location_filter = locs
        p1.filter_locations.side_effect = side_effect
        
        collection = ProcessorCollection()
        collection.add_processor(p1)
        
        collection.filter_locations(["Loc1"])
        
        assert len(collection.processors) == 0
        # Check for removal log
        found = any("Removed" in call.args[0] for call in mock_logger.info.call_args_list)
        assert found

    @patch("ryan_library.processors.tuflow.processor_collection.logger")
    def test_check_duplicates_logging(self, mock_logger):
        """Test logging for duplicates."""
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
        
        collection.check_duplicates()
        
        assert mock_logger.warning.called
        assert "Potential duplicate group" in mock_logger.warning.call_args[0][0]

    @patch("ryan_library.processors.tuflow.processor_collection.logger")
    def test_merge_with_eof_data_logging_missing_cols(self, mock_logger):
        """Test logging when columns are missing during EOF merge."""
        collection = ProcessorCollection()
        
        # Case 1: Source missing Chan ID
        source_df = pd.DataFrame({"Val": [1]})
        eof_df = pd.DataFrame({"Chan ID": ["C1"]})
        
        collection._merge_with_eof_data(source_df, eof_df, source_label="Test", run_code="Run1")
        
        assert mock_logger.debug.called
        found = any("Skipping EOF merge" in call.args[0] for call in mock_logger.debug.call_args_list)
        assert found
        
        # Case 2: EOF missing Chan ID
        source_df = pd.DataFrame({"Chan ID": ["C1"]})
        eof_df = pd.DataFrame({"Val": [1]})
        
        collection._merge_with_eof_data(source_df, eof_df, source_label="Test", run_code="Run1")
        
        assert mock_logger.warning.called
        found = any("missing 'Chan ID'" in call.args[0] for call in mock_logger.warning.call_args_list)
        assert found

    @patch("ryan_library.processors.tuflow.processor_collection.logger")
    def test_merge_chan_and_eof_logging(self, mock_logger):
        """Test logging in static merge function."""
        # Missing Chan ID in one
        chan_df = pd.DataFrame({"Val": [1]})
        eof_df = pd.DataFrame({"Chan ID": ["C1"]})
        
        ProcessorCollection._merge_chan_and_eof(chan_df, eof_df)
        
        assert mock_logger.warning.called
        assert "Chan ID missing" in mock_logger.warning.call_args[0][0]
