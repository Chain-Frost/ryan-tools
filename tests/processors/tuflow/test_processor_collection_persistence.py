import json
import shutil
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from ryan_library.processors.tuflow.processor_collection import ProcessorCollection
from ryan_library.processors.tuflow.base_processor import BaseProcessor


class DummyProcessor(BaseProcessor):
    def process(self):
        pass


class TestProcessorCollectionPersistence:
    @pytest.fixture
    def setup_collection(self):
        """Create a collection with one dummy processor."""
        # Create a real file for the processor (needed for initialization)
        temp_dir = Path(tempfile.mkdtemp())
        csv_file = temp_dir / "test_1d_Q.csv"
        csv_file.touch()

        yield temp_dir
        # cleanup
        try:
            shutil.rmtree(temp_dir)
        except:
            pass

    def test_copy(self):
        coll = ProcessorCollection()
        p1 = MagicMock(spec=BaseProcessor)
        p1.file_name = "test_file.csv"
        p1.processed = True
        p1.df = pd.DataFrame({"A": [1]})
        coll.add_processor(p1)

        coll_copy = coll.copy()

        assert len(coll_copy.processors) == 1
        assert coll_copy.processors[0] is not p1
        pd.testing.assert_frame_equal(coll_copy.processors[0].df, p1.df)

        # Modify copy
        coll_copy.processors[0].df = pd.DataFrame()
        assert not p1.df.empty

    def test_hdf_roundtrip(self, setup_collection):
        """Test to_hdf and from_hdf with mocks."""
        # Mock sys.modules to satisfy 'import tables'
        import sys

        with pytest.MonkeyPatch.context() as m:
            m.setitem(sys.modules, "tables", MagicMock())

            coll = ProcessorCollection()
            p1 = MagicMock(spec=BaseProcessor)
            p1.file_path = Path("mock/path/run_hdf.csv")
            p1.__class__.__name__ = "MockProcessor"
            p1.processor_module = "mock_mod"
            p1.dataformat = "Timeseries"
            p1.data_type = "Q"
            p1.applied_location_filter = None
            p1.applied_entity_filter = None
            p1.df = pd.DataFrame({"Col1": [1, 2]})
            coll.processors.append(p1)

            hdf_path = setup_collection / "data.h5"
            # Ensure file exists check passes after save
            hdf_path.touch()

            # Mock pd.HDFStore context manager
            mock_store = MagicMock()
            mock_store.__enter__.return_value = mock_store

            store_data = {}

            def put_side_effect(key, value, format=None, **kwargs):
                store_data[key] = value

            def get_side_effect(key):
                if key not in store_data:
                    raise KeyError(key)
                return store_data[key]

            def contains_side_effect(key):
                return key in store_data

            mock_store.put.side_effect = put_side_effect
            mock_store.get.side_effect = get_side_effect
            mock_store.__contains__.side_effect = contains_side_effect

            # Mock HDFStore constructor
            with m.context() as m_pd:
                m_pd.setattr(pd, "HDFStore", MagicMock(return_value=mock_store))

                # 1. Test to_hdf
                coll.to_hdf(hdf_path)

                assert "metadata" in store_data
                assert "proc_0000" in store_data

                # Check compression kwargs were passed
                pd.HDFStore.assert_called_with(str(hdf_path), mode="w", complevel=9, complib="blosc:zstd")

                # 2. Test from_hdf
                class MockProcessorClass:
                    def __init__(self, file_path, entity_filter=None):
                        self.file_path = file_path
                        self.processed = False
                        self.applied_location_filter = None
                        self.df = pd.DataFrame()

                original_get = BaseProcessor.get_processor_class

                def get_side_effect_cls(class_name, **kwargs):
                    if class_name == "MockProcessor":
                        return MockProcessorClass
                    return original_get(class_name, **kwargs)

                m_pd.setattr(BaseProcessor, "get_processor_class", get_side_effect_cls)

                loaded = ProcessorCollection.from_hdf(hdf_path)
                assert len(loaded.processors) == 1
                pd.testing.assert_frame_equal(loaded.processors[0].df, p1.df)
