"""Retired HDF5 entrypoints fail explicitly without touching source/cache files."""

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection


def test_copy() -> None:
    collection = ProcessorCollection()
    processor = MagicMock(spec=BaseProcessor)
    processor.file_name = "test_file.csv"
    processor.processed = True
    processor.df = pd.DataFrame({"A": [1]})
    collection.add_processor(processor)

    copied = collection.copy()
    assert len(copied.processors) == 1
    assert copied.processors[0] is not processor
    pd.testing.assert_frame_equal(copied.processors[0].df, processor.df)


@pytest.mark.parametrize("overwrite", [False, True])
def test_retired_hdf_write_raises_without_creating_file(tmp_path: Path, *, overwrite: bool) -> None:
    destination = tmp_path / "nonexistent.h5"
    with pytest.raises(NotImplementedError, match="ryan-tools==26"):
        ProcessorCollection().to_hdf(destination, overwrite=overwrite)
    assert not destination.exists()


def test_retired_hdf_write_preserves_existing_file(tmp_path: Path) -> None:
    destination = tmp_path / "existing.h5"
    destination.write_bytes(b"original-contents")
    with pytest.raises(NotImplementedError, match="2027-03-31"):
        ProcessorCollection().to_hdf(destination, overwrite=True)
    assert destination.read_bytes() == b"original-contents"


@pytest.mark.parametrize("verify_sources", [False, True])
def test_retired_hdf_read_raises_before_opening_file(*, verify_sources: bool) -> None:
    with pytest.raises(NotImplementedError, match="pypi.org/project/ryan-tools/26"):
        ProcessorCollection.from_hdf("nonexistent.h5", locations=["C1"], verify_sources=verify_sources)
