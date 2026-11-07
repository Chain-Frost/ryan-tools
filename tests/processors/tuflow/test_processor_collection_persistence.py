"""Real on-disk processor snapshot and atomic publication regression tests."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from contextlib import AbstractContextManager
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection


@pytest.fixture
def collection(tmp_path: Path, change_cwd: Callable[[Path], AbstractContextManager[None]]) -> ProcessorCollection:
    """Exercise the real Q, PO and POMM factories instead of mocked HDFStore."""
    pytest.importorskip("tables", reason="Install ryan-tools[cache] for HDF persistence tests")
    pytest.importorskip("pyarrow")

    q_path = tmp_path / "Run_01.0p_0060m_TP01_1d_Q.csv"
    q_path.write_text("Index,Time,Q C1 [run]\n1,0,1\n2,1,2\n", encoding="utf-8")
    po_path = tmp_path / "Run_01.0p_0060m_TP01_PO.csv"
    po_path.write_text("Dummy,Time,H\nDummy,Time,Loc1\nDummy,0,10\nDummy,1,11\n", encoding="utf-8")
    pomm_path = tmp_path / "Run_01.0p_0060m_TP01_POMM.csv"
    pomm_raw = {
        0: ["Run_01.0p_0060m_TP01"] * 6,
        1: [
            "Location",
            "Time",
            "Maximum (Extracted from Time Series)",
            "Time of Maximum",
            "Minimum (Extracted From Time Series)",
            "Time of Minimum",
        ],
        2: ["H", "Loc1", 12.5, 1, 10.0, 0],
        3: ["Q", "Loc2", 4.0, 1, -1.0, 0],
    }
    pd.DataFrame(pomm_raw).to_csv(pomm_path, header=False, index=False)

    processors: list[BaseProcessor] = []
    with change_cwd(tmp_path):
        for path in (q_path, po_path, pomm_path):
            processor = BaseProcessor.from_file(path)
            processor.process()
            assert processor.processed, f"Factory failed for {path}"
            assert not processor.df.empty
            processors.append(processor)

    q_processor = processors[0]
    assert len(q_processor.df) == 2
    q_processor.df["optional_integer"] = pd.Series([1, pd.NA], index=q_processor.df.index, dtype="Int64")
    q_processor.df["optional_real"] = pd.Series([2.5, pd.NA], index=q_processor.df.index, dtype="Float64")
    q_processor.df["optional_string"] = pd.Series(["alpha", pd.NA], index=q_processor.df.index, dtype="string")
    q_processor.df["optional_flag"] = pd.Series([True, pd.NA], index=q_processor.df.index, dtype="boolean")
    q_processor.df["ordered_category"] = pd.Categorical(
        ["first", "second"], categories=["second", "first"], ordered=True
    )
    q_processor.applied_location_filter = frozenset({"C1"})
    q_processor.applied_entity_filter = frozenset({"C1"})

    result = ProcessorCollection()
    for processor in processors:
        result.add_processor(processor)
    assert len(result.processors) == 3
    return result


def test_copy() -> None:
    coll = ProcessorCollection()
    processor = MagicMock(spec=BaseProcessor)
    processor.file_name = "test_file.csv"
    processor.processed = True
    processor.df = pd.DataFrame({"A": [1]})
    coll.add_processor(processor)

    copied = coll.copy()
    assert len(copied.processors) == 1
    assert copied.processors[0] is not processor
    pd.testing.assert_frame_equal(copied.processors[0].df, processor.df)


def test_hdf_roundtrip_real_processors_and_dtypes(collection: ProcessorCollection, tmp_path: Path) -> None:
    expected = [processor.df.copy(deep=True) for processor in collection.processors]
    cache = tmp_path / "snapshots" / "processors.h5"
    collection.to_hdf(cache)

    result = ProcessorCollection.from_hdf(cache)
    assert len(result.processors) == len(collection.processors)
    for actual, original, expected_df in zip(result.processors, collection.processors, expected, strict=True):
        assert type(actual) is type(original)
        assert actual.file_path == original.file_path
        assert actual.data_type == original.data_type
        assert actual.name_parser.raw_run_code == original.name_parser.raw_run_code
        assert actual.processed is True
        pd.testing.assert_frame_equal(actual.df, expected_df)
    assert result.processors[0].applied_location_filter == frozenset({"C1"})
    assert result.processors[0].applied_entity_filter == frozenset({"C1"})


def test_hdf_compacted_basic_info_roundtrip(collection: ProcessorCollection, tmp_path: Path) -> None:
    lookup = collection.compact_basic_info_columns()
    assert not lookup.empty
    original = collection.combine_raw()
    before = collection.attach_basic_info(original, drop_id=True)
    path = tmp_path / "compact.h5"
    collection.to_hdf(path)

    loaded = ProcessorCollection.from_hdf(path)
    assert loaded.basic_info_lookup is not None
    pd.testing.assert_frame_equal(loaded.basic_info_lookup, lookup)
    after = loaded.attach_basic_info(loaded.combine_raw(), drop_id=True)
    pd.testing.assert_frame_equal(after, before)


def test_hdf_existing_cache_survives_failed_overwrite(collection: ProcessorCollection, tmp_path: Path) -> None:
    path = tmp_path / "existing.h5"
    collection.to_hdf(path)
    before = hashlib.sha256(path.read_bytes()).hexdigest()

    with pytest.raises(FileExistsError, match="already exists"):
        collection.to_hdf(path)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before

    with patch.object(ProcessorCollection, "_hdf_frame_bytes", side_effect=RuntimeError("simulated failure")):
        with pytest.raises(RuntimeError, match="simulated failure"):
            collection.to_hdf(path, overwrite=True)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
    assert not list(tmp_path.glob(".existing.h5.*.part"))

    collection.processors[0].df.iloc[0, collection.processors[0].df.columns.get_loc("Q")] = 123.0
    collection.to_hdf(path, overwrite=True)
    assert ProcessorCollection.from_hdf(path).processors[0].df.iloc[0]["Q"] == 123.0


def test_hdf_cache_detects_changed_source(collection: ProcessorCollection, tmp_path: Path) -> None:
    cache = tmp_path / "source.h5"
    collection.to_hdf(cache)
    source = collection.processors[0].file_path
    source.write_text(source.read_text(encoding="utf-8") + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="source has changed"):
        ProcessorCollection.from_hdf(cache)
    historical = ProcessorCollection.from_hdf(cache, verify_sources=False)
    assert len(historical.processors) == 3


def test_hdf_missing_member_causes_explicit_failure(collection: ProcessorCollection, tmp_path: Path) -> None:
    tables = pytest.importorskip("tables")
    cache = tmp_path / "missing-member.h5"
    collection.to_hdf(cache)
    with tables.open_file(cache, mode="a") as store:
        store.remove_node("/proc_000000")
    with pytest.raises(ValueError, match="missing required member"):
        ProcessorCollection.from_hdf(cache)


def test_hdf_corrupt_manifest_and_stale_registry_are_rejected(collection: ProcessorCollection, tmp_path: Path) -> None:
    tables = pytest.importorskip("tables")
    cache = tmp_path / "bad-manifest.h5"
    collection.to_hdf(cache)
    with patch.object(ProcessorCollection, "_hdf_registry_digest", return_value="invalid"):
        with pytest.raises(ValueError, match="registry changed"):
            ProcessorCollection.from_hdf(cache)
    with tables.open_file(cache, mode="a") as store:
        store.get_node("/manifest")[:] = 0
    with pytest.raises(ValueError, match="manifest is corrupt"):
        ProcessorCollection.from_hdf(cache)


def test_hdf_alias_source_is_rejected(collection: ProcessorCollection) -> None:
    with pytest.raises(ValueError, match="aliases a processor source"):
        collection.to_hdf(collection.processors[0].file_path, overwrite=True)
