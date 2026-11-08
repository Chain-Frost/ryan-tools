"""Single-file Parquet collection bundle regression tests (real on-disk)."""

from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pandas as pd
import pytest

from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection


@pytest.fixture
def collection(tmp_path: Path) -> ProcessorCollection:
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

    result = ProcessorCollection()
    for path in (q_path, po_path, pomm_path):
        processor = BaseProcessor.from_file(path)
        processor.process()
        assert processor.processed, path
        assert not processor.df.empty
        result.add_processor(processor)
    q_processor = result.processors[0]
    q_processor.df["optional_int"] = pd.Series([1, pd.NA], index=q_processor.df.index, dtype="Int64")
    q_processor.df["optional_real"] = pd.Series([2.5, pd.NA], index=q_processor.df.index, dtype="Float64")
    q_processor.df["optional_text"] = pd.Series(["alpha", pd.NA], index=q_processor.df.index, dtype="string")
    q_processor.df["optional_bool"] = pd.Series([True, pd.NA], index=q_processor.df.index, dtype="boolean")
    q_processor.df["ordered_category"] = pd.Categorical(
        ["first", "second"], categories=["second", "first"], ordered=True
    )
    q_processor.applied_location_filter = frozenset({"C1"})
    q_processor.applied_entity_filter = frozenset({"C1"})
    return result


def test_parquet_bundle_restores_heterogeneous_processors_and_dtypes(
    collection: ProcessorCollection, tmp_path: Path
) -> None:
    path = tmp_path / "collection.rtparquet"
    collection.to_parquet(path)
    assert zipfile.is_zipfile(path)
    with zipfile.ZipFile(path) as archive:
        assert set(archive.namelist()) == {
            "processors/000000.parquet",
            "processors/000001.parquet",
            "processors/000002.parquet",
            "manifest.json",
        }
    loaded = ProcessorCollection.from_parquet(path)
    assert len(loaded.processors) == len(collection.processors)
    for actual, original in zip(loaded.processors, collection.processors, strict=True):
        assert type(actual) is type(original)
        assert actual.data_type == original.data_type
        assert actual.name_parser.raw_run_code == original.name_parser.raw_run_code
        assert actual.processed
        assert actual.resolved_file_path == original.resolved_file_path
        pd.testing.assert_frame_equal(actual.df, original.df)
    assert loaded.processors[0].applied_location_filter == frozenset({"C1"})
    assert loaded.processors[0].applied_entity_filter == frozenset({"C1"})


def test_parquet_bundle_restores_compacted_lookup(collection: ProcessorCollection, tmp_path: Path) -> None:
    original_lookup = collection.compact_basic_info_columns()
    original = collection.attach_basic_info(collection.combine_raw(), drop_id=True)
    path = tmp_path / "lookup.rtparquet"
    collection.to_parquet(path)

    loaded = ProcessorCollection.from_parquet(path)
    assert loaded.basic_info_lookup is not None
    pd.testing.assert_frame_equal(loaded.basic_info_lookup, original_lookup)
    restored = loaded.attach_basic_info(loaded.combine_raw(), drop_id=True)
    pd.testing.assert_frame_equal(restored, original)


def test_parquet_bundle_restricts_data_types_and_locations(
    collection: ProcessorCollection, tmp_path: Path
) -> None:
    path = tmp_path / "filtered.rtparquet"
    collection.to_parquet(path)
    restored = ProcessorCollection.from_parquet(path, data_types=["POMM"], locations=["Loc1"])
    assert len(restored.processors) == 1
    assert restored.processors[0].data_type == "POMM"
    assert set(restored.processors[0].df["Location"].astype(str)) == {"Loc1"}


def test_parquet_bundle_source_moved_or_changed_is_explicit(
    collection: ProcessorCollection, tmp_path: Path
) -> None:
    path = tmp_path / "source.rtparquet"
    collection.to_parquet(path)
    source = collection.processors[0].resolved_file_path
    source.write_text(source.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="source has changed"):
        ProcessorCollection.from_parquet(path)
    restored = ProcessorCollection.from_parquet(path, verify_sources=False)
    assert len(restored.processors) == 3
    source.unlink()
    with pytest.raises(FileNotFoundError):
        ProcessorCollection.from_parquet(path)
    assert len(ProcessorCollection.from_parquet(path, verify_sources=False).processors) == 3


def test_parquet_bundle_no_clobber_and_explicit_overwrite(collection: ProcessorCollection, tmp_path: Path) -> None:
    path = tmp_path / "existing.rtparquet"
    collection.to_parquet(path)
    original = path.read_bytes()
    with pytest.raises(FileExistsError, match="already exists"):
        collection.to_parquet(path)
    assert path.read_bytes() == original
    collection.processors[0].df["Q"] = 123.0
    collection.to_parquet(path, overwrite=True)
    assert ProcessorCollection.from_parquet(path).processors[0].df.iloc[0]["Q"] == 123.0
    assert not list(tmp_path.glob(".existing.rtparquet.*.part"))


def test_parquet_bundle_atomic_failure_preserves_existing(
    collection: ProcessorCollection, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "safe.rtparquet"
    collection.to_parquet(path)
    original = path.read_bytes()

    def failing_signature(_source: Path) -> dict[str, object]:
        message = "simulated interrupted write"
        raise OSError(message)

    from ryan_library.processors.tuflow import parquet_bundle

    monkeypatch.setattr(parquet_bundle, "_source_signature", failing_signature)
    with pytest.raises(OSError, match="simulated"):
        collection.to_parquet(path, overwrite=True)
    assert path.read_bytes() == original
    assert not list(tmp_path.glob(".safe.rtparquet.*.part"))


def test_parquet_bundle_corrupt_manifest_member_and_extra_entry(
    collection: ProcessorCollection, tmp_path: Path
) -> None:
    path = tmp_path / "corrupt.rtparquet"
    collection.to_parquet(path)
    with zipfile.ZipFile(path, "a") as archive:
        archive.writestr("unexpected.dat", b"junk")
    with pytest.raises(ValueError, match="unexpected members"):
        ProcessorCollection.from_parquet(path)

    second = tmp_path / "changed.rtparquet"
    collection.to_parquet(second)
    with zipfile.ZipFile(second, "a") as archive:
        archive.writestr("processors/000000.parquet", b"modified")
    with pytest.raises(ValueError, match="duplicate members"):
        ProcessorCollection.from_parquet(second)

    third = tmp_path / "bad-manifest.rtparquet"
    collection.to_parquet(third)
    with zipfile.ZipFile(third, "a") as archive:
        archive.writestr("manifest.json", json.dumps({"format": "wrong"}))
    with pytest.raises(ValueError, match="duplicate members"):
        ProcessorCollection.from_parquet(third)


def test_parquet_bundle_invalid_extension_and_path(collection: ProcessorCollection, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="rtparquet"):
        collection.to_parquet(tmp_path / "data.parquet")
    with pytest.raises(ValueError, match="rtparquet"):
        ProcessorCollection.from_parquet(tmp_path / "data.parquet")
