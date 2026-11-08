"""Native processor collection Parquet-directory persistence and failure regressions."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from ryan_library.processors.tuflow import parquet_bundle
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


def _manifest(directory: Path) -> dict[str, Any]:
    return json.loads((directory / "manifest.json").read_text(encoding="utf-8"))


def test_parquet_directory_restores_real_heterogeneous_processors_and_dtypes(
    collection: ProcessorCollection, tmp_path: Path
) -> None:
    path = tmp_path / "collection.parquetdir"
    collection.to_parquet(path)
    assert path.is_dir()
    assert {p.relative_to(path).as_posix() for p in path.rglob("*.parquet")} == {
        "processors/000000.parquet",
        "processors/000001.parquet",
        "processors/000002.parquet",
    }
    manifest = _manifest(path)
    assert manifest["format"] == "ryan-tools-tuflow-parquet-directory"
    assert manifest["version"] == 2
    assert manifest["sources_verified_at_export"] is True
    loaded = ProcessorCollection.from_parquet(path)
    assert len(loaded.processors) == len(collection.processors)
    for actual, original in zip(loaded.processors, collection.processors, strict=True):
        assert type(actual) is type(original)
        assert actual.data_type == original.data_type
        assert actual.name_parser.raw_run_code == original.name_parser.raw_run_code
        assert actual.processed
        assert actual.resolved_file_path == original.resolved_file_path
        assert actual.source_at_processing == original.source_at_processing
        pd.testing.assert_frame_equal(actual.df, original.df)
    assert loaded.processors[0].applied_location_filter == frozenset({"C1"})
    assert loaded.processors[0].applied_entity_filter == frozenset({"C1"})


def test_directory_restores_compacted_lookup(collection: ProcessorCollection, tmp_path: Path) -> None:
    original_lookup = collection.compact_basic_info_columns()
    original = collection.attach_basic_info(collection.combine_raw(), drop_id=True)
    path = tmp_path / "lookup.parquetdir"
    collection.to_parquet(path)
    assert (path / "basic_info_lookup.parquet").is_file()
    loaded = ProcessorCollection.from_parquet(path)
    assert loaded.basic_info_lookup is not None
    pd.testing.assert_frame_equal(loaded.basic_info_lookup, original_lookup)
    restored = loaded.attach_basic_info(loaded.combine_raw(), drop_id=True)
    pd.testing.assert_frame_equal(restored, original)


def test_directory_selects_type_without_loading_other_tables(
    collection: ProcessorCollection, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "filtered.parquetdir"
    collection.to_parquet(path)
    called: list[str] = []
    actual_read = pd.read_parquet

    def checked_read(file: Path, **kwargs: Any) -> pd.DataFrame:
        called.append(file.name)
        assert file.name == "000002.parquet"
        return actual_read(file, **kwargs)

    monkeypatch.setattr(parquet_bundle.pd, "read_parquet", checked_read)
    restored = ProcessorCollection.from_parquet(path, data_types=["POMM"], locations=["Loc1"])
    assert called == ["000002.parquet"]
    assert len(restored.processors) == 1
    assert restored.processors[0].data_type == "POMM"
    assert set(restored.processors[0].df["Location"].astype(str)) == {"Loc1"}


def test_source_modified_after_processing_is_not_exported_as_fresh(
    collection: ProcessorCollection, tmp_path: Path
) -> None:
    source = collection.processors[0].resolved_file_path
    old_signature = collection.processors[0].source_at_processing
    source.write_text(source.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="source has changed"):
        collection.to_parquet(tmp_path / "stale.parquetdir")
    assert collection.processors[0].source_at_processing == old_signature


def test_historical_reexport_keeps_original_signature(collection: ProcessorCollection, tmp_path: Path) -> None:
    path = tmp_path / "source.parquetdir"
    collection.to_parquet(path)
    recorded = _manifest(path)["processors"][0]["source"]
    source = collection.processors[0].resolved_file_path
    source.write_text(source.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="source has changed"):
        ProcessorCollection.from_parquet(path)
    historical = ProcessorCollection.from_parquet(path, verify_sources=False)
    assert historical.processors[0].source_at_processing == recorded
    with pytest.raises(ValueError, match="source has changed"):
        historical.to_parquet(tmp_path / "new.parquetdir")
    copy = tmp_path / "archived.parquetdir"
    historical.to_parquet(copy, verify_sources=False)
    assert _manifest(copy)["processors"][0]["source"] == recorded
    assert _manifest(copy)["sources_verified_at_export"] is False
    source.unlink()
    with pytest.raises(FileNotFoundError):
        ProcessorCollection.from_parquet(path)
    assert len(ProcessorCollection.from_parquet(copy, verify_sources=False).processors) == 3


def test_directory_no_clobber_overwrite_uses_immutable_generation(
    collection: ProcessorCollection, tmp_path: Path
) -> None:
    path = tmp_path / "existing.parquetdir"
    collection.to_parquet(path)
    first_manifest = (path / "manifest.json").read_bytes()
    first_data = (path / "processors" / "000000.parquet").read_bytes()
    with pytest.raises(FileExistsError, match="already exists"):
        collection.to_parquet(path)
    assert (path / "manifest.json").read_bytes() == first_manifest
    collection.processors[0].df["Q"] = 123.0
    collection.to_parquet(path, overwrite=True)
    manifest = _manifest(path)
    assert str(manifest["active_generation"]).startswith(".versions/")
    assert (path / "processors" / "000000.parquet").read_bytes() == first_data
    assert (path / "manifest.json").read_bytes() != first_manifest
    assert ProcessorCollection.from_parquet(path).processors[0].df.iloc[0]["Q"] == 123.0
    assert not list(tmp_path.glob(".existing.parquetdir.*.staging"))
    assert not (path / ".publish.lock").exists()
    assert not list(path.glob(".manifest-*.tmp"))


def test_interrupted_overwrite_leaves_original_readable(
    collection: ProcessorCollection, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "safe.parquetdir"
    collection.to_parquet(path)
    original = (path / "manifest.json").read_bytes()

    def interrupted(_source: Path, _destination: Path) -> None:
        message = "simulated interrupted manifest replacement"
        raise OSError(message)

    monkeypatch.setattr(Path, "replace", interrupted)
    with pytest.raises(OSError, match="simulated"):
        collection.to_parquet(path, overwrite=True)
    assert (path / "manifest.json").read_bytes() == original
    assert len(ProcessorCollection.from_parquet(path).processors) == 3
    assert not list(tmp_path.glob(".safe.parquetdir.*.staging"))
    assert not list((path / ".versions").glob("*"))
    assert not list(path.glob(".manifest-*.tmp"))
    assert not (path / ".publish.lock").exists()


def test_directory_validation_rejects_corruption_and_extra_members(
    collection: ProcessorCollection, tmp_path: Path
) -> None:
    path = tmp_path / "corrupt.parquetdir"
    collection.to_parquet(path)
    (path / "unexpected.dat").write_bytes(b"junk")
    with pytest.raises(ValueError, match="unexpected root"):
        ProcessorCollection.from_parquet(path)
    (path / "unexpected.dat").unlink()
    member = path / "processors" / "000000.parquet"
    member.write_bytes(member.read_bytes() + b"tampering")
    with pytest.raises(ValueError, match="checksum mismatch"):
        ProcessorCollection.from_parquet(path)


@pytest.mark.parametrize("invalid", ["../escape.parquet", "processors/000000.parquet"])
def test_directory_validation_rejects_bad_and_duplicate_paths(
    collection: ProcessorCollection, tmp_path: Path, invalid: str
) -> None:
    path = tmp_path / "bad.parquetdir"
    collection.to_parquet(path)
    manifest = _manifest(path)
    manifest["processors"][1]["member"] = invalid
    (path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="processor member path"):
        ProcessorCollection.from_parquet(path)


def test_directory_rejects_semantics_mismatch(collection: ProcessorCollection, tmp_path: Path) -> None:
    path = tmp_path / "semantics.parquetdir"
    collection.to_parquet(path)
    manifest = _manifest(path)
    manifest["processing_semantics_sha256"] = "0" * 64
    (path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="processing semantics"):
        ProcessorCollection.from_parquet(path, verify_sources=False)


def test_invalid_source_entity_is_not_misreported_as_absent(tmp_path: Path) -> None:
    path = tmp_path / "Run_01.0p_0060m_TP01_PO.csv"
    path.write_text(
        "Dummy,Time,H,H\nDummy,Time,Dry,Wet\nDummy,0,**********,10\nDummy,1,**********,11\n",
        encoding="utf-8",
    )
    processor = BaseProcessor.from_file(path)
    processor.process()
    assert processor.processed
    collection = ProcessorCollection()
    collection.add_processor(processor)
    dest = tmp_path / "entities.parquetdir"
    collection.to_parquet(dest)
    dry = ProcessorCollection.from_parquet(dest, locations=["Dry"])
    assert not dry.processors
    assert dry.parquet_selection_diagnostics[0]["status"] == "INVALID"
    missing = ProcessorCollection.from_parquet(dest, locations=["Absent"])
    assert not missing.processors
    assert missing.parquet_selection_diagnostics[0]["status"] == "ABSENT"
    wet = ProcessorCollection.from_parquet(dest, locations=["Wet"])
    assert len(wet.processors) == 1
    assert wet.parquet_selection_diagnostics[0]["status"] == "SELECTED"


def test_directory_requires_directory_not_single_parquet(collection: ProcessorCollection, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="directory"):
        collection.to_parquet(tmp_path / "results.parquet")
    with pytest.raises(ValueError, match="directory"):
        collection.to_parquet(tmp_path / "results.rtparquet")
    with pytest.raises(ValueError, match="directory"):
        ProcessorCollection.from_parquet(tmp_path / "results.parquet")
    empty = tmp_path / "occupied.parquetdir"
    empty.mkdir()
    with pytest.raises(FileExistsError):
        collection.to_parquet(empty)
    assert not list(empty.iterdir())


def test_hydraulic_health_and_source_presence_survive_rehydration(
    collection: ProcessorCollection, tmp_path: Path
) -> None:
    processor = collection.processors[1]
    processor.input_health = "PARTIAL"
    processor.rejected_samples = 4
    processor.rejection_reasons = {"no_data": 4}
    collection.discard_raw_dataframes()
    dest = tmp_path / "health.parquetdir"
    collection.to_parquet(dest)
    restored = ProcessorCollection.from_parquet(dest)
    selected = restored.processors[1]
    assert selected.input_health == "PARTIAL"
    assert selected.rejected_samples == 4
    assert selected.rejection_reasons == {"no_data": 4}
    assert selected.source_at_processing == processor.source_at_processing
    assert selected.source_entity_presence["Location"] == ["Loc1"]


def test_orphan_staging_is_not_published(collection: ProcessorCollection, tmp_path: Path) -> None:
    dest = tmp_path / "stable.parquetdir"
    collection.to_parquet(dest)
    leftover = tmp_path / ".stable.parquetdir.crashed.staging"
    leftover.mkdir()
    (leftover / "partial").write_text("incomplete", encoding="utf-8")
    assert len(ProcessorCollection.from_parquet(dest).processors) == 3
    collection.to_parquet(dest, overwrite=True)
    assert leftover.is_dir()
    assert (leftover / "partial").read_text(encoding="utf-8") == "incomplete"


def test_missing_member_and_missing_source_fail_closed(collection: ProcessorCollection, tmp_path: Path) -> None:
    dest = tmp_path / "missing.parquetdir"
    collection.to_parquet(dest)
    original_source = collection.processors[0].resolved_file_path
    original_bytes = original_source.read_bytes()
    (dest / "processors" / "000002.parquet").unlink()
    with pytest.raises(ValueError, match="missing or unexpected members"):
        ProcessorCollection.from_parquet(dest)
    assert original_source.read_bytes() == original_bytes
