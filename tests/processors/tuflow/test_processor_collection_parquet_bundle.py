"""Native processor collection Parquet-directory persistence and failure regressions."""

from __future__ import annotations

import json
import os
import sys
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
        ["first", "second"], categories=["third", "second", "first"], ordered=True
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
    actual_read = parquet_bundle._read_verified_frame  # pyright: ignore[reportPrivateUsage]

    def checked_read(directory: Path, entry: dict[str, Any]) -> pd.DataFrame:
        called.append(Path(entry["member"]).name)
        assert entry["member"] == "processors/000002.parquet"
        return actual_read(directory, entry)

    monkeypatch.setattr(parquet_bundle, "_read_verified_frame", checked_read)
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

    def interrupted(_source: Path, _destination: Path, _dir_fd: int | None) -> None:
        message = "simulated interrupted manifest replacement"
        raise OSError(message)

    monkeypatch.setattr(parquet_bundle, "_publish_manifest", interrupted)
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


def test_wp04_selected_unusable_processor_and_source_locations_round_trip(tmp_path: Path) -> None:
    source = tmp_path / "Run_01.0p_0060m_TP01_PO.csv"
    source.write_text(
        "Dummy,Time,H,H\nDummy,Time,Dry,Wet\nDummy,0,**********,10\nDummy,1,**********,11\n",
        encoding="utf-8",
    )
    processor = BaseProcessor.from_file(source)
    processor.process()
    assert processor.processed
    collection = ProcessorCollection()
    collection.add_processor(processor)
    collection.filter_locations(["Dry"])
    assert len(collection.processors) == 1
    assert processor.df.empty
    assert processor.selected_unusable
    assert processor.input_health == "INVALID"
    collection.discard_raw_dataframes()

    directory = tmp_path / "wp04-invalid.parquetdir"
    collection.to_parquet(directory)
    restored = ProcessorCollection.from_parquet(directory, locations=["Dry"])
    assert len(restored.processors) == 1
    invalid = restored.processors[0]
    assert invalid.df.empty
    assert invalid.processed
    assert invalid.selected_unusable
    assert invalid.input_health == "INVALID"
    assert "Dry" in invalid.source_entity_presence["Location"]
    assert restored.parquet_selection_diagnostics[0]["status"] == "INVALID"
    assert len(restored.get_processors_by_data_type("PO").processors) == 1

    absent = ProcessorCollection.from_parquet(directory, locations=["Absent"])
    assert not absent.processors
    assert absent.parquet_selection_diagnostics[0]["status"] == "ABSENT"


def test_wp04_reprocessing_refreshes_source_signature_and_entity_evidence(
    collection: ProcessorCollection,
) -> None:
    processor = collection.processors[0]
    previous = processor.source_at_processing
    assert previous is not None
    processor.source_entity_presence = {"Chan ID": ["stale-channel"]}
    source = processor.resolved_file_path
    source.write_text(source.read_text(encoding="utf-8") + "3,2,3\n", encoding="utf-8")
    processor.process()
    assert processor.processed
    assert processor.source_at_processing != previous
    assert "stale-channel" not in processor.source_entity_presence.get("Chan ID", [])


def test_wp04_selected_unusable_health_type_is_validated(collection: ProcessorCollection, tmp_path: Path) -> None:
    directory = tmp_path / "invalid-health.parquetdir"
    collection.to_parquet(directory)
    manifest = _manifest(directory)
    manifest["processors"][0]["health"]["selected_unusable"] = "false"
    (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="health metadata"):
        ProcessorCollection.from_parquet(directory)


def test_empty_processor_without_wp04_invalid_evidence_is_rejected(
    collection: ProcessorCollection, tmp_path: Path
) -> None:
    collection.processors[0].df = pd.DataFrame()
    collection.processors[0].selected_unusable = False
    with pytest.raises(ValueError, match="Only processed"):
        collection.to_parquet(tmp_path / "unproven-empty.parquetdir")


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


def test_competing_writer_cannot_bypass_default_no_clobber(
    collection: ProcessorCollection, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dest = tmp_path / "competing.parquetdir"
    original_stage = parquet_bundle._stage_tables  # pyright: ignore[reportPrivateUsage]

    def race(selected: ProcessorCollection, stage: Path, entries: list[dict[str, Any]]) -> dict[str, Any] | None:
        result = original_stage(selected, stage, entries)
        with monkeypatch.context() as patcher:
            patcher.setattr(parquet_bundle, "_stage_tables", original_stage)
            selected.to_parquet(dest)
        return result

    monkeypatch.setattr(parquet_bundle, "_stage_tables", race)
    with pytest.raises(FileExistsError, match="already exists"):
        collection.to_parquet(dest)
    assert len(ProcessorCollection.from_parquet(dest).processors) == 3
    assert _manifest(dest)["active_generation"] == "."
    assert not list(tmp_path.glob(".competing.parquetdir.*.staging"))


def test_failed_generation_rename_does_not_mask_error_or_leak_lock(
    collection: ProcessorCollection, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dest = tmp_path / "rename.parquetdir"
    collection.to_parquet(dest)
    old_manifest = (dest / "manifest.json").read_bytes()

    def blocked(*_args: object, **_kwargs: object) -> None:
        message = "atomic rename refused"
        raise OSError(message)

    rename_symbol = "_publish_new_directory" if os.name == "nt" else "_publish_directory_in_fd"
    monkeypatch.setattr(parquet_bundle, rename_symbol, blocked)
    with pytest.raises(OSError, match="atomic rename refused"):
        collection.to_parquet(dest, overwrite=True)
    assert (dest / "manifest.json").read_bytes() == old_manifest
    assert not (dest / ".publish.lock").exists()
    assert not list(tmp_path.glob(".rename.parquetdir.*.staging"))
    assert not list((dest / ".versions").glob("*"))
    assert len(ProcessorCollection.from_parquet(dest).processors) == 3


@pytest.mark.skipif(sys.platform != "linux", reason="directory-handle rename guard is Linux-specific")
def test_destination_swap_during_overwrite_cannot_mutate_other_dataset(
    collection: ProcessorCollection, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dest = tmp_path / "victim.parquetdir"
    other = tmp_path / "other.parquetdir"
    backup = tmp_path / "original-victim.parquetdir"
    collection.to_parquet(dest)
    collection.to_parquet(other)
    victim_before = (dest / "manifest.json").read_bytes()
    other_before = (other / "manifest.json").read_bytes()
    original_manifest = parquet_bundle._manifest_bytes  # pyright: ignore[reportPrivateUsage]

    def swap(*args: Any, **kwargs: Any) -> bytes:
        result = original_manifest(*args, **kwargs)
        dest.rename(backup)
        dest.symlink_to(other, target_is_directory=True)
        return result

    monkeypatch.setattr(parquet_bundle, "_manifest_bytes", swap)
    with pytest.raises(ValueError, match="Linked Parquet publication target"):
        collection.to_parquet(dest, overwrite=True)
    assert (backup / "manifest.json").read_bytes() == victim_before
    assert (other / "manifest.json").read_bytes() == other_before
    assert not (backup / ".publish.lock").exists()
    assert not list(tmp_path.glob(".victim.parquetdir.*.staging"))


@pytest.mark.skipif(os.name != "nt", reason="Windows directory SHARE_DELETE semantics only")
def test_windows_publication_guard_blocks_directory_swap(collection: ProcessorCollection, tmp_path: Path) -> None:
    dest = tmp_path / "held.parquetdir"
    collection.to_parquet(dest)
    backup = tmp_path / "moved.parquetdir"
    with parquet_bundle._guard_publication_directory(dest):  # pyright: ignore[reportPrivateUsage]
        with pytest.raises(OSError):
            dest.rename(backup)
    assert dest.is_dir()
    assert not backup.exists()


def _eof_alias_snapshot(tmp_path: Path, *, ambiguous: bool) -> Path:
    stem = "Selection_01.00p_00120m_TP01"
    full_ids = ["Bridge 159.975", "Bridge 159.979"] if ambiguous else ["Bridge 159.975"]
    cmx_file = tmp_path / f"{stem}_1d_Cmx.csv"
    cmx_file.write_text(
        "Chan ID,Time Qmax,Qmax,Vmax,Time Vmax\n"
        + "".join(f"{label},0.5,{index * 5.0},1.5,0.75\n" for index, label in enumerate(full_ids, 1)),
        encoding="utf-8",
    )
    eof_file = tmp_path / f"{stem}.eof"
    eof_file.write_text(
        "CULVERT AND PIPE DATA\n"
        "Channel  Type  Num_barrels  US_Invert  DS_Invert  US_Obvert  DS_Obvert  Length  Slope  "
        "Mannings_n  Diam_Width  Height  Inlet_Height  Inlet_Width  "
        "Entry Loss  Exit Loss  Fixed Loss  Ent/Exit Losses\n"
        "Bridge 159.97  C  1  10  9  12  11  100  0.01  0.013  2  2  2  2  0.5  1  0  1.5\n\n",
        encoding="utf-8",
    )
    collection = ProcessorCollection()
    for source in (cmx_file, eof_file):
        processor = BaseProcessor.from_file(source)
        processor.process()
        assert processor.processed
        assert not processor.df.empty
        collection.add_processor(processor)
    destination = tmp_path / "eof-alias.parquetdir"
    collection.to_parquet(destination)
    return destination


def test_eof_alias_uses_unloaded_full_reference_inventory(tmp_path: Path) -> None:
    dataset = _eof_alias_snapshot(tmp_path, ambiguous=False)
    selected = ProcessorCollection.from_parquet(dataset, data_types=["EOF"], locations=["Bridge 159.975"])
    assert len(selected.processors) == 1
    eof = selected.processors[0]
    assert eof.data_type == "EOF"
    assert eof.df["Chan ID"].astype(str).tolist() == ["Bridge 159.975"]
    assert eof.df["Height"].tolist() == [2.0]


def test_eof_alias_ambiguity_retained_with_selective_restore(tmp_path: Path) -> None:
    dataset = _eof_alias_snapshot(tmp_path, ambiguous=True)
    selected = ProcessorCollection.from_parquet(dataset, data_types=["EOF"], locations=["Bridge 159.975"])
    assert not selected.processors
    assert selected.parquet_selection_diagnostics[-1]["status"] == "ABSENT"
    full = ProcessorCollection.from_parquet(dataset, locations=["Bridge 159.975"])
    assert len(full.processors) == 1
    assert full.processors[0].data_type == "Cmx"


@pytest.mark.skipif(sys.platform != "linux", reason="POSIX generation identity check only")
def test_replaced_versions_directory_never_publishes_broken_manifest(
    collection: ProcessorCollection, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    destination = tmp_path / "versions.parquetdir"
    collection.to_parquet(destination)
    original = (destination / "manifest.json").read_bytes()
    replaced = destination / ".versions-moved"
    actual_manifest = parquet_bundle._manifest_bytes  # pyright: ignore[reportPrivateUsage]

    def swapped(*args: Any, **kwargs: Any) -> bytes:
        encoded = actual_manifest(*args, **kwargs)
        (destination / ".versions").rename(replaced)
        (destination / ".versions").mkdir()
        return encoded

    monkeypatch.setattr(parquet_bundle, "_manifest_bytes", swapped)
    with pytest.raises(ValueError, match="version folder identity changed"):
        collection.to_parquet(destination, overwrite=True)
    assert (destination / "manifest.json").read_bytes() == original
    assert not (destination / ".publish.lock").exists()
    (destination / ".versions").rmdir()
    replaced.rename(destination / ".versions")
    assert len(ProcessorCollection.from_parquet(destination).processors) == 3


@pytest.mark.parametrize("lookup", [False, True])
def test_member_replacement_after_verification_cannot_change_loaded_values(
    collection: ProcessorCollection, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, lookup: bool
) -> None:
    if lookup:
        collection.compact_basic_info_columns()
    destination = tmp_path / "replacement.parquetdir"
    collection.to_parquet(destination)
    member = destination / ("basic_info_lookup.parquet" if lookup else "processors/000000.parquet")
    actual_read = pd.read_parquet
    replacement = actual_read(member)
    if lookup:
        replacement["file"] = "changed.csv"
    else:
        replacement["Q"] = replacement["Q"] + 100
    calls = 0

    def replace_before_arrow_read(file: Any, **kwargs: Any) -> pd.DataFrame:
        nonlocal calls
        calls += 1
        # This is after checksum verification but before Arrow consumes bytes.
        # Replacing the public member must not affect the owned verified handle.
        if calls == (4 if lookup else 1):
            replacement.to_parquet(member, engine="pyarrow", index=None)
        return actual_read(file, **kwargs)

    monkeypatch.setattr(parquet_bundle.pd, "read_parquet", replace_before_arrow_read)
    restored = ProcessorCollection.from_parquet(destination)
    assert calls == 3 + int(lookup)
    for original, actual in zip(collection.processors, restored.processors, strict=True):
        pd.testing.assert_frame_equal(actual.df, original.df)
    if lookup:
        assert restored.basic_info_lookup is not None
        assert collection.basic_info_lookup is not None
        pd.testing.assert_frame_equal(restored.basic_info_lookup, collection.basic_info_lookup)
