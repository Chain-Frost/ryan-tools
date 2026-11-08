"""Portable Parquet-directory snapshots for native TUFLOW processor collections.

A dataset is an ordinary directory with a versioned manifest.json and one
ZSTD-compressed Parquet table per heterogeneous processor. Overwrites publish
immutable generations by atomically replacing only the small manifest pointer.
Neither pickle nor a custom archive/container format is involved.
"""

from __future__ import annotations

import ctypes
import errno
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
import uuid
from collections.abc import Collection
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import numpy as np
import pandas as pd
from pandas import DataFrame, MultiIndex

from .base_processor import BaseProcessor
from .source_fingerprint import source_signature as _source_signature

if TYPE_CHECKING:
    from .processor_collection import ProcessorCollection

_FORMAT = "ryan-tools-tuflow-parquet-directory"
_FORMAT_VERSION = 2
_MANIFEST = "manifest.json"
_LOOKUP = "basic_info_lookup.parquet"
_VERSIONS = ".versions"
_MAX_MANIFEST_SIZE = 16 * 1024 * 1024
_CHUNK_SIZE = 1024 * 1024
_GENERATION_RE = re.compile(r"\.versions/[0-9a-f]{32}\Z")


def _registry_digest() -> str:
    registry = Path(__file__).parents[2] / "classes" / "tuflow_results_validation_and_datatypes.json"
    return hashlib.sha256(registry.read_bytes()).hexdigest()


def _semantics_digest() -> str:
    """Conservatively invalidate a snapshot whenever hydraulic processing code changes."""
    library = Path(__file__).parents[2]
    files = list((library / "processors" / "tuflow").rglob("*.py"))
    files.extend(
        [
            library / "functions" / "tuflow" / "sample_validity.py",
            library / "functions" / "dataframe_helpers.py",
            library / "classes" / "tuflow_string_classes.py",
            library / "classes" / "suffixes_and_dtypes.py",
        ]
    )
    digest = hashlib.sha256(b"ryan-tools-processed-tuflow-semantics-v2\0")
    for file in sorted(set(files)):
        if not file.is_file():
            continue  # Optional WP04 helper is absent until that work pack is integrated.
        digest.update(file.relative_to(library).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(hashlib.sha256(file.read_bytes()).digest())
    return digest.hexdigest()


def _portable_frame(frame: DataFrame) -> DataFrame:
    """Make NumPy scalar column-axis labels compatible with Arrow pandas metadata."""
    portable = frame.copy(deep=False)
    if isinstance(portable.columns, MultiIndex):
        axis_names = cast("list[object]", portable.columns.names)
        names: list[str | None] = [str(name) if name is not None else None for name in axis_names]
        portable.columns = portable.columns.set_names(names)
    else:
        column_name: object = portable.columns.name
        if isinstance(column_name, np.generic):
            portable.columns.name = str(column_name)
    return portable


def _frame_contract(frame: DataFrame) -> dict[str, object]:
    index_dtypes = (
        [str(level.dtype) for level in frame.index.levels]
        if isinstance(frame.index, MultiIndex)
        else [str(frame.index.dtype)]
    )
    return {
        "columns": [str(column) for column in frame.columns],
        "dtypes": [str(dtype) for dtype in frame.dtypes],
        "categorical_ordered": [
            dtype.ordered if isinstance(dtype, pd.CategoricalDtype) else None for dtype in frame.dtypes
        ],
        "index_kind": type(frame.index).__name__,
        "index_names": [str(name) if name is not None else None for name in frame.index.names],
        "index_dtypes": index_dtypes,
        "rows": len(frame),
    }


def _run_components(processor: BaseProcessor) -> dict[str, str | None]:
    parser = processor.name_parser
    return {
        "aep": parser.aep.text_repr if parser.aep is not None else None,
        "tp": parser.tp.text_repr if parser.tp is not None else None,
        "duration": parser.duration.text_repr if parser.duration is not None else None,
    }


def _identifier_values(frame: DataFrame, name: str) -> set[str]:
    if name not in frame.columns:
        return set()
    return {str(value).strip() for value in frame[name].dropna().unique() if str(value).strip()}


def _entity_evidence(processor: BaseProcessor) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    frame_values: dict[str, list[str]] = {}
    source_values: dict[str, set[str]] = {key: set(values) for key, values in processor.source_entity_presence.items()}
    for name in dict.fromkeys(("Location", *processor.get_entity_id_columns())):
        if name in processor.df.columns:
            values = _identifier_values(processor.df, name)
            frame_values[name] = sorted(values)
            source_values.setdefault(name, set()).update(values)

    # PO and POMM may reject entire locations before generating the tidy frame.
    # Their source header is kept by the native parser in raw_df. Carry the
    # independently observed raw location names even if no processed row remains.
    if not processor.raw_df.empty and processor.data_type == "PO" and len(processor.raw_df) > 1:
        raw_values = processor.raw_df.iloc[1, 1:].dropna()
        source_values.setdefault("Location", set()).update(
            str(value).strip() for value in raw_values if str(value).strip() and str(value).strip().lower() != "time"
        )
    if not processor.raw_df.empty and processor.data_type == "POMM" and len(processor.raw_df) > 1:
        raw_values = processor.raw_df.iloc[1, 2:].dropna()
        source_values.setdefault("Location", set()).update(
            str(value).strip() for value in raw_values if str(value).strip()
        )
    return ({key: sorted(values) for key, values in source_values.items()}, frame_values)


def _source_entry(processor: BaseProcessor, index: int) -> dict[str, Any]:
    captured = processor.source_at_processing
    if captured is None or not isinstance(captured.get("path"), str):
        message = f"Processor has no pre-processing source fingerprint: {processor.file_path}"
        raise ValueError(message)
    source_entities, frame_entities = _entity_evidence(processor)
    rejections: dict[str, int] = processor.rejection_reasons.copy()
    return {
        "member": f"processors/{index:06d}.parquet",
        "source": dict(captured),  # Never substitute a newer export-time fingerprint.
        "class_name": type(processor).__name__,
        "class_module": type(processor).__module__,
        "data_type": processor.data_type,
        "dataformat": processor.dataformat,
        "raw_run_code": processor.name_parser.raw_run_code,
        "run_components": _run_components(processor),
        "include_path_columns": processor.include_path_columns,
        "entity_filter": sorted(processor.entity_filter or ()),
        "applied_location_filter": sorted(processor.applied_location_filter or ()),
        "applied_entity_filter": sorted(processor.applied_entity_filter or ()),
        "source_entities": source_entities,
        "frame_entities": frame_entities,
        "frame": _frame_contract(_portable_frame(processor.df)),
        "health": {
            "input_health": str(getattr(processor, "input_health", "OK")),
            "rejected_samples": int(getattr(processor, "rejected_samples", 0)),
            "rejection_reasons": rejections,
            "filtered_empty": bool(getattr(processor, "filtered_empty", False)),
        },
    }


def _sha_file(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as file:
        while block := file.read(_CHUNK_SIZE):
            digest.update(block)
            size += len(block)
    return {"sha256": digest.hexdigest(), "size": size}


def _check_source(entry: dict[str, Any]) -> None:
    source_raw: object = entry.get("source")
    if not isinstance(source_raw, dict):
        message = "Parquet directory has invalid source identity"
        raise ValueError(message)
    source = cast("dict[str, object]", source_raw)
    path_text = source.get("path")
    if not isinstance(path_text, str):
        message = "Parquet directory has invalid source path"
        raise ValueError(message)
    path = Path(path_text)
    if _source_signature(path) != source:
        message = f"Parquet directory source has changed: {path}"
        raise ValueError(message)


def _all_sources_unchanged(entries: list[dict[str, Any]]) -> None:
    for entry in entries:
        _check_source(entry)


def _validate_destination(collection: ProcessorCollection, destination: Path, *, overwrite: bool) -> None:
    if destination.suffix.casefold() in {".parquet", ".rtparquet"}:
        message = "Collection snapshots require a directory (e.g. results.parquetdir), not a Parquet file"
        raise ValueError(message)
    if destination.is_symlink() or (hasattr(destination, "is_junction") and destination.is_junction()):
        message = f"Refusing linked Parquet directory destination: {destination}"
        raise ValueError(message)
    if destination.exists() and not destination.is_dir():
        message = f"Refusing non-directory Parquet destination: {destination}"
        raise ValueError(message)
    if destination.exists() and not overwrite:
        message = f"Parquet directory already exists (set overwrite=True): {destination}"
        raise FileExistsError(message)
    for processor in collection.processors:
        if not processor.processed or processor.df.empty:
            message = f"Only processed nonempty processors can be snapshotted: {processor.file_path}"
            raise ValueError(message)
        if processor.source_at_processing is None:
            message = f"Missing original processing source signature: {processor.file_path}"
            raise ValueError(message)
        if destination == processor.resolved_file_path or destination in processor.resolved_file_path.parents:
            message = f"Parquet directory aliases a source path: {destination}"
            raise ValueError(message)


def _stage_tables(collection: ProcessorCollection, stage: Path, entries: list[dict[str, Any]]) -> dict[str, Any] | None:
    members = stage / "processors"
    members.mkdir()
    for entry, processor in zip(entries, collection.processors, strict=True):
        table = stage / cast("str", entry["member"])
        _portable_frame(processor.df).to_parquet(table, engine="pyarrow", compression="zstd", index=None)
        entry.update(_sha_file(table))
    if collection.basic_info_lookup is None:
        return None
    lookup_path = stage / _LOOKUP
    _portable_frame(collection.basic_info_lookup).to_parquet(
        lookup_path, engine="pyarrow", compression="zstd", index=None
    )
    return {"member": _LOOKUP, **_sha_file(lookup_path), "frame": _frame_contract(collection.basic_info_lookup)}


def _manifest_bytes(
    entries: list[dict[str, Any]], lookup: dict[str, Any] | None, generation: str, *, verified: bool
) -> bytes:
    manifest: dict[str, Any] = {
        "format": _FORMAT,
        "version": _FORMAT_VERSION,
        "created_at_utc": datetime.now(UTC).isoformat(),
        "active_generation": generation,
        "processor_registry_sha256": _registry_digest(),
        "processing_semantics_sha256": _semantics_digest(),
        "sources_verified_at_export": verified,
        "processors": entries,
        "basic_info_lookup": lookup,
    }
    encoded = json.dumps(manifest, ensure_ascii=False, sort_keys=True, allow_nan=False).encode("utf-8")
    if len(encoded) > _MAX_MANIFEST_SIZE:
        message = "Parquet directory manifest exceeds supported size"
        raise ValueError(message)
    return encoded


def _write_durable(path: Path, contents: bytes) -> None:
    with path.open("xb") as file:
        file.write(contents)
        file.flush()
        os.fsync(file.fileno())


def _publish_new_directory(stage: Path, destination: Path) -> None:
    """Atomic directory no-clobber; fail closed where no such OS primitive exists."""
    if os.name == "nt":
        stage.rename(destination)  # Windows MoveFile cannot replace an existing directory.
        return
    if sys.platform == "linux":
        renameat2: Any = getattr(ctypes.CDLL(None, use_errno=True), "renameat2", None)
        if renameat2 is None:
            raise OSError(errno.ENOTSUP, "Atomic no-clobber directory rename is unavailable")
        renameat2.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        renameat2.restype = ctypes.c_int
        if renameat2(-100, os.fsencode(stage), -100, os.fsencode(destination), 1) != 0:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error), str(destination))
        return
    raise OSError(errno.ENOTSUP, "Atomic no-clobber directory rename is unavailable on this OS")


def _unsafe_link(path: Path) -> bool:
    return path.is_symlink() or (hasattr(path, "is_junction") and path.is_junction())


def _checked_members(directory: Path, entries: list[dict[str, Any]], lookup: dict[str, Any] | None) -> None:
    expected = {cast("str", entry["member"]) for entry in entries}
    if lookup is not None:
        expected.add(_LOOKUP)
    actual: set[str] = set()
    for file in directory.rglob("*"):
        if _unsafe_link(file):
            message = "Parquet directory contains a linked member"
            raise ValueError(message)
        if file.is_file():
            actual.add(file.relative_to(directory).as_posix())
        elif not file.is_dir():
            message = "Parquet directory contains an unsupported member"
            raise ValueError(message)
    if actual != expected:
        message = "Parquet directory contains missing or unexpected members"
        raise ValueError(message)


def _validate_inventory(  # noqa: C901
    payload: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any] | None, str]:
    if payload.get("format") != _FORMAT or payload.get("version") != _FORMAT_VERSION:
        message = "Unsupported Parquet directory format or version"
        raise ValueError(message)
    if payload.get("processor_registry_sha256") != _registry_digest():
        message = "Parquet directory registry differs from installed processor registry"
        raise ValueError(message)
    if payload.get("processing_semantics_sha256") != _semantics_digest():
        message = "Parquet directory processing semantics have changed"
        raise ValueError(message)
    generation = payload.get("active_generation")
    if not isinstance(generation, str) or (generation != "." and not _GENERATION_RE.fullmatch(generation)):
        message = "Invalid Parquet directory generation path"
        raise ValueError(message)
    raw_entries: object = payload.get("processors")
    if not isinstance(raw_entries, list):
        message = "Parquet directory processor inventory is invalid"
        raise ValueError(message)
    entries: list[dict[str, Any]] = []
    for index, raw_entry in enumerate(cast("list[object]", raw_entries)):
        if not isinstance(raw_entry, dict):
            message = "Invalid Parquet directory processor entry"
            raise ValueError(message)
        typed_entry = cast("dict[str, object]", raw_entry)
        if typed_entry.get("member") != f"processors/{index:06d}.parquet":
            message = "Invalid or duplicate Parquet directory processor member path"
            raise ValueError(message)
        entries.append(cast("dict[str, Any]", typed_entry))
    raw_lookup: object = payload.get("basic_info_lookup")
    if raw_lookup is not None and (
        not isinstance(raw_lookup, dict) or cast("dict[str, object]", raw_lookup).get("member") != _LOOKUP
    ):
        message = "Invalid Parquet directory lookup member path"
        raise ValueError(message)
    lookup = cast("dict[str, Any] | None", raw_lookup)
    for entry in [*entries, *([lookup] if lookup is not None else [])]:
        sha, size = entry.get("sha256"), entry.get("size")
        if not isinstance(sha, str) or re.fullmatch(r"[0-9a-f]{64}", sha) is None or type(size) is not int or size < 0:
            message = "Parquet directory member hash/size metadata is invalid"
            raise ValueError(message)
    return entries, lookup, generation


def _checked_manifest(  # noqa: C901
    path: Path,
) -> tuple[dict[str, Any], Path, list[dict[str, Any]], dict[str, Any] | None]:
    if not path.is_dir() or _unsafe_link(path):
        message = f"Parquet collection path must be a real directory: {path}"
        raise ValueError(message)
    manifest_path = path / _MANIFEST
    if not manifest_path.is_file() or _unsafe_link(manifest_path):
        message = "Parquet directory has no regular manifest.json"
        raise ValueError(message)
    if manifest_path.stat().st_size > _MAX_MANIFEST_SIZE:
        message = "Parquet directory manifest is too large"
        raise ValueError(message)
    try:
        payload: object = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        message = "Parquet directory manifest is corrupt"
        raise ValueError(message) from exc
    if not isinstance(payload, dict):
        message = "Parquet directory manifest must be an object"
        raise ValueError(message)
    manifest = cast("dict[str, Any]", payload)
    entries, lookup, generation = _validate_inventory(manifest)
    if generation == ".":
        allowed = {_MANIFEST, "processors", _LOOKUP, _VERSIONS, ".publish.lock"}
        for child in path.iterdir():
            if child.name not in allowed and not re.fullmatch(r"\.manifest-[0-9a-f]{32}\.tmp", child.name):
                message = "Parquet directory contains unexpected root members"
                raise ValueError(message)
        directory = path
        # In this initial generation, ignore only the version history and manifest.
        expected = {cast("str", entry["member"]) for entry in entries}
        if lookup is not None:
            expected.add(_LOOKUP)
        processor_dir = path / "processors"
        if not processor_dir.is_dir() or _unsafe_link(processor_dir):
            message = "Parquet directory processors folder is invalid"
            raise ValueError(message)
        actual = {f"processors/{file.name}" for file in processor_dir.iterdir() if file.is_file()}
        if any(_unsafe_link(file) or not file.is_file() for file in processor_dir.iterdir()):
            message = "Parquet directory has invalid processor members"
            raise ValueError(message)
        if (path / _LOOKUP).is_file():
            actual.add(_LOOKUP)
        if actual != expected:
            message = "Parquet directory contains missing or unexpected members"
            raise ValueError(message)
    else:
        directory = path / generation
        if not directory.is_dir() or _unsafe_link(path / _VERSIONS) or _unsafe_link(directory):
            message = "Parquet directory generation does not exist or is linked"
            raise ValueError(message)
        _checked_members(directory, entries, lookup)
    return manifest, directory, entries, lookup


def _verify_member(directory: Path, entry: dict[str, Any]) -> Path:
    member = directory / cast("str", entry["member"])
    if _unsafe_link(member) or not member.is_file():
        message = f"Parquet directory member is missing or linked: {member}"
        raise ValueError(message)
    if _sha_file(member) != {"sha256": entry["sha256"], "size": entry["size"]}:
        message = f"Parquet directory member checksum mismatch: {entry['member']}"
        raise ValueError(message)
    return member


def _string_list(value: object, label: str) -> list[str]:
    if not isinstance(value, list):
        message = f"Parquet directory {label} must be a string list"
        raise ValueError(message)
    items = cast("list[object]", value)
    if any(not isinstance(item, str) for item in items):
        message = f"Parquet directory {label} must contain only strings"
        raise ValueError(message)
    return cast("list[str]", items)


def _entity_index(entry: dict[str, Any], key: str) -> dict[str, list[str]]:
    raw: object = entry.get(key)
    if not isinstance(raw, dict):
        message = f"Parquet directory {key} metadata is missing"
        raise ValueError(message)
    result: dict[str, list[str]] = {}
    entries = cast("dict[object, object]", raw)
    for column, values in entries.items():
        if not isinstance(column, str) or column not in BaseProcessor.ENTITY_ID_COLUMNS:
            message = f"Invalid {key} identifier column"
            raise ValueError(message)
        result[column] = _string_list(values, key)
    return result


def _check_frame(frame: DataFrame, entry: dict[str, Any]) -> None:
    actual = _frame_contract(frame)
    expected: object = entry.get("frame")
    if frame.empty or actual != expected:
        message = (
            f"Parquet directory processed DataFrame schema mismatch: {entry.get('member')}: "
            f"expected {expected!r}, got {actual!r}"
        )
        raise ValueError(message)


def _restore_processor(directory: Path, entry: dict[str, Any], *, verify_sources: bool) -> BaseProcessor:
    source_raw: object = entry.get("source")
    if not isinstance(source_raw, dict):
        message = "Parquet directory source identity is invalid"
        raise ValueError(message)
    source = cast("dict[str, object]", source_raw)
    source_path = source.get("path")
    if not isinstance(source_path, str):
        message = "Parquet directory source path is invalid"
        raise ValueError(message)
    if verify_sources:
        _check_source(entry)
    include_paths: object = entry.get("include_path_columns")
    if not isinstance(include_paths, bool):
        message = "Parquet directory include_path_columns is invalid"
        raise ValueError(message)
    processor = BaseProcessor.from_file(
        Path(source_path),
        entity_filter=_string_list(entry.get("entity_filter"), "entity_filter"),
        include_path_columns=include_paths,
    )
    if (
        type(processor).__name__ != entry.get("class_name")
        or type(processor).__module__ != entry.get("class_module")
        or processor.data_type != entry.get("data_type")
        or processor.dataformat != entry.get("dataformat")
        or processor.name_parser.raw_run_code != entry.get("raw_run_code")
        or _run_components(processor) != entry.get("run_components")
    ):
        message = f"Parquet directory trusted processor factory mismatch: {source_path}"
        raise ValueError(message)
    processor.df = pd.read_parquet(_verify_member(directory, entry), engine="pyarrow")
    _check_frame(processor.df, entry)
    processor.applied_location_filter = frozenset(
        _string_list(entry.get("applied_location_filter"), "applied_location_filter")
    )
    processor.applied_entity_filter = frozenset(
        _string_list(entry.get("applied_entity_filter"), "applied_entity_filter")
    )
    health_raw: object = entry.get("health")
    if not isinstance(health_raw, dict):
        message = "Parquet directory processor health metadata is missing"
        raise ValueError(message)
    health = cast("dict[str, object]", health_raw)
    input_health: object = health.get("input_health")
    rejected: object = health.get("rejected_samples")
    reasons: object = health.get("rejection_reasons")
    if (
        not isinstance(input_health, str)
        or type(rejected) is not int
        or rejected < 0
        or not isinstance(reasons, dict)
        or any(
            not isinstance(key, str) or type(value) is not int or value < 0
            for key, value in cast("dict[object, object]", reasons).items()
        )
        or not isinstance(health.get("filtered_empty"), bool)
    ):
        message = "Parquet directory processor health metadata is invalid"
        raise ValueError(message)
    processor.input_health = input_health
    processor.rejected_samples = rejected
    processor.rejection_reasons = cast("dict[str, int]", reasons)
    processor.filtered_empty = cast("bool", health["filtered_empty"])
    processor.source_entity_presence = _entity_index(entry, "source_entities")
    # The factory may have sampled a different source now. Restore the exact
    # processing-time identity, including for explicit offline/historical reads.
    processor.source_at_processing = cast("dict[str, object]", source)
    processor.processed = True
    return processor


def _lock_publication(path: Path) -> int:
    return os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)


def _recheck_staged(stage: Path, entries: list[dict[str, Any]], lookup: dict[str, Any] | None) -> None:
    _checked_members(stage, entries, lookup)
    for entry in [*entries, *([lookup] if lookup is not None else [])]:
        _verify_member(stage, entry)


def write_collection_bundle(  # noqa: C901
    collection: ProcessorCollection,
    directory: Path | str,
    *,
    overwrite: bool = False,
    verify_sources: bool = True,
) -> None:
    """Write a complete Parquet dataset directory, preserving the prior generation on failure."""
    destination = Path(directory).absolute()
    _validate_destination(collection, destination, overwrite=overwrite)
    destination.parent.mkdir(parents=True, exist_ok=True)
    entries = [_source_entry(processor, index) for index, processor in enumerate(collection.processors)]
    if verify_sources:
        _all_sources_unchanged(entries)
    stage = Path(tempfile.mkdtemp(prefix=f".{destination.name}.", suffix=".staging", dir=destination.parent))
    published = False
    moved_generation: Path | None = None
    lock_path = destination / ".publish.lock"
    locked = False
    temporary_manifest: Path | None = None
    try:
        lookup = _stage_tables(collection, stage, entries)
        _recheck_staged(stage, entries, lookup)
        if verify_sources:
            _all_sources_unchanged(entries)
        if not destination.exists():
            encoded = _manifest_bytes(entries, lookup, ".", verified=verify_sources)
            _write_durable(stage / _MANIFEST, encoded)
            if verify_sources:
                _all_sources_unchanged(entries)
            _publish_new_directory(stage, destination)
            published = True
        else:
            # Concurrent writers are serialised by a small O_EXCL lock. The
            # old manifest/data remain intact until os.replace commits the new
            # pointer, and old generations remain available to existing readers.
            lock_fd = _lock_publication(lock_path)
            locked = True
            os.close(lock_fd)
            _checked_manifest(destination)
            versions = destination / _VERSIONS
            if _unsafe_link(versions):
                message = "Linked Parquet directory generation folder"
                raise ValueError(message)
            versions.mkdir(exist_ok=True)
            token = uuid.uuid4().hex
            generation = f"{_VERSIONS}/{token}"
            encoded = _manifest_bytes(entries, lookup, generation, verified=verify_sources)
            moved_generation = destination / generation
            _publish_new_directory(stage, moved_generation)
            tmp_name = f".manifest-{uuid.uuid4().hex}.tmp"
            temporary_manifest = destination / tmp_name
            _write_durable(temporary_manifest, encoded)
            if verify_sources:
                _all_sources_unchanged(entries)
            temporary_manifest.replace(destination / _MANIFEST)
            temporary_manifest = None
            published = True
    finally:
        # Only remove paths this invocation created. Never touch previous
        # generations or manifest; interrupted writes leave the old readable.
        if temporary_manifest is not None:
            temporary_manifest.unlink(missing_ok=True)
        if moved_generation is not None and not published:
            shutil.rmtree(moved_generation)
        if not published:
            shutil.rmtree(stage, ignore_errors=True)
        if locked:
            lock_path.unlink(missing_ok=True)


def read_collection_bundle(  # noqa: C901
    directory: Path | str,
    locations: Collection[str] | None = None,
    *,
    data_types: Collection[str] | None = None,
    verify_sources: bool = True,
) -> ProcessorCollection:
    """Restore selected trusted native processors without reading unrelated Parquet tables.

    With verify_sources=False, only source freshness is skipped; schema,
    semantics and loaded-member integrity remain mandatory.
    """
    from .processor_collection import ProcessorCollection

    path = Path(directory).absolute()
    if path.suffix.casefold() in {".parquet", ".rtparquet"}:
        message = "ProcessorCollection.from_parquet requires a dataset directory, not a Parquet file"
        raise ValueError(message)
    _manifest, active, entries, lookup = _checked_manifest(path)
    desired_types = set(data_types) if data_types is not None else None
    desired_locations = BaseProcessor.normalize_locations(locations)
    collection = ProcessorCollection()
    selected: list[tuple[dict[str, Any], BaseProcessor, bool]] = []
    for entry in entries:
        data_type = entry.get("data_type")
        if not isinstance(data_type, str):
            message = "Parquet directory data_type is invalid"
            raise ValueError(message)
        source_entities = _entity_index(entry, "source_entities")
        frame_entities = _entity_index(entry, "frame_entities")
        if desired_types is not None and data_type not in desired_types:
            continue
        sources = {value for values in source_entities.values() for value in values}
        frame_values = {value for values in frame_entities.values() for value in values}
        source_hit = bool(desired_locations & sources)
        if desired_locations and sources and not source_hit:
            collection.parquet_selection_diagnostics.append(
                {"data_type": data_type, "source": entry["source"]["path"], "status": "ABSENT"}
            )
            continue
        if desired_locations and source_hit and frame_values and not (desired_locations & frame_values):
            collection.parquet_selection_diagnostics.append(
                {"data_type": data_type, "source": entry["source"]["path"], "status": "INVALID"}
            )
            continue
        restored = _restore_processor(active, entry, verify_sources=verify_sources)
        collection.add_processor(restored)
        selected.append((entry, restored, source_hit))
    if lookup is not None:
        result = pd.read_parquet(_verify_member(active, lookup), engine="pyarrow")
        if _frame_contract(result) != lookup.get("frame"):
            message = "Parquet directory basic-info lookup schema mismatch"
            raise ValueError(message)
        collection.basic_info_lookup = result
    if desired_locations:
        collection.filter_locations(desired_locations)
        present = {id(processor) for processor in collection.processors}
        for entry, processor, source_hit in selected:
            if id(processor) in present:
                status = "INVALID" if getattr(processor, "input_health", "OK") == "INVALID" else "SELECTED"
            elif source_hit:
                status = "INVALID"
            else:
                status = "ABSENT"
            collection.parquet_selection_diagnostics.append(
                {"data_type": processor.data_type, "source": entry["source"]["path"], "status": status}
            )
    return collection
