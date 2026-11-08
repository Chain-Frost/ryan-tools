"""Single-file Parquet bundles for processed TUFLOW ProcessorCollection snapshots.

An .rtparquet file is a ZIP_STORED container: each processor holds its own
columnar Parquet table and a small JSON manifest records trusted class identity
and processing metadata. It is not a plain .parquet file.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import zipfile
from collections.abc import Collection
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import numpy as np
import pandas as pd
from pandas import DataFrame, MultiIndex

from .base_processor import BaseProcessor

if TYPE_CHECKING:
    from .processor_collection import ProcessorCollection

_FORMAT = "ryan-tools-tuflow-parquet-collection"
_FORMAT_VERSION = 1
_MANIFEST = "manifest.json"
_LOOKUP = "basic_info_lookup.parquet"
_MAX_MANIFEST_SIZE = 8 * 1024 * 1024
_CHUNK_SIZE = 1024 * 1024


def _registry_digest() -> str:
    registry = Path(__file__).parents[2] / "classes" / "tuflow_results_validation_and_datatypes.json"
    return hashlib.sha256(registry.read_bytes()).hexdigest()


def _source_signature(source: Path) -> dict[str, Any]:
    absolute = source.absolute()
    if not absolute.is_file():
        msg = f"Processor source is missing or not a file: {absolute}"
        raise FileNotFoundError(msg)
    stat = absolute.stat()
    signature: dict[str, Any] = {
        "path": str(absolute),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }
    if absolute.suffix.casefold() == ".gpkg":
        wal = absolute.with_name(absolute.name + "-wal")
        if wal.is_file():
            wal_stat = wal.stat()
            signature["wal"] = {"size": wal_stat.st_size, "mtime_ns": wal_stat.st_mtime_ns}
        else:
            signature["wal"] = None
    return signature


def _portable_frame(frame: DataFrame) -> DataFrame:
    """Normalize NumPy scalar axis names for Arrow metadata without changing the input."""
    portable = frame.copy(deep=False)
    if isinstance(portable.columns, MultiIndex):
        names = [name.item() if isinstance(name, np.generic) else name for name in portable.columns.names]
        portable.columns = portable.columns.set_names(names)
    elif isinstance(portable.columns.name, np.generic):
        portable.columns = portable.columns.rename(portable.columns.name.item())
    return portable


def _digest_member(archive: zipfile.ZipFile, member: str) -> dict[str, Any]:
    checksum = hashlib.sha256()
    size = 0
    with archive.open(member) as input_file:
        while chunk := input_file.read(_CHUNK_SIZE):
            checksum.update(chunk)
            size += len(chunk)
    return {"member": member, "sha256": checksum.hexdigest(), "size": size}


def _source_entry(processor: BaseProcessor, index: int) -> dict[str, Any]:
    return {
        "member": f"processors/{index:06d}.parquet",
        "source": _source_signature(processor.resolved_file_path),
        "class_name": processor.__class__.__name__,
        "class_module": processor.__class__.__module__,
        "data_type": processor.data_type,
        "dataformat": processor.dataformat,
        "raw_run_code": processor.name_parser.raw_run_code,
        "include_path_columns": processor.include_path_columns,
        "entity_filter": sorted(processor.entity_filter or ()),
        "applied_location_filter": sorted(processor.applied_location_filter or ()),
        "applied_entity_filter": sorted(processor.applied_entity_filter or ()),
    }


def _publish(staged: Path, destination: Path, *, overwrite: bool) -> None:
    """Publish a complete ZIP; never overwrite without the explicit flag."""
    if overwrite:
        staged.replace(destination)
    elif os.name == "nt":
        # Windows rename refuses existing destinations.
        staged.rename(destination)
    else:
        # POSIX rename would overwrite; hard-link is atomic no-clobber.
        os.link(staged, destination)
        staged.unlink()


def _validate_export_destination(
    collection: ProcessorCollection, destination: Path, *, overwrite: bool
) -> None:
    if destination.suffix.casefold() != ".rtparquet":
        msg = "ProcessorCollection Parquet bundles require the .rtparquet extension (ZIP of Parquet files)"
        raise ValueError(msg)
    if destination.is_symlink() or (destination.exists() and not destination.is_file()):
        msg = f"Refusing non-regular Parquet bundle destination: {destination}"
        raise ValueError(msg)
    if destination.exists() and not overwrite:
        msg = f"Parquet bundle already exists (set overwrite=True to replace): {destination}"
        raise FileExistsError(msg)
    for processor in collection.processors:
        if not processor.processed or processor.df.empty:
            msg = f"Only successfully processed, nonempty processors can be exported: {processor.file_path}"
            raise ValueError(msg)
        if destination.exists() and destination.samefile(processor.resolved_file_path):
            msg = f"Parquet bundle destination aliases a processor source: {destination}"
            raise ValueError(msg)



def write_collection_bundle(
    collection: ProcessorCollection,
    file_path: Path | str,
    *,
    overwrite: bool = False,
) -> None:
    """Store heterogeneous processed frames in one portable .rtparquet archive."""
    destination = Path(file_path).absolute()
    _validate_export_destination(collection, destination, overwrite=overwrite)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".part", dir=destination.parent)
    os.close(fd)
    staged = Path(temporary)
    try:
        entries = [_source_entry(processor, index) for index, processor in enumerate(collection.processors)]
        with zipfile.ZipFile(staged, "w", compression=zipfile.ZIP_STORED, allowZip64=True) as archive:
            for entry, processor in zip(entries, collection.processors, strict=True):
                with archive.open(entry["member"], "w", force_zip64=True) as output:
                    _portable_frame(processor.df).to_parquet(
                        output, engine="pyarrow", compression="zstd", index=True
                    )
            if collection.basic_info_lookup is not None:
                with archive.open(_LOOKUP, "w", force_zip64=True) as output:
                    _portable_frame(collection.basic_info_lookup).to_parquet(
                        output, engine="pyarrow", compression="zstd", index=True
                    )

        with zipfile.ZipFile(staged, "r") as archive:
            for entry in entries:
                entry.update(_digest_member(archive, entry["member"]))
            lookup = _digest_member(archive, _LOOKUP) if collection.basic_info_lookup is not None else None
        manifest: dict[str, Any] = {
            "format": _FORMAT,
            "version": _FORMAT_VERSION,
            "created_at_utc": datetime.now(UTC).isoformat(),
            "processor_registry_sha256": _registry_digest(),
            "processors": entries,
            "basic_info_lookup": lookup,
        }
        encoded = json.dumps(manifest, ensure_ascii=False, sort_keys=True).encode("utf-8")
        if len(encoded) > _MAX_MANIFEST_SIZE:
            msg = "Parquet bundle manifest exceeds supported size"
            raise ValueError(msg)
        with zipfile.ZipFile(staged, "a", compression=zipfile.ZIP_STORED, allowZip64=True) as archive:
            archive.writestr(_MANIFEST, encoded)
        with zipfile.ZipFile(staged, "r") as archive:
            if archive.read(_MANIFEST) != encoded:
                msg = "Parquet bundle manifest verification failed"
                raise ValueError(msg)
        _publish(staged, destination, overwrite=overwrite)
    finally:
        staged.unlink(missing_ok=True)


def _validate_manifest_members(manifest: dict[str, Any], names: set[str]) -> None:
    entries: object = manifest.get("processors")
    if not isinstance(entries, list):
        msg = "Parquet bundle processor inventory is malformed"
        raise ValueError(msg)
    expected = {_MANIFEST}
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict) or entry.get("member") != f"processors/{index:06d}.parquet":
            msg = "Parquet bundle processor member name is invalid"
            raise ValueError(msg)
        expected.add(entry["member"])
    lookup: object = manifest.get("basic_info_lookup")
    if lookup is not None:
        if not isinstance(lookup, dict) or lookup.get("member") != _LOOKUP:
            msg = "Parquet bundle lookup member is invalid"
            raise ValueError(msg)
        expected.add(_LOOKUP)
    if names != expected:
        msg = "Parquet bundle inventory contains missing or unexpected members"
        raise ValueError(msg)


def _checked_manifest(archive: zipfile.ZipFile) -> dict[str, Any]:
    members = archive.infolist()
    names = [member.filename for member in members]
    if len(names) != len(set(names)) or _MANIFEST not in names:
        msg = "Parquet bundle is missing its manifest or contains duplicate members"
        raise ValueError(msg)
    if any(member.is_dir() or member.compress_type != zipfile.ZIP_STORED for member in members):
        msg = "Parquet bundle contains unsupported ZIP entries"
        raise ValueError(msg)
    if archive.getinfo(_MANIFEST).file_size > _MAX_MANIFEST_SIZE:
        msg = "Parquet bundle manifest is too large"
        raise ValueError(msg)
    try:
        payload: object = json.loads(archive.read(_MANIFEST).decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError, zipfile.BadZipFile) as exc:
        msg = "Parquet bundle manifest is corrupt"
        raise ValueError(msg) from exc
    if not isinstance(payload, dict):
        msg = "Parquet bundle manifest must be an object"
        raise ValueError(msg)
    manifest = cast("dict[str, Any]", payload)
    if manifest.get("format") != _FORMAT or manifest.get("version") != _FORMAT_VERSION:
        msg = "Unsupported Parquet bundle format or version"
        raise ValueError(msg)
    if manifest.get("processor_registry_sha256") != _registry_digest():
        msg = "Parquet bundle processor registry differs from current configuration"
        raise ValueError(msg)
    _validate_manifest_members(manifest, set(names))
    return manifest


def _string_list(value: object, field: str) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        msg = f"Parquet bundle {field} must contain string IDs"
        raise ValueError(msg)
    return cast("list[str]", value)


def _read_member(
    archive: zipfile.ZipFile, entry: dict[str, Any], temporary_dir: Path
) -> DataFrame:
    member: object = entry.get("member")
    expected_sha: object = entry.get("sha256")
    expected_size: object = entry.get("size")
    if (
        not isinstance(member, str)
        or not isinstance(expected_sha, str)
        or not isinstance(expected_size, int)
        or expected_size < 0
        or archive.getinfo(member).file_size != expected_size
    ):
        msg = "Parquet bundle member metadata is invalid"
        raise ValueError(msg)
    # One member at a time: avoids duplicating an entire archive in RAM.
    local = temporary_dir / "member.parquet"
    checksum = hashlib.sha256()
    size = 0
    with archive.open(member) as source, local.open("wb") as output:
        while chunk := source.read(_CHUNK_SIZE):
            output.write(chunk)
            checksum.update(chunk)
            size += len(chunk)
    if size != expected_size or checksum.hexdigest() != expected_sha:
        msg = f"Parquet bundle member checksum mismatch: {member}"
        raise ValueError(msg)
    result = pd.read_parquet(local, engine="pyarrow")
    local.unlink()
    return result


def _restore_processor(
    archive: zipfile.ZipFile, entry: dict[str, Any], temporary_dir: Path, *, verify_sources: bool
) -> BaseProcessor:
    source: object = entry.get("source")
    if not isinstance(source, dict) or not isinstance(source.get("path"), str):
        msg = "Parquet bundle is missing a valid source identity"
        raise ValueError(msg)
    source_info = cast("dict[str, Any]", source)
    source_path = Path(source_info["path"])
    if verify_sources and _source_signature(source_path) != source_info:
        msg = f"Parquet bundle source has changed: {source_path}"
        raise ValueError(msg)
    entity_filter = _string_list(entry.get("entity_filter"), "entity_filter")
    include_paths: object = entry.get("include_path_columns")
    if not isinstance(include_paths, bool):
        msg = "Parquet bundle include_path_columns must be a boolean"
        raise ValueError(msg)
    processor = BaseProcessor.from_file(
        source_path,
        entity_filter=entity_filter,
        include_path_columns=include_paths,
    )
    if (
        processor.__class__.__name__ != entry.get("class_name")
        or processor.__class__.__module__ != entry.get("class_module")
        or processor.data_type != data_type
        or processor.dataformat != entry.get("dataformat")
        or processor.name_parser.raw_run_code != entry.get("raw_run_code")
    ):
        msg = f"Parquet bundle processor configuration mismatches {source_path}"
        raise ValueError(msg)
    processor.df = _read_member(archive, entry, temporary_dir)
    if processor.df.empty:
        msg = f"Parquet bundle member has no processed rows: {source_path}"
        raise ValueError(msg)
    processor.applied_location_filter = frozenset(
        _string_list(entry.get("applied_location_filter"), "applied_location_filter")
    )
    processor.applied_entity_filter = frozenset(
        _string_list(entry.get("applied_entity_filter"), "applied_entity_filter")
    )
    processor.processed = True
    return processor


def read_collection_bundle(
    file_path: Path | str,
    locations: Collection[str] | None = None,
    *,
    data_types: Collection[str] | None = None,
    verify_sources: bool = True,
) -> ProcessorCollection:
    """Restore a trusted processor collection from one .rtparquet snapshot.

    By default, source metadata freshness is checked. Set verify_sources=False
    only for an intentional historical/offline snapshot.
    """
    from .processor_collection import ProcessorCollection

    path = Path(file_path)
    if path.suffix.casefold() != ".rtparquet":
        msg = "ProcessorCollection Parquet bundles require a .rtparquet file"
        raise ValueError(msg)
    collection = ProcessorCollection()
    wanted = set(data_types) if data_types is not None else None
    try:
        with zipfile.ZipFile(path, "r") as archive:
            manifest = _checked_manifest(archive)
            raw_entries: list[dict[str, Any]] = manifest["processors"]
            with tempfile.TemporaryDirectory(prefix="ryan-parquet-bundle-") as scratch:
                temporary_dir = Path(scratch)
                for entry in raw_entries:
                    data_type: object = entry.get("data_type")
                    if not isinstance(data_type, str):
                        msg = "Parquet bundle is missing a valid data type"
                        raise ValueError(msg)
                    if wanted is not None and data_type not in wanted:
                        continue
                    collection.add_processor(
                        _restore_processor(archive, entry, temporary_dir, verify_sources=verify_sources)
                    )
                lookup: dict[str, Any] | None = manifest["basic_info_lookup"]
                if lookup is not None:
                    collection.basic_info_lookup = _read_member(archive, lookup, temporary_dir)
    except zipfile.BadZipFile as exc:
        msg = f"Invalid Parquet bundle ZIP container: {path}"
        raise ValueError(msg) from exc
    if locations:
        collection.filter_locations(locations)
    return collection
