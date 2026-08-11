"""Coordinate File Geodatabase discovery and GDAL vector exports."""

from __future__ import annotations

import multiprocessing
from dataclasses import dataclass
from pathlib import Path

from loguru import logger

from ryan_library.functions.gdal.vector_conversion import (
    VectorFormat,
    get_vector_layer_names,
    require_vector_driver,
    translate_vector_dataset,
)
from ryan_library.functions.path_stuff import PathOrList, sanitize_windows_filename, to_path_list


@dataclass(frozen=True, slots=True)
class FileGeodatabaseExportResult:
    """Result counts and messages for one source File Geodatabase."""

    source: Path
    converted: int
    skipped: int
    errors: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class FileGeodatabaseExportSummary:
    """Aggregate result for a batch File Geodatabase export."""

    source_count: int
    converted: int
    skipped: int
    errors: tuple[str, ...]

    @property
    def succeeded(self) -> bool:
        return not self.errors


@dataclass(frozen=True, slots=True)
class _ExportTask:
    source: Path
    output_root: Path
    output_format: VectorFormat
    single_database: bool


def discover_file_geodatabases(input_paths: PathOrList) -> list[Path]:
    """Discover unique File Geodatabase directories from explicit paths and search roots."""
    targets = list(dict.fromkeys(path.resolve() for path in to_path_list(input_paths)))
    if not targets:
        raise ValueError("At least one input path is required")

    invalid_targets = [target for target in targets if not target.is_dir()]
    if invalid_targets:
        raise FileNotFoundError(f"Input directory does not exist: {invalid_targets[0]}")

    geodatabases: list[Path] = []
    for target in targets:
        if target.suffix.lower() == ".gdb":
            geodatabases.append(target)
        else:
            geodatabases.extend(path for path in target.rglob("*") if path.is_dir() and path.suffix.lower() == ".gdb")
    return list(dict.fromkeys(geodatabases))


def _unique_layer_stems(layer_names: list[str]) -> list[str]:
    used_stems: dict[str, int] = {}
    output_stems: list[str] = []
    for layer_name in layer_names:
        base_stem = sanitize_windows_filename(layer_name, fallback="unnamed_layer")
        stem_key = base_stem.casefold()
        occurrence = used_stems.get(stem_key, 0) + 1
        used_stems[stem_key] = occurrence
        output_stems.append(base_stem if occurrence == 1 else f"{base_stem}_{occurrence}")
    return output_stems


def export_file_geodatabase(
    source: str | Path,
    output_root: str | Path,
    *,
    output_format: str = "gpkg",
    single_database: bool = False,
) -> FileGeodatabaseExportResult:
    """Export one File Geodatabase as separate layer files or one database."""
    source_path = Path(source).resolve()
    destination_root = Path(output_root).resolve()
    normalized_format, spec = require_vector_driver(output_format)
    if source_path.suffix.lower() != ".gdb" or not source_path.is_dir():
        raise ValueError(f"Source is not an existing File Geodatabase: {source_path}")
    if single_database and not spec.supports_multiple_layers:
        raise ValueError(f"Format '{normalized_format}' does not support a multi-layer database")

    if single_database:
        output_stem = sanitize_windows_filename(source_path.stem)
        output_path = destination_root / f"{output_stem}{spec.extension}"
        try:
            translate_vector_dataset(source_path, output_path, vector_format=normalized_format)
        except FileExistsError:
            return FileGeodatabaseExportResult(source_path, 0, 1, ())
        except Exception as exc:
            return FileGeodatabaseExportResult(source_path, 0, 0, (str(exc),))
        return FileGeodatabaseExportResult(source_path, 1, 0, ())

    try:
        layer_names = get_vector_layer_names(source_path)
    except Exception as exc:
        return FileGeodatabaseExportResult(source_path, 0, 0, (str(exc),))

    output_directory = destination_root / sanitize_windows_filename(source_path.stem)
    output_stems = _unique_layer_stems(layer_names)
    converted = 0
    skipped = 0
    errors: list[str] = []
    for layer_name, output_stem in zip(layer_names, output_stems, strict=True):
        output_path = output_directory / f"{output_stem}{spec.extension}"
        try:
            translate_vector_dataset(
                source_path,
                output_path,
                vector_format=normalized_format,
                layer_name=layer_name,
            )
        except FileExistsError:
            skipped += 1
        except Exception as exc:
            errors.append(f"{layer_name}: {exc}")
        else:
            converted += 1
    return FileGeodatabaseExportResult(source_path, converted, skipped, tuple(errors))


def _run_export_task(task: _ExportTask) -> FileGeodatabaseExportResult:
    return export_file_geodatabase(
        task.source,
        task.output_root,
        output_format=task.output_format,
        single_database=task.single_database,
    )


def export_file_geodatabases(
    input_paths: PathOrList,
    output_root: str | Path,
    *,
    output_format: str = "gpkg",
    single_database: bool = False,
    max_workers: int | None = None,
) -> FileGeodatabaseExportSummary:
    """Discover and export a batch of File Geodatabases."""
    normalized_format, spec = require_vector_driver(output_format)
    if single_database and not spec.supports_multiple_layers:
        raise ValueError(f"Format '{normalized_format}' does not support a multi-layer database")
    if max_workers is not None and max_workers < 1:
        raise ValueError("max_workers must be at least 1")

    geodatabases = discover_file_geodatabases(input_paths)
    if not geodatabases:
        return FileGeodatabaseExportSummary(0, 0, 0, ())

    output_names = [sanitize_windows_filename(source.stem).casefold() for source in geodatabases]
    if len(output_names) != len(set(output_names)):
        raise ValueError("Multiple input GDBs share a name and would use the same output location")

    destination_root = Path(output_root).resolve()
    destination_root.mkdir(parents=True, exist_ok=True)
    tasks = [_ExportTask(source, destination_root, normalized_format, single_database) for source in geodatabases]
    worker_limit = max(1, multiprocessing.cpu_count() // 2) if max_workers is None else max_workers
    workers = min(len(tasks), worker_limit)

    if workers == 1:
        results = [_run_export_task(task) for task in tasks]
    else:
        with multiprocessing.Pool(processes=workers) as pool:
            results = list(pool.imap_unordered(_run_export_task, tasks))

    errors: list[str] = []
    for result in results:
        logger.debug(
            "Processed {}: {} converted, {} skipped, {} errors",
            result.source.name,
            result.converted,
            result.skipped,
            len(result.errors),
        )
        errors.extend(f"{result.source.name}: {error}" for error in result.errors)

    return FileGeodatabaseExportSummary(
        source_count=len(geodatabases),
        converted=sum(result.converted for result in results),
        skipped=sum(result.skipped for result in results),
        errors=tuple(errors),
    )
