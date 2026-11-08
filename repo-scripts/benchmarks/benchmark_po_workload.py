"""WP11: repeatable PO processor/aggregation/export baseline (issue #229).

Edit the settings below and run from the repository root with Python 3.14.
Only synthetic data is generated. No production result files are accessed.
The default is deliberately small; FULL requires an independent opt-in and
sufficient free disk/RAM. Measurements are local evidence, not claimed speedups.
"""

from __future__ import annotations

import csv
import importlib
import importlib.metadata
import inspect
import json
import math
import platform
import shutil
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal, cast

import pandas as pd

# User-editable benchmark configuration (never automatically enable the full case).
PROFILE: Literal["scaled", "full"] = "scaled"
ALLOW_FULL_WORKLOAD: bool = False
OUTPUT_ROOT: Path = Path("wp11-po-benchmark-output")
WORKERS: int = 2
MAX_FULL_SERIES_NUMERIC_GIB: float = 1.0
DISK_SAFETY_FACTOR: float = 1.5
EXPORT_FORMAT: Literal["csv", "parquet"] = "csv"

AEP_CODES: list[str] = ["01p", "02p", "05p", "10p", "20p"]
DURATIONS_MIN: list[int] = [60, 120, 360, 720, 1440, 2880]
TEMPORAL_PATTERNS: list[int] = list(range(1, 11))
VARIABLES: list[str] = ["H", "Q", "V"]


@dataclass(frozen=True)
class Workload:
    """A subset of the fixed 5 AEP x 6 durations x 10 TP input grid."""

    aep_codes: list[str]
    durations_min: list[int]
    temporal_patterns: list[int]
    locations: int
    final_minute: int
    selected_locations: int

    @property
    def run_codes(self) -> list[str]:
        return [
            f"M11_{aep}_{duration:05d}m_TP{tp:02d}"
            for aep in self.aep_codes
            for duration in self.durations_min
            for tp in self.temporal_patterns
        ]

    @property
    def timestamps(self) -> int:
        return self.final_minute + 1  # Include 00:00 and 48:00, not only 2,880 samples.

    def observations(self, selected: bool) -> int:
        locations = self.selected_locations if selected else self.locations
        return len(self.run_codes) * locations * len(VARIABLES) * self.timestamps


def workload(profile: Literal["scaled", "full"]) -> Workload:
    if profile == "full":
        return Workload(AEP_CODES, DURATIONS_MIN, TEMPORAL_PATTERNS, 100, 2880, 20)
    if profile == "scaled":
        return Workload(AEP_CODES[:1], DURATIONS_MIN[:1], TEMPORAL_PATTERNS[:2], 6, 120, 2)
    message = f"Unknown workload profile: {profile}"
    raise ValueError(message)


def value_at(run_index: int, location_index: int, minute: int, variable: str) -> float:
    """Independent, deterministic float64 fixture values with negative and zero Q."""
    if variable == "H":
        return 10.0 + run_index * 0.01 + location_index * 0.1 + minute * 0.002
    if variable == "Q":
        return (minute - 100) * 0.01 + location_index * 0.05 + run_index * 0.001
    if variable == "V":
        return 0.5 + (minute % 9) * 0.02 + location_index * 0.005 + run_index * 0.001
    message = f"Unsupported measurement: {variable}"
    raise ValueError(message)


def write_po_csv(path: Path, run_index: int, locations: int, final_minute: int) -> None:
    """Write real PO two-row headers, dummy leading column and hourly Time."""
    with path.open("x", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(["", "Time", *(kind for _ in range(locations) for kind in VARIABLES)])
        writer.writerow(["", "Time", *(f"L{loc + 1:03d}" for loc in range(locations) for _ in VARIABLES)])
        for minute in range(final_minute + 1):
            writer.writerow(
                [
                    "",
                    f"{minute / 60:.10f}",
                    *(
                        f"{value_at(run_index, loc, minute, kind):.8f}"
                        for loc in range(locations)
                        for kind in VARIABLES
                    ),
                ]
            )


def make_fixture(root: Path, spec: Workload) -> list[Path]:
    """Create a new owned fixture directory; never overwrite an earlier run."""
    root.mkdir(parents=True, exist_ok=False)
    run_codes = spec.run_codes
    if len(set(run_codes)) != len(run_codes):
        message = "Fixture run identifiers must be unique"
        raise ValueError(message)
    files: list[Path] = []
    for run_index, run_code in enumerate(run_codes):
        target = root / f"{run_code}_PO.csv"
        write_po_csv(target, run_index, spec.locations, spec.final_minute)
        files.append(target)
    return files


def _selected_ids(spec: Workload, selected: bool) -> list[str]:
    n = spec.selected_locations if selected else spec.locations
    return [f"L{i:03d}" for i in range(1, n + 1)]


def _peak_rows(frame: pd.DataFrame) -> pd.DataFrame:
    """One exact absolute-peak row per run/location/measurement, before IPC.

    This is a benchmark reduction, NOT a replacement for TP/closure statistics.
    """
    indices = frame.groupby(["internalName", "Location", "Type"], observed=True)["AbsValue"].idxmax()
    selected = cast("pd.DataFrame", frame.loc[indices.to_numpy()])
    return selected.copy()


def _process_one(path: Path, selected_ids: list[str], summary_only: bool) -> pd.DataFrame:
    """Use the actual public PO processor, not a synthetic pandas substitute."""
    from ryan_library.processors.tuflow.base_processor import BaseProcessor

    processor = BaseProcessor.from_file(path, entity_filter=selected_ids, include_path_columns=False)
    processor.process()
    if not processor.processed or processor.df.empty:
        message = f"Supported generated PO fixture was rejected or emptied: {path.name}"
        raise AssertionError(message)
    processor.discard_raw_dataframe()
    result = processor.df
    if summary_only:
        result = _peak_rows(result)
    return result


def _process_for_pool(args: tuple[Path, list[str], bool]) -> pd.DataFrame:
    """Top-level spawn-safe worker; never launch a second nested process pool."""
    return _process_one(*args)


@dataclass
class MemorySample:
    peak_parent_rss_bytes: int | None = None
    peak_workers_rss_bytes: int | None = None
    peak_combined_rss_bytes: int | None = None


class ProcessMemorySampler:
    """Sample parent plus direct/descendant workers; not an exact RSS high-water mark."""

    def __init__(self) -> None:
        self.sample = MemorySample()
        self.stop_signal = threading.Event()
        self.thread: threading.Thread | None = None
        try:
            self.psutil: Any = importlib.import_module("psutil")
        except ImportError:
            self.psutil = None

    def __enter__(self) -> ProcessMemorySampler:
        if self.psutil is not None:
            self.thread = threading.Thread(target=self._sample_loop, daemon=True)
            self.thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.stop_signal.set()
        if self.thread is not None:
            self.thread.join()

    def _sample_loop(self) -> None:
        process = self.psutil.Process()
        while not self.stop_signal.is_set():
            try:
                parent = int(process.memory_info().rss)
                children = sum(int(child.memory_info().rss) for child in process.children(recursive=True))
                self.sample.peak_parent_rss_bytes = max(self.sample.peak_parent_rss_bytes or 0, parent)
                self.sample.peak_workers_rss_bytes = max(self.sample.peak_workers_rss_bytes or 0, children)
                self.sample.peak_combined_rss_bytes = max(self.sample.peak_combined_rss_bytes or 0, parent + children)
            except self.psutil.Error, OSError:
                pass  # A short-lived worker can exit between discovery and sampling.
            self.stop_signal.wait(0.05)


def _export(frame: pd.DataFrame, target: Path, file_format: Literal["csv", "parquet"]) -> None:
    if file_format == "csv":
        frame.to_csv(target, index=False)
    else:
        # Do not silently substitute CSV or treat a missing Parquet engine as success.
        frame.to_parquet(target, index=False)


def run_case(  # noqa: C901 - Keep measured stages and their resource guards together.
    files: list[Path],
    spec: Workload,
    *,
    selected: bool,
    summary_only: bool,
    parallel: bool,
    output_root: Path,
    file_format: Literal["csv", "parquet"],
    workers: int,
    max_numeric_gib: float,
) -> dict[str, Any]:
    """Time parsing, aggregation and export; record observed and calculated limits."""
    expected_rows = spec.observations(selected)
    output_rows = len(files) * (spec.selected_locations if selected else spec.locations) * len(VARIABLES)
    if not summary_only and expected_rows * 24 > max_numeric_gib * 1024**3:
        message = "In-memory full-series numeric lower bound exceeds configured RAM budget"
        raise ValueError(message)

    ids = _selected_ids(spec, selected)
    area = "selected" if selected else "all"
    product = "summary" if summary_only else "series"
    method = "parallel" if parallel else "serial"
    case = f"{area}_{product}_{method}"
    suffix = ".parquet" if file_format == "parquet" else ".csv"
    target = output_root / f"{case}{suffix}"
    start = time.perf_counter()
    with ProcessMemorySampler() as memory:
        parse_start = time.perf_counter()
        collection = None
        if not parallel and not summary_only:
            # Exercise the public collection path for the serial full-series baseline.
            from ryan_library.processors.tuflow.base_processor import BaseProcessor
            from ryan_library.processors.tuflow.processor_collection import ProcessorCollection

            collection = ProcessorCollection()
            for path in files:
                processor = BaseProcessor.from_file(path, entity_filter=ids, include_path_columns=False)
                processor.process()
                if not processor.processed or processor.df.empty:
                    message = f"Supported generated PO fixture was rejected: {path.name}"
                    raise AssertionError(message)
                processor.discard_raw_dataframe()
                collection.add_processor(processor)
            frames: list[pd.DataFrame] = []
        elif parallel:
            with ProcessPoolExecutor(max_workers=workers) as pool:
                frames = list(pool.map(_process_for_pool, ((p, ids, summary_only) for p in files)))
        else:
            frames = [_process_one(p, ids, summary_only) for p in files]
        parse_seconds = time.perf_counter() - parse_start
        aggregate_start = time.perf_counter()
        if not parallel and not summary_only:
            if collection is None:
                message = "Serial full-series collection was not created"
                raise AssertionError(message)
            combined = collection.po_combine()
        else:
            # Parallel receives data through IPC; summary reduces inside workers.
            combined = pd.concat(frames, ignore_index=True)
        aggregate_seconds = time.perf_counter() - aggregate_start
        required_rows = output_rows if summary_only else expected_rows
        if len(combined) != required_rows:
            message = f"{case}: got {len(combined)} rows instead of {required_rows}"
            raise AssertionError(message)
        if not combined["Value"].notna().all():
            message = f"{case}: missing Value samples after processing"
            raise AssertionError(message)
        if not combined["AbsValue"].notna().all():
            message = f"{case}: missing AbsValue samples after processing"
            raise AssertionError(message)
        # Reserve space for the next retained output, not just the source fixtures.
        export_rows = output_rows if summary_only else expected_rows
        available = shutil.disk_usage(target.parent).free
        if export_rows * ESTIMATED_LONG_ROW_BYTES * DISK_SAFETY_FACTOR > available:
            message = f"Insufficient free disk for retained {case} output"
            raise OSError(message)
        export_start = time.perf_counter()
        _export(combined, target, file_format)
        export_seconds = time.perf_counter() - export_start
        del frames, combined
    return {
        "case": case,
        "method": (
            "POProcessor + ProcessorCollection.po_combine"
            if not parallel and not summary_only
            else "POProcessor + per-file absolute peak + concat"
            if summary_only
            else "POProcessor + worker DataFrame IPC + concat"
        ),
        "rows": output_rows if summary_only else expected_rows,
        "seconds": time.perf_counter() - start,
        "parse_seconds": parse_seconds,
        "aggregate_seconds": aggregate_seconds,
        "export_seconds": export_seconds,
        "input_bytes": sum(p.stat().st_size for p in files),
        "output_bytes": target.stat().st_size,
        "sampled_rss": asdict(memory.sample),
    }


# Source-reference and output readback are deliberately outside the timed stages.
# They read the generated CSV independently of POProcessor and its result DataFrame.

SOURCE_SAMPLE_MINUTE: int = 100
ESTIMATED_LONG_ROW_BYTES: int = 512


@dataclass(frozen=True)
class SourceReference:
    series_samples: dict[tuple[str, str, str, int], float]
    per_run_peaks: dict[tuple[str, str, str], tuple[int, float]]
    sample_locations: frozenset[str]
    sample_minutes: frozenset[int]


def _source_reference(files: list[Path], spec: Workload) -> SourceReference:  # noqa: C901 - Independent source oracle.
    """Read actual generated CSVs to derive a separate value and peak-time oracle."""
    sample_indices = {0, spec.selected_locations - 1, spec.locations // 2, spec.locations - 1}
    sample_minutes = frozenset({0, min(SOURCE_SAMPLE_MINUTE, spec.final_minute), spec.final_minute})
    sample_locations = frozenset(f"L{i + 1:03d}" for i in sample_indices)
    series_samples: dict[tuple[str, str, str, int], float] = {}
    per_run_peaks: dict[tuple[str, str, str], tuple[int, float]] = {}
    expected_kinds = [kind for _ in range(spec.locations) for kind in VARIABLES]
    expected_locations = [f"L{i + 1:03d}" for i in range(spec.locations) for _ in VARIABLES]
    if len(files) != len(spec.run_codes):
        message = "Source file count does not match workload run-code grid"
        raise ValueError(message)
    for run_code, path in zip(spec.run_codes, files, strict=True):
        if path.name != f"{run_code}_PO.csv":
            message = f"Unexpected source filename: {path.name}"
            raise ValueError(message)
        with path.open(newline="", encoding="utf-8") as stream:
            reader = csv.reader(stream)
            expected_header = ["", "Time", *expected_kinds]
            expected_second = ["", "Time", *expected_locations]
            if next(reader) != expected_header or next(reader) != expected_second:
                message = f"Generated PO header drift: {path.name}"
                raise ValueError(message)
            n = 0
            for minute, row in enumerate(reader):
                if len(row) != len(expected_header) or not math.isclose(
                    float(row[1]), minute / 60, rel_tol=0, abs_tol=1e-8
                ):
                    message = f"Bad source column count/time at {path.name} minute {minute}"
                    raise ValueError(message)
                for loc_index in range(spec.locations):
                    location = f"L{loc_index + 1:03d}"
                    for var_index, kind in enumerate(VARIABLES):
                        value = float(row[2 + loc_index * len(VARIABLES) + var_index])
                        key = (run_code, location, kind)
                        old = per_run_peaks.get(key)
                        if old is None or abs(value) > abs(old[1]):
                            per_run_peaks[key] = (minute, value)
                        if minute in sample_minutes and loc_index in sample_indices:
                            series_samples[(*key, minute)] = value
                n += 1
            if n != spec.timestamps:
                message = f"Wrong source time-step count: {path.name}: {n}"
                raise ValueError(message)
    return SourceReference(series_samples, per_run_peaks, sample_locations, sample_minutes)


def _output_rows(spec: Workload, *, selected: bool, summary_only: bool) -> int:
    if summary_only:
        locations = spec.selected_locations if selected else spec.locations
        return len(spec.run_codes) * locations * len(VARIABLES)
    return spec.observations(selected)


def _enabled_case(spec: Workload, *, selected: bool, summary_only: bool, max_numeric_gib: float) -> bool:
    return summary_only or spec.observations(selected) * 24 <= max_numeric_gib * 1024**3


def _disk_reservation_bytes(spec: Workload, max_numeric_gib: float) -> int:
    """Reserve input fixtures plus every enabled output retained on the volume."""
    source_bytes = spec.observations(False) * 21
    export_bytes = 2 * sum(
        _output_rows(spec, selected=selected, summary_only=summary_only) * ESTIMATED_LONG_ROW_BYTES
        for selected in (False, True)
        for summary_only in (True, False)
        if _enabled_case(spec, selected=selected, summary_only=summary_only, max_numeric_gib=max_numeric_gib)
    )
    return int((source_bytes + export_bytes) * DISK_SAFETY_FACTOR)


def _export_chunks(path: Path) -> Iterator[pd.DataFrame]:
    """Stream readback instead of retaining another giant exported DataFrame."""
    if path.suffix == ".csv":
        yield from pd.read_csv(path, chunksize=100_000)
    elif path.suffix == ".parquet":
        import pyarrow.parquet as pq

        parquet = pq.ParquetFile(path)
        # PyArrow's batch iterator is incompletely typed; narrow its pandas result at this I/O boundary.
        for batch in cast(Any, parquet).iter_batches(batch_size=100_000):
            yield cast(pd.DataFrame, batch.to_pandas())
    else:
        message = f"Unsupported benchmark readback: {path.suffix}"
        raise ValueError(message)


def _validate_export(  # noqa: C901 - Keep independent readback assertions in one validation pass.
    target: Path,
    reference: SourceReference,
    spec: Workload,
    *,
    selected: bool,
    summary_only: bool,
) -> dict[str, int | float | str]:
    """Verify exported counts and independent source-keyed values and peak times."""
    count = spec.selected_locations if selected else spec.locations
    required = {"internalName", "Location", "Type", "Time", "Value", "AbsValue"}
    sample_expected = {key: value for key, value in reference.series_samples.items() if int(key[1][1:]) <= count}
    peak_expected = {key: value for key, value in reference.per_run_peaks.items() if int(key[1][1:]) <= count}
    observed_keys: set[tuple[str, str, str, int] | tuple[str, str, str]] = set()
    rows = 0
    started = time.perf_counter()
    for chunk in _export_chunks(target):
        if not required.issubset(chunk.columns):
            message = f"Export missing contract columns: {sorted(required - set(chunk.columns))}"
            raise AssertionError(message)
        rows += len(chunk)
        chosen = chunk
        if not summary_only:
            minutes = (chunk["Time"].astype(float) * 60).round().astype(int)
            mask = chunk["Location"].isin(reference.sample_locations) & minutes.isin(reference.sample_minutes)
            chosen = chunk.loc[mask]
        for run, loc, kind, hours, raw_value, abs_value in chosen[
            ["internalName", "Location", "Type", "Time", "Value", "AbsValue"]
        ].itertuples(index=False, name=None):
            minute = round(float(hours) * 60)
            key = (str(run), str(loc), str(kind))
            full_key: tuple[str, str, str, int] | tuple[str, str, str] = key if summary_only else (*key, minute)
            if full_key in observed_keys:
                message = f"Duplicate exported source sample: {full_key}"
                raise AssertionError(message)
            observed_keys.add(full_key)
            if summary_only:
                result = peak_expected.get(key)
                if result is None:
                    message = f"Unexpected peak key in export: {key}"
                    raise AssertionError(message)
                expected_minute, expected_value = result
            else:
                result = sample_expected.get((*key, minute))
                if result is None:
                    message = f"Unexpected series key in export: {full_key}"
                    raise AssertionError(message)
                expected_minute, expected_value = minute, result
            if minute != expected_minute or not math.isclose(float(hours), expected_minute / 60, abs_tol=1e-8):
                message = f"Wrong peak/sample time for {full_key}: {hours}"
                raise AssertionError(message)
            if not math.isclose(float(raw_value), expected_value, rel_tol=1e-9, abs_tol=1e-8):
                message = f"Wrong exported source Value for {full_key}: {raw_value}, expected {expected_value}"
                raise AssertionError(message)
            if not math.isclose(float(abs_value), abs(expected_value), rel_tol=1e-9, abs_tol=1e-8):
                message = f"Wrong exported AbsValue for {full_key}: {abs_value}, expected {abs(expected_value)}"
                raise AssertionError(message)
    expected_rows = _output_rows(spec, selected=selected, summary_only=summary_only)
    expected_keys = set(peak_expected if summary_only else sample_expected)
    if rows != expected_rows or observed_keys != expected_keys:
        message = (
            f"Export fidelity failed: {rows} rows vs {expected_rows}; "
            f"{len(observed_keys)} keyed values vs {len(expected_keys)}"
        )
        raise AssertionError(message)
    return {
        "status": "PASS",
        "validated_rows": rows,
        "source_keys_checked": len(expected_keys),
        "validation_seconds": time.perf_counter() - started,
    }


def _git_revision(directory: Path) -> str | None:
    git = shutil.which("git")
    if git is None:
        return None
    try:
        completed = subprocess.run(
            [git, "-C", str(directory), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
    except OSError, subprocess.TimeoutExpired:
        return None
    return completed.stdout.strip() if completed.returncode == 0 else None


def _processor_identity() -> dict[str, str | None]:
    """Record what code was actually imported, even if a wheel shadows the source checkout."""
    from ryan_library.processors.tuflow import base_processor
    from ryan_library.processors.tuflow.other_processors.POProcessor import POProcessor

    po_processor_path = Path(inspect.getfile(POProcessor)).resolve()
    filename = base_processor.__file__
    processor_path = Path(filename).resolve() if filename else None
    try:
        version = importlib.metadata.version("ryan-tools")
    except importlib.metadata.PackageNotFoundError:
        version = "source-checkout-uninstalled"
    return {
        "ryan_tools_distribution_version": version,
        "processor_source_path": str(processor_path) if processor_path else None,
        "po_processor_source_path": str(po_processor_path),
        "processor_source_git_revision": _git_revision(processor_path.parent) if processor_path else None,
        "benchmark_source_path": str(Path(__file__).resolve()),
        "benchmark_source_git_revision": _git_revision(Path(__file__).parent),
    }


def run_benchmark(
    output_root: Path,
    spec: Workload,
    *,
    file_format: Literal["csv", "parquet"] = "csv",
    workers: int = 2,
    max_numeric_gib: float = 1.0,
) -> dict[str, Any]:
    if workers < 1 or spec.locations < 1 or not (1 <= spec.selected_locations <= spec.locations):
        message = "Invalid workers or location selection"
        raise ValueError(message)
    if spec.final_minute < 0 or not spec.run_codes:
        message = "No valid benchmark observations"
        raise ValueError(message)
    # Reserve all retained source and enabled output files before generation.
    estimated_bytes = _disk_reservation_bytes(spec, max_numeric_gib)
    if estimated_bytes > shutil.disk_usage(output_root.parent).free:
        message = "Insufficient free disk for planned inputs and retained exports"
        raise OSError(message)
    output_root.mkdir(parents=True, exist_ok=False)
    generation_start = time.perf_counter()
    files = make_fixture(output_root / "inputs", spec)
    generation_seconds = time.perf_counter() - generation_start
    manifest: dict[str, Any] = {
        "processor_identity": _processor_identity(),
        "estimated_disk_reservation_bytes": estimated_bytes,
        "profile": {**asdict(spec), "number_of_runs": len(files), "timestamps": spec.timestamps},
        "python": sys.version,
        "pandas": pd.__version__,
        "platform": platform.platform(),
        "export_format": file_format,
        "generated_csv_bytes": sum(p.stat().st_size for p in files),
        "generation_seconds": generation_seconds,
        "expected_observations_all": spec.observations(False),
        "expected_observations_selected": spec.observations(True),
        "numeric_lower_bound_all_bytes": spec.observations(False) * 24,
        "numeric_lower_bound_selected_bytes": spec.observations(True) * 24,
        "results": [],
        "limits": [
            "RSS is sampled at 50 ms; worker peaks can be missed; not a process high-water mark.",
            "Operating-system CSV cache is uncontrolled; no cold-cache claim.",
            "Summary rows are per-run absolute peaks, not ensemble or closure statistics.",
            "Parallel collection uses DataFrame IPC and pandas concat, not ProcessorCollection.po_combine.",
            "Full-series aggregation retains all per-run DataFrames; this is an intentionally RAM-gated baseline.",
            "This baseline does not implement partitioned export or Dask.",
        ],
    }
    reference: SourceReference | None = None
    for selected in (False, True):
        for summary in (True, False):
            for parallel in (False, True):
                if not summary and spec.observations(selected) * 24 > max_numeric_gib * 1024**3:
                    manifest["results"].append(
                        {
                            "case": f"{'selected' if selected else 'all'}_series_"
                            f"{'parallel' if parallel else 'serial'}",
                            "status": "NOT RUN",
                            "reason": "Numeric payload lower bound exceeds configured RAM budget",
                        }
                    )
                    continue
                result = run_case(
                    files,
                    spec,
                    selected=selected,
                    summary_only=summary,
                    parallel=parallel,
                    output_root=output_root,
                    file_format=file_format,
                    workers=workers,
                    max_numeric_gib=max_numeric_gib,
                )
                # Build the independent oracle only after the first measured case.
                # Later timings have no controlled cold-cache guarantee.
                if reference is None:
                    oracle_start = time.perf_counter()
                    reference = _source_reference(files, spec)
                    manifest["source_oracle_seconds"] = time.perf_counter() - oracle_start
                target = output_root / f"{result['case']}.{file_format}"
                try:
                    result["fidelity"] = _validate_export(
                        target, reference, spec, selected=selected, summary_only=summary
                    )
                    result["status"] = "PASS"
                except (AssertionError, OSError, ValueError, KeyError, TypeError, ImportError) as exc:
                    manifest["results"].append({"case": result["case"], "status": "FAILED", "reason": str(exc)})
                    (output_root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
                    raise
                manifest["results"].append(result)
                print(f"{result['case']}: {result['rows']:,} rows in {result['seconds']:.3f}s, fidelity PASS")
    (output_root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return manifest


def main() -> None:
    if PROFILE == "full" and not ALLOW_FULL_WORKLOAD:
        message = "Set ALLOW_FULL_WORKLOAD=True explicitly before generating the 300-run full dataset"
        raise ValueError(message)
    result = run_benchmark(
        output_root=OUTPUT_ROOT.absolute(),
        spec=workload(PROFILE),
        file_format=EXPORT_FORMAT,
        workers=WORKERS,
        max_numeric_gib=MAX_FULL_SERIES_NUMERIC_GIB,
    )
    print(f"Recorded {len(result['results'])} benchmark cases at {OUTPUT_ROOT.absolute()}")


if __name__ == "__main__":
    main()
