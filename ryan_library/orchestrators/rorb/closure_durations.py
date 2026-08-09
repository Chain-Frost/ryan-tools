"""Coordinate RORB threshold-exceedance processing and exports."""
__lazy_modules__ = ['pandas']

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from multiprocessing import Pool
import os
from pathlib import Path

from loguru import logger
import pandas as pd
from pandas import DataFrame

from ryan_library.functions.RORB.read_rorb_files import analyze_hydrograph, find_batch_files, parse_batch_output
from ryan_library.functions.loguru_helpers import LogQueue, setup_logger, worker_initializer
from ryan_library.functions.pandas.median_calc import median_stats as median_stats_func


@dataclass(slots=True, frozen=True)
class HydrographJob:
    """Picklable inputs for one RORB hydrograph analysis."""

    aep: str
    duration: str
    tp: int
    csv_path: Path
    out_path: Path
    thresholds: tuple[float, ...]


def _analyze_job(job: HydrographJob) -> pd.DataFrame:
    """Analyze one hydrograph job in either the parent or a pool worker."""

    return analyze_hydrograph(
        aep=job.aep,
        duration=job.duration,
        tp=job.tp,
        csv_path=job.csv_path,
        out_path=job.out_path,
        thresholds=list(job.thresholds),
    )


def _collect_batch_data(paths: Iterable[Path]) -> pd.DataFrame:
    """Collect parsed run tables from all discovered batch outputs."""

    batch_files: list[Path] = find_batch_files(paths=paths)
    tables: list[DataFrame] = [parse_batch_output(batchout_file=path) for path in batch_files]
    populated_tables = [table for table in tables if not table.empty]
    return pd.concat(objs=populated_tables, ignore_index=True) if populated_tables else pd.DataFrame()


def _build_jobs(batch_df: pd.DataFrame, thresholds: list[float]) -> list[HydrographJob]:
    """Convert parsed batch rows into typed worker jobs."""

    threshold_values = tuple(float(value) for value in thresholds)
    return [
        HydrographJob(
            aep=str(row.AEP),
            duration=str(row.Duration),
            tp=int(float(str(row.TPat))),
            csv_path=Path(str(row.csv)),
            out_path=Path(str(row.Path)),
            thresholds=threshold_values,
        )
        for row in batch_df.itertuples(index=False)
    ]


def _worker_count(job_count: int, pool_size: int | None) -> int:
    """Return a safe worker count for the available jobs and CPUs."""

    if job_count < 1:
        return 1
    if pool_size is not None:
        if pool_size < 1:
            raise ValueError("pool_size must be at least 1")
        return min(pool_size, job_count)
    available_workers: int = max((os.cpu_count() or 1) - 1, 1)
    return min(available_workers, job_count, 20)


def _process_hydrographs(
    batch_df: pd.DataFrame,
    thresholds: list[float],
    *,
    log_queue: LogQueue | None = None,
    pool_size: int | None = None,
) -> pd.DataFrame:
    """Analyze all hydrographs, using a process pool when beneficial."""

    jobs: list[HydrographJob] = _build_jobs(batch_df=batch_df, thresholds=thresholds)
    worker_count: int = _worker_count(job_count=len(jobs), pool_size=pool_size)
    logger.info("Processing {} RORB hydrographs with {} worker(s)", len(jobs), worker_count)

    if worker_count == 1:
        results: list[DataFrame] = [_analyze_job(job) for job in jobs]
    elif log_queue is None:
        with Pool(processes=worker_count) as pool:
            results = pool.map(func=_analyze_job, iterable=jobs)
    else:
        with Pool(
            processes=worker_count,
            initializer=worker_initializer,
            initargs=(log_queue,),
        ) as pool:
            results = pool.map(func=_analyze_job, iterable=jobs)

    populated_results: list[DataFrame] = [result for result in results if not result.empty]
    return pd.concat(objs=populated_results, ignore_index=True) if populated_results else pd.DataFrame()


def _summarise_results(df: pd.DataFrame) -> pd.DataFrame:
    """Summarise each path, location, threshold, and AEP population.

    The central value is the upper-middle temporal-pattern result for each
    duration, followed by the duration with the greatest such value. Low and
    high are the extrema across all durations; the critical-duration average
    includes zeroes.
    """

    final_columns: list[str] = [
        "Path",
        "Location",
        "ThresholdFlow",
        "AEP",
        "Central_Value",
        "Critical_Duration",
        "Critical_Tp",
        "Low_Value",
        "High_Value",
        "Average_Value",
        "Closest_Tpcrit",
        "Closest_Value",
    ]
    rows: list[list[object]] = []
    grouped = df.groupby(["out_path", "Location", "ThresholdFlow", "AEP"], sort=False)
    for name, group in grouped:
        stats, _ = median_stats_func(group, "Duration_Exceeding", "TP", "Duration")
        rows.append(
            [
                *name,
                stats.get("median"),
                stats.get("median_duration"),
                stats.get("median_TP"),
                stats.get("low"),
                stats.get("high"),
                stats.get("mean_including_zeroes"),
                stats.get("median_TP"),
                stats.get("median"),
            ]
        )
    return pd.DataFrame(rows, columns=final_columns)


def _default_thresholds() -> list[float]:
    """Return the maintained, ascending default flow thresholds."""

    values: list[int] = [*range(1, 10), *range(10, 100, 2), *range(100, 2100, 10)]
    return [float(value) for value in values]


def run_closure_durations(
    paths: Iterable[Path] | None = None,
    thresholds: list[float] | None = None,
    log_level: str = "INFO",
    pool_size: int | None = None,
) -> None:
    """Process RORB ``batch.out`` files and export exceedance tables.

    Args:
        paths: Roots searched recursively. Defaults to the working directory.
        thresholds: Flow thresholds. Defaults to the maintained ascending set.
        log_level: Minimum console log level.
        pool_size: Worker count, or ``None`` to select one automatically.
    """

    search_paths: list[Path] = list(paths) if paths is not None else [Path.cwd()]
    threshold_values: list[float] = (
        _default_thresholds() if thresholds is None else [float(value) for value in thresholds]
    )
    if not threshold_values:
        raise ValueError("At least one threshold is required")

    with setup_logger(console_log_level=log_level) as log_queue:
        batch_df: DataFrame = _collect_batch_data(paths=search_paths)
        if batch_df.empty:
            logger.warning("No RORB batch.out data found.")
            return

        timestamp = datetime.now().strftime("%Y%m%d-%H%M")
        batch_df.to_csv(path_or_buf=f"{timestamp}_batchouts.csv", index=False)

        result_df: DataFrame = _process_hydrographs(
            batch_df=batch_df,
            thresholds=threshold_values,
            log_queue=log_queue,
            pool_size=pool_size,
        )
        if result_df.empty:
            logger.warning("No RORB hydrograph data processed.")
            return

        result_df.to_parquet(path=f"{timestamp}_durex.parquet.gzip", compression="gzip", index=False)
        result_df.to_csv(path_or_buf=f"{timestamp}_durex.csv", index=False)
        summary_df: DataFrame = _summarise_results(df=result_df)
        summary_df["AEP_sort_key"] = pd.to_numeric(
            summary_df["AEP"].astype(str).str.extract(r"([0-9]*\.?[0-9]+)")[0],
            errors="coerce",
        )
        summary_df.sort_values(
            by=["Path", "Location", "ThresholdFlow", "AEP_sort_key", "AEP"],
            ignore_index=True,
            inplace=True,
        )
        summary_df.drop(columns="AEP_sort_key", inplace=True)
        summary_df.to_csv(path_or_buf=f"{timestamp}_QvsTexc.csv", index=False)
        logger.success("RORB closure-duration processing complete")
