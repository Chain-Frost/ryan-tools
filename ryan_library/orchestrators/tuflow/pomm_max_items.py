# ryan_library/orchestrators/tuflow/pomm_max_items.py
"""POMM Peak Reporting Utilities.

This module provides functions to generate "Peak Reports" from POMM (Plot Output Maximums/Minimums) data.
It allows extracting Mean or Median peak values across multiple events/durations for specific locations.
It includes both modern workflow functions and deprecated wrappers for backward compatibility.
"""

__lazy_modules__: list[str] = ["pandas"]

import warnings
from collections.abc import Callable, Collection, Sequence
from datetime import datetime
from functools import partial
from pathlib import Path

import pandas as pd
from loguru import logger

from ryan_library.functions.live_dashboard import LiveWorkflowDashboard
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.tuflow.pomm_utils import (
    STANDARD_DURATIONS_MINUTES,
    aggregated_from_paths,
    save_peak_report_mean,
    save_peak_report_median,
)
from ryan_library.functions.tuflow.tuflow_common import FileBatchProgress
from ryan_library.functions.tuflow.wrapper_helpers import (
    normalize_data_types,
    validate_discovery_settings,
    warn_on_invalid_types,
)
from ryan_library.processors.tuflow.base_processor import BaseProcessor

DEFAULT_DATA_TYPES: tuple[str, ...] = ("POMM", "RLL_Qmx")
ACCEPTED_DATA_TYPES: frozenset[str] = frozenset(DEFAULT_DATA_TYPES)


def run_peak_report(script_directory: Path | None = None) -> None:
    """Legacy entry point retained for backwards compatibility.

    .. deprecated::
       Use `export_median_peak_report` instead.
    """
    print()
    print("You are using an old wrapper")
    print()
    export_median_peak_report(script_directory=script_directory)


def run_peak_report_workflow(
    *,
    script_directory: Path | None = None,
    paths_to_process: Sequence[Path] | None = None,
    log_level: str = "INFO",
    include_pomm: bool = True,
    locations_to_include: Collection[str] | None = None,
    include_data_types: Collection[str] | None = None,
    exporter: Callable[..., None],
    use_live_dashboard: bool = True,
    live_refresh_per_second: float = 2.0,
    live_max_rows: int = 10,
) -> bool:
    """Aggregate and export POMM peak data with a truthful whole-workflow outcome.

    The underlying aggregator owns discovery and parsing; the dashboard tracks
    the report as one workflow task rather than asserting per-file UI success.
    Shared file outcomes prohibit exporting an incomplete input batch.
    """
    script_directory = script_directory or Path.cwd()
    effective_paths_to_process: list[Path] = list((script_directory,) if paths_to_process is None else paths_to_process)
    resolved_data_types, invalid_types = normalize_data_types(
        requested=include_data_types,
        default=DEFAULT_DATA_TYPES,
        accepted=ACCEPTED_DATA_TYPES,
    )
    validate_discovery_settings(
        paths_to_process=effective_paths_to_process,
        selected_data_types=resolved_data_types,
        accepted_data_types=ACCEPTED_DATA_TYPES,
        context="POMM peak report",
    )
    normalized_locations: frozenset[str] = BaseProcessor.normalize_locations(locations=locations_to_include)
    location_filter: frozenset[str] | None = normalized_locations or None
    batch_progress = FileBatchProgress()
    dashboard = LiveWorkflowDashboard(
        title="TUFLOW POMM Peak Report",
        subtitle=str(script_directory),
        enabled=use_live_dashboard,
        refresh_per_second=live_refresh_per_second,
        max_rows=live_max_rows,
    )

    with setup_logger(console_log_level=log_level) as log_queue:
        with dashboard:
            dashboard.set_tasks(labels=["POMM peak report"])
            dashboard.mark_running(index=1)
            try:
                dashboard.set_phase("Processing")
                logger.info("Current Working Directory: {}", Path.cwd())
                warn_on_invalid_types(
                    invalid_types=invalid_types,
                    accepted_types=ACCEPTED_DATA_TYPES,
                    context="POMM peak report",
                )
                if locations_to_include and not normalized_locations:
                    logger.warning(
                        "Location filter provided but no valid values found. All locations will be included."
                    )

                aggregated_df: pd.DataFrame = aggregated_from_paths(
                    paths=effective_paths_to_process,
                    locations_to_include=location_filter,
                    include_data_types=resolved_data_types,
                    log_queue=log_queue,
                    batch_progress=batch_progress,
                )
                dashboard.set_phase("Aggregating")
                if batch_progress.failed or batch_progress.completed != batch_progress.scheduled:
                    msg = (
                        f"POMM peak report: {batch_progress.failed}/{batch_progress.scheduled} input(s) failed; "
                        f"{batch_progress.completed} completed; report not exported"
                    )
                    raise RuntimeError(msg)
                if aggregated_df.empty:
                    warn_on_invalid_types(
                        invalid_types=invalid_types,
                        accepted_types=ACCEPTED_DATA_TYPES,
                        context="POMM peak report completed",
                    )
                    if location_filter:
                        logger.warning("No rows remain after applying the Location filter. Exiting.")
                    else:
                        logger.warning("No POMM CSV files found. Exiting.")
                    dashboard.mark_finished(index=1, status="SKIP", detail="no aggregated rows")
                    dashboard.finish(outcome="SKIPPED", detail="no report generated")
                    return True

                timestamp = datetime.now().strftime(format="%Y%m%d-%H%M")
                dashboard.set_phase("Exporting")
                exporter(
                    aggregated_df=aggregated_df,
                    script_directory=script_directory,
                    timestamp=timestamp,
                    include_pomm=include_pomm,
                )
                warn_on_invalid_types(
                    invalid_types=invalid_types,
                    accepted_types=ACCEPTED_DATA_TYPES,
                    context="POMM peak report completed",
                )
                dashboard.mark_finished(index=1, status="OK", detail=f"{len(aggregated_df)} aggregated row(s)")
                dashboard.finish(outcome="SUCCESS", detail="peak report exported")
                return True
            except KeyboardInterrupt:
                dashboard.mark_finished(index=1, status="FAIL", detail="interrupted")
                dashboard.finish(outcome="INTERRUPTED", detail="processing interrupted")
                raise
            except Exception:
                logger.exception("POMM peak report failed during aggregation or export.")
                dashboard.mark_finished(index=1, status="FAIL", detail="processing or export failed")
                dashboard.finish(outcome="FAILED", detail="processing or export failed")
                raise


def export_median_peak_report(
    *,
    script_directory: Path | None = None,
    paths_to_process: Sequence[Path] | None = None,
    log_level: str = "INFO",
    include_pomm: bool = True,
    locations_to_include: Collection[str] | None = None,
    include_data_types: Collection[str] | None = None,
    use_live_dashboard: bool = True,
    live_refresh_per_second: float = 2.0,
    live_max_rows: int = 10,
) -> bool:
    """Locate and process POMM files and export **median-based** peak values.

    This function is a wrapper around `run_peak_report_workflow` injecting the `save_peak_report_median` exporter.
    """
    return run_peak_report_workflow(
        script_directory=script_directory,
        paths_to_process=paths_to_process,
        log_level=log_level,
        include_pomm=include_pomm,
        locations_to_include=locations_to_include,
        include_data_types=include_data_types,
        exporter=save_peak_report_median,
        use_live_dashboard=use_live_dashboard,
        live_refresh_per_second=live_refresh_per_second,
        live_max_rows=live_max_rows,
    )


def export_mean_peak_report(
    *,
    script_directory: Path | None = None,
    paths_to_process: Sequence[Path] | None = None,
    log_level: str = "INFO",
    include_pomm: bool = True,
    locations_to_include: Collection[str] | None = None,
    include_data_types: Collection[str] | None = None,
    standard_durations: Sequence[int] = STANDARD_DURATIONS_MINUTES,
    expected_tp_count: int = 10,
    use_live_dashboard: bool = True,
    live_refresh_per_second: float = 2.0,
    live_max_rows: int = 10,
) -> bool:
    """Locate and process POMM files and export **mean-based** peak values.

    This function is a wrapper around `run_peak_report_workflow` injecting the `save_peak_report_mean` exporter.
    """
    return run_peak_report_workflow(
        script_directory=script_directory,
        paths_to_process=paths_to_process,
        log_level=log_level,
        include_pomm=include_pomm,
        locations_to_include=locations_to_include,
        include_data_types=include_data_types,
        exporter=partial(
            save_peak_report_mean, standard_durations=standard_durations, expected_tp_count=expected_tp_count
        ),
        use_live_dashboard=use_live_dashboard,
        live_refresh_per_second=live_refresh_per_second,
        live_max_rows=live_max_rows,
    )


def run_median_peak_report(
    script_directory: Path | None = None,
    log_level: str = "INFO",
    include_pomm: bool = True,
    locations_to_include: Collection[str] | None = None,
) -> None:
    """Deprecated wrapper around :func:`export_median_peak_report`.

    .. deprecated::
       Use `export_median_peak_report` instead.
    """
    warnings.warn(
        message="run_median_peak_report is deprecated; use export_median_peak_report instead.",
        category=DeprecationWarning,
        stacklevel=2,
    )
    export_median_peak_report(
        script_directory=script_directory,
        log_level=log_level,
        include_pomm=include_pomm,
        locations_to_include=locations_to_include,
    )


def run_mean_peak_report(
    script_directory: Path | None = None,
    log_level: str = "INFO",
    include_pomm: bool = True,
    locations_to_include: Collection[str] | None = None,
) -> None:
    """Deprecated wrapper around :func:`export_mean_peak_report`.

    .. deprecated::
       Use `export_mean_peak_report` instead.
    """
    warnings.warn(
        message="run_mean_peak_report is deprecated; use export_mean_peak_report instead.",
        category=DeprecationWarning,
        stacklevel=2,
    )
    export_mean_peak_report(
        script_directory=script_directory,
        log_level=log_level,
        include_pomm=include_pomm,
        locations_to_include=locations_to_include,
    )
