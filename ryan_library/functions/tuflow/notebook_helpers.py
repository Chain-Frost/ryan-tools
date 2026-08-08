"""
Helper functions to enable easy usage of TUFLOW workflows in Jupyter Notebooks.

This module provides notebook-friendly wrappers around the TUFLOW orchestrators in
``ryan_library.orchestrators.tuflow``.  Each helper returns a ``pandas.DataFrame``
(or a small collection of them) so the results are immediately available for
interactive inspection, plotting, and further analysis.

Jupyter / multiprocessing notes
-------------------------------
On Windows, ``multiprocessing.Pool`` requires a ``if __name__ == "__main__":`` guard
in the entry module.  Jupyter kernels do not provide this, so parallel processing can
silently hang or crash.  By default, all helpers in this module **detect the Jupyter
environment and fall back to serial processing**.  You can force parallel mode by
passing ``parallel=True``, but be aware this may not work reliably in all notebook
configurations.
"""

from __future__ import annotations

import importlib
import multiprocessing
from collections.abc import Callable, Collection, Sequence
from pathlib import Path
from typing import cast

import pandas as pd
from loguru import logger

from ryan_library.classes.suffixes_and_dtypes import SuffixesConfig
from ryan_library.functions.loguru_helpers import configure_notebook_logging, setup_logger
from ryan_library.functions.tuflow.po_timeseries_checks import StabilityCheckResult
from ryan_library.functions.tuflow.tuflow_common import (
    collect_files,
    process_file,
    process_files_in_parallel,
)
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection

# Default console log level for notebooks
DEFAULT_NOTEBOOK_LOG_LEVEL = "INFO"

# ---------------------------------------------------------------------------
# Windows multiprocessing safety
# ---------------------------------------------------------------------------
multiprocessing.freeze_support()


# ---------------------------------------------------------------------------
# Environment detection
# ---------------------------------------------------------------------------


def is_notebook() -> bool:
    """Return ``True`` when running inside a Jupyter / IPython notebook kernel.

    Detection uses the ``IPKernelApp`` check which is reliable across classic
    Jupyter, JupyterLab, VS Code notebooks, and Google Colab.
    """
    try:
        ipython_module = importlib.import_module("IPython")
        get_ipython = cast(Callable[[], object | None], getattr(ipython_module, "get_ipython"))
        shell: object | None = get_ipython()
        if shell is None:
            return False
        return type(shell).__name__ == "ZMQInteractiveShell"
    except AttributeError, ImportError:
        return False


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------


def init_notebook_logging(
    level: str = DEFAULT_NOTEBOOK_LOG_LEVEL,
    *,
    log_file: str | None = None,
    file_log_level: str = "DEBUG",
) -> None:
    """Configure loguru for clean notebook output.

    Existing sinks are replaced on every call, making it safe to rerun after
    changing levels without accumulating duplicate output. Use
    ``level="SUCCESS"`` for low-volume AI/MCP consumption while retaining
    detailed records in an optional ``log_file``.
    """
    configure_notebook_logging(
        console_log_level=level,
        log_file=log_file,
        file_log_level=file_log_level,
    )


# ---------------------------------------------------------------------------
# Resolve whether to use parallel processing
# ---------------------------------------------------------------------------


def _resolve_parallel(parallel: bool | None) -> bool:
    """Decide whether to use multiprocessing.

    * ``None`` (default) → auto-detect: serial in notebooks, parallel otherwise.
    * Explicit ``True`` / ``False`` → honour the caller's choice, but warn if
      ``True`` inside a notebook.
    """
    if parallel is None:
        if is_notebook():
            logger.info(
                "Notebook detected — defaulting to serial processing. "
                "Pass parallel=True to override (may be unreliable on Windows)."
            )
            return False
        return True

    if parallel and is_notebook():
        logger.warning(
            "Parallel processing forced inside a Jupyter notebook. "
            "This may hang on Windows due to multiprocessing limitations."
        )
    return parallel


# ---------------------------------------------------------------------------
# Core data-loading helper
# ---------------------------------------------------------------------------


def load_tuflow_data(
    paths: Sequence[str | Path],
    data_types: Collection[str],
    parallel: bool | None = None,
    log_level: str = DEFAULT_NOTEBOOK_LOG_LEVEL,
    locations: Collection[str] | None = None,
) -> ProcessorCollection:
    """Load TUFLOW result files into a :class:`ProcessorCollection`.

    This function abstracts away the complexity of setting up multiprocessing
    loggers and configuration loading, making it straightforward to use in
    interactive environments like Jupyter Notebooks.

    Args:
        paths: Directory paths to search for files.
        data_types: TUFLOW data types to load (e.g. ``["Q", "V", "H", "POMM"]``).
        parallel: ``None`` = auto-detect (serial in notebooks), ``True`` = force
            parallel, ``False`` = force serial.
        log_level: Logging verbosity.  Defaults to ``"INFO"``.
        locations: Optional location IDs to filter the results by.

    Returns:
        A :class:`ProcessorCollection` containing the loaded and processed data.
    """
    use_parallel: bool = _resolve_parallel(parallel)

    # Normalize paths to Path objects
    path_objects: list[Path] = [Path(p) for p in paths]

    # Initialize SuffixesConfig (idempotent Singleton)
    suffixes_config: SuffixesConfig = SuffixesConfig.get_instance()

    # Collect files first
    files: list[Path] = collect_files(
        paths_to_process=path_objects,
        include_data_types=data_types,
        suffixes_config=suffixes_config,
    )

    if not files:
        print(f"No files found matching types {list(data_types)} in {[str(p) for p in path_objects]}")
        return ProcessorCollection()

    print(f"Found {len(files)} file(s) matching types {list(data_types)}")

    # Process files
    collection = ProcessorCollection()

    if use_parallel:
        with setup_logger(console_log_level=log_level) as log_queue:
            collection: ProcessorCollection = process_files_in_parallel(
                file_list=files,
                log_queue=log_queue,
                log_level=log_level,
            )
    else:
        for file_path in files:
            proc: BaseProcessor | None = process_file(file_path=file_path)
            if proc:
                collection.add_processor(proc)

    # Apply location filtering if requested
    if locations:
        collection.filter_locations(locations)

    print(f"Loaded {len(collection.processors)} processor(s).")
    return collection


# ---------------------------------------------------------------------------
# Workflow: POMM Combine
# ---------------------------------------------------------------------------


def run_pomm_combine(
    paths: Sequence[str | Path],
    *,
    data_types: Collection[str] = ("POMM", "RLL_Qmx"),
    locations: Collection[str] | None = None,
    parallel: bool | None = None,
    log_level: str = DEFAULT_NOTEBOOK_LOG_LEVEL,
) -> pd.DataFrame:
    """Combine POMM (Plot Output Maximums/Minimums) files into a single DataFrame.

    Notebook equivalent of ``ryan-scripts/TUFLOW-python/POMM_combine.py``.

    Args:
        paths: Directories to scan for POMM CSV files.
        data_types: File types to include.  Defaults to ``("POMM", "RLL_Qmx")``.
        locations: Optional location filter.
        parallel: Parallelism mode (see :func:`load_tuflow_data`).
        log_level: Console log level.

    Returns:
        Combined POMM DataFrame, or an empty DataFrame if no data was found.
    """
    collection: ProcessorCollection = load_tuflow_data(
        paths=list(paths),
        data_types=data_types,
        parallel=parallel,
        log_level=log_level,
        locations=locations,
    )
    if not collection.processors:
        return pd.DataFrame()
    return collection.pomm_combine()


# ---------------------------------------------------------------------------
# Workflow: PO Combine
# ---------------------------------------------------------------------------


def run_po_combine(
    paths: Sequence[str | Path],
    *,
    data_types: Collection[str] = ("PO",),
    locations: Collection[str] | None = None,
    parallel: bool | None = None,
    log_level: str = DEFAULT_NOTEBOOK_LOG_LEVEL,
) -> pd.DataFrame:
    """Combine PO (Plot Output) timeseries CSV files into a single DataFrame.

    Notebook equivalent of ``ryan-scripts/TUFLOW-python/PO_combine.py``.

    Args:
        paths: Directories to scan for PO CSV files.
        data_types: File types to include.  Defaults to ``("PO",)``.
        locations: Optional location filter.
        parallel: Parallelism mode (see :func:`load_tuflow_data`).
        log_level: Console log level.

    Returns:
        Combined PO timeseries DataFrame.
    """
    collection: ProcessorCollection = load_tuflow_data(
        paths=list(paths),
        data_types=data_types,
        parallel=parallel,
        log_level=log_level,
        locations=locations,
    )
    if not collection.processors:
        return pd.DataFrame()
    return collection.po_combine()


# ---------------------------------------------------------------------------
# Workflow: Culvert Maximums
# ---------------------------------------------------------------------------


def run_culvert_maximums(
    paths: Sequence[str | Path],
    *,
    data_types: Collection[str] = ("Nmx", "Cmx", "Chan", "ccA", "RLL_Qmx", "EOF"),
    locations: Collection[str] | None = None,
    parallel: bool | None = None,
    log_level: str = DEFAULT_NOTEBOOK_LOG_LEVEL,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load and combine culvert maximum data.

    Notebook equivalent of ``ryan-scripts/TUFLOW-python/TUFLOW_Culvert_Maximums.py``.

    Args:
        paths: Directories to scan.
        data_types: File types to include.
        locations: Optional location filter.
        parallel: Parallelism mode (see :func:`load_tuflow_data`).
        log_level: Console log level.

    Returns:
        A tuple of ``(maximums_df, raw_df)``.  ``maximums_df`` is the grouped
        1D maximums result; ``raw_df`` is the raw concatenation of all files.
    """
    collection: ProcessorCollection = load_tuflow_data(
        paths=list(paths),
        data_types=data_types,
        parallel=parallel,
        log_level=log_level,
        locations=locations,
    )
    if not collection.processors:
        return pd.DataFrame(), pd.DataFrame()

    collection.align_eof_channel_ids()
    maximums_df: pd.DataFrame = collection.combine_1d_maximums()
    raw_df: pd.DataFrame = collection.combine_raw()
    return maximums_df, raw_df


# ---------------------------------------------------------------------------
# Workflow: Culvert Timeseries
# ---------------------------------------------------------------------------


def run_culvert_timeseries(
    paths: Sequence[str | Path],
    *,
    data_types: Collection[str] = ("Q", "V", "H", "Chan", "EOF"),
    locations: Collection[str] | None = None,
    parallel: bool | None = None,
    log_level: str = DEFAULT_NOTEBOOK_LOG_LEVEL,
) -> pd.DataFrame:
    """Load and combine culvert timeseries data.

    Notebook equivalent of ``ryan-scripts/TUFLOW-python/TUFLOW_Culvert_Timeseries.py``.

    Args:
        paths: Directories to scan.
        data_types: File types to include.
        locations: Optional location filter.
        parallel: Parallelism mode.
        log_level: Console log level.

    Returns:
        Combined 1D timeseries DataFrame.
    """
    collection: ProcessorCollection = load_tuflow_data(
        paths=list(paths),
        data_types=data_types,
        parallel=parallel,
        log_level=log_level,
        locations=locations,
    )
    if not collection.processors:
        return pd.DataFrame()
    return collection.combine_1d_timeseries()


# ---------------------------------------------------------------------------
# Workflow: Culvert Mean / Median Peaks
# ---------------------------------------------------------------------------


def run_culvert_mean_peaks(
    paths: Sequence[str | Path],
    *,
    data_types: Collection[str] = ("Nmx", "Cmx", "Chan", "ccA", "RLL_Qmx", "EOF"),
    locations: Collection[str] | None = None,
    parallel: bool | None = None,
    log_level: str = DEFAULT_NOTEBOOK_LOG_LEVEL,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Calculate mean peak values per AEP/duration for culvert maximums.

    Notebook equivalent of ``ryan-scripts/TUFLOW-python/TUFLOW_Culvert-mean-max-aep-dur.py``.

    This first loads and combines the culvert maximums, then calculates the
    AEP/duration mean using :func:`find_culvert_aep_dur_mean` and the
    critical-duration mean-max using :func:`find_culvert_aep_mean_max`.

    Args:
        paths: Directories to scan.
        data_types: File types to include.
        locations: Optional location filter.
        parallel: Parallelism mode.
        log_level: Console log level.

    Returns:
        A tuple of ``(aep_dur_mean_df, aep_mean_max_df)``.
    """
    from ryan_library.orchestrators.tuflow.tuflow_culverts_mean import (
        find_culvert_aep_dur_mean,
        find_culvert_aep_mean_max,
    )

    collection: ProcessorCollection = load_tuflow_data(
        paths=list(paths),
        data_types=data_types,
        parallel=parallel,
        log_level=log_level,
        locations=locations,
    )
    if not collection.processors:
        return pd.DataFrame(), pd.DataFrame()

    maximums_df: pd.DataFrame = collection.combine_1d_maximums()
    if maximums_df.empty:
        print("No maximums data found for mean peak calculation.")
        return pd.DataFrame(), pd.DataFrame()

    aep_dur_mean: pd.DataFrame = find_culvert_aep_dur_mean(aggregated_df=maximums_df)
    aep_mean_max: pd.DataFrame = find_culvert_aep_mean_max(aep_dur_mean=maximums_df)
    return aep_dur_mean, aep_mean_max


# ---------------------------------------------------------------------------
# Workflow: Log Summary
# ---------------------------------------------------------------------------


def run_log_summary(
    paths: Sequence[str | Path],
    *,
    log_level: str = DEFAULT_NOTEBOOK_LOG_LEVEL,
) -> pd.DataFrame:
    """Parse TUFLOW log files (.tlf) and return a summary DataFrame.

    Notebook equivalent of ``ryan-scripts/TUFLOW-python/LogSummary.py``.

    Unlike the wrapper script, this function does **not** use a live dashboard
    and processes files serially to avoid multiprocessing issues in notebooks.

    Args:
        paths: Directories to scan for ``.tlf`` files.
        log_level: Console log level.

    Returns:
        A DataFrame with one row per completed simulation log.
    """
    from ryan_library.orchestrators.tuflow.tuflow_logsummary import (
        LogFileProcessingResult,
        build_log_summary_dataframe,
        discover_log_files,
        process_log_file_for_dashboard,
    )

    path_objects: list[Path] = [Path(p) for p in paths]

    all_files: list[Path] = []
    for root in path_objects:
        if root.is_dir():
            all_files.extend(discover_log_files(root_dir=root))
        else:
            logger.warning(f"Skipping non-directory path: {root}")

    if not all_files:
        print("No .tlf log files found.")
        return pd.DataFrame()

    print(f"Found {len(all_files)} log file(s).  Processing serially...")

    results: list[LogFileProcessingResult] = []
    for logfile in all_files:
        result = process_log_file_for_dashboard(logfile=logfile)
        results.append(result)

    summary_df: pd.DataFrame = build_log_summary_dataframe(
        files=all_files,
        processing_results=results,
    )
    ok_count: int = sum(1 for r in results if r.status == "OK")
    print(f"Processed {len(all_files)} log(s): {ok_count} completed, {len(all_files) - ok_count} skipped/failed.")
    return summary_df


# ---------------------------------------------------------------------------
# Workflow: Closure Durations
# ---------------------------------------------------------------------------


def run_closure_durations(
    paths: Sequence[str | Path],
    *,
    thresholds: list[float] | None = None,
    data_type: str = "Flow",
    locations: Collection[str] | None = None,
    log_level: str = DEFAULT_NOTEBOOK_LOG_LEVEL,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compute closure (exceedance) durations from PO timeseries files.

    Notebook equivalent of ``ryan-scripts/TUFLOW-python/TUFLOW-find-closure-durations.py``.

    Args:
        paths: Directories to scan for PO CSV files.
        thresholds: Flow thresholds (m³/s) at which to compute exceedance durations.
            ``None`` uses a wide default set from 1 to 2090.
        data_type: The measurement column to analyze (default ``"Flow"``).
        locations: Optional location filter.
        log_level: Console log level.

    Returns:
        A tuple of ``(durations_df, summary_df)``.
    """
    from ryan_library.functions.tuflow.closure_durations_functions import (
        calculate_threshold_durations,
        collect_po_data,
        summarise_results,
    )

    if thresholds is None:
        values: set[int] = set(list(range(1, 10)) + list(range(10, 100, 2)) + list(range(100, 2100, 10)))
        thresholds = [float(v) for v in sorted(values)]

    # Load PO data — use load_tuflow_data to handle notebook parallel safety
    collection: ProcessorCollection = load_tuflow_data(
        paths=paths,
        data_types=["PO"],
        parallel=False,  # Closure durations is typically fast; serial is fine
        log_level=log_level,
        locations=locations,
    )

    if not collection.processors:
        print("No PO processors found.")
        return pd.DataFrame(), pd.DataFrame()

    po_df: pd.DataFrame = collect_po_data(collection=collection)

    if po_df.empty:
        print("No PO data found after collection.")
        return pd.DataFrame(), pd.DataFrame()

    print(f"Calculating closure durations for {len(thresholds)} threshold(s)...")
    durations_df: pd.DataFrame = calculate_threshold_durations(
        po_df=po_df,
        thresholds=thresholds,
        measurement_type=data_type,
    )
    if durations_df.empty:
        print(f"No exceedances found for measurement type '{data_type}'.")
        return pd.DataFrame(), pd.DataFrame()

    summary_df: pd.DataFrame = summarise_results(df=durations_df)
    return durations_df, summary_df


# ---------------------------------------------------------------------------
# Workflow: Timeseries Stability
# ---------------------------------------------------------------------------


def run_timeseries_stability(
    paths: Sequence[str | Path],
    *,
    result_types: Sequence[str] = ("PO",),
    datatype_include: Sequence[str] = ("Flow", "Q"),
    location_include: Sequence[str] = (),
    location_exclude: Sequence[str] = (),
    flat_tol: float = 1e-6,
    diff_rel_tol: float = 0.01,
    diff_abs_tol: float = 1e-6,
    max_sign_changes: int = 2,
    min_points: int = 5,
    log_level: str = DEFAULT_NOTEBOOK_LOG_LEVEL,
) -> pd.DataFrame:
    """Run stability checks on PO and/or 1D Q timeseries CSVs.

    Notebook equivalent of ``ryan-scripts/TUFLOW-python/TUFLOW_Timeseries_Stability.py``.

    Runs serially in the notebook process to avoid multiprocessing issues.

    Args:
        paths: Directories to scan.
        result_types: File families: ``"PO"``, ``"Q"``, or both.
        datatype_include: Measurement types to include.
        location_include: Optional location allow-list.
        location_exclude: Optional location block-list.
        flat_tol: Range threshold for treating a series as flat.
        diff_rel_tol: Relative tolerance for ignoring step noise.
        diff_abs_tol: Absolute tolerance for ignoring step noise.
        max_sign_changes: Maximum sign changes before flagging instability.
        min_points: Minimum points for stability evaluation.
        log_level: Console log level.

    Returns:
        DataFrame of stability check results.
    """
    from ryan_library.functions.tuflow.po_timeseries_checks import (
        StabilityCheckConfig,
        analyze_stability_csv,
        analyze_stability_q_csv,
        flatten_stability_results,
    )

    path_objects: list[Path] = [Path(p) for p in paths]

    config = StabilityCheckConfig(
        datatype_include=datatype_include,
        datatype_case_sensitive=False,
        location_include=location_include,
        location_exclude=location_exclude,
        location_case_sensitive=False,
        flat_tol=flat_tol,
        diff_rel_tol=diff_rel_tol,
        diff_abs_tol=diff_abs_tol,
        max_sign_changes=max_sign_changes,
        min_points=min_points,
    )

    # Map result types to glob patterns (inlined to avoid private imports)
    result_type_globs: dict[str, str] = {"PO": "**/*_PO.csv", "Q": "**/*_1d_Q.csv"}
    effective_result_types: list[str] = []
    for rt in result_types:
        canonical: str = rt.strip().upper()
        if canonical == "ALL":
            effective_result_types = list(result_type_globs.keys())
            break
        if canonical in result_type_globs:
            effective_result_types.append(canonical)
    if not effective_result_types:
        effective_result_types = ["PO"]

    # Collect files matching the glob patterns
    files: list[tuple[Path, str]] = []
    seen: set[tuple[Path, str]] = set()
    for root in path_objects:
        if not root.is_dir():
            logger.warning(f"Skipping non-directory path: {root}")
            continue
        for rt in effective_result_types:
            for match in root.rglob(result_type_globs[rt]):
                key: tuple[Path, str] = (match, rt)
                if key not in seen:
                    seen.add(key)
                    files.append(key)
    if not files:
        print("No timeseries CSV files found for stability checking.")
        return pd.DataFrame()

    print(f"Running stability checks on {len(files)} file(s) serially...")
    all_rows: list[dict[str, object]] = []
    for csv_path, rt in files:
        if rt == "Q":
            results: list[StabilityCheckResult] = analyze_stability_q_csv(path=csv_path, config=config)
        else:
            results = analyze_stability_csv(path=csv_path, config=config)
        all_rows.extend(flatten_stability_results(results=results))

    if not all_rows:
        print("No matching data columns after filters.")
        return pd.DataFrame()

    return pd.DataFrame(data=all_rows)


# ---------------------------------------------------------------------------
# Workflow: Timeseries Peak Checks
# ---------------------------------------------------------------------------


def run_timeseries_peaks_check(
    paths: Sequence[str | Path],
    *,
    csv_glob: str = "**/*_PO.csv",
    datatype_include: Sequence[str] = ("Flow",),
    location_include: Sequence[str] = (),
    location_exclude: Sequence[str] = (),
    warn_2hours: float = 2.0,
    warn_1hour: float = 1.0,
    flat_tol: float = 1e-6,
    log_level: str = DEFAULT_NOTEBOOK_LOG_LEVEL,
) -> pd.DataFrame:
    """Check peak timing relative to end of simulation in PO CSVs.

    Notebook equivalent of ``ryan-scripts/TUFLOW-python/TUFLOW_Timeseries_Peaks_Check.py``.

    Runs serially to avoid multiprocessing issues in notebooks.

    Args:
        paths: Directories to scan.
        csv_glob: Glob pattern for PO CSV files.
        datatype_include: Measurement types to include.
        location_include: Optional location allow-list.
        location_exclude: Optional location block-list.
        warn_2hours: Threshold (hours from end) for WARN_2H.
        warn_1hour: Threshold (hours from end) for WARN_1H.
        flat_tol: Tolerance for treating peak deviations as flat.
        log_level: Console log level.

    Returns:
        DataFrame of peak check results.
    """
    from ryan_library.functions.tuflow.po_timeseries_checks import (
        PeakCheckConfig,
        analyze_peak_csv,
        flatten_peak_results,
    )

    path_objects: list[Path] = [Path(p) for p in paths]

    config = PeakCheckConfig(
        datatype_include=datatype_include,
        datatype_case_sensitive=False,
        location_include=location_include,
        location_exclude=location_exclude,
        location_case_sensitive=False,
        warn_2hours=warn_2hours,
        warn_1hour=warn_1hour,
        flat_tol=flat_tol,
    )

    # Collect PO CSV files (inlined to avoid private imports)
    files: list[Path] = []
    seen: set[Path] = set()
    for root in path_objects:
        if not root.is_dir():
            logger.warning(f"Skipping non-directory path: {root}")
            continue
        for match in root.rglob(csv_glob):
            if match not in seen:
                seen.add(match)
                files.append(match)
    files.sort()
    if not files:
        print(f"No files matched '{csv_glob}' in the provided directories.")
        return pd.DataFrame()

    print(f"Running peak checks on {len(files)} PO CSV file(s) serially...")
    all_rows: list[dict[str, object]] = []
    for csv_path in files:
        results = analyze_peak_csv(path=csv_path, config=config)
        all_rows.extend(flatten_peak_results(results=results))

    if not all_rows:
        print("No matching data columns after filters.")
        return pd.DataFrame()

    out_df = pd.DataFrame(data=all_rows)
    # Apply standard column ordering
    first_cols: list[str] = [
        "run_code",
        "status",
        "datatype",
        "location",
        "peak_kind",
        "peak_value",
        "peak_time",
        "end_time",
        "hours_from_end",
    ]
    ordered: list[str] = [c for c in first_cols if c in out_df.columns] + [
        c for c in out_df.columns if c not in first_cols
    ]
    return out_df.reindex(columns=ordered)


# ---------------------------------------------------------------------------
# Plotting helpers
# ---------------------------------------------------------------------------


def plot_hydrographs(
    df: pd.DataFrame,
    *,
    time_col: str = "Time",
    value_col: str = "Q",
    group_col: str = "Chan ID",
    title: str = "Hydrographs",
    xlabel: str = "Time (h)",
    ylabel: str = "Flow (m³/s)",
) -> None:
    """Plot timeseries data grouped by a column.

    A simple convenience wrapper around matplotlib for quick visual inspection.

    Args:
        df: DataFrame containing timeseries data (e.g. from :func:`run_culvert_timeseries`).
        time_col: Column name for the x-axis (time).
        value_col: Column name for the y-axis (value to plot).
        group_col: Column to group/colour by.
        title: Plot title.
        xlabel: X-axis label.
        ylabel: Y-axis label.
    """
    import matplotlib.pyplot as plt

    if df.empty:
        print("No data to plot.")
        return

    if time_col not in df.columns or value_col not in df.columns:
        print(f"Required columns '{time_col}' and/or '{value_col}' not found in DataFrame.")
        return

    _fig, ax = plt.subplots(figsize=(12, 6))  # pyright: ignore[reportUnknownMemberType]

    if group_col in df.columns:
        for label, group in df.groupby(group_col, observed=True):
            group_sorted = group.sort_values(time_col)
            ax.plot(  # pyright: ignore[reportUnknownMemberType]
                group_sorted[time_col], group_sorted[value_col], label=str(label)
            )
        ax.legend(loc="best", fontsize=8)  # pyright: ignore[reportUnknownMemberType]
    else:
        sorted_df = df.sort_values(time_col)
        ax.plot(sorted_df[time_col], sorted_df[value_col])  # pyright: ignore[reportUnknownMemberType]

    ax.set_xlabel(xlabel)  # pyright: ignore[reportUnknownMemberType]
    ax.set_ylabel(ylabel)  # pyright: ignore[reportUnknownMemberType]
    ax.set_title(title)  # pyright: ignore[reportUnknownMemberType]
    ax.grid(True, alpha=0.3)  # pyright: ignore[reportUnknownMemberType]
    plt.tight_layout()  # pyright: ignore[reportUnknownMemberType]
    plt.show()  # pyright: ignore[reportUnknownMemberType]
