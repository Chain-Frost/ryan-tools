# ryan_library/orchestrators/tuflow/tlf_missing_runs.py
"""Orchestrator for checking missing TUFLOW runs."""

__lazy_modules__: list[str] = ["pandas"]
from pathlib import Path

import pandas as pd
from loguru import logger

from ...functions.excel_export import ExcelExporter
from ...functions.live_dashboard import LiveWorkflowDashboard
from ...functions.tlf_ensemble_inventory import EnsembleInventory, VerificationStatus, scan_ensemble_inventory
from ...functions.tlf_missing_runs import summarize_for_cli
from .tuflow_logsummary import (
    LOG_SUMMARY_DASHBOARD_COLUMNS,
    LogFileProcessingResult,
    build_log_summary_dataframe,
    process_log_file_for_dashboard,
)


def orchestrate_ensemble_scan(
    root: Path,
    *,
    recursive: bool = True,
    max_combinations: int = 1_000_000,
    verify_success: bool = False,
    use_live_dashboard: bool = True,
    live_refresh_per_second: float = 2.0,
    live_max_rows: int = 10,
    export_log_summary: bool = False,
    log_summary_directory: Path | None = None,
) -> EnsembleInventory:
    """Verify selected logs with shared progress and optionally export the canonical log summary."""
    if export_log_summary and not verify_success:
        message = "Log summary export requires verify_success=True"
        raise ValueError(message)
    dashboard = LiveWorkflowDashboard(
        title="TUFLOW Success Scan",
        subtitle=str(root),
        enabled=verify_success and use_live_dashboard,
        refresh_per_second=live_refresh_per_second,
        max_rows=live_max_rows,
        columns=LOG_SUMMARY_DASHBOARD_COLUMNS,
    )
    selected: list[Path] = []
    summaries: list[LogFileProcessingResult] = []

    def start(paths: list[Path]) -> None:
        selected.extend(paths)
        dashboard.set_tasks(labels=[path.name for path in paths], files=paths)
        dashboard.set_phase("Verifying log status")
        if not use_live_dashboard:
            logger.success("Checking {} selected TLF logs", len(paths))

    def progress(index: int, path: Path, status: VerificationStatus) -> None:
        if status == "RUNNING":
            dashboard.mark_running(index=index, detail="Reading completion status")
            return
        detail = {"OK": "Simulation FINISHED", "SKIP": "No stable success marker", "FAIL": "Unable to read log"}[status]
        if status == "OK" and export_log_summary:
            dashboard.update_task(index=index, detail="Parsing log summary metadata")
            parsed = _parse_summary(path, dashboard, index)
            summaries.append(parsed)
            detail = "Success confirmed; summary " + parsed.status
        dashboard.mark_finished(index=index, status=status, detail=detail)
        _log_scan_progress(index, len(selected), use_live_dashboard)

    with dashboard:
        dashboard.set_phase("Discovering and selecting latest logs")
        try:
            inventory = scan_ensemble_inventory(
                root,
                recursive=recursive,
                max_combinations=max_combinations,
                verify_success=verify_success,
                on_verification_start=start,
                on_verification_progress=progress,
            )
            if export_log_summary:
                _export_summary(
                    selected, summaries, dashboard, log_summary_directory if log_summary_directory is not None else root
                )
            dashboard.set_phase("Complete")
            dashboard.finish(outcome="SUCCESS", detail="Log scan and requested summary export complete")
            return inventory
        except KeyboardInterrupt:
            dashboard.finish(outcome="INTERRUPTED", detail="Log scan interrupted")
            raise
        except Exception:
            dashboard.finish(outcome="FAILED", detail="Log scan or summary export failed")
            raise


def _log_scan_progress(index: int, total: int, use_live_dashboard: bool) -> None:
    if not use_live_dashboard and (index % 25 == 0 or index == total):
        logger.success("Checked {} / {} logs", index, total)


def _export_summary(
    selected: list[Path],
    summaries: list[LogFileProcessingResult],
    dashboard: LiveWorkflowDashboard,
    directory: Path,
) -> None:
    dashboard.set_phase("Aggregating log summary")
    frame = build_log_summary_dataframe(files=selected, processing_results=summaries)
    if frame.empty:
        logger.warning("No completed logs with summary metadata; no log-summary workbook exported")
    else:
        dashboard.set_phase("Exporting log summary")
        ExcelExporter().save_to_excel(
            data_frame=frame,
            file_name_prefix="ModellingLog",
            sheet_name="Log Summary",
            output_directory=directory,
            include_data_dictionary=True,
            data_dictionary_metadata={"Workflow": "TUFLOW log summary"},
        )


def _parse_summary(path: Path, dashboard: LiveWorkflowDashboard, index: int) -> LogFileProcessingResult:
    parsed = process_log_file_for_dashboard(path)
    if parsed.status == "FAIL":
        dashboard.mark_finished(index=index, status="FAIL", detail=parsed.detail)
        message = f"Log summary parsing failed for {path}: {parsed.detail}"
        raise RuntimeError(message)
    if parsed.status == "SKIP":
        logger.warning("Completed log omitted from metadata summary: {}", path)
    return parsed


def orchestrate_missing_runs_check(input_path: Path, sheet_name: str | int = 0) -> Path:
    """Read run-tracking data, log missing sets, and exclusively create a summary CSV.

    Existing summaries are never overwritten; FileExistsError propagates to callers.

    Args:
        input_path: Path to the tracking table (CSV or Excel).
        sheet_name: If Excel, which sheet to read from.

    Returns:
        The Path to the generated CSV summary.
    """
    input_str = str(input_path)
    if input_str.lower().endswith((".xlsx", ".xls")):
        df: pd.DataFrame = pd.read_excel(io=input_path, sheet_name=sheet_name)  # pyright: ignore[reportUnknownMemberType]
    else:
        df = pd.read_csv(filepath_or_buffer=input_path)

    text, table = summarize_for_cli(df)

    # Log the summary text
    for line in text.splitlines():
        if line.strip():
            logger.success("{}", line)
        else:
            logger.info("")

    base: str = input_str.rsplit(sep=".", maxsplit=1)[0]
    out_csv = Path(f"{base}__missing_runs_summary.csv")
    table.to_csv(path_or_buf=out_csv, index=False, mode="x")

    logger.success("Wrote missing runs summary to: {}", out_csv)
    return out_csv
