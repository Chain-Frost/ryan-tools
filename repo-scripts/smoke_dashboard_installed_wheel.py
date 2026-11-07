"""Check the copied TUFLOW log-summary wrapper against an isolated installed wheel.

Run after the package CI job has installed the freshly built wheel under
--expected-root; the inputs and reports are disposable and no TUFLOW solver is used.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

from openpyxl import Workbook, load_workbook

import ryan_library
from ryan_library.mcp.installed_scripts import copy_wrapper

WRAPPER = "TUFLOW-python/log_processing/create_log_summary_report.py"
MASTER_WRAPPER = "TUFLOW-python/log_processing/append_log_summary_to_master_workbook.py"
COMPLETE_LOG = """Build: 2023-03-AF
Computer Name: SyntheticHost
Simulation Started: 2026-Nov-01 14:00
No Specified Events.
No Specified Scenarios.
Initialisation Times
Clock Time: [0.01 h]
Final Times
Clock Time: [0.03 h]
Simulation FINISHED
Final Cumulative ME: 0.0%
"""
INCOMPLETE_LOG = """Build: 2023-03-AF
Computer Name: SyntheticHost
Simulation Started: 2026-Nov-01 15:00
No Specified Events.
No Specified Scenarios.
"""


def _run(script: Path, work: Path, root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(root)
    environment["PYTHONNOUSERSITE"] = "1"
    environment.pop("RYAN_TOOLS_REPOSITORY_ROOT", None)
    return subprocess.run(
        [sys.executable, str(script), "--working-directory", str(work), "--no-pause", *args],
        cwd=work,
        env=environment,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=90,
        stdin=subprocess.DEVNULL,
        check=False,
    )


def _workbook_record(work: Path) -> tuple[str, tuple[str, ...]]:
    files = list(work.glob("*_ModellingLog.xlsx"))
    if len(files) != 1:
        msg = f"Expected exactly one log summary workbook in {work}, found {files!r}"
        raise RuntimeError(msg)
    workbook = load_workbook(files[0], read_only=True, data_only=True)
    try:
        sheet = workbook["Log Summary"]
        rows = list(sheet.values)
        if len(rows) != 2 or "Runcode" not in rows[0]:
            msg = f"Unexpected log summary rows/columns: {rows!r}"
            raise RuntimeError(msg)
        record = dict(zip(rows[0], rows[1], strict=True))
        if record["Runcode"] != "Complete_Run":
            msg = f"Unexpected completed run: {record['Runcode']!r}"
            raise RuntimeError(msg)
        return sheet.title, tuple(str(key) for key in rows[0])
    finally:
        workbook.close()


def _check_copied_wrapper_help(script: Path, work: Path, root: Path) -> None:
    result = _run(script, work, root, "--help")
    if result.returncode != 0 or "--live-dashboard" not in result.stdout:
        msg = f"Copied wrapper help/options failed: {result.stdout[-900:]} {result.stderr[-900:]}"
        raise RuntimeError(msg)


def _check_console_diagnostics(report: subprocess.CompletedProcess[str], *, dashboard_enabled: bool) -> None:
    """Keep ordinary progress visible only with the dashboard disabled."""
    if report.returncode != 0:
        msg = f"Copied wrapper failed (dashboard={dashboard_enabled}): {report.stdout[-1800:]} {report.stderr[-1800:]}"
        raise RuntimeError(msg)
    if "did not complete" not in report.stdout:
        msg = f"Missing incomplete-run warning (dashboard={dashboard_enabled}): {report.stdout[-1400:]}"
        raise RuntimeError(msg)
    routine_info_visible = "Completed Complete_Run.tlf" in report.stdout
    if routine_info_visible == dashboard_enabled:
        msg = "Console did not honour concise dashboard / useful ordinary logging defaults."
        raise RuntimeError(msg)
    if "\x1b[" in report.stdout:
        msg = "Redirected console contained ANSI escape sequences."
        raise RuntimeError(msg)
    if dashboard_enabled and "TUFLOW Log Summary: SUCCESS" not in report.stdout:
        msg = f"Redirected dashboard success summary is absent: {report.stdout[-1400:]}"
        raise RuntimeError(msg)


def _check_one_mode(work: Path, root: Path, *, dashboard_enabled: bool) -> tuple[str, tuple[str, ...]]:
    """Exercise a whole copied-wrapper cycle in a disposable working directory."""
    work.mkdir()
    complete = work / "Complete_Run.tlf"
    incomplete = work / "Incomplete_Run.tlf"
    complete.write_text(COMPLETE_LOG, encoding="utf-8")
    incomplete.write_text(INCOMPLETE_LOG, encoding="utf-8")
    original = (complete.read_bytes(), incomplete.read_bytes())
    script = copy_wrapper(WRAPPER, work)
    _check_copied_wrapper_help(script, work, root)
    if not script.is_file() or not script.absolute().is_relative_to(work):
        msg = f"Wrapper was not copied outside the checkout: {script}"
        raise RuntimeError(msg)
    report = _run(
        script,
        work,
        root,
        "--live-dashboard" if dashboard_enabled else "--no-live-dashboard",
        "--log-file",
        "debug.log",
        "--file-log-level",
        "DEBUG",
    )
    _check_console_diagnostics(report, dashboard_enabled=dashboard_enabled)
    if (complete.read_bytes(), incomplete.read_bytes()) != original:
        msg = "Copied wrapper modified an input TLF."
        raise RuntimeError(msg)
    file_text = (work / "debug.log").read_text(encoding="utf-8")
    if "search_for_completion" not in file_text or "Processing Complete_Run" not in file_text:
        msg = f"Worker DEBUG/INFO records not retained in file: {file_text[-1700:]}"
        raise RuntimeError(msg)
    try:
        result = _workbook_record(work)
    except RuntimeError as exc:
        msg = (
            f"{exc}; copied-wrapper stdout={report.stdout[-2800:]!r}, "
            f"stderr={report.stderr[-1500:]!r}, "
            f"debug tail={file_text[-3200:]!r}"
        )
        raise RuntimeError(msg) from exc
    failed = _run(script, work, root, "--live-max-rows", "0")
    if failed.returncode != 1 or "live_max_rows" not in (failed.stdout + failed.stderr):
        msg = f"Invalid setting did not return actionable failure: {failed.returncode} {failed.stderr[-1000:]}"
        raise RuntimeError(msg)
    if len(list(work.glob("*_ModellingLog.xlsx"))) != 1:
        msg = "Invalid display settings unexpectedly generated another workbook."
        raise RuntimeError(msg)
    return result


def _master_workbook_rows(path: Path) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Return headers and run codes, preserving the existing master rows."""
    workbook = load_workbook(path, read_only=True, data_only=True)
    try:
        rows = list(workbook["Log Summary"].values)
        if len(rows) != 3 or "Runcode" not in rows[0]:
            msg = f"Unexpected master log append rows: {rows}"
            raise RuntimeError(msg)
        headers = tuple(str(value) for value in rows[0])
        column = headers.index("Runcode")
        run_codes = tuple(str(row[column]) for row in rows[1:])
        if set(run_codes) != {"Existing_Run", "Complete_Run"}:
            msg = f"Master workbook did not retain old and new runs: {run_codes}"
            raise RuntimeError(msg)
        return headers, run_codes
    finally:
        workbook.close()


def _check_master_append_one_mode(work: Path, root: Path, *, dashboard_enabled: bool) -> tuple[str, ...]:
    """Exercise copied master append with real disposable workbook and failure paths."""
    work.mkdir(parents=True)
    master = work / "master.xlsx"
    workbook = Workbook()
    sheet = workbook.active
    assert sheet is not None
    sheet.title = "Log Summary"
    sheet.append(["Runcode", "orig_log_path"])
    sheet.append(["Existing_Run", "Existing_Run.tlf"])
    workbook.save(master)
    workbook.close()
    (work / "Complete_Run.tlf").write_text(COMPLETE_LOG, encoding="utf-8")
    script = copy_wrapper(MASTER_WRAPPER, work)
    if not script.is_file() or not script.absolute().is_relative_to(work):
        msg = f"Master wrapper not copied outside repository: {script}"
        raise RuntimeError(msg)

    help_result = _run(script, work, root, "--help")
    if help_result.returncode != 0 or "--master-workbook" not in help_result.stdout:
        msg = f"Missing copied master wrapper CLI: {help_result.stdout[-1000:]} {help_result.stderr[-900:]}"
        raise RuntimeError(msg)
    mode = "--live-dashboard" if dashboard_enabled else "--no-live-dashboard"
    report = _run(script, work, root, "--master-workbook", str(master), mode)
    if report.returncode != 0:
        msg = f"Copied master append failed: {report.stdout[-2000:]} {report.stderr[-1200:]}"
        raise RuntimeError(msg)
    if dashboard_enabled and "TUFLOW Master Log Append: SUCCESS" not in report.stdout:
        msg = f"Full-workflow SUCCESS missing from master append: {report.stdout[-1200:]}"
        raise RuntimeError(msg)
    if "\x1b[" in report.stdout:
        msg = "Copied master append emitted ANSI cursor codes to redirected stdout"
        raise RuntimeError(msg)

    headers, _ = _master_workbook_rows(master)
    saved = master.read_bytes()
    duplicate = _run(script, work, root, "--master-workbook", str(master), mode)
    if duplicate.returncode != 0 or master.read_bytes() != saved:
        msg = f"Duplicate log append modified workbook or failed: {duplicate.stdout[-1000:]}"
        raise RuntimeError(msg)

    (work / "Incomplete_Run.tlf").write_text(INCOMPLETE_LOG, encoding="utf-8")
    incomplete = _run(script, work, root, "--master-workbook", str(master), mode)
    if incomplete.returncode != 0 or master.read_bytes() != saved:
        msg = f"Incomplete-only skip changed master: {incomplete.stdout[-1100:]}"
        raise RuntimeError(msg)

    invalid = _run(script, work, root, "--master-workbook", str(master), "--live-max-rows", "0")
    if invalid.returncode != 1 or "live_max_rows" not in invalid.stdout + invalid.stderr:
        msg = f"Invalid master dashboard setting not rejected: {invalid.stdout[-1000:]}"
        raise RuntimeError(msg)
    if master.read_bytes() != saved:
        msg = "Invalid display settings modified the master workbook"
        raise RuntimeError(msg)

    (work / "New_Run.tlf").write_text(COMPLETE_LOG, encoding="utf-8")
    launcher = work / "master_write_failure.py"
    launcher.write_text(
        "from openpyxl.workbook.workbook import Workbook\n"
        "import runpy\n"
        "def fail(*args, **kwargs):\n    raise PermissionError('synthetic append save denial')\n"
        "Workbook.save = fail\n"
        f"runpy.run_path({str(script)!r}, run_name='__main__')\n",
        encoding="utf-8",
    )
    failure = _run(launcher, work, root, "--master-workbook", str(master), mode)
    if failure.returncode != 1 or (dashboard_enabled and "TUFLOW Master Log Append: FAILED" not in failure.stdout):
        msg = f"Copied master append write failure returned false success: {failure.stdout[-1600:]}"
        raise RuntimeError(msg)
    if master.read_bytes() != saved or list(work.glob(".master-*.xlsx")):
        msg = "Failed copied master append damaged workbook or left owned staging"
        raise RuntimeError(msg)
    return headers


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-root", type=Path, required=True)
    args = parser.parse_args()
    # Deliberately resolve for installed-module identity verification, not user path normalization.
    root = args.expected_root.resolve()
    actual_library = Path(ryan_library.__file__).resolve()
    if not actual_library.is_relative_to(root):
        msg = f"Dashboard smoke imported source instead of wheel: {actual_library}, expected root {root}"
        raise RuntimeError(msg)
    expected: tuple[str, tuple[str, ...]] | None = None
    with TemporaryDirectory(prefix="ryan-tools-logsummary-wheel-") as temp:
        base = Path(temp)
        for dashboard_enabled in (False, True):
            work = base / ("dashboard" if dashboard_enabled else "ordinary")
            result = _check_one_mode(work, root, dashboard_enabled=dashboard_enabled)
            if expected is not None and expected != result:
                msg = f"Dashboard enabled/disabled workbook mismatch: {expected} != {result}"
                raise RuntimeError(msg)
            expected = result

        master_headers: tuple[str, ...] | None = None
        for dashboard_enabled in (False, True):
            work = base / ("master_dashboard" if dashboard_enabled else "master_ordinary")
            headers = _check_master_append_one_mode(work, root, dashboard_enabled=dashboard_enabled)
            if master_headers is not None and master_headers != headers:
                msg = "Master append dashboard changed workbook column schema"
                raise RuntimeError(msg)
            master_headers = headers

    print(
        f"Copied log-summary and master-append installed-wheel smoke passed at {actual_library}: "
        "dashboard on/off, workbook parity, duplicate/empty, DEBUG, invalid settings and writer failure."
    )


if __name__ == "__main__":
    main()
