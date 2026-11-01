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

from openpyxl import load_workbook

import ryan_library
from ryan_library.mcp.installed_scripts import copy_wrapper

WRAPPER = "TUFLOW-python/log_processing/create_log_summary_report.py"
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


def _check_one_mode(work: Path, root: Path, *, dashboard_enabled: bool) -> tuple[str, tuple[str, ...]]:
    """Exercise a whole copied-wrapper cycle in a disposable working directory."""
    work.mkdir()
    complete = work / "Complete_Run.tlf"
    incomplete = work / "Incomplete_Run.tlf"
    complete.write_text(COMPLETE_LOG, encoding="utf-8")
    incomplete.write_text(INCOMPLETE_LOG, encoding="utf-8")
    original = (complete.read_bytes(), incomplete.read_bytes())
    script = copy_wrapper(WRAPPER, work)
    if not script.is_file() or not script.absolute().is_relative_to(work):
        msg = f"Wrapper was not copied outside the checkout: {script}"
        raise RuntimeError(msg)
    report = _run(
        script,
        work,
        root,
        "--live-dashboard" if dashboard_enabled else "--no-live-dashboard",
        "--console-log-level",
        "INFO",
        "--log-file",
        "debug.log",
        "--file-log-level",
        "DEBUG",
    )
    if report.returncode != 0:
        msg = f"Copied wrapper failed (dashboard={dashboard_enabled}): {report.stdout[-1800:]} {report.stderr[-1800:]}"
        raise RuntimeError(msg)
    if "did not complete" not in report.stdout:
        msg = f"Missing incomplete-run warning (dashboard={dashboard_enabled}): {report.stdout[-1400:]}"
        raise RuntimeError(msg)
    if dashboard_enabled and ("TUFLOW Log Summary: SUCCESS" not in report.stdout or "\x1b[" in report.stdout):
        msg = f"Redirected dashboard summary is absent or contains cursor escape sequences: {report.stdout[-1400:]}"
        raise RuntimeError(msg)
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

    print(
        f"Copied log-summary installed-wheel smoke passed at {actual_library}: on/off, workbook, DEBUG, warning and failure."
    )


if __name__ == "__main__":
    main()
