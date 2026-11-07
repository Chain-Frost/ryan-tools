"""Smoke-test six copied TUFLOW result wrappers against an isolated installed wheel.

Run from package CI after the wheel has been installed under --expected-root.
All data and outputs are synthetic and disposable; no solver or submodule is used.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from openpyxl import load_workbook

import ryan_library
from ryan_library.mcp.installed_scripts import copy_wrapper


@dataclass(frozen=True, slots=True)
class Case:
    wrapper: str
    data_type: str
    sheet: str
    additional_sheets: tuple[str, ...] = ()
    flags: tuple[str, ...] = ()


CASES = (
    Case("TUFLOW-python/culvert_results/combine_culvert_maximums.py", "Cmx", "Maximums", ("raw_data",)),
    Case("TUFLOW-python/culvert_results/combine_culvert_timeseries.py", "Q", "1d_timeseries_data"),
    Case(
        "TUFLOW-python/culvert_results/create_mean_culvert_peak_report.py",
        "Cmx",
        "aep-dur-mean",
        ("aep-mean-max", "culvert-maximums"),
        ("--export-raw",),
    ),
    Case(
        "TUFLOW-python/culvert_results/create_median_culvert_peak_report.py",
        "Cmx",
        "aep-dur-med",
        ("aep-med-max", "culvert-maximums"),
        ("--export-raw",),
    ),
    Case("TUFLOW-python/po_and_timeseries/combine_po_results.py", "PO", "combined_PO"),
    Case("TUFLOW-python/po_and_timeseries/combine_pomm_results.py", "POMM", "combined_POMM"),
)

type WorkbookSnapshot = dict[str, tuple[tuple[str, ...], tuple[tuple[float, ...], ...]]]


def _write_inputs(folder: Path) -> None:
    folder.mkdir(parents=True)
    for tp, multiplier in ((1, 1.0), (2, 2.0)):
        run = f"Synthetic_01.00p_00120m_TP{tp:02d}"
        data = {
            "_1d_Cmx.csv": (
                "Chan ID,Time Qmax,Qmax,Vmax,Time Vmax\n"
                f"C01,0.5,{5 * multiplier},1.5,0.5\n"
                f"C02,0.5,{50 * multiplier},3.0,0.5\n"
            ),
            "_1d_Q.csv": (
                "Descriptor,Time,Q C01,Q C02\n"
                f"row,0,{2 * multiplier},{20 * multiplier}\n"
                f"row,1,{4 * multiplier},{40 * multiplier}\n"
            ),
            "_PO.csv": (
                "Descriptor,Time,Flow,Flow\nLocation,Time,C01,C02\n"
                f"row,0,{2 * multiplier},{20 * multiplier}\n"
                f"row,1,{4 * multiplier},{40 * multiplier}\n"
            ),
            "_POMM.csv": (
                f"{run},Location,Flow,Flow\n"
                "value,Time,C01,C02\n"
                f"value,Maximum (Extracted from Time Series),{5 * multiplier},{50 * multiplier}\n"
                "value,Time of Maximum,0.5,0.5\n"
                "value,Minimum (Extracted From Time Series),-1,-2\n"
                "value,Time of Minimum,0.0,0.0\n"
            ),
        }
        for suffix, content in data.items():
            (folder / f"{run}{suffix}").write_text(content, encoding="utf-8")


def _run(script: Path, work: Path, root: Path, *flags: str) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(root)
    environment["PYTHONNOUSERSITE"] = "1"
    environment.pop("RYAN_TOOLS_REPOSITORY_ROOT", None)
    return subprocess.run(
        [
            sys.executable,
            str(script),
            "--working-directory",
            str(work),
            "--no-pause",
            "--paths-to-process",
            "inputs",
            "--locations",
            "C01",
            *flags,
        ],
        cwd=work.parent,
        env=environment,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=120,
        stdin=subprocess.DEVNULL,
        check=False,
    )


def _snapshot(work: Path, case: Case) -> WorkbookSnapshot:
    books = list(work.glob("*.xlsx"))
    if len(books) != 1:
        msg = f"Expected one workbook for {case.sheet}, found {books}"
        raise RuntimeError(msg)
    workbook = load_workbook(books[0], read_only=True, data_only=True)
    try:
        snapshot: WorkbookSnapshot = {}
        for sheet in (case.sheet, *case.additional_sheets):
            if sheet not in workbook.sheetnames:
                msg = f"Missing output sheet {sheet}: {workbook.sheetnames}"
                raise RuntimeError(msg)
            rows = list(workbook[sheet].values)
            if len(rows) < 2:
                msg = f"No data rows in {sheet}: {rows}"
                raise RuntimeError(msg)
            headers = tuple(str(item) for item in rows[0])
            identifier = next((name for name in ("Chan ID", "Location") if name in headers), None)
            if identifier is None:
                msg = f"Missing channel/location identifier in {sheet}: {headers}"
                raise RuntimeError(msg)
            channel_index = headers.index(identifier)
            channels = {str(row[channel_index]) for row in rows[1:]}
            if channels != {"C01"}:
                msg = f"Channel selection was not retained in {sheet}: {channels}"
                raise RuntimeError(msg)
            # Working-folder and path columns differ between the two disposable
            # copies. Compare every formatted/raw sheet's numeric values in row order.
            values = tuple(
                tuple(float(value) for value in row if isinstance(value, (int, float)) and not isinstance(value, bool))
                for row in rows[1:]
            )
            snapshot[sheet] = headers, values
        return snapshot
    finally:
        workbook.close()


def _one_case(case: Case, root: Path, work: Path, *, dashboard: bool) -> WorkbookSnapshot:
    work.mkdir(parents=True)
    _write_inputs(work / "inputs")
    script = copy_wrapper(case.wrapper, work)
    if not script.is_file() or not script.absolute().is_relative_to(work):
        msg = f"Wrapper was not copied into disposable work folder: {script}"
        raise RuntimeError(msg)
    help_result = _run(script, work, root, "--help")
    if help_result.returncode != 0 or "--live-dashboard" not in help_result.stdout:
        msg = f"Missing copied wrapper dashboard help for {case.wrapper}: {help_result.stderr[-900:]}"
        raise RuntimeError(msg)
    result = _run(
        script,
        work,
        root,
        "--data-types",
        case.data_type,
        *case.flags,
        "--live-dashboard" if dashboard else "--no-live-dashboard",
        "--log-file",
        "detail.log",
        "--file-log-level",
        "DEBUG",
    )
    if result.returncode != 0:
        msg = (
            f"Copied {case.wrapper} (dashboard={dashboard}) exited {result.returncode}: "
            f"stdout={result.stdout[-2600:]!r}, stderr={result.stderr[-1800:]!r}"
        )
        raise RuntimeError(msg)
    if "\x1b[" in result.stdout:
        msg = f"Redirected dashboard contained ANSI updates in {case.wrapper}"
        raise RuntimeError(msg)
    if dashboard and ": SUCCESS" not in result.stdout:
        msg = f"Missing full-workflow success in {case.wrapper}: {result.stdout[-1500:]}"
        raise RuntimeError(msg)
    if not (work / "detail.log").is_file():
        msg = f"Detailed file sink missing in {case.wrapper}"
        raise RuntimeError(msg)
    output = _snapshot(work, case)
    if dashboard:
        _export_failure_cases(case, root, work, script)
    invalid = _run(script, work, root, "--data-types", case.data_type, "--live-max-rows", "0")
    if invalid.returncode == 0 or "live_max_rows" not in (invalid.stdout + invalid.stderr):
        msg = f"Invalid display settings were silently accepted by {case.wrapper}"
        raise RuntimeError(msg)
    return output


def _export_failure_cases(case: Case, root: Path, work: Path, script: Path) -> None:
    """Exercise installed writer errors and collisions through copied process boundaries."""
    before = {path: path.read_bytes() for path in work.glob("*.xlsx")}
    first = next(iter(before))
    timestamp = datetime.strptime(first.name.split("_", 1)[0], "%Y%m%d-%H%M")
    launcher = work / "collision_probe.py"
    launcher.write_text(
        "from datetime import datetime\nfrom unittest.mock import Mock\nimport runpy\n"
        "from ryan_library.functions import excel_export\n"
        "excel_export.datetime = Mock(wraps=datetime)\n"
        f"excel_export.datetime.now.return_value = datetime.fromisoformat({timestamp.isoformat()!r})\n"
        f"runpy.run_path({str(script)!r}, run_name='__main__')\n",
        encoding="utf-8",
    )
    collision = _run(launcher, work, root, "--data-types", case.data_type, *case.flags, "--live-dashboard")
    if collision.returncode != 1 or ": FAILED" not in collision.stdout:
        msg = f"Existing-report collision did not fail {case.wrapper}: {collision.stdout[-1800:]}"
        raise RuntimeError(msg)
    if {path: path.read_bytes() for path in work.glob("*.xlsx")} != before:
        msg = f"Collision changed an existing workbook in {case.wrapper}"
        raise RuntimeError(msg)

    failure_work = work / "writer_failure"
    _write_inputs(failure_work / "inputs")
    launcher = work / "write_failure_probe.py"
    launcher.write_text(
        "import runpy\nimport pandas as pd\n"
        "def fail(*args, **kwargs):\n    raise PermissionError('synthetic installed export denial')\n"
        "pd.DataFrame.to_excel = fail\n"
        f"runpy.run_path({str(script)!r}, run_name='__main__')\n",
        encoding="utf-8",
    )
    failure = _run(launcher, failure_work, root, "--data-types", case.data_type, *case.flags, "--live-dashboard")
    if failure.returncode != 1 or ": FAILED" not in failure.stdout:
        msg = f"Writer failure did not fail {case.wrapper}: {failure.stdout[-1800:]}"
        raise RuntimeError(msg)
    if list(failure_work.rglob("*.xlsx")) or list(failure_work.rglob(".ryan-export-*")):
        msg = f"Writer failure left partial reports in {case.wrapper}"
        raise RuntimeError(msg)


QA_WRAPPERS: tuple[str, ...] = (
    "TUFLOW-python/po_and_timeseries/check_po_timeseries_peaks.py",
    "TUFLOW-python/po_and_timeseries/check_timeseries_stability.py",
)


def _qa_snapshot(work: Path) -> WorkbookSnapshot:
    """Compare copied QA exports by workbook schema and numerical content."""
    books = list(work.glob("*.xlsx"))
    if len(books) != 1:
        msg = f"Expected exactly one PO QA report in {work}, received {books}"
        raise RuntimeError(msg)
    workbook = load_workbook(books[0], read_only=True, data_only=True)
    try:
        if "Summary" not in workbook.sheetnames:
            msg = f"Missing QA Summary sheet: {workbook.sheetnames}"
            raise RuntimeError(msg)
        rows = list(workbook["Summary"].values)
        if len(rows) < 2:
            msg = f"QA report had no result rows: {rows}"
            raise RuntimeError(msg)
        headers = tuple(str(value) for value in rows[0])
        if "location" not in headers:
            msg = f"QA report missing location column: {headers}"
            raise RuntimeError(msg)
        location_column = headers.index("location")
        if {str(row[location_column]) for row in rows[1:]} != {"C01"}:
            msg = f"PO QA copied wrapper did not preserve location selection: {rows}"
            raise RuntimeError(msg)
        numbers = tuple(
            tuple(float(value) for value in row if isinstance(value, (int, float)) and not isinstance(value, bool))
            for row in rows[1:]
        )
        return {"Summary": (headers, numbers)}
    finally:
        workbook.close()


def _one_qa_case(wrapper: str, root: Path, work: Path, *, dashboard: bool) -> WorkbookSnapshot:
    """Run a copied QA wrapper from an unrelated working directory against wheel imports."""
    work.mkdir(parents=True)
    _write_inputs(work / "inputs")
    script = copy_wrapper(wrapper, work)
    if not script.is_file() or not script.absolute().is_relative_to(work):
        msg = f"PO QA wrapper was not copied into disposable work folder: {script}"
        raise RuntimeError(msg)
    help_result = _run(script, work, root, "--help")
    if help_result.returncode != 0 or "--no-live-dashboard" not in help_result.stdout:
        msg = f"Copied QA help failed for {wrapper}: {help_result.stdout[-600:]} {help_result.stderr[-600:]}"
        raise RuntimeError(msg)

    result = _run(
        script,
        work,
        root,
        "--data-types",
        "Flow",
        "--live-dashboard" if dashboard else "--no-live-dashboard",
    )
    if result.returncode != 0:
        msg = (
            f"Copied QA wrapper {wrapper} (dashboard={dashboard}) failed with {result.returncode}: "
            f"{result.stdout[-1500:]} {result.stderr[-1500:]}"
        )
        raise RuntimeError(msg)
    if dashboard and ": SUCCESS" not in result.stdout:
        msg = f"QA workflow was not marked successful after export: {wrapper}: {result.stdout[-1500:]}"
        raise RuntimeError(msg)
    if "\\x1b[" in result.stdout:
        msg = f"Unexpected ANSI updates in redirected QA workflow: {wrapper}"
        raise RuntimeError(msg)

    snapshot = _qa_snapshot(work)
    invalid = _run(script, work, root, "--live-max-rows", "0")
    if invalid.returncode == 0 or "live_max_rows" not in invalid.stdout + invalid.stderr:
        msg = f"Copied QA wrapper accepted invalid dashboard settings: {wrapper}"
        raise RuntimeError(msg)
    return snapshot


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-root", type=Path, required=True)
    args = parser.parse_args()
    # Physical identity check is intentional; user-configured input paths
    # elsewhere retain their original mapped-drive/UNC spelling.
    root = args.expected_root.resolve()
    actual = Path(ryan_library.__file__).resolve()
    if not actual.is_relative_to(root):
        msg = f"Result smoke imported checkout code, not installed wheel: {actual}"
        raise RuntimeError(msg)
    with TemporaryDirectory(prefix="ryan-tools-result-dashboards-") as location:
        base = Path(location)
        for index, case in enumerate(CASES):
            without = _one_case(case, root, base / f"{index}_ordinary", dashboard=False)
            with_dashboard = _one_case(case, root, base / f"{index}_dashboard", dashboard=True)
            if without != with_dashboard:
                msg = f"Dashboard changed workbook values/headers for {case.wrapper}: {without!r} vs {with_dashboard!r}"
                raise RuntimeError(msg)

    for index, wrapper in enumerate(QA_WRAPPERS):
        without = _one_qa_case(wrapper, root, base / f"qa_{index}_ordinary", dashboard=False)
        with_dashboard = _one_qa_case(wrapper, root, base / f"qa_{index}_dashboard", dashboard=True)
        if without != with_dashboard:
            msg = f"PO QA dashboard changed copied-wheel workbook values: {wrapper}: {without!r} vs {with_dashboard!r}"
            raise RuntimeError(msg)

    print(
        f"Six copied result wrappers and two PO QA wrappers passed installed-wheel smoke at {actual}: "
        "dashboard on/off workbook parity and settings."
    )


if __name__ == "__main__":
    main()
