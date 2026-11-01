"""Sweep RORB_CMD Kc and loss parameters and rank runs against observed flow.

Inputs: RORB .catg and .stm, RORB_CMD.exe, explicit observed/modelled columns
(or an observed CSV with matching time values). Outputs: isolated RORB
parameter/output/log folders, results.csv, and optional hydrograph PNG plots.

Run "py -3.14 rorb_calibrate.py --help" or inspect an existing output with
"py -3.14 rorb_calibrate.py --inspect-output example.out".
"""

from __future__ import annotations

import argparse
import csv
import itertools
import math
import re
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from statistics import mean
from typing import Any, cast

SCRIPT_VERSION = "2026-11-02.1"
HEADER = re.compile(r"\bInc\s+Time\b", re.IGNORECASE)


@dataclass(frozen=True)
class Combination:
    kc: Decimal
    il: Decimal
    continuing: Decimal


def values(text: str) -> list[Decimal]:
    """Parse comma-separated numbers and inclusive start:end:step ranges."""
    result: list[Decimal] = []
    try:
        for item in text.split(","):
            fields = [Decimal(part.strip()) for part in item.split(":")]
            if any(not part.is_finite() for part in fields):
                message = "Values must be finite"
                raise ValueError(message)
            if len(fields) == 1:
                result.append(fields[0])
            elif len(fields) == 3:
                start, stop, step = fields
                if step <= 0 or stop < start or (stop - start) % step != 0:
                    message = "Ranges require positive steps and an inclusive exact endpoint"
                    raise ValueError(message)
                if (stop - start) / step > 10_000:
                    message = "Parameter range exceeds 10,001 entries"
                    raise ValueError(message)
                result.extend(start + step * i for i in range(int((stop - start) / step) + 1))
            else:
                message = "Use comma-separated numbers or start:end:step"
                raise ValueError(message)
    except InvalidOperation as exc:
        message = f"Invalid numeric parameter: {text}"
        raise ValueError(message) from exc
    if not result:
        message = "A parameter list cannot be empty"
        raise ValueError(message)
    return list(dict.fromkeys(result))


def parse_hydrographs(path: Path) -> dict[str, list[float]]:  # noqa: C901 -- strict table parser
    """Read a labelled Inc/Time/HydNNN numeric table from RORB text output.

    Fail on ambiguity rather than treating a second calculated hydrograph as
    an observed series. Validate the local RORB_CMD output format before use.
    """
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    blocks: list[dict[str, list[float]]] = []
    for i, line in enumerate(lines):
        if not HEADER.search(line):
            continue
        columns = line.split()
        if len(columns) < 3 or columns[:2] != ["Inc", "Time"]:
            continue
        data: dict[str, list[float]] = {column: [] for column in columns}
        started = False
        for row_number, row in enumerate(lines[i + 1 :], start=i + 2):
            parts = row.split()
            if not parts:
                if started:
                    break
                continue
            # RORB report footers start with text; a numeric Inc denotes a data
            # row that must be complete. Never score only the valid prefix of a
            # damaged table (including an invalid first or middle row).
            try:
                float(parts[0])
            except ValueError:
                if started:
                    break
                continue
            if len(parts) != len(columns):
                message = (
                    f"Malformed hydrograph row {row_number} in {path}: expected {len(columns)} values, got {len(parts)}"
                )
                raise ValueError(message)
            try:
                numbers = [float(value) for value in parts]
            except ValueError as exc:
                message = f"Malformed hydrograph row {row_number} in {path}: nonnumeric value"
                raise ValueError(message) from exc
            if not all(math.isfinite(value) for value in numbers):
                message = f"Non-finite hydrograph values in {path} at row {row_number}"
                raise ValueError(message)
            started = True
            for column, value in zip(columns, numbers, strict=True):
                data[column].append(value)
        if data["Time"]:
            blocks.append(data)
    if not blocks:
        message = f"No Inc/Time hydrograph table found in {path}"
        raise ValueError(message)
    if len(blocks) != 1:
        message = f"Found {len(blocks)} hydrograph tables in {path}; inspect manually"
        raise ValueError(message)
    data = blocks[0]
    if any(b <= a for a, b in itertools.pairwise(data["Time"])):
        message = "Hydrograph times are not strictly increasing"
        raise ValueError(message)
    return data


def csv_observations(path: Path, time_field: str, flow_field: str) -> tuple[list[float], list[float]]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        if not reader.fieldnames or time_field not in reader.fieldnames or flow_field not in reader.fieldnames:
            message = f"Observed CSV requires columns {time_field!r}, {flow_field!r}"
            raise ValueError(message)
        points = [(float(row[time_field]), float(row[flow_field])) for row in reader]
    if not points or not all(math.isfinite(t) and math.isfinite(q) for t, q in points):
        message = "Observed CSV is empty or contains non-finite values"
        raise ValueError(message)
    return [t for t, _ in points], [q for _, q in points]


def score(observed: list[float], simulated: list[float], time: list[float]) -> dict[str, float]:
    if not observed or len(observed) != len(simulated) or len(time) != len(observed):
        message = "Observed and simulated hydrographs require matching nonempty lengths"
        raise ValueError(message)
    squared = [((s - o) ** 2) for s, o in zip(simulated, observed, strict=True)]
    rmse = math.sqrt(mean(squared))
    obs_mean = mean(observed)
    denom = sum((o - obs_mean) ** 2 for o in observed)
    obs_peak = max(observed)
    sim_peak = max(simulated)
    return {
        "rmse": rmse,
        "nse": 1 - sum(squared) / denom if denom else math.nan,
        "bias": mean(simulated) - obs_mean,
        "observed_peak": obs_peak,
        "simulated_peak": sim_peak,
        "observed_peak_time": time[observed.index(obs_peak)],
        "simulated_peak_time": time[simulated.index(sim_peak)],
    }


def render_par(catg: Path, stm: Path, case: Combination, m: Decimal, loss_model: int) -> str:
    return "\n".join(
        (
            "# BEGIN",
            f"Cat file :{catg}",
            f"Stm file :{stm}",
            "Lumped kc:T",
            "Verbosity:3",
            f"Lossmodel:{loss_model}",
            "Num ISA  :1",
            f"ISA 1    :{case.kc},{m}",
            "Num burst:1",
            f"ISA 1    :{case.il},{case.continuing}",
            "# END",
            "",
        )
    )


def plot_hydrograph(path: Path, time: list[float], observed: list[float], simulated: list[float]) -> None:
    import matplotlib as mpl

    mpl.use("Agg")
    from matplotlib import pyplot as plt

    fig, ax = plt.subplots()
    ax.plot(time, observed, label="Observed")  # pyright: ignore[reportUnknownMemberType]
    ax.plot(time, simulated, label="Calculated")  # pyright: ignore[reportUnknownMemberType]
    ax.set(xlabel="RORB time", ylabel="Flow (m³/s)")
    ax.legend()  # pyright: ignore[reportUnknownMemberType]
    fig.savefig(path, dpi=150, bbox_inches="tight")  # pyright: ignore[reportUnknownMemberType]
    plt.close(fig)


def evaluate_output(
    output_file: Path,
    *,
    sim_column: str,
    obs_column: str | None,
    observed: tuple[list[float], list[float]] | None,
    plots: bool,
    plot_path: Path,
    hma: bool,
    hma_weight: float,
    hma_window: int,
) -> dict[str, float]:
    """Score a RORB output identically for archived and newly executed runs."""
    data = parse_hydrographs(output_file)
    if sim_column not in data:
        message = f"Modelled column {sim_column!r} missing; available: {list(data)}"
        raise ValueError(message)
    if observed is None:
        if not obs_column or obs_column not in data:
            message = f"Observed column {obs_column!r} missing; available: {list(data)}"
            raise ValueError(message)
        if obs_column == sim_column:
            message = "Modelled and observed columns must differ"
            raise ValueError(message)
        observed_flow = data[obs_column]
    else:
        reference_time, observed_flow = observed
        if len(reference_time) != len(data["Time"]) or any(
            not math.isclose(a, b, rel_tol=0, abs_tol=1e-6) for a, b in zip(reference_time, data["Time"], strict=True)
        ):
            message = "Observed CSV times differ from RORB times; no implicit interpolation"
            raise ValueError(message)
    simulated = data[sim_column]
    metrics = score(observed_flow, simulated, data["Time"])
    if hma:
        import numpy as np

        from vendor.PyHMA import HMA

        matching = HMA(
            np.asarray(observed_flow),
            np.asarray(simulated),
            b=hma_weight,  # pyright: ignore[reportArgumentType] -- upstream accepts numeric weights
            max_lag=hma_window,
            max_lead=hma_window,
            measure="nse",
            keep_internals=False,
            calc_rays=False,
        )
        metrics["hma"] = cast(float, matching.calc_dense2())
    if plots:
        plot_hydrograph(plot_path, data["Time"], observed_flow, simulated)
    return metrics


def run_one(
    index: int,
    case: Combination,
    *,
    executable: Path,
    catg: Path,
    stm: Path,
    root: Path,
    m: Decimal,
    loss_model: int,
    sim_column: str,
    obs_column: str | None,
    observed: tuple[list[float], list[float]] | None,
    observed_source: str,
    plots: bool,
    hma: bool,
    hma_weight: float,
    hma_window: int,
    timeout: float,
) -> dict[str, Any]:
    run_dir = root / f"run_{index:05d}"
    result: dict[str, Any] = {
        "run": index,
        "kc": str(case.kc),
        "m": str(m),
        "loss_model": loss_model,
        "il": str(case.il),
        "continuing": str(case.continuing),
        "modelled_column": sim_column,
        "observed_source": observed_source,
        "status": "failed",
        "error": "",
        "directory": str(run_dir),
    }
    try:
        run_dir.mkdir()  # never overwrite a previous calibration run
        par = run_dir / "calibration.par"
        par.write_text(render_par(catg, stm, case, m, loss_model), encoding="utf-8")
        process = subprocess.run(
            [str(executable), par.name],
            cwd=run_dir,
            capture_output=True,
            text=True,
            errors="replace",
            timeout=timeout,
            check=False,
        )
        (run_dir / "rorb_stdout.txt").write_text(process.stdout, encoding="utf-8")
        (run_dir / "rorb_stderr.txt").write_text(process.stderr, encoding="utf-8")
        if process.returncode != 0:
            message = f"RORB exited {process.returncode}; inspect rorb_stderr.txt"
            raise RuntimeError(message)
        outputs = sorted(run_dir.glob("*.out"))
        if len(outputs) != 1:
            message = f"Expected exactly one .out in isolated run directory, found {len(outputs)}"
            raise RuntimeError(message)
        result["output_report"] = str(outputs[0])
        result.update(
            evaluate_output(
                outputs[0],
                sim_column=sim_column,
                obs_column=obs_column,
                observed=observed,
                plots=plots,
                plot_path=run_dir / "hydrograph.png",
                hma=hma,
                hma_weight=hma_weight,
                hma_window=hma_window,
            )
        )
        result["status"] = "success"
    except Exception as exc:  # noqa: BLE001 -- report any individual RORB failure without cancelling sweep
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--catg", type=Path, help="RORB catchment file (.catg)")
    p.add_argument("--stm", type=Path, help="RORB rainfall/storm file (.stm)")
    p.add_argument("--rorb", default="RORB_CMD.exe", help="RORB_CMD executable or executable on PATH")
    p.add_argument("--kc", default="15:20:5", help="Kc values or inclusive start:end:step")
    p.add_argument("--il", default="10:20:5", help="IL values or inclusive start:end:step")
    p.add_argument("--continuing", default="5:10:5", help="CL or RoC values or inclusive range")
    p.add_argument("--m", default="0.8", help="RORB exponent m")
    p.add_argument("--loss-model", type=int, choices=[1, 2], default=1, help="1 = IL/CL; 2 = IL/RoC")
    p.add_argument("--sim-column", help="Modelled column in RORB output, e.g. Hyd001")
    p.add_argument("--obs-column", help="Observed column in RORB output; explicitly verify source")
    p.add_argument("--observed-csv", type=Path, help="Alternative CSV of observed time and flow")
    p.add_argument("--obs-time", default="Time", help="Time column of observed CSV")
    p.add_argument("--obs-flow", default="Flow", help="Flow column of observed CSV")
    p.add_argument("--inspect-output", type=Path, help="List columns of existing RORB .out and exit")
    p.add_argument(
        "--score-output",
        type=Path,
        help="Score an archived RORB .out without launching RORB_CMD or requiring .catg/.stm",
    )
    p.add_argument("--output", type=Path, default=Path("rorb_calibration"), help="Output parent folder")
    p.add_argument("--workers", type=int, default=1, help="RORB processes; 1 safest for licence/shared files")
    p.add_argument("--timeout", type=float, default=1800, help="Seconds per RORB process")
    p.add_argument("--max-runs", type=int, default=1000, help="Guard against unbounded sweeps")
    p.add_argument("--plots", action="store_true", help="Save comparison hydrograph PNGs")
    p.add_argument("--hma", action="store_true", help="Also calculate optional vendored PyHMA match index")
    p.add_argument("--hma-weight", type=float, default=4.0, help="PyHMA timing-error weight (default: 4)")
    p.add_argument("--hma-window", type=int, default=50, help="PyHMA maximum lead/lag in time increments")
    p.add_argument(
        "--dry-run", "--preflight", action="store_true", help="Validate inputs/parameters; do not execute RORB"
    )
    p.add_argument("--no-pause", action="store_true", help="Noninteractive mode; accepted for wrapper compatibility")
    p.add_argument("--version", action="version", version=f"%(prog)s {SCRIPT_VERSION}")
    return p


def main(argv: list[str] | None = None) -> int:  # noqa: C901 -- standalone CLI dispatch
    p = parser()
    args = p.parse_args(argv)
    try:
        package_version = version("ryan-tools")
    except PackageNotFoundError:
        package_version = "not installed (standalone)"
    print(f"{Path(__file__).name} {SCRIPT_VERSION} | Python {sys.version.split()[0]} | ryan-tools {package_version}")
    try:
        if args.inspect_output:
            data = parse_hydrographs(args.inspect_output.absolute())
            print(f"Columns: {', '.join(data)} | Rows: {len(data['Time'])}")
            print("Column names alone do not establish observed/modelled provenance.")
            return 0
        if args.score_output:
            # The archived-report mode deliberately never invokes RORB_CMD.
            # Retain exactly the same comparison path as active calibration.
            if not args.sim_column:
                message = "--sim-column required to score an archived report"
                raise ValueError(message)
            if (args.obs_column is None) == (args.observed_csv is None):
                message = "Provide exactly one of --obs-column or --observed-csv"
                raise ValueError(message)
            if args.obs_column == args.sim_column:
                message = "Modelled and observed columns must differ"
                raise ValueError(message)
            if args.hma and (args.hma_window < 1 or not math.isfinite(args.hma_weight) or args.hma_weight <= 0):
                message = "PyHMA window and weight must be positive"
                raise ValueError(message)
            report_file = args.score_output.absolute()
            if not report_file.is_file():
                message = f"Archived report not found: {report_file}"
                raise FileNotFoundError(message)
            reference = (
                csv_observations(args.observed_csv.absolute(), args.obs_time, args.obs_flow)
                if args.observed_csv
                else None
            )
            # Score before writing anything, so invalid sources leave no output folder.
            metrics = evaluate_output(
                report_file,
                sim_column=args.sim_column,
                obs_column=args.obs_column,
                observed=reference,
                plots=False,
                plot_path=report_file,
                hma=args.hma,
                hma_weight=args.hma_weight,
                hma_window=args.hma_window,
            )
            if args.dry_run:
                print(f"Offline preflight passed: {report_file}")
                print(f"RMSE: {metrics['rmse']:.6g} | NSE: {metrics['nse']:.6g}")
                print("No output files created.")
                return 0
            output_parent = args.output.absolute()
            output_parent.mkdir(parents=True, exist_ok=True)
            archive_root = output_parent / f"archived_{datetime.now().strftime('%Y%m%d-%H%M%S-%f')}"
            archive_root.mkdir()
            if args.plots:
                # Repeat the lightweight parsing/scoring for plot creation only.
                evaluate_output(
                    report_file,
                    sim_column=args.sim_column,
                    obs_column=args.obs_column,
                    observed=reference,
                    plots=True,
                    plot_path=archive_root / "hydrograph.png",
                    hma=False,
                    hma_weight=args.hma_weight,
                    hma_window=args.hma_window,
                )
            row: dict[str, str | float] = {
                "source_file": str(report_file),
                "modelled_column": args.sim_column,
                "observed_source": str(args.observed_csv.absolute()) if args.observed_csv else str(args.obs_column),
                **metrics,
            }
            with (archive_root / "metrics.csv").open("w", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(stream, fieldnames=list(row))
                writer.writeheader()
                writer.writerow(row)
            print(f"Archived output scored without RORB_CMD: {report_file}")
            print(f"RMSE: {metrics['rmse']:.6g} | NSE: {metrics['nse']:.6g}")
            print(f"Results: {archive_root / 'metrics.csv'}")
            return 0
        if not args.catg or not args.stm:
            p.error("--catg and --stm are required unless --inspect-output is used")
        catg, stm = args.catg.absolute(), args.stm.absolute()
        for path in (catg, stm):
            if not path.is_file():
                message = f"Required input file not found: {path}"
                raise FileNotFoundError(message)
        if args.workers < 1 or args.timeout <= 0 or args.max_runs < 1:
            message = "workers, timeout and max-runs must be positive"
            raise ValueError(message)
        if args.hma and (args.hma_window < 1 or not math.isfinite(args.hma_weight) or args.hma_weight <= 0):
            message = "PyHMA window and weight must be positive"
            raise ValueError(message)
        m = Decimal(args.m)
        if not m.is_finite() or m <= 0:
            message = "m must be positive and finite"
            raise ValueError(message)
        kcs, ils, continuing = values(args.kc), values(args.il), values(args.continuing)
        if min(kcs) <= 0 or min(ils) < 0 or min(continuing) < 0:
            message = "Kc must be positive; losses must be nonnegative"
            raise ValueError(message)
        run_count = len(kcs) * len(ils) * len(continuing)
        if run_count > args.max_runs:
            message = f"{run_count} runs exceeds --max-runs {args.max_runs}"
            raise ValueError(message)
        runs = [Combination(kc, il, cont) for kc, il, cont in itertools.product(kcs, ils, continuing)]
        rorb_value = str(args.rorb)
        exe = Path(rorb_value)
        path_on_system: str | None = shutil.which(rorb_value)
        executable = exe.absolute() if exe.is_file() else Path(path_on_system).absolute() if path_on_system else None
        print(f"Catchment: {catg}\nStorm: {stm}\nRORB executable: {executable or 'NOT FOUND'}")
        print(f"Runs: {len(runs)} | workers: {args.workers} | loss model: {args.loss_model}")
        if args.dry_run:
            print("Preflight only: no processes launched and no output files created.")
            return 0 if executable is not None else 1
        if executable is None:
            message = "RORB_CMD.exe not found; supply --rorb or add it to PATH"
            raise FileNotFoundError(message)
        if not args.sim_column:
            message = "--sim-column required; identify it from a verified RORB .out file"
            raise ValueError(message)
        if (args.obs_column is None) == (args.observed_csv is None):
            message = "Provide exactly one of --obs-column or --observed-csv"
            raise ValueError(message)
        if args.obs_column == args.sim_column:
            message = "Modelled and observed columns must differ"
            raise ValueError(message)
        observed = (
            csv_observations(args.observed_csv.absolute(), args.obs_time, args.obs_flow) if args.observed_csv else None
        )
        output_parent = args.output.absolute()
        output_parent.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        root = output_parent / f"calibration_{stamp}"
        root.mkdir()  # refusal on collision; never overwrite previous results
        cases: list[dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            jobs = [
                executor.submit(
                    run_one,
                    i,
                    case,
                    executable=executable,
                    catg=catg,
                    stm=stm,
                    root=root,
                    m=m,
                    loss_model=args.loss_model,
                    sim_column=args.sim_column,
                    obs_column=args.obs_column,
                    observed=observed,
                    observed_source=str(args.observed_csv.absolute()) if args.observed_csv else str(args.obs_column),
                    plots=args.plots,
                    hma=args.hma,
                    hma_weight=args.hma_weight,
                    hma_window=args.hma_window,
                    timeout=args.timeout,
                )
                for i, case in enumerate(runs, 1)
            ]
            for done in as_completed(jobs):
                case_result = done.result()
                cases.append(case_result)
                print(f"[{len(cases)}/{len(jobs)}] {case_result['status']}: run {case_result['run']}")
        cases.sort(key=lambda row: (row["status"] != "success", row.get("rmse", math.inf), row["run"]))
        fields = [
            "run",
            "kc",
            "m",
            "loss_model",
            "il",
            "continuing",
            "modelled_column",
            "observed_source",
            "status",
            "error",
            "directory",
            "output_report",
            "rmse",
            "nse",
            "bias",
            "observed_peak",
            "simulated_peak",
            "observed_peak_time",
            "simulated_peak_time",
            "hma",
        ]
        with (root / "results.csv").open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            writer.writerows(cases)
        count = sum(case["status"] == "success" for case in cases)
        print(f"Successful: {count}/{len(cases)} | results: {root / 'results.csv'}")
        return 0 if count == len(cases) else 1
    except (OSError, ValueError, InvalidOperation) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
