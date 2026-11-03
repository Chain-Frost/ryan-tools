"""Benchmark read-only TUFLOW GPU and CodeMeter polling.

This harness never launches TUFLOW, changes licences, or scans project logs.
Run it while independently controlled licensed simulations enter and leave the
desired states, then retain each JSON result with the hardware test notes.
"""

from __future__ import annotations

import argparse
import json
import math
import platform
import statistics
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import psutil

from ryan_library.functions.tuflow.codemeter import fetch_codemeter_snapshot
from ryan_library.functions.tuflow.gpu_occupancy import local_gpu_occupancy

type JsonValue = None | bool | int | float | str | list[JsonValue] | JsonObject
type JsonObject = dict[str, JsonValue]


@dataclass(frozen=True, slots=True)
class Measurement:
    """One polling observation and its resource costs."""

    iteration: int
    method: str
    elapsed_ms: float
    cpu_ms: float
    rss_delta_bytes: int
    error: str
    result: JsonObject

    def as_json(self) -> JsonObject:
        return {
            "iteration": self.iteration,
            "method": self.method,
            "elapsed_ms": self.elapsed_ms,
            "cpu_ms": self.cpu_ms,
            "rss_delta_bytes": self.rss_delta_bytes,
            "error": self.error,
            "result": self.result,
        }


@dataclass(frozen=True, slots=True)
class BenchmarkOptions:
    """Validated command-line options."""

    samples: int
    methods: tuple[str, ...]
    sleep_seconds: float
    codemeter_url: str
    codemeter_timeout: float
    scenario: str
    output: Path | None


def _local_snapshot() -> JsonObject:
    snapshot = local_gpu_occupancy()
    return {
        "assigned": [
            {"pid": process.pid, "gpu_ids": sorted(process.gpu_ids)} for process in snapshot.assigned
        ],
        "unknown_pids": list(snapshot.unknown_pids),
        "inspection_incomplete": snapshot.inspection_incomplete,
    }


def _codemeter_snapshot(*, base_url: str, timeout_seconds: float) -> JsonObject:
    snapshot = fetch_codemeter_snapshot(base_url=base_url, timeout_seconds=timeout_seconds)
    return {
        "products": [
            {
                "product_code": product.product_code,
                "name": product.name,
                "total": product.total,
                "used": product.used,
                "available": product.available,
            }
            for product in snapshot.products
        ],
        "allocations": [
            {
                "process_id": allocation.process_id,
                "product_code": allocation.product_code,
                "handle_id": allocation.handle_id,
            }
            for allocation in snapshot.allocations
        ],
        "warnings": list(snapshot.warnings),
    }


def _measure(*, iteration: int, method: str, operation: Callable[[], JsonObject]) -> Measurement:
    process = psutil.Process()
    rss_before = process.memory_info().rss
    cpu_before = time.process_time_ns()
    started = time.perf_counter_ns()
    error = ""
    result: JsonObject = {}
    try:
        result = operation()
    except Exception as exc:  # noqa: BLE001 - failures are benchmark observations
        error = f"{type(exc).__name__}: {exc}"
    elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
    cpu_ms = (time.process_time_ns() - cpu_before) / 1_000_000
    rss_delta = process.memory_info().rss - rss_before
    return Measurement(
        iteration=iteration,
        method=method,
        elapsed_ms=elapsed_ms,
        cpu_ms=cpu_ms,
        rss_delta_bytes=rss_delta,
        error=error,
        result=result,
    )


def _percentile_95(values: Sequence[float]) -> float:
    ordered = sorted(values)
    index = max(math.ceil(0.95 * len(ordered)) - 1, 0)
    return ordered[index]


def _statistics(values: Sequence[float]) -> JsonObject:
    return {
        "samples": len(values),
        "median": statistics.median(values),
        "p95": _percentile_95(values),
        "maximum": max(values),
    }


def _summaries(measurements: Sequence[Measurement], methods: Sequence[str]) -> JsonObject:
    summaries: JsonObject = {}
    for method in methods:
        rows = [row for row in measurements if row.method == method]
        successful = [row for row in rows if not row.error]
        summary: JsonObject = {
            "attempts": len(rows),
            "successes": len(successful),
            "failures": len(rows) - len(successful),
        }
        if successful:
            summary["elapsed_ms"] = _statistics([row.elapsed_ms for row in successful])
            summary["cpu_ms"] = _statistics([row.cpu_ms for row in successful])
            summary["rss_delta_bytes"] = _statistics([float(row.rss_delta_bytes) for row in successful])
        summaries[method] = summary
    return summaries


def _arguments(argv: Sequence[str] | None = None) -> BenchmarkOptions:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=int, default=30, help="Measurements per method (default: 30).")
    parser.add_argument(
        "--methods",
        nargs="+",
        choices=("local", "codemeter", "combined"),
        default=("local", "codemeter", "combined"),
    )
    parser.add_argument("--sleep-seconds", type=float, default=0.1, help="Delay between samples (default: 0.1).")
    parser.add_argument("--codemeter-url", default="http://localhost:22352")
    parser.add_argument("--codemeter-timeout", type=float, default=5.0)
    parser.add_argument("--scenario", required=True, help="Independent ground-truth scenario label.")
    parser.add_argument("--output", type=Path, help="Optional new JSON path; existing files are never overwritten.")
    args = parser.parse_args(argv)
    if args.samples < 1:
        parser.error("--samples must be at least 1")
    if args.sleep_seconds < 0:
        parser.error("--sleep-seconds must be nonnegative")
    if not math.isfinite(args.codemeter_timeout) or args.codemeter_timeout <= 0:
        parser.error("--codemeter-timeout must be a finite positive number")
    return BenchmarkOptions(
        samples=int(args.samples),
        methods=tuple(str(method) for method in args.methods),
        sleep_seconds=float(args.sleep_seconds),
        codemeter_url=str(args.codemeter_url),
        codemeter_timeout=float(args.codemeter_timeout),
        scenario=str(args.scenario),
        output=args.output if isinstance(args.output, Path) else None,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = _arguments(argv)
    methods = args.methods

    def codemeter() -> JsonObject:
        return _codemeter_snapshot(base_url=args.codemeter_url, timeout_seconds=args.codemeter_timeout)

    operations: dict[str, Callable[[], JsonObject]] = {
        "local": _local_snapshot,
        "codemeter": codemeter,
        "combined": lambda: {"local": _local_snapshot(), "codemeter": codemeter()},
    }
    measurements: list[Measurement] = []
    for iteration in range(1, args.samples + 1):
        for method in methods:
            measurements.append(_measure(iteration=iteration, method=method, operation=operations[method]))
        if iteration < args.samples and args.sleep_seconds:
            time.sleep(args.sleep_seconds)

    report: JsonObject = {
        "schema_version": 1,
        "checked_at": datetime.now().astimezone().isoformat(),
        "scenario": args.scenario,
        "python": sys.version,
        "platform": platform.platform(),
        "codemeter_url": args.codemeter_url,
        "codemeter_timeout_seconds": args.codemeter_timeout,
        "measurements": [measurement.as_json() for measurement in measurements],
        "summary": _summaries(measurements, methods),
        "interpretation_note": (
            "CodeMeter allocations are licence observations. GPU IDs are reported only by local TUFLOW command lines."
        ),
    }
    rendered = json.dumps(report, indent=2, sort_keys=True)
    print(rendered)
    if args.output is not None:
        output = args.output.absolute()
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(f"{rendered}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
