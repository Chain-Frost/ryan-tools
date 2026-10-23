"""Read-only eager/deferred/batch evaluation for a private model; never execute its launcher."""

from __future__ import annotations

import argparse
import gc
import hashlib
import importlib
import json
import re
import shutil
import subprocess
import sys
import time
from collections import Counter, defaultdict
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import fields
from pathlib import Path
from threading import Event, Thread
from typing import Any, cast

import psutil
from packaging.specifiers import SpecifierSet

from ryan_library.functions.tuflow.run_flags import read_run_commands
from ryan_library.orchestrators.tuflow.input_preflight import RunRequest, check_simulation_inputs

from .run_checks import GRAPH_MODES, collect, compare_collections, comparison_modes


class Measurements:
    """Measure phases and Python audit events without replacing filesystem functions."""

    def __init__(self) -> None:
        self.phase = ""
        self.calls: dict[str, Counter[str]] = {}
        self.seconds: defaultdict[str, float] = defaultdict(float)
        self.batch_metrics: dict[str, Any] = {}

    def audit(self, event: str, _arguments: tuple[object, ...]) -> None:
        """Count supported audit events only during a measured comparison."""
        if self.phase and event in {"open", "os.scandir", "os.listdir", "sqlite3.connect", "pathlib.Path.glob"}:
            self.calls.setdefault(self.phase, Counter())[event] += 1

    def factory(self, upstream: Any, path: str) -> Any:
        """Load a fresh model and time its contexts separately from bulk checks."""
        self.phase = "parse"
        start = time.perf_counter()
        try:
            model = upstream.TCF(path)
        finally:
            self.seconds["parse"] += time.perf_counter() - start
            self.phase = "bulk_and_collection"
        measurements = self

        class TimedModel:
            def context(self, flags: str, *, file_resolution: str = "eager") -> Any:
                measurements.phase = "context"
                start = time.perf_counter()
                try:
                    return model.context(flags, file_resolution=file_resolution)
                finally:
                    measurements.seconds["context"] += time.perf_counter() - start
                    measurements.phase = "bulk_and_collection"

            def collect_batch_dependencies(
                self, flags: tuple[str, ...], *, cache_scopes: bool = True, compile_tree: bool | None = None
            ) -> Any:
                measurements.phase = "batch"
                start = time.perf_counter()
                try:
                    result = (
                        model.collect_batch_dependencies(flags)
                        if compile_tree is None
                        else model.collect_batch_dependencies(
                            flags, cache_scopes=cache_scopes, compile_tree=compile_tree
                        )
                    )
                    measurements.batch_metrics.update(
                        {
                            field.name: value
                            for field in fields(result)
                            if isinstance(value := getattr(result, field.name), (int, float, str))
                            or value is None
                            or field.name in {"input_group_fallbacks", "input_group_bypass_reasons"}
                        }
                    )
                    return result
                finally:
                    measurements.seconds["batch"] += time.perf_counter() - start
                    measurements.phase = "bulk_and_collection"

        return TimedModel()


@contextmanager
def _sample_memory() -> Generator[dict[str, int]]:
    """Sample process RSS every 20 ms, including native allocations; not an exact peak."""
    process = psutil.Process()
    baseline = process.memory_info().rss
    memory = {"rss_start_bytes": baseline, "rss_peak_sampled_bytes": baseline}
    stop = Event()

    def sample() -> None:
        while not stop.wait(0.02):
            memory["rss_peak_sampled_bytes"] = max(memory["rss_peak_sampled_bytes"], process.memory_info().rss)

    thread = Thread(target=sample, daemon=True)
    thread.start()
    try:
        yield memory
    finally:
        stop.set()
        thread.join()
        memory["rss_peak_sampled_bytes"] = max(memory["rss_peak_sampled_bytes"], process.memory_info().rss)
        memory["rss_peak_delta_bytes"] = memory["rss_peak_sampled_bytes"] - baseline


def _measure_collection(
    tcf: Path, runs: list[RunRequest], mode: str, upstream: Any, measurements: Measurements
) -> tuple[set[tuple[Any, ...]], dict[str, Any]]:
    """Include the actual batch call in measured work, then fully materialise sources."""
    measurements.calls.clear()
    measurements.seconds.clear()
    measurements.batch_metrics.clear()
    gc.collect()

    def factory(path: str) -> Any:
        return measurements.factory(upstream, path)

    identities: set[tuple[Any, ...]]
    stats: dict[str, Any]
    try:
        with _sample_memory() as memory:
            measurements.phase = "bulk_and_collection"
            start = time.perf_counter()
            try:
                identities, stats = collect(tcf, runs, mode, factory)
            except Exception as exc:  # noqa: BLE001 - preserve failed-mode timings and continue diagnostic coverage
                identities, stats = set(), {"error": f"{type(exc).__name__}: {exc}"}
            stats["seconds"] = time.perf_counter() - start
        stats["phase_seconds"] = dict(measurements.seconds)
        stats["bulk_and_collection_seconds"] = stats["seconds"] - sum(measurements.seconds.values())
        stats["audit_calls"] = {name: dict(counts) for name, counts in measurements.calls.items()}
        stats["memory"] = memory
        if mode == "batch" or mode in GRAPH_MODES:
            stats["batch_metrics"] = dict(measurements.batch_metrics)
        return identities, stats
    finally:
        measurements.phase = ""


def _read_flags(options: argparse.Namespace, parser: argparse.ArgumentParser) -> list[dict[str, str]]:
    """Validate the private run list before reading any model inputs."""
    raw_flags: Any = (
        read_run_commands(options.commands_file)
        if options.commands_file
        else json.loads(options.runs_json.read_text(encoding="utf-8"))
    )
    if not isinstance(raw_flags, list):
        parser.error("Run list does not match --expect-runs")
    entries = cast(list[object], raw_flags)
    if not entries or len(entries) != options.expect_runs:
        parser.error("Run list does not match --expect-runs")
    flags: list[dict[str, str]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            parser.error("Each run must be a dictionary of string event/scenario values")
        mapping = cast(dict[object, object], entry)
        if any(not isinstance(key, str) or not isinstance(value, str) for key, value in mapping.items()):
            parser.error("Each run must be a dictionary of string event/scenario values")
        flags.append(cast(dict[str, str], mapping))
    return flags


def _record_comparisons(
    row: dict[str, Any], identities: dict[str, set[tuple[Any, ...]]], output: Path, phase: str
) -> None:
    """Compare complete materialised identities, refusing any partial collection."""
    differences = compare_collections(row, identities, tuple(row["mode_order"]))
    # Physical identities can disclose private model paths; output stays outside the repository.
    (output / f"{phase}-differences.json").write_text(json.dumps(differences, indent=2), encoding="utf-8")


def _public_preflight(tcf: Path, runs: list[RunRequest], output: Path, label: str, batch: bool) -> bool:
    """Require matching public preflight paths/issues as well as identity parity."""
    report = check_simulation_inputs(tcf, runs)
    report.write_csv(output / "preflight-issues.csv")
    summary: dict[str, Any] = {
        "label": label,
        "runs": len(runs),
        "passed": report.passed,
        "issues": len(report.issues),
        "timed": False,
        "batch_graph_policy": "parser_default",
        "graph_comparison": False,
    }
    if batch:
        projected = check_simulation_inputs(tcf, runs, context_mode="batch")
        projected.write_csv(output / "preflight-batch-issues.csv")
        summary["batch_passed"] = projected.passed
        summary["batch_issues"] = len(projected.issues)
        summary["paths_equal"] = report.checked_paths == projected.checked_paths
        summary["issues_equal"] = set(report.issues) == set(projected.issues)
        summary["passed"] = all(summary[key] for key in ("passed", "batch_passed", "paths_equal", "issues_equal"))
    (output / "preflight.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary), flush=True)
    return bool(summary["passed"])


def _git_revision(root: Path) -> dict[str, Any]:
    """Record exact source revision and dirtiness without traversing submodules."""
    executable = shutil.which("git")
    if executable is None:
        msg = "Git is required to record the actual parser and diagnostic revisions"
        raise ValueError(msg)

    def git(*arguments: str) -> str:
        return subprocess.run(
            [executable, "-C", str(root), *arguments],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout.strip()

    try:
        # Physical comparison ensures Git describes this checkout, not an unrelated ancestor.
        if Path(git("rev-parse", "--show-toplevel")).resolve() != root.resolve():
            msg = f"Source directory is not a Git checkout root: {root}"
            raise ValueError(msg)
        head = git("rev-parse", "HEAD")
        if re.fullmatch(r"[0-9a-f]{40}", head) is None:
            msg = f"Cannot identify an exact Git revision: {root}"
            raise ValueError(msg)
        status = git(
            "status",
            "--porcelain",
            "--untracked-files=normal",
            "--ignore-submodules=all",
            "--",
            ".",
            ":(exclude)unsorted",
        )
    except (OSError, subprocess.SubprocessError) as exc:
        msg = f"Could not record Git source provenance for {root}: {exc}"
        raise ValueError(msg) from exc
    return {"head": head, "dirty": bool(status), "status": status}


def main() -> int:  # noqa: C901 - explicit arguments, source verification and diagnostic acceptance
    """Check representative contexts, then every requested run, with private reports."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tcf", type=Path, required=True)
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument("--commands-file", type=Path, help="Launcher export for this TCF only; never executed")
    inputs.add_argument("--runs-json", type=Path, help="Private JSON list of event/scenario flag dictionaries")
    parser.add_argument("--expect-runs", type=int, required=True)
    parser.add_argument("--label", required=True, help="Anonymous report label, for example P or B")
    parser.add_argument("--pytuflow-checkout", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True, help="Private report directory outside the repository")
    parser.add_argument(
        "--repeats", type=int, help="Full trials; graph comparison requires a multiple of four (default 4)"
    )
    parser.add_argument("--batch", action="store_true", help="Include experimental batch materialisation and preflight")
    parser.add_argument("--graph-compare", action="store_true", help="Isolate cached graph-off/on; requires --batch")
    parser.add_argument(
        "--storage-and-cache-conditions", help="Anonymous storage/cache description; required for graph comparison"
    )
    parser.add_argument(
        "--continue-on-findings",
        action="store_true",
        help="Complete diagnostic coverage despite findings; acceptance still fails with exit 1",
    )
    parser.add_argument(
        "--representative-only", action="store_true", help="Gate first/middle/last runs without a full sweep"
    )
    options = parser.parse_args()
    try:
        modes = comparison_modes(options.batch, options.graph_compare)
    except ValueError as exc:
        parser.error(str(exc))
    if options.repeats is None:
        options.repeats = 4 if options.graph_compare else 1
    if options.repeats < 1:
        parser.error("--repeats must be positive")
    if options.graph_compare and (options.repeats < 4 or options.repeats % 4):
        parser.error("--graph-compare requires at least four full trials and --repeats must be a multiple of four")
    if options.graph_compare and not (options.storage_and_cache_conditions or "").strip():
        parser.error("--graph-compare requires --storage-and-cache-conditions")
    try:
        flags = _read_flags(options, parser)
    except (OSError, ValueError) as exc:
        parser.error(f"Could not read run flags: {exc}")
    runs = [RunRequest(index, item) for index, item in enumerate(flags, 1)]
    tcf = options.tcf.absolute()
    output = options.output.absolute()
    output.mkdir(parents=True, exist_ok=True)
    # Explicit dependency selection for this diagnostic, never a production import override.
    sys.path.insert(0, str(options.pytuflow_checkout.absolute()))
    upstream = importlib.import_module("pytuflow")
    # Follow links deliberately to verify the imported module's physical checkout.
    checkout = options.pytuflow_checkout.resolve()
    module_file = upstream.__file__
    if module_file is None:
        parser.error("Imported PyTUFLOW has no source location")
    module_path = Path(module_file).resolve()
    if not module_path.is_relative_to(checkout / "pytuflow"):
        parser.error("Imported PyTUFLOW is not from --pytuflow-checkout")
    if not SpecifierSet(">=1.2,<2").contains(upstream.__version__):
        parser.error("The selected checkout must provide pytuflow>=1.2,<2")
    sources = [
        "pytuflow/_tmf/context.py",
        "pytuflow/_tmf/db/mat.py",
        "pytuflow/_tmf/cf/tcf_run_state.py",
        "pytuflow/_tmf/inp/inp_run_state.py",
        "pytuflow/_tmf/inp/inp_build_state.py",
        "pytuflow/_tmf/inp/db.py",
        "pytuflow/_tmf/deferred_database.py",
    ]
    if options.batch:
        sources.append("pytuflow/_tmf/batch_dependencies.py")
    provenance = {
        "pytuflow_version": upstream.__version__,
        "module_path": str(module_path),
        "python_version": sys.version,
        "source_sha256": {name: hashlib.sha256((checkout / name).read_bytes()).hexdigest() for name in sources},
        "storage_and_cache_conditions": options.storage_and_cache_conditions or "unspecified",
        "modes": modes,
        "expected_runs": len(runs),
        "distinct_complete_flags": len({run.arguments for run in runs}),
        "full_trials": 0 if options.representative_only else options.repeats,
        "timing_boundary": "parse + context/public batch + remaining collection/provenance/materialisation/inventory",
        "excluded_from_timings": ["process startup", "report writing", "public preflight"],
        "batch_metric_timings_overlap": True,
        "audit_events_are_disk_or_smb_operations": False,
        "rss_sample_interval_seconds": 0.02,
    }
    try:
        provenance["parser_git"] = _git_revision(checkout)
        provenance["consumer_git"] = _git_revision(Path(__file__).absolute().parents[3])
    except ValueError as exc:
        parser.error(str(exc))
    (output / "provenance.json").write_text(json.dumps(provenance, indent=2), encoding="utf-8")
    # Reusing an output directory must never leave an earlier accepted summary
    # or public-preflight result behind when this invocation stops at its gate.
    pending = {
        "label": options.label,
        "passed": False,
        "completed": False,
        "validation_scope": "representative_only" if options.representative_only else "full_workload",
        "full_workload_validated": False,
        "accepted_for_benchmark": False,
    }
    (output / "summary.json").write_text(json.dumps(pending, indent=2), encoding="utf-8")
    (output / "preflight.json").write_text(
        json.dumps(
            {
                "passed": False,
                "completed": False,
                "timed": False,
                "batch_graph_policy": "parser_default",
                "graph_comparison": False,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    measurements = Measurements()

    sys.addaudithook(measurements.audit)
    rows: list[dict[str, Any]] = []
    all_passed = True
    sample = [runs[index] for index in sorted({0, len(runs) // 2, len(runs) - 1})]
    phases: list[tuple[str, list[RunRequest], tuple[str, ...]]] = [("representative", sample, modes)]
    if not options.representative_only:
        phases.extend(
            (f"all-{index + 1}", runs, modes[index % len(modes) :] + modes[: index % len(modes)])
            for index in range(options.repeats)
        )
    for phase, selected, modes in phases:
        identities: dict[str, set[tuple[Any, ...]]] = {}
        row: dict[str, Any] = {
            "label": options.label,
            "phase": phase,
            "runs": len(selected),
            "mode_order": modes,
            "validation_scope": "representative_only" if options.representative_only else "full_workload",
            "full_workload_validated": False,
            "accepted_for_benchmark": False,
        }
        for mode in modes:
            try:
                collected, row[mode] = _measure_collection(tcf, selected, mode, upstream, measurements)
                if not row[mode].get("error"):
                    identities[mode] = collected
            except Exception as exc:  # noqa: BLE001 - preserve missing/failed modes as diagnostic findings
                row[mode] = {"error": f"{type(exc).__name__}: {exc}"}
        _record_comparisons(row, identities, output, phase)
        rows.append(row)
        (output / "parity.json").write_text(json.dumps(rows, indent=2), encoding="utf-8")
        console_row = dict(row)
        for mode in modes:
            console_row[mode] = {
                key: value
                for key, value in row[mode].items()
                if key not in {"command_provenance", "reference_provenance"}
            }
        print(json.dumps(console_row), flush=True)
        all_passed = all_passed and row["passed"]
        if not row["passed"] and not options.continue_on_findings:
            pending["stopped_at"] = phase
            (output / "summary.json").write_text(json.dumps(pending, indent=2), encoding="utf-8")
            return 1
    try:
        public_passed = _public_preflight(
            tcf, sample if options.representative_only else runs, output, options.label, options.batch
        )
    except Exception as exc:  # noqa: BLE001 - public-preflight failure must remain a recorded failed gate
        public_passed = False
        (output / "preflight.json").write_text(
            json.dumps(
                {
                    "passed": False,
                    "error": f"{type(exc).__name__}: {exc}",
                    "timed": False,
                    "batch_graph_policy": "parser_default",
                    "graph_comparison": False,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
    summary = {
        "label": options.label,
        "completed": True,
        "passed": bool(all_passed and public_passed),
        "validation_scope": "representative_only" if options.representative_only else "full_workload",
        "full_workload_validated": bool(all_passed and public_passed and not options.representative_only),
        "accepted_for_benchmark": bool(all_passed and public_passed and not options.representative_only),
        "public_preflight": {"timed": False, "batch_graph_policy": "parser_default", "graph_comparison": False},
    }
    (output / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary), flush=True)
    return 0 if all_passed and public_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
