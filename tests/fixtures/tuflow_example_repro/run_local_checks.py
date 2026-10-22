"""Read-only eager/deferred comparison for a private model; never execute its launcher."""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, cast

from packaging.specifiers import SpecifierSet

from ryan_library.functions.tuflow.run_flags import read_run_commands
from ryan_library.orchestrators.tuflow.input_preflight import RunRequest, check_simulation_inputs

from .run_checks import collect


class Measurements:
    """Measure phases and Python audit events without replacing filesystem functions."""

    def __init__(self) -> None:
        self.phase = ""
        self.calls: dict[str, Counter[str]] = {}
        self.seconds: defaultdict[str, float] = defaultdict(float)

    def audit(self, event: str, _arguments: tuple[object, ...]) -> None:
        """Count supported audit events only during a measured comparison."""
        if self.phase and event in {"open", "os.scandir", "os.listdir", "sqlite3.connect", "pathlib.Path.glob"}:
            self.calls.setdefault(self.phase, Counter())[event] += 1

    def factory(self, upstream: Any, path: str) -> Any:
        """Load a fresh model and time its contexts separately from bulk checks."""
        self.phase = "parse"
        start = time.perf_counter()
        model = upstream.TCF(path)
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

        return TimedModel()


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


def main() -> int:
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
    parser.add_argument("--repeats", type=int, default=1, help="Alternate eager/deferred order across repeats")
    options = parser.parse_args()
    if options.repeats < 1:
        parser.error("--repeats must be positive")
    flags = _read_flags(options, parser)
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
    sources = (
        "pytuflow/_tmf/cf/tcf_run_state.py",
        "pytuflow/_tmf/inp/inp_run_state.py",
        "pytuflow/_tmf/inp/inp_build_state.py",
    )
    provenance = {
        "pytuflow_version": upstream.__version__,
        "module_path": str(module_path),
        "python_version": sys.version,
        "source_sha256": {name: hashlib.sha256((checkout / name).read_bytes()).hexdigest() for name in sources},
    }
    (output / "provenance.json").write_text(json.dumps(provenance, indent=2), encoding="utf-8")
    measurements = Measurements()

    def factory(path: str) -> Any:
        return measurements.factory(upstream, path)

    sys.addaudithook(measurements.audit)
    rows: list[dict[str, Any]] = []
    sample = [runs[index] for index in sorted({0, len(runs) // 2, len(runs) - 1})]
    phases = [("representative", sample, ("eager", "deferred"))]
    phases.extend(
        (f"all-{index + 1}", runs, ("eager", "deferred") if index % 2 == 0 else ("deferred", "eager"))
        for index in range(options.repeats)
    )
    for phase, selected, modes in phases:
        identities: dict[str, set[tuple[Any, ...]]] = {}
        row: dict[str, Any] = {"label": options.label, "phase": phase, "runs": len(selected)}
        for mode in modes:
            measurements.calls.clear()
            measurements.seconds.clear()
            measurements.phase = "bulk_and_collection"
            start = time.perf_counter()
            try:
                identities[mode], stats = collect(tcf, selected, mode, factory)
                stats["seconds"] = time.perf_counter() - start
                stats["phase_seconds"] = dict(measurements.seconds)
                stats["bulk_and_collection_seconds"] = stats["seconds"] - sum(measurements.seconds.values())
                stats["audit_calls"] = {name: dict(counts) for name, counts in measurements.calls.items()}
                row[mode] = stats
            finally:
                measurements.phase = ""
        missing = identities["eager"] - identities["deferred"]
        extra = identities["deferred"] - identities["eager"]
        row.update(missing=len(missing), extra=len(extra))
        row["passed"] = (
            not missing
            and not extra
            and all(not row[mode][key] for mode in modes for key in ("collection_issues", "inventory_issues"))
        )
        # Full physical identities can disclose model paths: retain only in private output.
        (output / f"{phase}-differences.json").write_text(
            json.dumps({"missing": sorted(missing), "extra": sorted(extra)}, indent=2), encoding="utf-8"
        )
        rows.append(row)
        (output / "parity.json").write_text(json.dumps(rows, indent=2), encoding="utf-8")
        print(json.dumps(row), flush=True)
        if not row["passed"]:
            return 1
    report = check_simulation_inputs(tcf, runs)
    report.write_csv(output / "preflight-issues.csv")
    summary = {"label": options.label, "runs": len(runs), "passed": report.passed, "issues": len(report.issues)}
    (output / "preflight.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary), flush=True)
    return 0 if report.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
