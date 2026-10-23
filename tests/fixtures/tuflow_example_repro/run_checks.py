"""Repository regression harness for read-only Windows TUFLOW dependency parity."""

from __future__ import annotations

import argparse
import fnmatch
import gc
import hashlib
import importlib
import json
import re
import sqlite3
import sys
import tempfile
from collections import Counter
from contextlib import closing
from dataclasses import fields, replace
from pathlib import Path
from typing import Any
from zipfile import ZipFile

from ryan_library.functions.tuflow.dependency_index import check_unique_dependencies, expand_file_patterns
from ryan_library.functions.tuflow.run_flags import extract_flag_pairs
from ryan_library.orchestrators.tuflow.batch_preflight_adapter import BatchModelView
from ryan_library.orchestrators.tuflow.input_preflight import (
    PreflightReport,
    RunRequest,
    _collect_references,  # pyright: ignore[reportPrivateUsage] - repository regression contract
    _materialise_attribute_references,  # pyright: ignore[reportPrivateUsage] - repository regression contract
    check_simulation_inputs,
)


def verify_bundle(bundle: Path) -> int:
    """Verify the archive and every member without extracting or importing PyTUFLOW."""
    manifest: dict[str, Any] = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    archive = bundle / "examples.zip"
    if hashlib.sha256(archive.read_bytes()).hexdigest() != manifest["archive_sha256"]:
        msg = "Archive checksum mismatch"
        raise ValueError(msg)
    with ZipFile(archive) as stream:
        expected = {item["path"] for item in manifest["files"]}
        if set(stream.namelist()) != expected or len(stream.namelist()) != len(expected):
            msg = "Archive member list mismatch"
            raise ValueError(msg)
        for item in manifest["files"]:
            data = stream.read(item["path"])
            if len(data) != item["size"] or hashlib.sha256(data).hexdigest() != item["sha256"]:
                msg = f"Member checksum mismatch: {item['path']}"
                raise ValueError(msg)
    return len(expected)


def extract_bundle(bundle: Path, destination: Path) -> None:
    """Extract only into a new temporary directory, enforcing physical containment."""
    # Resolve deliberately: containment must follow links, not just compare path spellings.
    boundary = destination.resolve()
    with ZipFile(bundle / "examples.zip") as archive:
        for name in archive.namelist():
            member = (destination / name).resolve()
            if not member.is_relative_to(boundary):
                msg = f"Archive member escapes extraction directory: {name}"
                raise ValueError(msg)
        archive.extractall(destination)


def discover_cases(base: Path) -> dict[Path, list[dict[str, str]]]:
    """Read literal batch commands, never execute them, and cover remaining plain TCFs."""
    cases: dict[Path, list[dict[str, str]]] = {}
    for batch in sorted((base / "runs").glob("*.bat")):
        for raw in batch.read_text(encoding="utf-8-sig").splitlines():
            line = raw.strip()
            if not line or line.lower().startswith(("set ", "rem ", "@rem ", "::", "echo ", "@echo ")):
                continue
            match = re.search(r'([^\s"/\\]+\.tcf)', line, re.IGNORECASE)
            if match is None:
                continue
            path = batch.parent / match.group(1)
            if not path.is_file():
                continue
            flags = dict(extract_flag_pairs(line))
            cases.setdefault(path, [])
            if flags not in cases[path]:
                cases[path].append(flags)
    for path in sorted((base / "runs").glob("*.tcf")):
        if path in cases or path.name.lower().startswith("create_empt"):
            continue
        if re.search(r"~[es]\d~", path.name, re.IGNORECASE):
            msg = f"Parameterised TCF has no batch-defined context: {path.name}"
            raise ValueError(msg)
        cases[path] = [{}]
    return cases


GRAPH_MODES = {"batch_graph_off": False, "batch_graph_on": True}


def comparison_modes(batch: bool, graph_compare: bool = False) -> tuple[str, ...]:
    """Select explicit diagnostic modes without changing the production defaults."""
    if graph_compare:
        if not batch:
            msg = "--graph-compare requires --batch"
            raise ValueError(msg)
        return ("eager", "deferred", *GRAPH_MODES)
    return ("eager", "deferred", "batch") if batch else ("eager", "deferred")


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _command_identity(ordinal: int, owner: Any, line: Any, name: Any, text: Any) -> tuple[Any, ...]:
    """Compare public command provenance, never UUID bytes from independent parses."""
    if (
        owner is None
        or not isinstance(line, int)
        or isinstance(line, bool)
        or not str(name).strip()
        or not str(text).strip()
    ):
        msg = "Incomplete diagnostic command provenance"
        raise ValueError(msg)
    return (ordinal, str(Path(owner).absolute()).casefold(), line, str(name), str(text))


class _Provenance:
    """Bounded per-run hashes of complete ordered public projections."""

    def __init__(self, runs: list[RunRequest]) -> None:
        self.runs = runs
        self.commands: list[tuple[int, int, str]] = []
        self.references: list[tuple[int, str]] = []
        self.next_run = 0

    def begin(self, flags: str) -> int:
        """Consume the requested ordinal even when its parser context later fails."""
        index = self.next_run
        self.next_run += 1
        if index >= len(self.runs) or flags != self.runs[index].arguments:
            msg = "Diagnostic context order or flags mismatch"
            raise ValueError(msg)
        return index

    def context(self, context: Any, flags: str, index: int, *, deferred: bool) -> None:
        inputs = tuple(context.find_input())
        commands: list[tuple[Any, ...]] = []
        references: list[tuple[int, tuple[tuple[str, str], ...]]] = []
        for ordinal, inp in enumerate(inputs):
            command = inp.command()
            commands.append(_command_identity(ordinal, command.parent, inp.line_number, inp.lhs, command.original_text))
            if deferred:
                references.extend((ordinal, self.reference(ref)) for ref in inp.file_references)
        self.commands.append((self.runs[index].index, len(commands), _digest((flags, commands))))
        if deferred:
            self.references.append((self.runs[index].index, _digest(references)))

    @staticmethod
    def reference(ref: Any) -> tuple[tuple[str, str], ...]:
        # FileReference is a public immutable dataclass. Retain every field,
        # including scan requirements, so a partial projection cannot pass.
        return tuple((item.name, str(getattr(ref, item.name))) for item in fields(ref))

    def batch(self, result: Any, *, required: bool) -> None:
        for run, case in zip(self.runs, result.runs, strict=True):
            if case.error is not None:
                continue  # BatchModelView retains and reports the per-run failure.
            ordered = getattr(case, "ordered_inputs", None)
            if ordered is None:
                if required:
                    msg = "Graph comparison requires complete ordered-input provenance"
                    raise ValueError(msg)
                return  # Preserve the established legacy batch adapter contract.
            commands = [
                _command_identity(
                    item.command_index, item.owner_control_file, item.line_number, item.command_name, item.command_text
                )
                for item in ordered
            ]
            self.commands.append((run.index, len(commands), _digest((case.flags, commands))))
            references = [(item.command_index, self.reference(item.reference)) for item in case.input_references]
            self.references.append((run.index, _digest(references)))
            by_uuid = {item.input_uuid: item for item in ordered}
            for activation in case.database_activations:
                command = by_uuid.get(activation.input_uuid)
                if command is None or (
                    activation.sequence != command.command_index
                    or activation.owner_control_file != command.owner_control_file
                    or activation.line_number != command.line_number
                ):
                    msg = "Batch database activation command/provenance mismatch"
                    raise ValueError(msg)


class EagerModel:
    """Expose eager mode without advertising deferred support to the consumer."""

    def __init__(self, model: Any, provenance: _Provenance) -> None:
        self.model = model
        self.provenance = provenance

    def context(self, flags: str) -> Any:
        index = self.provenance.begin(flags)
        context = self.model.context(flags, file_resolution="eager")
        self.provenance.context(context, flags, index, deferred=False)
        return context


class DeferredModel:
    """Use the public upstream deferred API without monkeypatching its internals."""

    def __init__(self, model: Any, provenance: _Provenance) -> None:
        self.model = model
        self.provenance = provenance

    def context(self, flags: str, *, file_resolution: str | None = None) -> Any:
        index = self.provenance.begin(flags)
        context = self.model.context(flags, file_resolution=file_resolution or "deferred")
        self.provenance.context(context, flags, index, deferred=True)
        return context


class BatchDiagnosticModel:
    """Forward exactly one ordered public batch call with explicit graph policy."""

    def __init__(self, model: Any, mode: str, provenance: _Provenance) -> None:
        self.model = model
        self.mode = mode
        self.provenance = provenance

    def collect_batch_dependencies(self, flags: tuple[str, ...]) -> Any:
        batch = getattr(self.model, "collect_batch_dependencies", None)
        if not callable(batch):
            msg = "Diagnostic batch mode requires public collect_batch_dependencies()"
            raise ValueError(msg)
        # No fallback/monkeypatch: unsupported keyword APIs must fail visibly.
        result = (
            batch(flags, cache_scopes=True, compile_tree=GRAPH_MODES[self.mode])
            if self.mode in GRAPH_MODES
            else batch(flags)
        )
        self.provenance.batch(result, required=self.mode in GRAPH_MODES)
        return result


def collect(  # noqa: C901 - read-only materialisation and invocation-local identity/layer caches
    tcf: Path, runs: list[RunRequest], mode: str, factory: Any
) -> tuple[set[tuple[Any, ...]], dict[str, Any]]:
    """Compare complete supported references; refuse partial attribute parity."""
    if mode not in {"eager", "deferred", "batch", *GRAPH_MODES}:
        msg = f"Unknown diagnostic mode: {mode}"
        raise ValueError(msg)
    report = PreflightReport(len(runs))
    provenance = _Provenance(runs)
    model: Any
    if mode == "batch" or mode in GRAPH_MODES:
        model = BatchModelView(
            BatchDiagnosticModel(factory(str(tcf)), mode, provenance), tuple(run.arguments for run in runs)
        )
    else:
        model = (EagerModel if mode == "eager" else DeferredModel)(factory(str(tcf)), provenance)
    references, _ = _collect_references(tcf, runs, model, report)
    if mode == "batch" or mode in GRAPH_MODES:
        model.assert_consumed()
    patterns = {ref.path for ref in references if any(char in ref.path.name for char in "*?[")}
    expanded, failures, _ = expand_file_patterns(patterns)
    if failures:
        msg = "Wildcard materialisation failed"
        raise ValueError(msg)
    # Share the production attribute materialiser: both Shapefile and
    # GeoPackage secondary references retain the originating run and command.
    # Unsupported attribute forms still become collection issues and cannot
    # be accepted as parity merely because a partial file set matches.
    materialised = [replace(ref, path=path) for ref in references for path in expanded.get(ref.path, (ref.path,))]
    references = _materialise_attribute_references(materialised, report)
    keys: set[tuple[Any, ...]] = set()
    unique: dict[Path, set[str]] = {}
    inventory_spellings: dict[Path, Path] = {}
    physical_by_spelling: dict[str, Path] = {}
    layer_names_by_spelling: dict[str, tuple[str, ...]] = {}
    for ref in references:
        for path in expanded.get(ref.path, (ref.path,)):
            path_spelling = str(path)
            layers = [ref.layer]
            if ref.layer and (any(char in ref.layer for char in "*?[") or ref.layer.lower() == "all"):
                names = layer_names_by_spelling.get(path_spelling)
                if names is None:
                    # Keep the original SQLite spelling, cache only a complete
                    # successful enumeration, and close the read-only handle.
                    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
                        names = tuple(str(row[0]) for row in connection.execute("SELECT table_name FROM gpkg_contents"))
                    layer_names_by_spelling[path_spelling] = names
                layers = [
                    name
                    for name in names
                    if ref.layer.lower() == "all" or fnmatch.fnmatchcase(name.lower(), ref.layer.lower())
                ]
            # Resolve deliberately to compare physical aliases and linked paths,
            # once per exact spelling in this invocation. String keys retain
            # case distinctions that Windows Path equality would collapse.
            # Keep containment checks separate; failed resolutions still raise.
            physical = physical_by_spelling.get(path_spelling)
            if physical is None:
                physical = path.resolve()
                physical_by_spelling[path_spelling] = physical
            # Physical identity deduplicates aliases, but I/O must retain the
            # supplied drive spelling. Resolving T: to UNC here produces SQLite
            # file://host URIs rejected by Windows builds without URI authority.
            spelling = inventory_spellings.setdefault(physical, path.absolute())
            unique.setdefault(spelling, set()).update(layer for layer in layers if layer)
            owner = ref.origin
            if owner.casefold().startswith("gis attribute: "):
                owner = owner[len("GIS attribute: ") :]
            origin = " ".join(owner.split("==", 1)[0].casefold().split())
            for layer in layers:
                keys.add((ref.run.index, ref.run.description, origin, str(physical).casefold(), layer.casefold()))
    inventory = check_unique_dependencies(unique)
    stats: dict[str, Any] = {
        "collection_issues": len(report.issues),
        "collection_reasons": dict(Counter(issue.detail for issue in report.issues)),
        "inventory_issues": sum(item.status != "present" for item in inventory.files.values())
        + sum(item.status != "present" for item in inventory.layers.values()),
        "entries": len(keys),
        "files": len(unique),
        "command_provenance": provenance.commands,
        "reference_provenance": provenance.references,
        "provenance_complete": len(provenance.commands) == len(runs),
    }
    return keys, stats


def compare_collections(
    row: dict[str, Any], identities: dict[str, set[tuple[Any, ...]]], modes: tuple[str, ...]
) -> dict[str, Any]:
    """Fail closed for missing modes, findings, identity or public-provenance differences."""
    differences: dict[str, Any] = {}
    passed = bool(modes) and "eager" in modes and set(identities) == set(modes)
    graph = any(mode in GRAPH_MODES for mode in modes)
    pairs = [("eager", mode, "" if mode == "deferred" else f"{mode}_") for mode in modes if mode != "eager"]
    if graph:
        pairs.append(("batch_graph_off", "batch_graph_on", "graph_"))
    for baseline, mode, prefix in pairs:
        if baseline not in identities or mode not in identities:
            passed = False
            differences[f"{baseline}:{mode}"] = {"error": "Missing comparison mode"}
            continue
        missing = identities[baseline] - identities[mode]
        extra = identities[mode] - identities[baseline]
        row.update({f"{prefix}missing": len(missing), f"{prefix}extra": len(extra)})
        left, right = row.get(baseline, {}), row.get(mode, {})
        lineage_equal = left.get("command_provenance") == right.get("command_provenance")
        # Legacy batch builds lack ordered descriptors. New graph acceptance
        # always requires complete provenance in all four modes.
        compare_lineage = graph or (left.get("provenance_complete") and right.get("provenance_complete"))
        row[f"{prefix}provenance_equal"] = bool(lineage_equal) if compare_lineage else None
        differences[f"{baseline}:{mode}"] = {
            "missing": sorted(missing),
            "extra": sorted(extra),
            "provenance_equal": row[f"{prefix}provenance_equal"],
        }
        passed = passed and not missing and not extra and (not compare_lineage or lineage_equal)
    if graph:
        reference_equal = all(
            row.get(mode, {}).get("reference_provenance") is not None
            and row.get("deferred", {}).get("reference_provenance") is not None
            and row.get(mode, {}).get("reference_provenance") == row.get("deferred", {}).get("reference_provenance")
            for mode in GRAPH_MODES
        )
        row["reference_provenance_equal"] = reference_equal
        passed = passed and reference_equal
    for mode in modes:
        stats = row.get(mode, {})
        passed = passed and (
            not stats.get("error")
            and stats.get("collection_issues") == 0
            and stats.get("inventory_issues") == 0
            and (
                not graph or (stats.get("provenance_complete") is True and stats.get("command_provenance") is not None)
            )
        )
    row["passed"] = bool(passed)
    return differences


def main() -> int:  # noqa: C901 - multi-mode regression results and fail-closed acceptance
    """Verify or replay the public archive; default reports use a temporary directory."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument(
        "--pytuflow-checkout", type=Path, help="Explicit source checkout to import instead of installed PyTUFLOW"
    )
    parser.add_argument("--tcf", action="append", help="TCF filename to select; repeat for multiple cases")
    parser.add_argument("--batch", action="store_true", help="Include experimental batch source parity checks")
    parser.add_argument("--graph-compare", action="store_true", help="Compare cached graph-off/on; requires --batch")
    parser.add_argument("--output", type=Path, help="Report directory; default is a new temporary directory")
    options = parser.parse_args()
    try:
        modes = comparison_modes(options.batch, options.graph_compare)
    except ValueError as exc:
        parser.error(str(exc))
    bundle = Path(__file__).absolute().parents[2] / "test_data" / "tuflow" / "example_input_preflight"
    print(f"Verified {verify_bundle(bundle)} archive members")
    if options.verify_only:
        return 0
    if options.pytuflow_checkout:
        # Explicit dependency-checkout selection for this diagnostic harness, not wrapper import repair.
        sys.path.insert(0, str(options.pytuflow_checkout.absolute()))
    upstream = importlib.import_module("pytuflow")
    factory: Any = upstream.TCF
    output = options.output.absolute() if options.output else Path(tempfile.mkdtemp(prefix="tuflow-example-reports-"))
    output.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    # PyTUFLOW/GDAL may keep native handles alive until garbage collection.
    # On Windows, tolerate the rare still-locked temporary example file;
    # all original inputs and diagnostic reports are outside this folder.
    with tempfile.TemporaryDirectory(prefix="tuflow-example-inputs-", ignore_cleanup_errors=True) as temporary:
        base = Path(temporary)
        extract_bundle(bundle, base)
        cases = discover_cases(base)
        if options.tcf:
            selected = set(options.tcf)
            cases = {path: flags for path, flags in cases.items() if path.name in selected}
            if selected != {path.name for path in cases}:
                msg = "A selected TCF does not exist in the archive"
                raise ValueError(msg)
        print(f"PyTUFLOW: {upstream.__file__}; {len(cases)} TCFs; {sum(map(len, cases.values()))} runs")
        for tcf, combinations in cases.items():
            runs = [RunRequest(index, flags) for index, flags in enumerate(combinations, 1)]
            row: dict[str, Any] = {
                "tcf": tcf.name,
                "runs": len(runs),
                "mode_order": modes,
                "public_preflight": {"timed": False, "batch_graph_policy": "parser_default", "graph_comparison": False},
            }
            identities: dict[str, set[tuple[Any, ...]]] = {}
            for mode in modes:
                try:
                    identities[mode], row[mode] = collect(tcf, runs, mode, factory)
                except Exception as exc:  # noqa: BLE001 - record each failure and continue the diagnostic sweep
                    row[mode] = {"error": str(exc)}
            row["differences"] = compare_collections(row, identities, modes)
            # Retain historical report fields for existing archive consumers.
            if "eager:deferred" in row["differences"]:
                comparison = row["differences"]["eager:deferred"]
                row["eager_only_identities"] = comparison.get("missing", [])
                row["deferred_only_identities"] = comparison.get("extra", [])
            if "eager:batch" in row["differences"]:
                comparison = row["differences"]["eager:batch"]
                row["batch_eager_only_identities"] = comparison.get("missing", [])
                row["batch_extra_identities"] = comparison.get("extra", [])
            try:
                report = check_simulation_inputs(tcf, runs)
                report.write_csv(output / f"{tcf.stem}-issues.csv")
                row["preflight_passed"] = report.passed
                row["preflight_reasons"] = dict(Counter(issue.detail for issue in report.issues))
                if options.batch:
                    batch_report = check_simulation_inputs(tcf, runs, context_mode="batch")
                    batch_report.write_csv(output / f"{tcf.stem}-batch-issues.csv")
                    row["batch_preflight_passed"] = (
                        batch_report.passed == report.passed
                        and set(batch_report.issues) == set(report.issues)
                        and batch_report.checked_paths == report.checked_paths
                    )
                    row["batch_preflight_issue_count"] = len(batch_report.issues)
            except Exception as exc:  # noqa: BLE001 - preserve parser failures as failed cases
                row["preflight_passed"] = False
                row["preflight_error"] = str(exc)
            row["accepted_parity"] = bool(
                row["passed"]
                and row["preflight_passed"]
                and (not options.batch or row.get("batch_preflight_passed") is True)
            )
            rows.append(row)
            (output / "results.json").write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
            print(f"{tcf.name}: {'PASS' if row['accepted_parity'] else 'UNRESOLVED'}", flush=True)
        # PyTUFLOW/pyogrio may hold GDAL objects until their finalizers run.
        gc.collect()
    print(f"Reports: {output}")
    return 0 if rows and all(row["accepted_parity"] for row in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
