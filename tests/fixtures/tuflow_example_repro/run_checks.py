"""Temporary, read-only example-data reproduction harness; remove before PR merge."""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import importlib
import json
import re
import sqlite3
import sys
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any
from zipfile import ZipFile

from ryan_library.functions.tuflow.dependency_index import check_unique_dependencies, expand_file_patterns
from ryan_library.functions.tuflow.run_flags import extract_flag_pairs
from ryan_library.orchestrators.tuflow.input_preflight import (
    PreflightReport,
    RunRequest,
    _collect_references,  # pyright: ignore[reportPrivateUsage] - temporary in-repository diagnostic
    _FileReference,  # pyright: ignore[reportPrivateUsage] - temporary in-repository diagnostic
    _materialise_attribute_references,  # pyright: ignore[reportPrivateUsage] - temporary diagnostic
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


class EagerModel:
    """Expose eager mode without advertising deferred support to the consumer."""

    def __init__(self, model: Any) -> None:
        self.model = model

    def context(self, flags: str) -> Any:
        return self.model.context(flags, file_resolution="eager")


class DeferredModel:
    """Use the public upstream deferred API without monkeypatching its internals."""

    def __init__(self, model: Any) -> None:
        self.model = model

    def context(self, flags: str, *, file_resolution: str | None = None) -> Any:
        return self.model.context(flags, file_resolution=file_resolution or "deferred")


def collect(tcf: Path, runs: list[RunRequest], mode: str, factory: Any) -> tuple[set[tuple[Any, ...]], dict[str, Any]]:
    """Compare complete supported references; refuse partial attribute parity."""
    report = PreflightReport(len(runs))
    model: Any = (EagerModel if mode == "eager" else DeferredModel)(factory(str(tcf)))
    references, _ = _collect_references(tcf, runs, model, report)
    patterns = {ref.path for ref in references if any(char in ref.path.name for char in "*?[")}
    expanded, failures, _ = expand_file_patterns(patterns)
    if failures:
        msg = "Wildcard materialisation failed"
        raise ValueError(msg)
    # Share the production attribute materialiser: both Shapefile and
    # GeoPackage secondary references retain the originating run and command.
    # Unsupported attribute forms still become collection issues and cannot
    # be accepted as parity merely because a partial file set matches.
    materialised = [
        _FileReference(ref.run, ref.origin, path, ref.layer, ref.scan_attributes, ref.database_kind)
        for ref in references
        for path in expanded.get(ref.path, (ref.path,))
    ]
    references = _materialise_attribute_references(materialised, report)
    keys: set[tuple[Any, ...]] = set()
    unique: dict[Path, set[str]] = {}
    for ref in references:
        for path in expanded.get(ref.path, (ref.path,)):
            layers = [ref.layer]
            if ref.layer and (any(char in ref.layer for char in "*?[") or ref.layer.lower() == "all"):
                with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as connection:
                    names = [str(row[0]) for row in connection.execute("SELECT table_name FROM gpkg_contents")]
                layers = [
                    name
                    for name in names
                    if ref.layer.lower() == "all" or fnmatch.fnmatchcase(name.lower(), ref.layer.lower())
                ]
            # Resolve deliberately to compare physical files across path aliases and linked paths.
            physical = path.resolve()
            unique.setdefault(physical, set()).update(layer for layer in layers if layer)
            origin = " ".join(ref.origin.split("==", 1)[0].casefold().split())
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
    }
    return keys, stats


def main() -> int:
    """Verify or run the temporary fixture, writing reports outside the checkout."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument(
        "--pytuflow-checkout", type=Path, help="Explicit source checkout to import instead of installed PyTUFLOW"
    )
    parser.add_argument("--tcf", action="append", help="TCF filename to select; repeat for multiple cases")
    parser.add_argument("--output", type=Path, help="Report directory; default is a new temporary directory")
    options = parser.parse_args()
    bundle = Path(__file__).absolute().parent
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
    with tempfile.TemporaryDirectory(prefix="tuflow-example-inputs-") as temporary:
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
            row: dict[str, Any] = {"tcf": tcf.name, "runs": len(runs)}
            identities: dict[str, set[tuple[Any, ...]]] = {}
            for mode in ("eager", "deferred"):
                try:
                    identities[mode], row[mode] = collect(tcf, runs, mode, factory)
                except Exception as exc:  # noqa: BLE001 - record each failure and continue the diagnostic sweep
                    row[mode] = {"error": str(exc)}
            if len(identities) == 2:
                row["missing"] = len(identities["eager"] - identities["deferred"])
                row["extra"] = len(identities["deferred"] - identities["eager"])
            try:
                report = check_simulation_inputs(tcf, runs)
                report.write_csv(output / f"{tcf.stem}-issues.csv")
                row["preflight_passed"] = report.passed
                row["preflight_reasons"] = dict(Counter(issue.detail for issue in report.issues))
            except Exception as exc:  # noqa: BLE001 - preserve parser failures as failed cases
                row["preflight_passed"] = False
                row["preflight_error"] = str(exc)
            row["accepted_parity"] = (
                row["preflight_passed"]
                and row.get("missing") == 0
                and row.get("extra") == 0
                and all(
                    not any(row[mode].get(key) for key in ("error", "collection_issues", "inventory_issues"))
                    for mode in ("eager", "deferred")
                )
            )
            rows.append(row)
            (output / "results.json").write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
            print(f"{tcf.name}: {'PASS' if row['accepted_parity'] else 'UNRESOLVED'}", flush=True)
    print(f"Reports: {output}")
    return 0 if rows and all(row["accepted_parity"] for row in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
