"""Repository regression harness for read-only TUFLOW dependency parity."""

from __future__ import annotations

import argparse
import fnmatch
import gc
import hashlib
import importlib
import json
import os
import re
import sqlite3
import sys
import tempfile
from collections import Counter
from contextlib import closing
from dataclasses import replace
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


def _physical_identity_key(path: Path) -> str:
    """Retain Windows identity encoding but distinguish POSIX case-only files.

    Physical path alias resolution is done separately with Path.resolve().
    Do not conflate two distinct Linux inode paths through Unicode casefold.
    """
    spelling = str(path)
    return spelling.casefold() if os.name == "nt" else spelling


# Explicitly documented archive-internal source -> TCF-requested POSIX spelling.
# Do not alter the immutable ZIP. Linux examples with these casing errors are
# invalid in strict -cs0 mode until the extracted TEMPORARY test copy is repaired.
_LINUX_EXAMPLE_CASE_ALIASES: tuple[tuple[str, str], ...] = (
    ("model/Materials.csv", "model/materials.csv"),
    ("model/gis/2d_rf_EG03_007_R.shp", "model/gis/2d_RF_EG03_007_R.shp"),
    ("model/gis/1d_WLL_EG14_001_L.shp", "model/gis/1d_wll_EG14_001_L.shp"),
    ("model/gis/1d_WLLp_EG14_001_P.shp", "model/gis/1d_wllp_EG14_001_P.shp"),
    ("model/gis/2d_qnl_E13_001_R.shp", "model/gis/2d_qnl_E13_001_R.SHP"),
    ("model/gis/2d_rf_EG03_014_R.shp", "model/gis/2d_RF_EG03_014_R.shp"),
    ("model/gis/2d_qnl_E13_003_R.shp", "model/gis/2d_qnl_E13_003_R.SHP"),
    ("model/gis/2d_qnl_EG15_003_R.shp", "model/gis/2d_qnl_EG15_003_R.SHP"),
)


def _link_shapefile_case_sidecars(source: Path, alias: Path, base: Path) -> tuple[str, ...]:
    """Link only the known Shapefile companion extensions under an alias stem."""
    if source.suffix.casefold() != ".shp":
        return ()
    created: list[str] = []
    for part in sorted(source.parent.glob(f"{source.stem}.*")):
        if part == source or part.suffix.casefold() not in {".shx", ".dbf", ".prj", ".cpg"}:
            continue
        if not part.is_file() or part.is_symlink():
            message = f"Unexpected Shapefile sidecar: {part.name}"
            raise ValueError(message)
        destination = alias.with_name(f"{alias.stem}{part.suffix}")
        if destination == part:
            continue
        if destination.exists() or destination.is_symlink():
            message = f"Conflicting example sidecar alias: {destination.name}"
            raise ValueError(message)
        destination.symlink_to(part.name)
        created.append(destination.relative_to(base).as_posix())
    return tuple(created)


def prepare_linux_example_case_overlay(base: Path) -> tuple[str, ...]:
    """Add bounded, explicit aliases only in an already-extracted public fixture.

    The original ZIP and its manifest are untouched; this is a separate
    *case-corrected* parity campaign, never evidence that the raw archive is
    valid under TUFLOW's strict Linux -cs0 filename policy.
    """
    if sys.platform != "linux":
        message = "Linux example case overlay requires a native Linux filesystem"
        raise ValueError(message)
    created: list[str] = []
    for source_rel, alias_rel in _LINUX_EXAMPLE_CASE_ALIASES:
        source = base / source_rel
        alias = base / alias_rel
        if not source.is_file() or source.is_symlink():
            message = f"Expected original archive file is missing: {source_rel}"
            raise ValueError(message)
        if alias.parent != source.parent or alias.exists() or alias.is_symlink():
            message = f"Unsafe or pre-existing example alias: {alias_rel}"
            raise ValueError(message)
        alias.symlink_to(source.name)
        created.append(alias_rel)
        created.extend(_link_shapefile_case_sidecars(source, alias, base))
    return tuple(created)


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


def collect(  # noqa: C901 - read-only materialisation and invocation-local identity/layer caches
    tcf: Path, runs: list[RunRequest], mode: str, factory: Any
) -> tuple[set[tuple[Any, ...]], dict[str, Any]]:
    """Compare complete supported references; refuse partial attribute parity."""
    report = PreflightReport(len(runs))
    model: Any
    if mode == "batch":
        model = BatchModelView(factory(str(tcf)), tuple(run.arguments for run in runs))
    else:
        model = (EagerModel if mode == "eager" else DeferredModel)(factory(str(tcf)))
    references, _ = _collect_references(tcf, runs, model, report)
    if mode == "batch":
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
                keys.add(
                    (ref.run.index, ref.run.description, origin, _physical_identity_key(physical), layer.casefold())
                )
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


def main() -> int:  # noqa: C901 - multi-mode regression results and fail-closed acceptance
    """Verify or replay the public archive; default reports use a temporary directory."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument(
        "--pytuflow-checkout", type=Path, help="Explicit source checkout to import instead of installed PyTUFLOW"
    )
    parser.add_argument("--tcf", action="append", help="TCF filename to select; repeat for multiple cases")
    parser.add_argument("--batch", action="store_true", help="Include experimental batch source parity checks")
    parser.add_argument(
        "--linux-case-overlay",
        action="store_true",
        help="Add documented case-only symlinks in a temporary Linux example copy (not the original archive)",
    )
    parser.add_argument("--output", type=Path, help="Report directory; default is a new temporary directory")
    options = parser.parse_args()
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
        if options.linux_case_overlay:
            case_aliases = prepare_linux_example_case_overlay(base)
            (output / "case_overlay.json").write_text(
                json.dumps({"mode": "temporary-linux-case-corrected", "aliases": case_aliases}, indent=2) + "\n",
                encoding="utf-8",
            )
            print(f"Linux case-corrected temporary fixture: {len(case_aliases)} declared aliases", flush=True)
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
            modes = ("eager", "deferred", "batch") if options.batch else ("eager", "deferred")
            for mode in modes:
                try:
                    identities[mode], row[mode] = collect(tcf, runs, mode, factory)
                except Exception as exc:  # noqa: BLE001 - record each failure and continue the diagnostic sweep
                    row[mode] = {"error": str(exc)}
            # The three-mode run still requires the original eager versus
            # individual-deferred comparison. Comparing len == 2 here would
            # skip the baseline exactly when all three modes succeeded.
            if "eager" in identities and "deferred" in identities:
                eager_only = identities["eager"] - identities["deferred"]
                deferred_only = identities["deferred"] - identities["eager"]
                row["missing"] = len(eager_only)
                row["extra"] = len(deferred_only)
                # Store complete identity mismatches so an apparent pass with
                # equal counts cannot hide wrong owning commands or GIS layers.
                row["eager_only_identities"] = [list(value) for value in sorted(eager_only)]
                row["deferred_only_identities"] = [list(value) for value in sorted(deferred_only)]
            if options.batch and "batch" in identities and "eager" in identities:
                eager_only = identities["eager"] - identities["batch"]
                batch_only = identities["batch"] - identities["eager"]
                row["batch_missing"] = len(eager_only)
                row["batch_extra"] = len(batch_only)
                row["batch_eager_only_identities"] = [list(value) for value in sorted(eager_only)]
                row["batch_extra_identities"] = [list(value) for value in sorted(batch_only)]
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
            row["accepted_parity"] = (
                row["preflight_passed"]
                and (
                    not options.batch
                    or (
                        row.get("batch_missing") == 0
                        and row.get("batch_extra") == 0
                        and row.get("batch_preflight_passed") is True
                    )
                )
                and row.get("missing") == 0
                and row.get("extra") == 0
                and all(
                    not any(row[mode].get(key) for key in ("error", "collection_issues", "inventory_issues"))
                    for mode in modes
                )
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
