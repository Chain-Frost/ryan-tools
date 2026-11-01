"""Smoke-test ryan-tools and separately installed PyPI dependencies."""

from __future__ import annotations

import argparse
import csv
import importlib.metadata
import importlib.resources
import importlib.util
import os
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory

import culvert_solver
import numpy as np
import run_hy8

import ryan_library
import vendor
import vendor.simil as vendor_simil
from ryan_library.functions.geometry import find_similarity_transform
from ryan_library.functions.path_stuff import sanitize_windows_filename
from ryan_library.mcp.installed_scripts import available_wrappers, copy_wrapper
from ryan_library.mcp.registry import WorkflowRegistry
from ryan_library.mcp.server import REGISTRY as SERVER_REGISTRY, list_workflows


def _require_installed_below(module_file: str | None, expected_root: Path, package: str) -> None:
    """Require an imported package to resolve below the isolated install root."""
    if module_file is None or not Path(module_file).resolve().is_relative_to(expected_root):
        msg = f"{package} resolved outside the expected install root: {module_file}"
        raise RuntimeError(msg)


def _check_installed_scripts() -> None:
    """Require every catalogued script to resolve without a checkout."""
    registry = WorkflowRegistry(discover_repository=False, configured_profile="privileged")
    if registry.repository_root is not None or registry.scripts_directory is None:
        msg = "Installed wheel cannot locate its sibling ryan-scripts directory without a checkout."
        raise RuntimeError(msg)
    for workflow in registry.list_workflows(include_unavailable=True)["workflows"]:
        if workflow["execution_kind"] == "script" and not workflow["available"]:
            msg = f"Installed wheel is missing catalogued script {workflow['id']}."
            raise RuntimeError(msg)


def _check_installed_keyword_discovery() -> None:
    """Prove discovery using only packaged catalogues and the actual installed wheel."""
    registry = WorkflowRegistry(discover_repository=False, configured_profile="privileged")
    for domain, query, workflow_id in (
        ("gdal", "CrOp", "clip_rasters_to_polygon"),
        ("gdal", "cropping", "clip_rasters_to_polygon"),
        ("gdal", "pyramids", "build_external_overviews"),
        ("tuflow", "logs", "tuflow_log_summary"),
    ):
        result = registry.list_workflows(domain=domain, query=query, include_unavailable=True)
        ids = [item["id"] for item in result["workflows"]]
        if workflow_id not in ids or len(ids) != len(set(ids)):
            msg = f"Installed workflow discovery failed for {domain}/{query}: {ids}"
            raise RuntimeError(msg)
        detail = registry.get_workflow(workflow_id)
        if query.casefold() not in {keyword.casefold() for keyword in detail["keywords"]}:
            msg = f"Installed keywords missing for {workflow_id}: {detail['keywords']}"
            raise RuntimeError(msg)

    if SERVER_REGISTRY.repository_root is not None:
        msg = "Installed MCP server unexpectedly discovered the source checkout."
        raise RuntimeError(msg)
    full = list_workflows(include_unavailable=True, limit=25)
    overview = list_workflows(overview=True, include_unavailable=True, limit=1)
    if (
        overview["workflow_count"] <= len(full["workflows"])
        or overview["domain_count"] != len(overview["domains"])
        or overview["returned_count"] != 1
        or "workflows" in overview
    ):
        msg = "Installed MCP overview omitted full totals or leaked workflow payloads."
        raise RuntimeError(msg)
    domains = []
    for offset in range(overview["domain_count"]):
        page = list_workflows(overview=True, include_unavailable=True, limit=1, offset=offset)
        domains.extend(page["domain_overview"])
    if sum(row["workflow_count"] for row in domains) != overview["workflow_count"]:
        msg = "Installed MCP overview domain counts do not sum to the visible catalogue."
        raise RuntimeError(msg)
    if any(len(row["examples"]) > 3 for row in domains):
        msg = "Installed MCP overview returned more than three examples per domain."
        raise RuntimeError(msg)
    crop = list_workflows(domain="gdal", query="crop", include_unavailable=True)
    if "clip_rasters_to_polygon" not in {item["id"] for item in crop["workflows"]}:
        msg = "Installed MCP tool failed keyword discovery for raster crop."
        raise RuntimeError(msg)
    if list_workflows(overview=True, query="never-matches-xyz", include_unavailable=True)["domain_count"] != 0:
        msg = "Installed MCP overview did not return an empty page for unknown terms."
        raise RuntimeError(msg)
    print("Wheel-only MCP keyword search and domain overview passed.")


def _check_installed_wrapper_copy() -> None:
    """Check wrapper discovery and a byte-for-byte copy from the installed wheel."""
    if "culvert.py" not in available_wrappers():
        msg = "Installed wheel cannot enumerate its curated wrappers."
        raise RuntimeError(msg)
    with TemporaryDirectory(prefix="ryan-tools-script-copy-") as directory:
        target = copy_wrapper("culvert.py", Path(directory))
        source = Path(ryan_library.__file__).absolute().parent.parent / "ryan-scripts" / "culvert.py"
        if target.read_bytes() != source.read_bytes():
            msg = "Installed wrapper copy differs from the packaged source."
            raise RuntimeError(msg)


def _run_installed_wrapper(
    relative: str, work: Path, expected_root: Path, *arguments: str
) -> subprocess.CompletedProcess[str]:
    """Copy from the wheel and execute outside the checkout, importing only its installed library."""
    script = copy_wrapper(relative, work, overwrite=True)
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(expected_root)
    environment["PYTHONNOUSERSITE"] = "1"
    environment.pop("RYAN_TOOLS_REPOSITORY_ROOT", None)
    return subprocess.run(
        [sys.executable, str(script), *arguments],
        cwd=work,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=60,
        stdin=subprocess.DEVNULL,
    )


def _check_installed_po_labels(expected_root: Path, work: Path) -> None:
    """Exercise a copied wheel's PO label CLI with disposable SQLite records."""
    # Exercise a pure-stdlib GIS-labelled wrapper from the installed wheel,
    # without relying on a source checkout or proprietary project data.
    # This is deliberately a SQLite/GPKG-like fixture, not GDAL acceptance.
    po_source = work / "2d_po_wheel_café.gpkg"
    with closing(sqlite3.connect(po_source)) as connection:
        connection.execute("CREATE TABLE gpkg_contents (table_name TEXT, data_type TEXT)")
        connection.execute("INSERT INTO gpkg_contents VALUES ('2d_po_wheel', 'features')")
        connection.execute('CREATE TABLE "2d_po_wheel" (fid INTEGER PRIMARY KEY, Label TEXT, Comment TEXT)')
        connection.execute(
            'INSERT INTO "2d_po_wheel" VALUES (?, ?, ?)',
            (1, "PO_Installed", "wheel fixture"),
        )
        connection.commit()
    unchanged = po_source.read_bytes()
    po_result = _run_installed_wrapper(
        "TUFLOW-python/po_and_timeseries/generate_2d_po_label_list.py",
        work,
        expected_root,
        str(po_source),
        "--layer",
        "2d_po_wheel",
    )
    po_output = po_source.with_suffix(".txt")
    if po_result.returncode != 0 or not po_output.is_file():
        msg = f"Wheel-installed 2d_po label export failed: {(po_result.stdout + po_result.stderr)[-1000:]}"
        raise RuntimeError(msg)
    text_output = po_output.read_text(encoding="utf-8")
    if "'PO_Installed'," not in text_output or "# wheel fixture" not in text_output:
        msg = "Wheel-installed 2d_po label export had unexpected records."
        raise RuntimeError(msg)
    if po_source.read_bytes() != unchanged:
        msg = "Wheel-installed 2d_po label export modified its input."
        raise RuntimeError(msg)


def _check_installed_tuflow_wrappers(expected_root: Path, work: Path) -> None:
    """Exercise wheel-only TUFLOW wrappers without a licensed solver or model."""
    for relative in (
        "TUFLOW-python/model_management/check_tuflow_inputs.py",
        "TUFLOW-python/model_management/run_tuflow_simulations.py",
    ):
        help_result = _run_installed_wrapper(relative, work, expected_root, "--help")
        if help_result.returncode != 0 or "usage:" not in help_result.stdout.lower():
            msg = f"Wheel-installed {relative} failed --help: {(help_result.stdout + help_result.stderr)[-900:]}"
            raise RuntimeError(msg)

    command_file = work / "tuflow_model_selections.txt"
    command_file.write_text(
        'TUFLOW.exe -e1 Synthetic "C:\\models\\Missing.tcf"\n',
        encoding="utf-8",
    )
    original = command_file.read_bytes()
    result = _run_installed_wrapper(
        "TUFLOW-python/model_management/run_tuflow_simulations.py",
        work,
        expected_root,
        "--mode",
        "textfiles",
        "--input-file",
        str(command_file),
        "--no-tcf",
        "--solver-exe",
        r"C:\not-installed\TUFLOW_iSP_w64.exe",
        "--export-only",
        "--no-pause",
    )
    if result.returncode != 0:
        msg = f"Wheel-installed TUFLOW runner's safe export-only mode failed: {(result.stdout + result.stderr)[-1600:]}"
        raise RuntimeError(msg)
    export = work / "run_tuflow_simulations.txt"
    history = work / "run_tuflow_simulations_runs.csv"
    if not export.is_file() or history.exists() or command_file.read_bytes() != original:
        msg = "Wheel-installed TUFLOW export-only changed input, failed to export TXT or created execution history."
        raise RuntimeError(msg)
    if not export.read_text(encoding="utf-8").strip():
        msg = "Wheel-installed TUFLOW runner exported empty command text."
        raise RuntimeError(msg)


def _check_installed_wrapper_behaviour(expected_root: Path) -> None:
    """Run nondestructive CLI/output/exit checks on wrappers copied from the *installed wheel*."""
    with TemporaryDirectory(prefix="ryan-tools-wheel-behaviour-") as temporary:
        work = Path(temporary)
        input_dir = work / "input"
        input_dir.mkdir()
        (input_dir / "ABC-001_Rev2.txt").write_text("one", encoding="utf-8")
        (input_dir / "XYZ-002_Rev1.txt").write_text("two", encoding="utf-8")
        output_dir = work / "output"

        for wrapper in (
            "file-management-python/file_inventory.py",
            "file-management-python/list_files_recursive.py",
        ):
            help_result = _run_installed_wrapper(wrapper, work, expected_root, "--help")
            if help_result.returncode != 0 or "usage:" not in help_result.stdout.lower():
                msg = f"Wheel-installed {wrapper} failed --help: {(help_result.stdout + help_result.stderr)[-700:]}"
                raise RuntimeError(msg)

        inventory = _run_installed_wrapper(
            "file-management-python/file_inventory.py",
            work,
            expected_root,
            str(input_dir),
            "--output-directory",
            str(output_dir),
            "--basename",
            "wheel-inventory",
            "--no-excel",
            "--no-pause",
        )
        if inventory.returncode != 0:
            msg = (
                f"Wheel-installed file_inventory.py CSV export failed: {(inventory.stdout + inventory.stderr)[-1000:]}"
            )
            raise RuntimeError(msg)
        exported = output_dir / "wheel-inventory.csv"
        if not exported.is_file() or (output_dir / "wheel-inventory.xlsx").exists():
            msg = "Wheel-installed file_inventory.py did not honour output/default overrides."
            raise RuntimeError(msg)
        with exported.open(encoding="utf-8-sig", newline="") as stream:
            rows = list(csv.DictReader(stream))
        if len(rows) != 2 or {row["filename"] for row in rows} != {"ABC-001_Rev2.txt", "XYZ-002_Rev1.txt"}:
            msg = "Wheel-installed file_inventory.py returned unexpected CSV records."
            raise RuntimeError(msg)

        invalid = _run_installed_wrapper(
            "file-management-python/file_inventory.py",
            work,
            expected_root,
            "--basename",
            "invalid/subdirectory",
            "--no-excel",
            "--no-pause",
        )
        if invalid.returncode != 2 or (work / "invalid").exists():
            msg = f"Wheel-installed file inventory validation/exit contract failed: {invalid.returncode}"
            raise RuntimeError(msg)

        listing = _run_installed_wrapper(
            "file-management-python/list_files_recursive.py",
            work,
            expected_root,
            "--input_directories",
            str(input_dir),
            "--no-pause",
        )
        listing_path = input_dir / "filenames.txt"
        if listing.returncode != 0 or not listing_path.is_file():
            msg = f"Wheel-installed recursive listing failed: {(listing.stdout + listing.stderr)[-1000:]}"
            raise RuntimeError(msg)
        lines = listing_path.read_text(encoding="utf-8").splitlines()
        if str(input_dir / "ABC-001_Rev2.txt") not in lines or str(input_dir / "XYZ-002_Rev1.txt") not in lines:
            msg = "Wheel-installed recursive listing has missing source paths."
            raise RuntimeError(msg)

        _check_installed_po_labels(expected_root, work)
        _check_installed_tuflow_wrappers(expected_root, work)

        print("Wheel-only wrapper behaviour passed: help, CSV, invalid input, file listing, PO labels, TUFLOW TXT.")


def _check_vendored_similarity() -> None:
    """Verify the installed simil algorithm without loading PyHMA."""
    if "vendor.PyHMA" in sys.modules:
        msg = "Importing vendored simil unexpectedly loads PyHMA."
        raise RuntimeError(msg)

    source = np.array([[0, 0, 0], [0, 2, 2], [2, 3, 1], [3, 1, 2], [1, 1, 3]], dtype=np.float64)
    target = np.array([[3, 7, 5], [6, 7, 2], [4.5, 4, 0.5], [6, 2.5, 3.5], [7.5, 5.5, 3.5]])
    multiplier, rotation, translation = find_similarity_transform(source, target)
    transformed = (multiplier * rotation @ source.T + translation).T
    if not np.isclose(multiplier, 1.5) or not np.allclose(transformed, target, atol=1e-10):
        msg = "Installed vendored similarity transform failed its known-control-point check."
        raise RuntimeError(msg)


def _check_installed_script_help() -> None:
    """Run only help for representative installed CLI wrappers, without a checkout."""
    if importlib.util.find_spec("geopandas") is None:
        # The isolated wheel smoke intentionally uses --no-deps. Do not claim
        # full CLI readiness when runtime GIS dependencies are absent.
        print("CLI --help not exercised: isolated --no-deps environment lacks geopandas.")
        return
    scripts_directory = Path(ryan_library.__file__).absolute().parent.parent / "ryan-scripts"
    for relative in ("culvert.py", "floodway.py"):
        script = scripts_directory / relative
        result = subprocess.run(
            [sys.executable, str(script), "--help"],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
        )
        if result.returncode != 0 or not result.stdout.strip():
            msg = f"Installed {relative} --help failed (exit {result.returncode}): {result.stderr[-500:]}"
            raise RuntimeError(msg)


def main(argv: list[str] | None = None) -> int:
    """Verify metadata, import locations, resources and small public operations."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--expected-root",
        type=Path,
        help="Require imported packages to resolve below this isolated install directory.",
    )
    args = parser.parse_args(argv)

    installed_version = importlib.metadata.version("ryan-tools")
    metadata = importlib.metadata.metadata("ryan-tools")
    if metadata["Requires-Python"] != ">=3.14":
        msg = "Installed wheel does not declare the supported Python baseline."
        raise RuntimeError(msg)

    if args.expected_root is not None:
        expected_root = args.expected_root.resolve()
        for package, module_file in (
            ("culvert_solver", culvert_solver.__file__),
            ("run_hy8", run_hy8.__file__),
            ("ryan_library", ryan_library.__file__),
            ("vendor", vendor.__file__),
            ("vendor.simil", vendor_simil.__file__),
        ):
            _require_installed_below(module_file, expected_root, package)

    resources = importlib.resources.files("ryan_library.resources")
    for relative_path in (
        "mcp/workflows.json",
        "qgis/tuflow/_1d_ccA_L.qml",
        "tuflow_templates/runs/empties.bat",
    ):
        if not resources.joinpath(relative_path).is_file():
            msg = f"Installed wheel is missing required resource {relative_path}."
            raise RuntimeError(msg)

    if sanitize_windows_filename('a<b>:c"d/e\\f|g?h*i') != "a_b__c_d_e_f_g_h_i":
        msg = "Installed ryan_library filename sanitization failed."
        raise RuntimeError(msg)
    geometry = culvert_solver.CircularGeometry(diameter=1.0)
    if abs(geometry.area(0.5) - geometry.area_full / 2.0) > 1e-12:
        msg = "Installed culvert_solver geometry calculation failed."
        raise RuntimeError(msg)
    if not hasattr(run_hy8, "UnitSystem"):
        msg = "Installed run_hy8 public API is incomplete."
        raise RuntimeError(msg)

    _check_installed_scripts()
    _check_installed_keyword_discovery()
    _check_installed_wrapper_copy()
    if args.expected_root is not None:
        _check_installed_wrapper_behaviour(args.expected_root.resolve())
    _check_installed_script_help()
    _check_vendored_similarity()

    print(f"Installed-wheel smoke test passed for ryan-tools {installed_version}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
