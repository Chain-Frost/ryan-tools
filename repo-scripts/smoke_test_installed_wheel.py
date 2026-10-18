"""Smoke-test ryan-tools and separately installed PyPI dependencies."""

from __future__ import annotations

import argparse
import importlib.metadata
import importlib.resources
from pathlib import Path
from tempfile import TemporaryDirectory

import culvert_solver
import run_hy8

import ryan_library
from ryan_library.functions.path_stuff import sanitize_windows_filename
from ryan_library.mcp.installed_scripts import available_wrappers, copy_wrapper
from ryan_library.mcp.registry import WorkflowRegistry


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
    if metadata["Requires-Python"] != ">=3.13":
        msg = "Installed wheel does not declare the supported Python baseline."
        raise RuntimeError(msg)

    if args.expected_root is not None:
        expected_root = args.expected_root.resolve()
        for package, module_file in (
            ("culvert_solver", culvert_solver.__file__),
            ("run_hy8", run_hy8.__file__),
            ("ryan_library", ryan_library.__file__),
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
    _check_installed_wrapper_copy()

    print(f"Installed-wheel smoke test passed for ryan-tools {installed_version}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
