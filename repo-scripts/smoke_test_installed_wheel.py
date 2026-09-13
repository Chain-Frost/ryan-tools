"""Smoke-test the bundled public packages from an isolated wheel install."""

from __future__ import annotations

import argparse
import importlib.metadata
import importlib.resources
from pathlib import Path

import culvert_solver
import run_hy8

import ryan_library
from ryan_library.functions.path_stuff import sanitize_windows_filename


def _require_installed_below(module_file: str | None, expected_root: Path, package: str) -> None:
    """Require an imported package to resolve below the isolated install root."""
    if module_file is None or not Path(module_file).resolve().is_relative_to(expected_root):
        msg = f"{package} resolved outside the expected install root: {module_file}"
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

    installed_version = importlib.metadata.version("ryan_functions")
    metadata = importlib.metadata.metadata("ryan_functions")
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

    print(f"Installed-wheel smoke test passed for ryan_functions {installed_version}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
