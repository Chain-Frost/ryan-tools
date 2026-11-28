"""Confirm lightweight ryan_library imports cannot depend on GIS, GUI or MCP modules.

Run in a fresh process against either an ordinary source checkout or an isolated
installed-wheel root. The caller selects the import root through PYTHONPATH and
passes that same root via --expected-root for a physical-origin assertion.
"""

from __future__ import annotations

import argparse
import importlib
import importlib.abc
import sys
from collections.abc import Sequence
from importlib.machinery import ModuleSpec
from pathlib import Path
from types import ModuleType

# HY-8 and the native culvert solver are required distributions, not optional
# dependencies, but lightweight utility/package imports must not load them.
BLOCKED_ROOTS: frozenset[str] = frozenset({"osgeo", "qgis", "mcp", "tkinter", "run_hy8", "culvert_solver"})
MODULE_FILES: dict[str, Path] = {
    "ryan_library": Path("ryan_library/__init__.py"),
    "ryan_library.functions": Path("ryan_library/functions/__init__.py"),
    "ryan_library.functions.gdal": Path("ryan_library/functions/gdal/__init__.py"),
    "ryan_library.functions.path_stuff": Path("ryan_library/functions/path_stuff.py"),
}


class BlockedImportFinder(importlib.abc.MetaPathFinder):
    """Reject and record imports that would create an unwanted eager dependency."""

    def __init__(self) -> None:
        self.attempts: list[str] = []

    def find_spec(
        self, fullname: str, path: Sequence[str] | None = None, target: ModuleType | None = None
    ) -> ModuleSpec | None:
        del path, target
        if fullname.partition(".")[0] not in BLOCKED_ROOTS:
            return None
        self.attempts.append(fullname)
        msg = f"Blocked eager import of {fullname}"
        raise ModuleNotFoundError(msg, name=fullname)


def verify_import_boundary(expected_root: Path) -> None:
    """Require exact source/wheel provenance and no eager blocked imports."""
    root = expected_root.resolve(strict=True)
    loaded = sorted(name for name in sys.modules if name.partition(".")[0] in BLOCKED_ROOTS)
    if loaded:
        msg = f"Optional/engine imports preloaded before probe: {loaded}"
        raise RuntimeError(msg)

    finder = BlockedImportFinder()
    sys.meta_path.insert(0, finder)
    try:
        for module_name, relative_file in MODULE_FILES.items():
            module = importlib.import_module(module_name)
            source = module.__file__
            if source is None:
                msg = f"Imported {module_name} has no file provenance"
                raise RuntimeError(msg)
            # This is an intentional physical-identity check, not a user path conversion.
            actual_file = Path(source).resolve(strict=True)
            expected_file = (root / relative_file).resolve(strict=True)
            if actual_file != expected_file:
                msg = f"{module_name} came from {actual_file}; expected {expected_file}"
                raise RuntimeError(msg)

        # A downstream import could catch ModuleNotFoundError and hide the attempt.
        # Require that no blocked module was even requested during this probe.
        if finder.attempts:
            msg = f"Unexpected eager optional/engine imports: {finder.attempts}"
            raise RuntimeError(msg)
        loaded = sorted(name for name in sys.modules if name.partition(".")[0] in BLOCKED_ROOTS)
        if loaded:
            msg = f"Unexpected optional/engine imports loaded: {loaded}"
            raise RuntimeError(msg)
    finally:
        sys.meta_path.remove(finder)

    print(f"Import-isolation and provenance check passed: {root}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-root", type=Path, required=True)
    args = parser.parse_args(argv)
    verify_import_boundary(args.expected_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
