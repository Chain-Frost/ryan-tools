"""Inventory tracked ryan-scripts files with non-executing static review signals.

The output is evidence to guide per-file maintenance decisions, not an assertion
that a wrapper is safe to run or eligible for wheel/MCP publication.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any, cast

PROJECT_ROOT: Path = Path(__file__).absolute().parents[1]
DRIVE_LITERAL: re.Pattern[str] = re.compile(r"(?i)\b[A-Z]:[\\/]")


def tracked_scripts(root: Path) -> tuple[str, ...]:
    """List only git-tracked script-tree files without traversing submodules."""
    git_executable = shutil.which("git")
    if git_executable is None:
        msg = "git executable is required to list tracked scripts."
        raise RuntimeError(msg)
    result = subprocess.run(
        [git_executable, "ls-files", "-z", "--", "ryan-scripts"],
        cwd=root,
        check=True,
        capture_output=True,
    )
    items = result.stdout.decode("utf-8").split("\0")
    return tuple(sorted(path.removeprefix("ryan-scripts/") for path in items if path.startswith("ryan-scripts/")))


def analyse_python(source: str) -> dict[str, Any]:
    """Inspect syntax only; never import or execute a script."""
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        return {"parse_status": "syntax_error", "parse_error": f"{exc.msg} (line {exc.lineno})"}
    imports: set[str] = set()
    literals: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name.split(".", maxsplit=1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module.split(".", maxsplit=1)[0])
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            literals.add(node.value)
    guarded = any(
        isinstance(node, ast.If)
        and isinstance(node.test, ast.Compare)
        and isinstance(node.test.left, ast.Name)
        and node.test.left.id == "__name__"
        and any(
            isinstance(comparator, ast.Constant) and comparator.value == "__main__"
            for comparator in node.test.comparators
        )
        for node in tree.body
    )
    call_sites = sum(isinstance(node, ast.Expr) and isinstance(node.value, ast.Call) for node in tree.body)
    has_wrapper_version = any(
        isinstance(node, (ast.Assign, ast.AnnAssign))
        and any(
            isinstance(target, ast.Name) and target.id == "WRAPPER_VERSION"
            for target in (node.targets if isinstance(node, ast.Assign) else [node.target])
        )
        for node in tree.body
    )
    return {
        "parse_status": "ok",
        "imports": sorted(imports),
        "main_guard": guarded,
        "wrapper_version_declared": has_wrapper_version,
        "argparse_imported": "argparse" in imports,
        "no_pause_literal": "--no-pause" in literals,
        "file_relative_reference": any(isinstance(node, ast.Name) and node.id == "__file__" for node in ast.walk(tree)),
        "absolute_drive_literal": any(DRIVE_LITERAL.search(value) is not None for value in literals),
        "top_level_call_expressions": call_sites,
        "subprocess_imported": "subprocess" in imports,
    }


def audit_repository(root: Path) -> dict[str, Any]:
    """Compare the selected wheel catalogue with all tracked script paths."""
    resources = root / "ryan_library" / "resources" / "mcp"
    policy = cast("dict[str, object]", json.loads((resources / "installed_scripts.json").read_text(encoding="utf-8")))
    general = cast("dict[str, object]", json.loads((resources / "workflows.json").read_text(encoding="utf-8")))
    gdal = cast("dict[str, object]", json.loads((resources / "gdal_cli_tools.json").read_text(encoding="utf-8")))
    selected = set(cast("list[str]", policy["installed_files"]))
    mcp_targets: dict[str, list[str]] = {}
    for workflow in cast("list[dict[str, object]]", general["workflows"]):
        script = workflow.get("script")
        if isinstance(script, str):
            relative = script.removeprefix("ryan-scripts/")
            mcp_targets.setdefault(relative, []).append(str(workflow["id"]))
    for workflow in cast("list[dict[str, object]]", gdal["tools"]):
        script = workflow.get("script")
        if isinstance(script, str) and "module" not in workflow:
            relative = f"gdal-python/{script}"
            mcp_targets.setdefault(relative, []).append(str(workflow["id"]))
    tracked = tracked_scripts(root)
    missing_selected = sorted(selected.difference(tracked))
    missing_mcp = sorted(set(mcp_targets).difference(selected))
    items: list[dict[str, Any]] = []
    for relative in tracked:
        path = root / "ryan-scripts" / relative
        entry: dict[str, Any] = {
            "path": relative,
            "in_wheel": relative in selected,
            "mcp_workflow_ids": sorted(mcp_targets.get(relative, [])),
            "review_status": "static_signals_only",
        }
        if path.is_symlink():
            entry["parse_status"] = "skipped_symlink"
        elif path.suffix.lower() == ".py":
            try:
                entry.update(analyse_python(path.read_text(encoding="utf-8-sig")))
            except UnicodeError:
                entry["parse_status"] = "undecodable_python"
        else:
            entry["parse_status"] = "non_python"
        items.append(entry)
    return {
        "schema_version": 1,
        "assessment_scope": "static signals only; no import, execution or manual acceptance",
        "tracked_file_count": len(tracked),
        "selected_file_count": len(selected),
        "mcp_script_target_count": len(mcp_targets),
        "missing_selected": missing_selected,
        "mcp_targets_not_selected": missing_mcp,
        "files": items,
    }


def main(argv: list[str] | None = None) -> int:
    """Export auditable, reproducible signals without changing source files."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--output", type=Path, help="Write JSON report here instead of stdout.")
    parser.add_argument("--check", action="store_true", help="Fail if wheel/MCP references are missing.")
    args = parser.parse_args(argv)
    report = audit_repository(args.root.absolute())
    payload = json.dumps(report, indent=2, ensure_ascii=False) + "\n"
    if args.output is None:
        print(payload, end="")
    else:
        target = args.output.expanduser().absolute()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(payload, encoding="utf-8")
        print(f"Wrote {report['tracked_file_count']} static inventory records to {target}")
    if args.check and (report["missing_selected"] or report["mcp_targets_not_selected"]):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
