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


# Effects that must not happen merely because an installed wrapper is imported.
# Keep this list deliberately narrow: a generic call such as Path() or a
# logger constructor is not equivalent to executing a copy/delete operation.
UNSAFE_IMPORT_CALLS: frozenset[str] = frozenset(
    {
        "WNetAddConnection2W",
        "WNetCancelConnection2W",
        "copy",
        "copy2",
        "copyfile",
        "copytree",
        "rmtree",
        "remove",
        "unlink",
        "replace",
        "rename",
        "mkdir",
        "makedirs",
        "Popen",
        "run",
        "check_call",
        "check_output",
        "system",
    }
)


def _call_name(node: ast.Call) -> str:
    """Return the final callable identifier without evaluating the call."""
    current: ast.expr = node.func
    if isinstance(current, ast.Name):
        return current.id
    if isinstance(current, ast.Attribute):
        return current.attr
    return "<dynamic>"


def _is_main_guard(test: ast.expr) -> bool:
    """Accept only a standalone equality check against the Python entry-point name."""
    if not isinstance(test, ast.Compare) or len(test.ops) != 1 or not isinstance(test.ops[0], ast.Eq):
        return False
    if len(test.comparators) != 1:
        return False
    left, right = test.left, test.comparators[0]
    return (
        isinstance(left, ast.Name)
        and left.id == "__name__"
        and isinstance(right, ast.Constant)
        and right.value == "__main__"
    ) or (
        isinstance(right, ast.Name)
        and right.id == "__name__"
        and isinstance(left, ast.Constant)
        and left.value == "__main__"
    )


class _ImportTimeMutationVisitor(ast.NodeVisitor):
    """Walk only expressions and statements that can execute during import."""

    def __init__(self, module: ast.Module) -> None:
        self.names: set[str] = set()
        # Keep Python 3.13 semantics unless the source explicitly postpones
        # annotation evaluation. The wheel still supports Python 3.13.
        self.deferred_annotations = any(
            isinstance(statement, ast.ImportFrom)
            and statement.module == "__future__"
            and any(alias.name == "annotations" for alias in statement.names)
            for statement in module.body
        )
        # Bind aliases in statement order to account for later reassignment.
        self.aliases: dict[str, str] = {}

    def _remember_binding(self, target: ast.expr, value: ast.expr | None) -> None:
        if not isinstance(target, ast.Name):
            return
        if isinstance(value, ast.Attribute) and value.attr in UNSAFE_IMPORT_CALLS:
            self.aliases[target.id] = value.attr
        elif isinstance(value, ast.Name) and value.id in self.aliases:
            self.aliases[target.id] = self.aliases[value.id]
        else:
            self.aliases.pop(target.id, None)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.module in {"shutil", "os", "subprocess", "pathlib"}:
            for alias in node.names:
                name = alias.asname or alias.name
                if alias.name in UNSAFE_IMPORT_CALLS:
                    self.aliases[name] = alias.name
                else:
                    self.aliases.pop(name, None)
        else:
            for alias in node.names:
                self.aliases.pop(alias.asname or alias.name, None)

    def visit_Assign(self, node: ast.Assign) -> None:
        self.visit(node.value)
        for target in node.targets:
            self._remember_binding(target, node.value)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if node.value is not None:
            self.visit(node.value)
            self._remember_binding(node.target, node.value)
        if not self.deferred_annotations:
            self.visit(node.annotation)

    def _visit_function_signature(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        for expression in (*node.args.defaults, *node.args.kw_defaults):
            if expression is not None:
                self.visit(expression)
        if not self.deferred_annotations:
            for arg in (*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs):
                if arg.annotation is not None:
                    self.visit(arg.annotation)
            if node.args.vararg is not None and node.args.vararg.annotation is not None:
                self.visit(node.args.vararg.annotation)
            if node.args.kwarg is not None and node.args.kwarg.annotation is not None:
                self.visit(node.args.kwarg.annotation)
            if node.returns is not None:
                self.visit(node.returns)
        for expression in node.decorator_list:
            self.visit(expression)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """Function signatures evaluate at definition time; bodies do not."""
        self._visit_function_signature(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        """Async functions have the same definition-time evaluation rule."""
        self._visit_function_signature(node)

    def visit_Lambda(self, node: ast.Lambda) -> None:
        """Lambda defaults are evaluated, whereas the expression body is deferred."""
        self.visit(node.args)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        """Decorators, bases, keywords and class body are evaluated immediately."""
        for expression in (*node.decorator_list, *node.bases, *node.keywords):
            self.visit(expression)
        for statement in node.body:
            self.visit(statement)

    def visit_If(self, node: ast.If) -> None:
        """Skip only the true branch of a genuine `__main__` guard."""
        if _is_main_guard(node.test):
            self.visit(node.test)
            for statement in node.orelse:
                self.visit(statement)
            return
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        """Record known mutating calls and inspect their nested arguments."""
        name = _call_name(node)
        if isinstance(node.func, ast.Name):
            name = self.aliases.get(node.func.id, name)
        if name in UNSAFE_IMPORT_CALLS:
            self.names.add(name)
        self.generic_visit(node)


def _import_time_call_names(module: ast.Module) -> list[str]:
    """Conservatively find import-time calls, without importing any script."""
    visitor = _ImportTimeMutationVisitor(module)
    visitor.visit(module)
    return sorted(visitor.names)


def _editable_setting_names(module: ast.Module) -> list[str]:
    """Identify editable constants without exposing sensitive configured values."""
    names: set[str] = set()
    for statement in module.body:
        if isinstance(statement, ast.Assign):
            targets = statement.targets
        elif isinstance(statement, ast.AnnAssign):
            targets = [statement.target]
        else:
            continue
        for target in targets:
            if isinstance(target, ast.Name) and (
                target.id.startswith("DEFAULT_")
                or target.id in {"WORKING_DIR", "PATHS_TO_PROCESS", "INPUT_DIR", "OUTPUT_DIR"}
            ):
                names.add(target.id)
    return sorted(names)


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
    guarded = any(isinstance(node, ast.If) and _is_main_guard(node.test) for node in tree.body)
    call_sites = sum(isinstance(node, ast.Expr) and isinstance(node.value, ast.Call) for node in tree.body)
    has_wrapper_version = any(
        isinstance(node, (ast.Assign, ast.AnnAssign))
        and any(
            isinstance(target, ast.Name) and target.id == "WRAPPER_VERSION"
            for target in (node.targets if isinstance(node, ast.Assign) else [node.target])
        )
        for node in tree.body
    )
    unsafe_calls = _import_time_call_names(tree)
    callable_names = {_call_name(node) for node in ast.walk(tree) if isinstance(node, ast.Call)}
    return {
        "parse_status": "ok",
        "import_time_mutation_calls": unsafe_calls,
        "import_time_mutation_risk": bool(unsafe_calls),
        "imports": sorted(imports),
        "main_guard": guarded,
        "wrapper_version_declared": has_wrapper_version,
        "argparse_imported": "argparse" in imports,
        "no_pause_literal": "--no-pause" in literals,
        "file_relative_reference": any(isinstance(node, ast.Name) and node.id == "__file__" for node in ast.walk(tree)),
        "absolute_drive_literal": any(DRIVE_LITERAL.search(value) is not None for value in literals),
        "top_level_call_expressions": call_sites,
        "subprocess_imported": "subprocess" in imports,
        "editable_setting_names": _editable_setting_names(tree),
        "wrapper_banner_called": "print_wrapper_banner" in callable_names,
        "system_exit_called": "SystemExit" in callable_names or "exit" in callable_names,
        "dry_run_literal": "--dry-run" in literals,
        "destructive_option_literals": sorted(
            literals.intersection({"--delete", "--replace", "--overwrite", "--yes", "--force"})
        ),
    }


def _selected_script_risks(relative: str, entry: dict[str, Any]) -> list[str]:
    """Return fail-closed packaging risks without judging domain accuracy."""
    path = Path(relative)
    if not entry["in_wheel"] or path.suffix.casefold() != ".py" or path.name == "__init__.py":
        return []
    reasons: list[str] = []
    if entry.get("parse_status") != "ok":
        reasons.append("not valid or decodable Python")
    if entry.get("main_guard") is not True:
        reasons.append("no protected __main__ entry point")
    if entry.get("import_time_mutation_risk"):
        reasons.append("potential import-time mutation: " + ", ".join(entry["import_time_mutation_calls"]))
    return reasons


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
    unsafe_selected: list[dict[str, str]] = []
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
        reasons = _selected_script_risks(relative, entry)
        if reasons:
            unsafe_selected.append({"path": relative, "reason": "; ".join(reasons)})
        items.append(entry)
    return {
        "schema_version": 1,
        "assessment_scope": "static signals only; no import, execution or manual acceptance",
        "tracked_file_count": len(tracked),
        "selected_file_count": len(selected),
        "mcp_script_target_count": len(mcp_targets),
        "missing_selected": missing_selected,
        "mcp_targets_not_selected": missing_mcp,
        "unsafe_selected": unsafe_selected,
        "files": items,
    }


def _markdown_row(entry: dict[str, Any]) -> str:
    """Format one statically inspected script without implying runtime approval."""
    path = str(entry["path"]).replace("|", "\\|").replace("`", "")
    wheel = "wheel" if entry["in_wheel"] else "repo-only"
    mcp = ", ".join(entry["mcp_workflow_ids"]) if entry["mcp_workflow_ids"] else "—"
    if entry.get("parse_status") != "ok":
        context = str(entry.get("parse_status", "unknown"))
        signals = "Non-Python/resource or requires separate review"
        flags = "—"
        behavior = "N/A"
    else:
        guarded = "guard" if entry["main_guard"] else "NO GUARD"
        version = "version" if entry["wrapper_version_declared"] else "no version"
        context = guarded + "; " + version
        flags = "argparse" if entry["argparse_imported"] else "no argparse"
        if entry["no_pause_literal"]:
            flags += "; no-pause"
        if entry["dry_run_literal"]:
            flags += "; dry-run"
        if entry["destructive_option_literals"]:
            flags += "; " + ", ".join(entry["destructive_option_literals"])
        settings = entry["editable_setting_names"]
        behavior = f"{len(settings)} visible defaults"
        behavior += "; SystemExit" if entry["system_exit_called"] else "; exit unverified"
        behavior += "; banner" if entry["wrapper_banner_called"] else "; no banner"
        details: list[str] = []
        if entry["import_time_mutation_risk"]:
            details.append("UNSAFE IMPORT: " + ", ".join(entry["import_time_mutation_calls"]))
        if entry["absolute_drive_literal"]:
            details.append("drive literal")
        if entry["file_relative_reference"]:
            details.append("__file__ use")
        if entry["subprocess_imported"]:
            details.append("subprocess")
        if entry["top_level_call_expressions"]:
            details.append(f"module call expressions={entry['top_level_call_expressions']}")
        signals = ", ".join(details) if details else "no flagged static signals"
    return f"| `{path}` | {wheel} | {mcp} | {context} | {flags} | {behavior} | {signals} |"


def render_markdown(report: dict[str, Any]) -> str:
    """Produce per-file evidence for review, without claiming engineering approval."""
    rows: list[str] = [
        "# Installed-script source review evidence",
        "",
        "Machine-produced, non-executing review of tracked `ryan-scripts/` files.",
        "A positive AST signal does **not** establish CLI runtime safety, external",
        "software readiness, original-default parity, or maintainer acceptance.",
        "",
        (
            f"Tracked: **{report['tracked_file_count']}**; selected wheel files: **{report['selected_file_count']}**; "
            f"MCP script paths: **{report['mcp_script_target_count']}**."
        ),
        "",
        "| Source file | Distribution | MCP | Guard / version | CLI / safety flags | Defaults / exit / banner | Import / environment signals |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for entry in report["files"]:
        rows.append(_markdown_row(entry))
    rows.extend(
        [
            "",
            "## Blocking installed-file structural findings",
            "",
            *[f"- `{issue['path']}`: {issue['reason']}" for issue in report["unsafe_selected"]],
        ]
    )
    if not report["unsafe_selected"]:
        rows.append("None detected by this conservative static policy.")
    rows.extend(
        [
            "",
            "## Required acceptance outside static analysis",
            "",
            "Assess every retained wrapper's true defaults, --help, --no-pause,",
            "exit codes, overwrite behavior, output artifacts and dependent resources;",
            "validate external applications and long/mapped/UNC network paths in",
            "the supported engineering environments before claiming full parity.",
            "",
        ]
    )
    return "\n".join(rows)


def main(argv: list[str] | None = None) -> int:
    """Export auditable, reproducible signals without changing source files."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--output", type=Path, help="Write report here instead of stdout.")
    parser.add_argument("--format", choices=("json", "markdown"), default="json")
    parser.add_argument(
        "--check", action="store_true", help="Fail on missing wheel/MCP paths or unsafe selected Python entry points."
    )
    args = parser.parse_args(argv)
    report = audit_repository(args.root.absolute())
    payload = (
        render_markdown(report)
        if args.format == "markdown"
        else json.dumps(report, indent=2, ensure_ascii=False) + "\n"
    )
    if args.output is None:
        print(payload, end="")
    else:
        target = args.output.expanduser().absolute()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(payload, encoding="utf-8")
        print(f"Wrote {report['tracked_file_count']} static inventory records to {target}")
    if args.check and (report["missing_selected"] or report["mcp_targets_not_selected"] or report["unsafe_selected"]):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
