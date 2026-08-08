"""Enforce the non-eager diagnostic subset of the Loguru message policy.

Parameterized Loguru formatting is preferred at every level. Existing
user-facing eager messages are allowed during migration, while DEBUG and TRACE
must avoid f-strings, percent formatting, and ``str.format()``.
"""

from __future__ import annotations

import ast
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCAN_ROOTS = (
    REPO_ROOT / "ryan_library",
    REPO_ROOT / "ryan-scripts",
    REPO_ROOT / "repo-scripts",
    REPO_ROOT / "examples",
)
DEVELOPER_METHODS = frozenset({"debug", "trace"})
LOG_METHODS = frozenset({"info", "success", "warning", "error", "critical", "exception"}) | DEVELOPER_METHODS


@dataclass(frozen=True, slots=True)
class Violation:
    """One policy violation at a source location."""

    path: Path
    line: int
    message: str


def _python_files() -> Iterable[Path]:
    """Yield active repository-owned Python files in deterministic order."""

    for root in SCAN_ROOTS:
        for path in sorted(root.rglob("*.py")):
            relative_path = path.relative_to(REPO_ROOT)
            if relative_path.parts[:2] == ("ryan_library", "scripts"):
                continue
            yield path


def _is_rooted_in_logger(node: ast.expr) -> bool:
    """Return whether an attribute/call chain begins at the Loguru ``logger``."""

    if isinstance(node, ast.Name):
        return node.id == "logger"
    if isinstance(node, ast.Attribute):
        return _is_rooted_in_logger(node.value)
    if isinstance(node, ast.Call):
        return _is_rooted_in_logger(node.func)
    return False


def _is_eager_expression(node: ast.expr) -> bool:
    """Return whether a debug expression is visibly rendered before Loguru filters it."""

    if isinstance(node, ast.JoinedStr):
        return True
    if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.Mod, ast.Add)):
        return True
    return isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "format"


def check_file(path: Path) -> list[Violation]:
    """Return all Loguru formatting violations in ``path``."""

    source: str = path.read_text(encoding="utf-8-sig")
    tree: ast.AST = ast.parse(source, filename=str(path))
    if not _imports_loguru_logger(tree):
        return []
    violations: list[Violation] = []
    relative_path: Path = path.relative_to(REPO_ROOT)

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        method: str = node.func.attr
        if method not in LOG_METHODS:
            continue
        if not _is_rooted_in_logger(node.func.value) or not node.args:
            continue

        if any(keyword.arg == "exc_info" for keyword in node.keywords):
            violations.append(
                Violation(
                    path=relative_path,
                    line=node.lineno,
                    message="Loguru traceback capture must use logger.opt(exception=True)",
                )
            )

        if method in DEVELOPER_METHODS and _is_eager_expression(node.args[0]):
            violations.append(
                Violation(
                    path=relative_path,
                    line=node.lineno,
                    message=(
                        f"logger.{method}() must use Loguru parameterized formatting; "
                        "use opt(lazy=True) when evaluating a value is expensive"
                    ),
                )
            )

    return violations


def _imports_loguru_logger(tree: ast.AST) -> bool:
    """Return whether ``logger`` is imported directly from Loguru."""

    return any(
        isinstance(node, ast.ImportFrom)
        and node.module == "loguru"
        and any(alias.name == "logger" and alias.asname in (None, "logger") for alias in node.names)
        for node in ast.walk(tree)
    )


def main() -> int:
    """Check active code and return a process-friendly status code."""

    violations: list[Violation] = []
    for path in _python_files():
        violations.extend(check_file(path))

    if not violations:
        print("Loguru formatting policy passed.")
        return 0

    print("Loguru formatting policy violations:", file=sys.stderr)
    for violation in violations:
        print(f"{violation.path}:{violation.line}: {violation.message}", file=sys.stderr)
    print(f"Found {len(violations)} violation(s).", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
