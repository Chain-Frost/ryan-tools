"""Check repository-owned README links and file references without entering submodules."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import re
from urllib.parse import unquote

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DOCUMENTS: tuple[Path, ...] = (
    Path("README.md"),
    Path("docs/README.md"),
    Path("ryan-scripts/README.md"),
    Path("ryan-scripts/TUFLOW-python/README.md"),
    Path("ryan-scripts/gdal-python/README.md"),
    Path("ryan-scripts/gdal-bat/README.md"),
    Path("ryan_library/processors/tuflow/README.md"),
)
SUBMODULE_PATHS: tuple[str, ...] = (
    "excel-resources",
    "qgis-resources",
    "tests/test_data",
    "unsorted",
    "vendor/run_hy8",
)
REPOSITORY_PREFIXES: tuple[str, ...] = (
    ".github/",
    ".vscode/",
    "docs/",
    "repo-scripts/",
    "ryan-scripts/",
    "ryan_library/",
    "tests/",
    "vendor/",
)
ROOT_FILENAMES: frozenset[str] = frozenset(
    {
        ".coveragerc",
        ".gitmodules",
        "AGENTS.md",
        "README.md",
        "pyproject.toml",
        "pytest.ini",
        "requirements.txt",
        "setup.py",
    }
)
FILE_SUFFIXES: frozenset[str] = frozenset({".bat", ".cmd", ".json", ".md", ".py", ".ps1", ".toml"})
MARKDOWN_LINK_RE: re.Pattern[str] = re.compile(r"\[[^\]]*\]\((?![<>])([^\s)]+)(?:\s+[^)]*)?\)")
FENCED_CODE_RE: re.Pattern[str] = re.compile(r"```.*?```", flags=re.DOTALL)
INLINE_CODE_RE: re.Pattern[str] = re.compile(r"(?<!`)`([^`\r\n]+)`(?!`)")
URI_SCHEME_RE: re.Pattern[str] = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")
WINDOWS_ABSOLUTE_RE: re.Pattern[str] = re.compile(r"^[A-Za-z]:[\\/]")


@dataclass(frozen=True)
class DocumentationIssue:
    """One unresolved local documentation reference."""

    document: Path
    kind: str
    reference: str


def _display_path(path: Path) -> str:
    """Return a repository-relative path when possible."""
    try:
        return path.relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return str(path)


def _is_submodule_content(reference: str) -> bool:
    """Return whether a repository-relative reference enters a configured submodule."""
    normalized = reference.replace("\\", "/").removeprefix("./")
    return any(normalized.startswith(f"{submodule}/") for submodule in SUBMODULE_PATHS)


def _resolve_markdown_target(document: Path, reference: str) -> Path | None:
    """Resolve a local Markdown target, returning ``None`` for URLs and anchors."""
    decoded = unquote(reference.strip())
    if not decoded or decoded.startswith("#") or decoded.startswith("//"):
        return None
    if URI_SCHEME_RE.match(decoded) or WINDOWS_ABSOLUTE_RE.match(decoded):
        return None

    path_text = decoded.partition("#")[0].replace("\\", "/")
    if not path_text:
        return None
    return (document.parent / path_text).resolve(strict=False)


def _inline_path_target(document: Path, reference: str) -> Path | None:
    """Resolve an inline-code token when it unambiguously names a repository path."""
    candidate = reference.strip().replace("\\", "/")
    if not candidate or WINDOWS_ABSOLUTE_RE.match(candidate):
        return None
    if any(character in candidate for character in "*{}<>[]=()\"'"):
        return None
    if candidate.startswith("--") or " " in candidate:
        return None

    normalized = candidate.removeprefix("./")
    if _is_submodule_content(normalized):
        return None
    if normalized in ROOT_FILENAMES or normalized.startswith(REPOSITORY_PREFIXES):
        return (REPO_ROOT / normalized).resolve(strict=False)
    if normalized.startswith("../"):
        return (document.parent / normalized).resolve(strict=False)
    if Path(normalized).suffix.lower() in FILE_SUFFIXES:
        return (document.parent / normalized).resolve(strict=False)
    return None


def check_document(document: Path, *, check_inline_paths: bool = True) -> list[DocumentationIssue]:
    """Return unresolved local links and, optionally, unambiguous inline file references."""
    issues: list[DocumentationIssue] = []
    text = document.read_text(encoding="utf-8")

    for match in MARKDOWN_LINK_RE.finditer(text):
        reference = match.group(1)
        target = _resolve_markdown_target(document=document, reference=reference)
        if target is not None and not target.exists():
            issues.append(DocumentationIssue(document=document, kind="link", reference=reference))

    if check_inline_paths:
        prose = FENCED_CODE_RE.sub("", text)
        prose = MARKDOWN_LINK_RE.sub("", prose)
        for match in INLINE_CODE_RE.finditer(prose):
            reference = match.group(1)
            target = _inline_path_target(document=document, reference=reference)
            if target is not None and not target.exists():
                issues.append(DocumentationIssue(document=document, kind="path", reference=reference))

    return issues


def parse_arguments() -> argparse.Namespace:
    """Parse optional repository-relative documentation paths."""
    parser = argparse.ArgumentParser(
        description="Check repository-owned README links and inline file references without entering submodules."
    )
    parser.add_argument(
        "documents",
        nargs="*",
        type=Path,
        help="Repository-relative Markdown files; defaults to the seven repository-owned READMEs.",
    )
    parser.add_argument(
        "--links-only",
        action="store_true",
        help="Check Markdown links but skip heuristic validation of inline-code file references.",
    )
    return parser.parse_args()


def main() -> int:
    """Run documentation checks and return a process exit code."""
    args = parse_arguments()
    requested: list[Path] = args.documents or list(DEFAULT_DOCUMENTS)
    documents = [path if path.is_absolute() else REPO_ROOT / path for path in requested]

    issues: list[DocumentationIssue] = []
    for document in documents:
        if not document.is_file():
            issues.append(DocumentationIssue(document=document, kind="document", reference="file does not exist"))
            continue
        issues.extend(check_document(document=document, check_inline_paths=not args.links_only))

    if issues:
        for issue in issues:
            print(f"{_display_path(issue.document)}: unresolved {issue.kind}: {issue.reference}")
        return 1

    print(f"Documentation check passed for {len(documents)} file(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
