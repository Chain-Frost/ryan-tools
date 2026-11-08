"""Select CI work from the complete change against the event's comparison base."""

import json
import os
import re
import subprocess
import sys
import tomllib
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath
from typing import cast

EXCLUDED_ROOTS = ("vendor", "tests/test_data", "unsorted", "qgis-resources", "excel-resources")
PYTHON_SUFFIXES = frozenset({".py", ".pyi"})
MARKDOWN_SUFFIXES = frozenset({".md", ".markdown"})
RUFF_CONFIG_NAMES = frozenset({"ruff.toml", ".ruff.toml", "pyproject.toml"})


@dataclass(frozen=True, slots=True)
class CIScope:
    """Runner-independent paths and the decision to require full validation."""

    full: bool
    comparison_available: bool
    lint_files: tuple[str, ...]
    type_files: tuple[str, ...]
    markdown_files: tuple[str, ...]


def _git(root: Path, *arguments: str, errors: str = "surrogateescape") -> str:
    """Read Git metadata without shell expansion or submodule traversal."""
    result = subprocess.run(
        ["git", "-C", str(root), *arguments],  # noqa: S607 - Git is supplied by the CI runner.
        check=True,
        capture_output=True,
        timeout=30,
    )
    # Decode here: Windows subprocess reader threads do not propagate decode errors.
    return result.stdout.decode("utf-8", errors=errors)


def _changed_paths(root: Path, base_sha: str, head_sha: str) -> list[str] | None:
    """Return all changed paths, or None when a trustworthy diff is unavailable."""
    if not base_sha or not base_sha.strip("0"):
        return None
    try:
        # Renames must include the deleted path: source.py -> notes.md is code work.
        output = _git(root, "diff", "--no-renames", "--name-only", "-z", base_sha, head_sha, "--")
    except (OSError, subprocess.SubprocessError):
        return None
    return [path for path in output.split("\0") if path]


def _is_first_party(path: str) -> bool:
    """Exclude independent content before making any filesystem query."""
    return not any(path == prefix or path.startswith(f"{prefix}/") for prefix in EXCLUDED_ROOTS)


def _existing_files(root: Path, paths: list[str], suffixes: frozenset[str]) -> tuple[str, ...]:
    """Keep owned regular files; never follow a tracked symlink into other content."""
    selected: list[str] = []
    for name in sorted(set(paths)):
        if not _is_first_party(name) or PurePosixPath(name).suffix.casefold() not in suffixes:
            continue
        path = root / name
        if not path.is_symlink() and path.is_file():
            selected.append(name)
    return tuple(selected)


def _is_type_config(path: str) -> bool:
    """Recognise project metadata and every named Pyright project configuration."""
    name = PurePosixPath(path).name
    return name == "pyproject.toml" or (name.startswith("pyright") and name.endswith(".json"))


def _same_toml_values(before: object, after: object) -> bool:
    """Compare parsed values without conflating TOML integers, floats and booleans."""
    if type(before) is not type(after):
        return False
    if isinstance(before, dict) and isinstance(after, dict):
        left = cast(dict[str, object], before)
        right = cast(dict[str, object], after)
        return left.keys() == right.keys() and all(_same_toml_values(value, right[key]) for key, value in left.items())
    if isinstance(before, list) and isinstance(after, list):
        left_items = cast(list[object], before)
        right_items = cast(list[object], after)
        return len(left_items) == len(right_items) and all(
            _same_toml_values(left, right) for left, right in zip(left_items, right_items, strict=True)
        )
    return before == after


def _is_statically_neutral_pyproject_change(root: Path, path: str, base_sha: str, head_sha: str) -> bool:
    """Exempt numeric version changes and the exact accepted HDF-cache backend addition.

    Unknown metadata, tool settings and dependency changes still expand static
    checks. The narrow PyTables addition is exercised by the hosted HDF suite.
    This does not generalise dependency-only changes into unverified exemptions.
    """
    documents: list[dict[str, object]] = []
    versions: list[str] = []
    previous_mode: str | None = None
    try:
        for revision in (base_sha, head_sha):
            entry = _git(root, "--literal-pathspecs", "ls-tree", "-z", revision, "--", path)
            fields = entry.partition("\t")[0].split()
            if len(fields) != 3 or fields[0] not in {"100644", "100755"} or fields[1] != "blob":
                return False
            if previous_mode is not None and fields[0] != previous_mode:
                return False
            previous_mode = fields[0]
            document: dict[str, object] = tomllib.loads(_git(root, "show", f"{revision}:{path}", errors="strict"))
            project = document.get("project")
            if not isinstance(project, dict):
                return False
            project_fields = cast(dict[str, object], project)
            version = project_fields.pop("version", None)
            if not isinstance(version, str) or re.fullmatch(r"[0-9]+(?:\.[0-9]+)*", version) is None:
                return False
            dynamic = project_fields.get("dynamic", [])
            if not isinstance(dynamic, list):
                return False
            dynamic_fields = cast(list[object], dynamic)
            if any(not isinstance(field, str) for field in dynamic_fields) or "version" in dynamic_fields:
                return False
            documents.append(document)
            versions.append(version)
    except (OSError, UnicodeError, subprocess.SubprocessError, tomllib.TOMLDecodeError):
        return False
    if versions[0] == versions[1]:
        return False
    if _same_toml_values(documents[0], documents[1]):
        return True

    # WP05 adds exactly one optional HDF backend, not a Ruff/Pyright rule or
    # runtime dependency. The HDF tests require its actual installation.
    before_project = documents[0].get("project")
    after_project = documents[1].get("project")
    if not isinstance(before_project, dict) or not isinstance(after_project, dict):
        return False
    before_extras = cast(dict[str, object], before_project).get("optional-dependencies")
    after_extras = cast(dict[str, object], after_project).get("optional-dependencies")
    if not isinstance(before_extras, dict) or not isinstance(after_extras, dict):
        return False
    before_options = cast(dict[str, object], before_extras)
    after_options = cast(dict[str, object], after_extras)
    before_dev = before_options.get("dev")
    after_dev = after_options.get("dev")
    if not isinstance(before_dev, list) or not isinstance(after_dev, list):
        return False

    backend = "tables>=3.11.1"
    if (
        "cache" in before_options
        or after_options.get("cache") != [backend]
        or before_dev.count(backend) != 0
        or after_dev.count(backend) != 1
    ):
        return False
    after_options.pop("cache")
    after_dev.remove(backend)
    return _same_toml_values(documents[0], documents[1])


def determine_scope(root: Path, *, event_name: str, base_sha: str, head_sha: str = "HEAD") -> CIScope:
    """Fail open to full CI; broaden static checks when the comparison is unknown."""
    comparison_head = head_sha or "HEAD"
    changed = _changed_paths(root, base_sha, comparison_head)
    markdown_only = bool(changed) and all(
        PurePosixPath(path).suffix.casefold() in MARKDOWN_SUFFIXES for path in changed or []
    )
    full = event_name != "pull_request" or not markdown_only

    owned_changes = [path for path in changed or [] if _is_first_party(path)]
    configuration_changes = [
        path
        for path in owned_changes
        if PurePosixPath(path).name != "pyproject.toml"
        or not _is_statically_neutral_pyproject_change(root, path, base_sha, comparison_head)
    ]
    all_lint = changed is None or any(PurePosixPath(path).name in RUFF_CONFIG_NAMES for path in configuration_changes)
    all_types = changed is None or any(_is_type_config(path) for path in configuration_changes)
    tracked: list[str] = []
    if all_lint or all_types:
        # Without --recurse-submodules, ls-files lists gitlinks, not their contents.
        # Failure here must fail this job rather than falsely report empty checks.
        tracked = [path for path in _git(root, "ls-files", "--cached", "-z").split("\0") if path]

    lint_files = _existing_files(root, tracked if all_lint else owned_changes, PYTHON_SUFFIXES)
    type_files = _existing_files(root, tracked if all_types else owned_changes, PYTHON_SUFFIXES)
    markdown_files = _existing_files(root, tracked if changed is None else owned_changes, MARKDOWN_SUFFIXES)
    if changed is None:
        print("Comparison unavailable: require full CI and check all tracked first-party sources.", file=sys.stderr)
    return CIScope(
        full=full,
        comparison_available=changed is not None,
        lint_files=lint_files,
        type_files=type_files,
        markdown_files=markdown_files,
    )


def write_manifest(scope: CIScope, path: Path) -> None:
    """Save complete scope locally without transporting path lists through env vars."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(scope), indent=2) + "\n", encoding="utf-8", newline="\n")


def write_outputs(scope: CIScope, output_path: Path | None) -> None:
    """Use single-line JSON arrays so spaces, Unicode and control characters survive."""
    values = {
        "full": str(scope.full).lower(),
        "comparison_available": str(scope.comparison_available).lower(),
        "lint_files": json.dumps(scope.lint_files, separators=(",", ":")),
        "type_files": json.dumps(scope.type_files, separators=(",", ":")),
        "markdown_files": json.dumps(scope.markdown_files, separators=(",", ":")),
    }
    output = "".join(f"{key}={value}\n" for key, value in values.items())
    if output_path is None:
        print(output, end="")
    else:
        with output_path.open("a", encoding="utf-8", newline="\n") as file:
            file.write(output)
        print(
            f"Full CI: {scope.full}; lint: {len(scope.lint_files)}; "
            f"types: {len(scope.type_files)}; Markdown: {len(scope.markdown_files)}."
        )


def main() -> int:
    """Classify the event environment and publish GitHub Actions step outputs."""
    try:
        scope = determine_scope(
            Path.cwd(),
            event_name=os.environ.get("EVENT_NAME", ""),
            base_sha=os.environ.get("BASE_SHA", ""),
            head_sha=os.environ.get("GITHUB_SHA") or "HEAD",
        )
        manifest = os.environ.get("CI_SCOPE_PATH")
        if manifest:
            # Publish step outputs only after the requested manifest is usable.
            write_manifest(scope, Path(manifest))
        destination = os.environ.get("GITHUB_OUTPUT")
        write_outputs(scope, Path(destination) if destination else None)
    except (OSError, subprocess.SubprocessError) as error:
        print(f"Cannot establish CI scope: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
