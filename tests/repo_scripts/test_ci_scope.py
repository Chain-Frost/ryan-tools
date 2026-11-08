"""Exercise CI routing against real Git histories without project dependencies."""

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import TypedDict, cast

import pytest

SCRIPT = Path(__file__).absolute().parents[2] / "repo-scripts" / "ci_scope.py"
PROJECT_METADATA = """[project]
name = "scope-example"
version = "26.11.8.1"
requires-python = ">=3.14"
dependencies = ["pandas>=2"]

[tool.ruff]
line-length = 120

[tool.pyright]
typeCheckingMode = "strict"

[tool.example]
flag = 1
values = [1.0]
"""
UPDATED_VERSION = PROJECT_METADATA.replace('version = "26.11.8.1"', 'version = "26.11.8.2"')


class ScopeResult(TypedDict):
    """Decoded workflow outputs from the classifier process."""

    full: bool
    comparison_available: bool
    lint_files: list[str]
    type_files: list[str]
    markdown_files: list[str]


def _git(root: Path, *arguments: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(root), *arguments],  # noqa: S607 - Exercise the runner's Git executable.
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=30,
    )
    return result.stdout.strip()


def _write(root: Path, name: str, content: str = "value = 1\n") -> None:
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _commit(root: Path) -> str:
    _git(root, "add", "--all")
    _git(root, "commit", "--quiet", "--allow-empty", "-m", "CI scope fixture")
    return _git(root, "rev-parse", "HEAD")


@pytest.fixture
def repository(tmp_path: Path) -> Path:
    root = tmp_path / "repository"
    root.mkdir()
    _git(root, "init", "--quiet", "-b", "main")
    _git(root, "config", "user.name", "CI scope tests")
    _git(root, "config", "user.email", "ci-scope@example.invalid")
    _git(root, "config", "core.autocrlf", "false")
    _write(root, "source.py")
    _write(root, "README.md", "# Example\n")
    _commit(root)
    return root


def _scope(
    root: Path, base: str, *, event: str = "pull_request", head: str = "", manifest: Path | None = None
) -> ScopeResult:
    output = root.parent / "scope-outputs.txt"
    output.unlink(missing_ok=True)
    env = os.environ | {
        "EVENT_NAME": event,
        "BASE_SHA": base,
        "GITHUB_SHA": head,
        "GITHUB_OUTPUT": str(output),
        "CI_SCOPE_PATH": str(manifest) if manifest else "",
    }
    result = subprocess.run(
        [sys.executable, str(SCRIPT)], cwd=root, env=env, check=False, capture_output=True, timeout=30
    )
    assert result.returncode == 0, result.stderr.decode("utf-8", errors="replace")
    lines = output.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 5
    values = dict(line.split("=", maxsplit=1) for line in lines)
    assert values["full"] in {"true", "false"}
    assert values["comparison_available"] in {"true", "false"}
    return {
        "full": values["full"] == "true",
        "comparison_available": values["comparison_available"] == "true",
        "lint_files": cast(list[str], json.loads(values["lint_files"])),
        "type_files": cast(list[str], json.loads(values["type_files"])),
        "markdown_files": cast(list[str], json.loads(values["markdown_files"])),
    }


@pytest.mark.parametrize("operation", ["edit", "delete", "rename", "add"])
def test_markdown_only_changes_skip_heavy_jobs(repository: Path, operation: str) -> None:
    base = _git(repository, "rev-parse", "HEAD")
    expected = ["README.md"]
    if operation == "edit":
        _write(repository, "README.md", "# Revised\n")
    elif operation == "delete":
        (repository / "README.md").unlink()
        expected = []
    elif operation == "rename":
        _git(repository, "mv", "README.md", "guide.MARKDOWN")
        expected = ["guide.MARKDOWN"]
    else:
        _write(repository, "guide.Md", "# Added\n")
        expected = ["guide.Md"]
    _commit(repository)

    result = _scope(repository, base)

    assert not result["full"]
    assert result["comparison_available"]
    assert result["lint_files"] == []
    assert result["type_files"] == []
    assert result["markdown_files"] == expected


def test_python_renamed_to_markdown_still_requires_full_ci(repository: Path) -> None:
    base = _git(repository, "rev-parse", "HEAD")
    _git(repository, "mv", "source.py", "source.md")
    _commit(repository)
    assert _git(repository, "diff", "--name-status", base, "HEAD").startswith("R100")

    result = _scope(repository, base)

    assert result["full"]
    assert result["lint_files"] == []
    assert result["type_files"] == []
    assert result["markdown_files"] == ["source.md"]


def test_deleted_python_requires_full_ci_without_linting_missing_path(repository: Path) -> None:
    base = _git(repository, "rev-parse", "HEAD")
    (repository / "source.py").unlink()
    _commit(repository)

    result = _scope(repository, base)

    assert result["full"]
    assert result["lint_files"] == []
    assert result["type_files"] == []


def test_complete_pr_diff_retains_earlier_python_changes(repository: Path) -> None:
    base = _git(repository, "rev-parse", "HEAD")
    _write(repository, "source.py", "value = 2\n")
    _commit(repository)
    _write(repository, "README.md", "# Follow-up documentation\n")
    _commit(repository)

    result = _scope(repository, base)

    assert result["full"]
    assert result["lint_files"] == ["source.py"]
    assert result["type_files"] == ["source.py"]
    assert result["markdown_files"] == ["README.md"]


def test_stacked_pr_uses_its_immediate_base(repository: Path) -> None:
    main = _git(repository, "rev-parse", "HEAD")
    _write(repository, "source.py", "value = 2\n")
    stacked_base = _commit(repository)
    _write(repository, "README.md", "# Child PR\n")
    _commit(repository)

    assert not _scope(repository, stacked_base)["full"]
    assert _scope(repository, main)["full"]


@pytest.mark.parametrize("event", ["push", "workflow_dispatch"])
def test_non_pr_events_always_require_full_ci(repository: Path, event: str) -> None:
    base = _git(repository, "rev-parse", "HEAD")
    _write(repository, "README.md", "# Updated\n")
    _commit(repository)

    assert _scope(repository, base, event=event)["full"]


def test_empty_diff_requires_full_ci(repository: Path) -> None:
    result = _scope(repository, _git(repository, "rev-parse", "HEAD"))

    assert result["full"]
    assert result["comparison_available"]
    assert result["lint_files"] == []


@pytest.mark.parametrize("base", ["", "0" * 40, "missing-reference"])
def test_unknown_comparison_checks_all_tracked_first_party_sources(repository: Path, base: str) -> None:
    for prefix in ("vendor", "tests/test_data", "unsorted", "qgis-resources", "excel-resources"):
        _write(repository, f"{prefix}/excluded.py")
        _write(repository, f"{prefix}/excluded.md", "# Independent content\n")
    _write(repository, "typings/typed module.pyi", "value: int\n")
    _write(repository, "docs/guide.md", "# Guide\n")
    _commit(repository)
    _write(repository, "untracked.py")

    result = _scope(repository, base)

    assert result["full"]
    assert not result["comparison_available"]
    assert result["lint_files"] == ["source.py", "typings/typed module.pyi"]
    assert result["type_files"] == result["lint_files"]
    assert result["markdown_files"] == ["README.md", "docs/guide.md"]


def test_invalid_head_also_falls_back_to_full_tracked_scope(repository: Path) -> None:
    result = _scope(repository, _git(repository, "rev-parse", "HEAD"), head="missing-head")

    assert result["full"]
    assert not result["comparison_available"]
    assert result["lint_files"] == ["source.py"]
    assert result["type_files"] == ["source.py"]


@pytest.mark.parametrize(
    ("config", "lint_all", "type_all"),
    [
        ("ruff.toml", True, False),
        (".ruff.toml", True, False),
        ("pyproject.toml", True, True),
        ("pyrightconfig.json", False, True),
        ("pyright-initialiser.json", False, True),
    ],
)
def test_configuration_changes_expand_the_affected_static_checks(
    repository: Path, config: str, *, lint_all: bool, type_all: bool
) -> None:
    _write(repository, "typings/types.pyi", "value: int\n")
    _write(repository, "vendor/excluded.py")
    base = _commit(repository)
    _write(repository, config, "{}\n" if config.endswith(".json") else "# Changed configuration\n")
    _commit(repository)
    _write(repository, "untracked.py")

    result = _scope(repository, base)
    all_python = ["source.py", "typings/types.pyi"]

    assert result["full"]
    assert result["lint_files"] == (all_python if lint_all else [])
    assert result["type_files"] == (all_python if type_all else [])


@pytest.mark.parametrize(
    ("event", "config", "change_python"),
    [
        ("pull_request", "pyproject.toml", True),
        ("push", "pyproject.toml", True),
        ("pull_request", "packages/local project/pyproject.toml", False),
    ],
)
def test_numeric_version_only_change_keeps_full_ci_and_changed_python_scope(
    repository: Path, event: str, config: str, *, change_python: bool
) -> None:
    _write(repository, config, PROJECT_METADATA)
    base = _commit(repository)
    _write(repository, config, UPDATED_VERSION)
    if change_python:
        _write(repository, "changed.py")
    head = _commit(repository)

    result = _scope(repository, base, event=event, head=head)

    assert result["full"]
    assert result["comparison_available"]
    assert result["lint_files"] == (["changed.py"] if change_python else [])
    assert result["type_files"] == result["lint_files"]


@pytest.mark.parametrize(
    ("config", "old", "new"),
    [
        ("pyproject.toml", "pandas>=2", "pandas>=3"),
        ("packages/local project/pyproject.toml", "line-length = 120", "line-length = 100"),
        ("pyproject.toml", 'requires-python = ">=3.14"', 'requires-python = ">=3.15"'),
        ("pyproject.toml", 'typeCheckingMode = "strict"', 'typeCheckingMode = "basic"'),
        ("pyproject.toml", "flag = 1", "flag = true"),
        ("pyproject.toml", "values = [1.0]", "values = [1]"),
    ],
)
def test_version_bump_never_hides_other_metadata_or_scalar_type_changes(
    repository: Path, config: str, old: str, new: str
) -> None:
    _write(repository, config, PROJECT_METADATA)
    base = _commit(repository)
    _write(repository, config, UPDATED_VERSION.replace(old, new))
    _commit(repository)

    result = _scope(repository, base)

    assert result["full"]
    assert result["lint_files"] == ["source.py"]
    assert result["type_files"] == ["source.py"]


@pytest.mark.parametrize(
    ("before", "after"),
    [
        (None, UPDATED_VERSION),
        (PROJECT_METADATA, None),
        ('[project]\nname = "scope-example"\n', UPDATED_VERSION),
        ('project = "not a table"\n', UPDATED_VERSION),
        ('[project\nversion = "26.11.8.1"\n', UPDATED_VERSION),
        (PROJECT_METADATA, UPDATED_VERSION.replace('version = "26.11.8.2"', "version = 2")),
        (PROJECT_METADATA, UPDATED_VERSION.replace('version = "26.11.8.2"', 'version = ""')),
        (PROJECT_METADATA, UPDATED_VERSION.replace('version = "26.11.8.2"', 'version = "not-a-version"')),
        (PROJECT_METADATA, UPDATED_VERSION.replace("[project]", '[project]\ndynamic = ["version"]')),
    ],
    ids=["added", "deleted", "missing-version", "non-table", "invalid-base", "integer", "empty", "invalid", "dynamic"],
)
def test_uncertain_pyproject_metadata_retains_full_static_scope(
    repository: Path, before: str | None, after: str | None
) -> None:
    config = "pyproject.toml"
    if before is not None:
        _write(repository, config, before)
    base = _commit(repository)
    if after is None:
        (repository / config).unlink()
    else:
        _write(repository, config, after)
    _commit(repository)

    result = _scope(repository, base)

    assert result["full"]
    assert result["comparison_available"]
    assert result["lint_files"] == ["source.py"]
    assert result["type_files"] == ["source.py"]


def test_version_comparison_uses_committed_blobs_not_worktree_metadata(repository: Path) -> None:
    _write(repository, "pyproject.toml", PROJECT_METADATA)
    base = _commit(repository)
    _write(repository, "pyproject.toml", UPDATED_VERSION.replace("pandas>=2", "pandas>=3"))
    head = _commit(repository)
    _write(repository, "pyproject.toml", UPDATED_VERSION)

    result = _scope(repository, base, head=head)

    assert result["full"]
    assert result["lint_files"] == ["source.py"]
    assert result["type_files"] == ["source.py"]


@pytest.mark.parametrize(("before_mode", "after_mode"), [("100644", "100755"), ("120000", "120000")])
def test_version_bump_requires_unchanged_regular_git_file_modes(
    repository: Path, before_mode: str, after_mode: str
) -> None:
    config = "pyproject.toml"
    revisions: list[str] = []
    for mode, content in ((before_mode, PROJECT_METADATA), (after_mode, UPDATED_VERSION)):
        _write(repository, config, content)
        blob = _git(repository, "hash-object", "-w", config)
        # Write only the synthetic index: no Windows symlink privilege is needed.
        _git(repository, "update-index", "--add", "--cacheinfo", mode, blob, config)
        _git(repository, "commit", "--quiet", "-m", "Version metadata mode fixture")
        revisions.append(_git(repository, "rev-parse", "HEAD"))

    result = _scope(repository, revisions[0], head=revisions[1])

    assert result["full"]
    assert result["lint_files"] == ["source.py"]
    assert result["type_files"] == ["source.py"]


@pytest.mark.parametrize("failure", ["unreadable-blob", "invalid-utf8"])
def test_pyproject_blob_read_and_decode_failures_expand_static_checks(repository: Path, failure: str) -> None:
    config = "pyproject.toml"
    _write(repository, config, PROJECT_METADATA)
    base = _commit(repository)
    _write(repository, config, UPDATED_VERSION)
    if failure == "invalid-utf8":
        with (repository / config).open("ab") as file:
            file.write(b"# invalid UTF-8: \xff\n")
    head = _commit(repository)
    if failure == "unreadable-blob":
        blob = _git(repository, "rev-parse", f"{head}:{config}")
        object_path = repository / ".git" / "objects" / blob[:2] / blob[2:]
        object_path.chmod(0o600)
        object_path.unlink()

    result = _scope(repository, base, head=head)

    assert result["full"]
    assert result["comparison_available"]
    assert result["lint_files"] == ["source.py"]
    assert result["type_files"] == ["source.py"]


def test_changed_python_and_stubs_preserve_spaces_and_unicode(repository: Path) -> None:
    base = _git(repository, "rev-parse", "HEAD")
    paths = ["source with spaces.py", "typings/測試 é.pyi"]
    for path in paths:
        _write(repository, path)
    _commit(repository)

    result = _scope(repository, base)

    assert result["full"]
    assert result["lint_files"] == paths
    assert result["type_files"] == paths


def test_excluded_changed_paths_do_not_expand_or_enter_static_scope(repository: Path) -> None:
    base = _git(repository, "rev-parse", "HEAD")
    _write(repository, "vendor/pyproject.toml", "# Independent metadata\n")
    _write(repository, "tests/test_data/example.py")
    _write(repository, "excel-resources/README.md", "# Independent documentation\n")
    _commit(repository)

    result = _scope(repository, base)

    assert result["full"]
    assert result["lint_files"] == []
    assert result["type_files"] == []
    assert result["markdown_files"] == []


def test_missing_repository_cannot_report_successful_empty_validation(tmp_path: Path) -> None:
    output = tmp_path / "outputs.txt"
    env = os.environ | {
        "EVENT_NAME": "pull_request",
        "BASE_SHA": "missing-reference",
        "GITHUB_OUTPUT": str(output),
        "CI_SCOPE_PATH": "",
    }

    result = subprocess.run(
        [sys.executable, str(SCRIPT)], cwd=tmp_path, env=env, check=False, capture_output=True, timeout=30
    )

    assert result.returncode != 0
    assert not output.exists()


@pytest.mark.parametrize("known_comparison", [True, False])
def test_manifest_matches_outputs_at_the_requested_nested_path(repository: Path, *, known_comparison: bool) -> None:
    base = _git(repository, "rev-parse", "HEAD") if known_comparison else "missing-reference"
    _write(repository, "types/測試 with spaces.pyi", "value: int\n")
    _write(repository, "docs/guide.md", "# Current documentation\n")
    _commit(repository)
    manifest = repository.parent / "nested reports" / "scope 測試.json"

    result = _scope(repository, base, manifest=manifest)

    assert result["comparison_available"] is known_comparison
    assert json.loads(manifest.read_text(encoding="utf-8")) == result
    assert "types/測試 with spaces.pyi" in result["type_files"]


def test_manifest_write_failure_cannot_publish_successful_step_outputs(repository: Path) -> None:
    output = repository.parent / "outputs.txt"
    manifest = repository.parent / "already a directory"
    manifest.mkdir()
    env = os.environ | {
        "EVENT_NAME": "pull_request",
        "BASE_SHA": _git(repository, "rev-parse", "HEAD"),
        "GITHUB_SHA": "HEAD",
        "GITHUB_OUTPUT": str(output),
        "CI_SCOPE_PATH": str(manifest),
    }

    result = subprocess.run(
        [sys.executable, str(SCRIPT)], cwd=repository, env=env, check=False, capture_output=True, timeout=30
    )

    assert result.returncode != 0
    assert not output.exists()
