"""Static inventory must not execute scripts or invent lifecycle classifications."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).absolute().parents[1]


def _load_inventory() -> ModuleType:
    source = ROOT / "repo-scripts" / "audit_installed_scripts.py"
    spec = importlib.util.spec_from_file_location("audit_installed_scripts", source)
    if spec is None or spec.loader is None:
        msg = f"Cannot import {source}"
        raise RuntimeError(msg)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


inventory = _load_inventory()


def test_static_audit_records_syntax_signals_without_importing() -> None:
    source = (
        'WRAPPER_VERSION = "1.2.3"\n'
        "import argparse\n"
        "import subprocess\n"
        'DEFAULT_PATH = "C:\\\\work\\\\foo"\n'
        'if __name__ == "__main__":\n'
        "    parser = argparse.ArgumentParser()\n"
        '    parser.add_argument("--no-pause")\n'
    )
    record = inventory.analyse_python(source)
    assert record["parse_status"] == "ok"
    assert record["main_guard"] is True
    assert record["argparse_imported"] is True
    assert record["no_pause_literal"] is True
    assert record["wrapper_version_declared"] is True
    assert record["absolute_drive_literal"] is True
    assert record["subprocess_imported"] is True
    assert record["top_level_call_expressions"] == 0


def test_static_audit_reports_syntax_errors_without_execution() -> None:
    record = inventory.analyse_python("def unfinished(\n")
    assert record["parse_status"] == "syntax_error"
    assert "line" in record["parse_error"]


def test_manifest_catalogue_and_tracked_sources_remain_accounted_for() -> None:
    report = inventory.audit_repository(ROOT)
    assert report["schema_version"] == 1
    assert report["missing_selected"] == []
    assert report["mcp_targets_not_selected"] == []
    assert report["tracked_file_count"] >= report["selected_file_count"]
    assert report["mcp_script_target_count"] > 0
    records = report["files"]
    assert len(records) == report["tracked_file_count"]
    assert len({entry["path"] for entry in records}) == len(records)
    assert all(entry["review_status"] == "static_signals_only" for entry in records)
    culvert = next(entry for entry in records if entry["path"] == "culvert.py")
    assert culvert["in_wheel"] is True
    assert "culvert_analysis_design" in culvert["mcp_workflow_ids"]


def test_import_time_mutation_detection_does_not_execute_script() -> None:
    """A network-copy script must never enter the ordinary wheel allowlist."""
    unguarded = (
        "import shutil\n"
        "from pathlib import Path\n"
        "source = Path('input.dat')\n"
        "shutil.copy2(source, Path('output.dat'))\n"
    )
    unsafe = inventory.analyse_python(unguarded)
    assert unsafe["import_time_mutation_risk"] is True
    assert unsafe["import_time_mutation_calls"] == ["copy2"]
    assert unsafe["main_guard"] is False

    guarded = (
        "import shutil\n"
        "def main():\n"
        "    shutil.copy2('input.dat', 'output.dat')\n"
        "if __name__ == '__main__':\n"
        "    main()\n"
    )
    safe = inventory.analyse_python(guarded)
    assert safe["import_time_mutation_risk"] is False
    assert safe["main_guard"] is True


def test_selected_installed_python_has_guard_and_no_import_time_mutation() -> None:
    """Assess every packaged Python file, not just representative wrappers."""
    report = inventory.audit_repository(ROOT)
    assert report["unsafe_selected"] == []
    records = report["files"]
    installed = [r for r in records if r["in_wheel"] and r["path"].endswith(".py")]
    assert installed
    for record in installed:
        assert record["parse_status"] == "ok", record["path"]
        if not record["path"].endswith("/__init__.py"):
            assert record["main_guard"], record["path"]
        assert not record["import_time_mutation_risk"], record["path"]


def test_import_executing_network_copy_scripts_are_repository_only() -> None:
    """Keep original editable scripts while excluding unsafe import side effects."""
    report = inventory.audit_repository(ROOT)
    for filename in ("file-management-python/copy_file.py", "file-management-python/copy_folder.py"):
        row = next(item for item in report["files"] if item["path"] == filename)
        assert not row["in_wheel"]
        assert not row["mcp_workflow_ids"]
        assert row["main_guard"] is False
        assert row["import_time_mutation_risk"] is True


def test_markdown_audit_lists_every_tracked_source_and_protects_status_words() -> None:
    """Reviewable evidence must cover every file, including non-Python resources."""
    report = inventory.audit_repository(ROOT)
    markdown = inventory.render_markdown(report)
    rows = [line for line in markdown.splitlines() if line.startswith("| `")]
    assert len(rows) == report["tracked_file_count"]
    assert "`file-management-python/copy_file.py` | repo-only" in markdown
    assert "`file-management-python/copy_folder.py` | repo-only" in markdown
    assert "UNSAFE IMPORT" in markdown
    assert "does **not** establish CLI runtime safety" in markdown
    assert str(report["selected_file_count"]) in markdown


def test_only_exact_main_equality_is_a_protected_entry_point() -> None:
    """Inverted and chained comparisons cannot conceal import-time writes."""
    unsafe_conditions = (
        '__name__ != "__main__"',
        '__name__ == "__main__" == "__main__"',
        '__name__ in ("__main__", "imported")',
        'not (__name__ == "__main__")',
    )
    for expression in unsafe_conditions:
        source = f"import shutil\nif {expression}:\n    shutil.copy2('source', 'destination')\n"
        record = inventory.analyse_python(source)
        assert record["main_guard"] is False, expression
        assert record["import_time_mutation_calls"] == ["copy2"], expression

    reversed_check = 'import shutil\nif "__main__" == __name__:\n    shutil.copy2("input", "output")\n'
    safe = inventory.analyse_python(reversed_check)
    assert safe["main_guard"] is True
    assert safe["import_time_mutation_risk"] is False


def test_import_time_else_branch_is_audited_even_with_genuine_main_guard() -> None:
    """The `else` of a real main guard executes when imported."""
    source = (
        "import shutil\n"
        'if __name__ == "__main__":\n'
        '    print("CLI only")\n'
        "else:\n"
        "    shutil.copy2('source', 'destination')\n"
    )
    record = inventory.analyse_python(source)
    assert record["main_guard"] is True
    assert record["import_time_mutation_risk"] is True
    assert record["import_time_mutation_calls"] == ["copy2"]


def test_nested_import_side_effect_is_not_protected_by_inner_equality() -> None:
    """Only the guard body may be excluded; unrelated branches stay visible."""
    source = (
        "import shutil\n"
        "if __name__ != '__main__':\n"
        "    if __name__ == '__main__':\n"
        "        print('impossible')\n"
        "    shutil.copy2('source', 'destination')\n"
    )
    record = inventory.analyse_python(source)
    assert record["main_guard"] is False
    assert record["import_time_mutation_calls"] == ["copy2"]


def test_function_definition_time_mutations_are_flagged_without_execution() -> None:
    """Defaults, decorators and annotations evaluate before the function runs."""
    expressions = (
        "@shutil.copy2('source', 'destination')\ndef method():\n    pass\n",
        "def method(x=shutil.copy2('source', 'destination')):\n    pass\n",
        "async def method(*, x=shutil.copy2('source', 'destination')):\n    pass\n",
        "def method(x: shutil.copy2('source', 'destination')):\n    pass\n",
        "def method() -> shutil.copy2('source', 'destination'):\n    pass\n",
        "value = lambda x=shutil.copy2('source', 'destination'): 3\n",
    )
    for expression in expressions:
        report = inventory.analyse_python("import shutil\n" + expression)
        assert report["parse_status"] == "ok", expression
        assert report["import_time_mutation_calls"] == ["copy2"], expression

    safe = inventory.analyse_python(
        "import shutil\n"
        "def method():\n"
        "    shutil.copy2('source', 'destination')\n"
        "value = lambda: shutil.copy2('source', 'destination')\n"
    )
    assert safe["import_time_mutation_risk"] is False


def test_class_definition_time_mutations_are_flagged_without_execution() -> None:
    """Class bodies and bases execute during import, even for nested methods."""
    expressions = (
        "class Model:\n    shutil.copy2('source', 'destination')\n",
        "@shutil.copy2('source', 'destination')\nclass Model:\n    pass\n",
        "class Model(shutil.copy2('source', 'destination')):\n    pass\n",
        "class Model(metaclass=shutil.copy2('source', 'destination')):\n    pass\n",
        "class Model:\n    def method(self, value=shutil.copy2('source', 'destination')):\n        pass\n",
    )
    for expression in expressions:
        report = inventory.analyse_python("import shutil\n" + expression)
        assert report["parse_status"] == "ok", expression
        assert report["import_time_mutation_calls"] == ["copy2"], expression

    safe = inventory.analyse_python(
        "import shutil\nclass Model:\n    def method(self):\n        shutil.copy2('source', 'destination')\n"
    )
    assert safe["import_time_mutation_risk"] is False


def test_documentation_indexes_track_live_inventory_counts() -> None:
    """Prevent stale 146-file documentation when scripts are added or removed."""
    report = inventory.audit_repository(ROOT)
    tracked = report["tracked_file_count"]
    selected = report["selected_file_count"]
    excluded = tracked - selected
    docs = (ROOT / "docs" / "README.md").read_text(encoding="utf-8")
    work = (ROOT / "docs" / "work" / "README.md").read_text(encoding="utf-8")
    assert f"All {tracked} source paths; {selected} selected and {excluded} excluded" in docs
    assert f"{tracked}-file lifecycle audit" in work


def test_aliased_mutating_import_is_detected_without_execution() -> None:
    """Renaming a dangerous imported function must not evade import checks."""
    unsafe = inventory.analyse_python(
        "from shutil import copy2 as cp\ncp('source', 'destination')\nif __name__ == '__main__':\n    pass\n"
    )
    assert unsafe["main_guard"] is True
    assert unsafe["import_time_mutation_calls"] == ["copy2"]
    assert unsafe["import_time_mutation_risk"] is True

    safe = inventory.analyse_python(
        "from shutil import copy2 as cp\n"
        "def main():\n"
        "    cp('source', 'destination')\n"
        "if __name__ == '__main__':\n"
        "    main()\n"
    )
    assert safe["import_time_mutation_risk"] is False

    # Similarly named local helpers must not be treated as shutil imports.
    unrelated = inventory.analyse_python("def helper():\n    pass\nhelper()\n")
    assert unrelated["import_time_mutation_risk"] is False


def test_postponed_annotations_are_not_misclassified_as_import_effects() -> None:
    """Future-annotations avoid eager annotation calls on Python 3.13."""
    source = (
        "from __future__ import annotations\n"
        "import shutil\n"
        "def f(value: shutil.copy2('a', 'b')) -> shutil.copy2('a', 'b'):\n"
        "    pass\n"
    )
    deferred = inventory.analyse_python(source)
    assert deferred["import_time_mutation_risk"] is False

    eager = inventory.analyse_python(source.replace("from __future__ import annotations\n", ""))
    assert eager["import_time_mutation_calls"] == ["copy2"]

    defaults = inventory.analyse_python(source + "def g(value=shutil.copy2('a', 'b')):\n    pass\n")
    assert defaults["import_time_mutation_calls"] == ["copy2"]


def test_import_time_local_mutator_bindings_are_flagged() -> None:
    """Simple module-level aliases are not a bypass for the static safety gate."""
    cases = (
        "import shutil\ncopier = shutil.copy2\ncopier('a', 'b')\n",
        "from shutil import copy2 as first\nsecond = first\nsecond('a', 'b')\n",
        "import shutil\ncopier = shutil.copy2\nclass A:\n    copier('a', 'b')\n",
    )
    for source in cases:
        record = inventory.analyse_python(source)
        assert record["parse_status"] == "ok"
        assert record["import_time_mutation_calls"] == ["copy2"]

    guarded = inventory.analyse_python(
        "import shutil\ncopier = shutil.copy2\ndef main():\n"
        "    copier('a', 'b')\nif __name__ == '__main__':\n    main()\n"
    )
    assert guarded["import_time_mutation_risk"] is False


def test_annotated_alias_and_rebinding_use_statement_order() -> None:
    unsafe = inventory.analyse_python("import shutil\ncopier: object = shutil.copy2\ncopier('a', 'b')\n")
    assert unsafe["import_time_mutation_calls"] == ["copy2"]

    rebound = inventory.analyse_python("import shutil\ncopier = shutil.copy2\ncopier = print\ncopier('safe')\n")
    assert rebound["import_time_mutation_risk"] is False

    second = inventory.analyse_python("import shutil\ncopier = print\ncopier = shutil.copy2\ncopier('a', 'b')\n")
    assert second["import_time_mutation_calls"] == ["copy2"]


def test_lazy_type_parameter_bounds_do_not_trigger_import_time_mutation() -> None:
    source = "import shutil\ndef f[T: shutil.copy2('a', 'b')]():\n    pass\n"
    record = inventory.analyse_python(source)
    assert record["parse_status"] == "ok"
    assert record["import_time_mutation_risk"] is False


def test_conditional_aliases_preserve_possible_mutations() -> None:
    """Platform branches must not erase an unsafe alias from another branch."""
    unsafe = inventory.analyse_python(
        "import shutil\nimport sys\n"
        "if sys.platform == 'win32':\n"
        "    copier = shutil.copy2\n"
        "else:\n"
        "    copier = print\n"
        "copier('a', 'b')\n"
    )
    assert unsafe["import_time_mutation_calls"] == ["copy2"]

    safe = inventory.analyse_python(
        "import shutil\ncopier = shutil.copy2\nif True:\n    copier = print\nelse:\n    copier = len\ncopier('safe')\n"
    )
    assert safe["import_time_mutation_risk"] is False

    retained = inventory.analyse_python(
        "import shutil\ncopier = shutil.copy2\nif unknown_condition:\n    copier = print\ncopier('a', 'b')\n"
    )
    assert retained["import_time_mutation_calls"] == ["copy2"]


def test_class_alias_scope_does_not_corrupt_module_bindings() -> None:
    """Class-local rebinding must not hide a module-level unsafe callable."""
    unsafe = inventory.analyse_python(
        "import shutil\ncopier = shutil.copy2\nclass Container:\n    copier = print\ncopier('a', 'b')\n"
    )
    assert unsafe["import_time_mutation_calls"] == ["copy2"]

    safe = inventory.analyse_python(
        "import shutil\ncopier = print\nclass Container:\n    copier = shutil.copy2\ncopier('safe')\n"
    )
    assert safe["import_time_mutation_risk"] is False

    class_body = inventory.analyse_python(
        "import shutil\nclass Container:\n    copier = shutil.copy2\n    copier('a', 'b')\n"
    )
    assert class_body["import_time_mutation_calls"] == ["copy2"]



def test_individual_lifecycle_ledger_tracks_manifest_and_mcp_targets() -> None:
    """No tracked source may silently vanish or be mislabeled when manifests change."""
    report = inventory.audit_repository(ROOT)
    ledger = (ROOT / "docs" / "audits" / "2026-10-19-installed-scripts-inventory.md").read_text(encoding="utf-8")
    parsed: dict[str, list[str]] = {}
    for line in ledger.splitlines():
        if not line.startswith("| \`"):
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        assert len(cells) == 6, line
        path = cells[0].strip("\`")
        assert path not in parsed, f"Duplicate source in audit ledger: {path}"
        parsed[path] = cells

    expected = {entry["path"]: entry for entry in report["files"]}
    assert set(parsed) == set(expected), "Ledger must list every tracked source exactly once"
    for path, entry in expected.items():
        cells = parsed[path]
        assert cells[1] == ("Yes" if entry["in_wheel"] else "No"), path
        assert cells[2] == ("Yes" if entry["mcp_workflow_ids"] else "No"), path
        assert all(cells[i] for i in (3, 4, 5)), path
