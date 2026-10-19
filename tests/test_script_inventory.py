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
