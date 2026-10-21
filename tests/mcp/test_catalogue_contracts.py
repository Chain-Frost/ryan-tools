"""MCP catalogue contracts independent of proprietary applications.

These tests verify discovery and safety metadata; they do not execute engineering tools
or establish installed-wheel/external-application readiness.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from ryan_library.mcp import registry as registry_module
from ryan_library.mcp.installed_scripts import installed_files
from ryan_library.mcp.models import CapabilityProfile, ExecutionTarget
from ryan_library.mcp.registry import WorkflowRegistry


def _all_catalogue_scripts() -> dict[str, str]:
    registry = WorkflowRegistry(configured_profile=CapabilityProfile.PRIVILEGED, discover_repository=False)
    listing = registry.list_workflows(include_unavailable=True)
    return {
        item["id"]: item["execution_target"]
        for item in listing["workflows"]
        if item["execution_kind"] == "script"
    }


def test_all_curated_script_targets_are_in_the_wheel_selection() -> None:
    targets = _all_catalogue_scripts()
    selected = {f"ryan-scripts/{path}" for path in installed_files()}

    # 35 workflow records share 33 script files: two HY-8/native pairs.
    assert len(targets) == 35
    assert len(set(targets.values())) == 33
    assert set(targets.values()) <= selected
    assert all(value.startswith("ryan-scripts/") for value in targets.values())


@pytest.mark.parametrize(
    "bad_path",
    [
        "../outside.py",
        "ryan-scripts/../outside.py",
        "ryan-scripts/../../outside.py",
        "/tmp/ryan-scripts/test.py",
        "C:/ryan-scripts/test.py",
        r"C:\\ryan-scripts\\test.py",
        r"ryan-scripts\\..\\outside.py",
        "ryan-scripts/a//b.py",
        "ryan-scripts/./test.py",
        "ryan-scripts/",
    ],
)
def test_unsafe_script_targets_are_rejected_on_every_platform(bad_path: str) -> None:
    with pytest.raises(ValueError, match="relative ryan-scripts path"):
        ExecutionTarget.from_mapping({"script": bad_path})


def test_legitimate_nested_script_target_remains_supported() -> None:
    target = ExecutionTarget.from_mapping({"script": "ryan-scripts/TUFLOW-python/log_processing/report.py"})
    assert target.value == "ryan-scripts/TUFLOW-python/log_processing/report.py"


def test_synthetic_installed_wheel_layout_discovers_all_targets_without_checkout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # This emulates the sibling site-packages layout, not a true pip-installed wheel.
    # The CI wheel smoke and the Windows acceptance checklist cover that separate gate.
    scripts_root = tmp_path / "site packages - 測試" / "ryan-scripts"
    for target in set(_all_catalogue_scripts().values()):
        path = scripts_root / Path(target).relative_to("ryan-scripts")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('WRAPPER_VERSION = "synthetic-only"\n', encoding="utf-8")

    monkeypatch.setattr(registry_module, "installed_scripts_directory", lambda: scripts_root)
    registry = WorkflowRegistry(configured_profile=CapabilityProfile.PRIVILEGED, discover_repository=False)
    listing = registry.list_workflows(include_unavailable=True)

    assert registry.repository_root is None
    assert listing["workflow_count"] == 35
    assert all(item["available"] for item in listing["workflows"])
    for workflow in listing["workflows"]:
        details = registry.get_workflow(workflow["id"])
        assert details["script_source"] == "installed_distribution"
        assert details["availability_scope"] == "entry_point_only"
        assert details["command_prefix"][0] == sys.executable
        assert str(scripts_root) in details["command_prefix"][1]
        assert details["wrapper_version"] == "synthetic-only"
        assert details["external_prerequisites"]["status"] != "verified"


def test_external_execution_never_implies_validated_application() -> None:
    registry = WorkflowRegistry(configured_profile=CapabilityProfile.PRIVILEGED, discover_repository=False)
    listing = registry.list_workflows(include_unavailable=True)
    external_ids = {item["id"] for item in listing["workflows"] if item["mutation"] == "executes_external"}

    assert {
        "tuflow_culvert_evaluate_maximums_hy8",
        "tuflow_culvert_evaluate_1d_nwk_hy8",
        "tuflow_asc2asc_groups",
        "run_tuflow_batch",
    } <= external_ids
    for workflow_id in external_ids:
        details = registry.get_workflow(workflow_id)
        assert details["profile"] == "privileged"
        assert details["requires_explicit_approval"] is True
        assert details["external_prerequisites"]["status"] in {"not_verified", "not_declared"}
        assert details["availability_scope"] == "entry_point_only"


def test_replace_and_delete_workflows_require_privileged_approval() -> None:
    registry = WorkflowRegistry(configured_profile=CapabilityProfile.PRIVILEGED, discover_repository=False)
    ids = {
        "audit_and_recompress_geotiffs",
        "tuflow_log_summary_append_master",
        "delete_duplicate_tuflow_files",
        "remove_excel_protection",
    }
    for workflow_id in ids:
        details = registry.get_workflow(workflow_id)
        assert details["profile"] == "privileged"
        assert details["requires_explicit_approval"] is True
    assert registry.get_workflow("audit_and_recompress_geotiffs")["mutation"] == "in_place"


def test_default_profile_hides_all_in_place_deleting_and_external_workflows() -> None:
    registry = WorkflowRegistry(configured_profile=CapabilityProfile.CREATE, discover_repository=False)
    listing = registry.list_workflows(include_unavailable=True)
    assert all(
        item["mutation"] not in {"in_place", "deletes", "executes_external"}
        for item in listing["workflows"]
    )


def test_unsafe_legacy_entrypoints_never_advertise_a_help_command(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "site-packages" / "ryan-scripts"
    targets = _all_catalogue_scripts()
    for workflow_id in ("run_tuflow_batch", "delete_duplicate_tuflow_files"):
        wrapper = root / Path(targets[workflow_id]).relative_to("ryan-scripts")
        wrapper.parent.mkdir(parents=True, exist_ok=True)
        wrapper.write_text("# synthetic and never run\\n", encoding="utf-8")

    monkeypatch.setattr(registry_module, "installed_scripts_directory", lambda: root)
    registry = WorkflowRegistry(configured_profile=CapabilityProfile.PRIVILEGED, discover_repository=False)
    for workflow_id in ("run_tuflow_batch", "delete_duplicate_tuflow_files"):
        details = registry.get_workflow(workflow_id)
        assert details["available"] is True
        assert details["profile"] == "privileged"
        assert details["requires_explicit_approval"] is True
        assert details["metadata"]["help_safe"] is False
        assert details["help_command"] is None
