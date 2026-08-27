"""Tests for staged MCP workflow discovery."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from ryan_library.mcp.models import CapabilityProfile, WorkflowSpec
from ryan_library.mcp.registry import WorkflowRegistry, WorkflowRegistryError

PROJECT_ROOT = Path(__file__).resolve().parents[2]
EXPECTED_GDAL_WORKFLOWS = {
    "batch_vector_clip",
    "build_external_overviews",
    "calculate_stage_storage",
    "clip_rasters_to_polygon",
    "create_raster_footprints",
    "split_vector_by_attribute",
    "translate_rasters_to_geotiff",
}


def test_default_profile_is_create(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RYAN_MCP_PROFILE", raising=False)
    registry = WorkflowRegistry(repository_root=PROJECT_ROOT)

    assert registry.configured_profile is CapabilityProfile.CREATE
    listing = registry.list_workflows()
    workflow_ids = {workflow["id"] for workflow in listing["workflows"]}
    assert "tuflow_log_summary" in workflow_ids
    assert "tuflow_log_summary_append_master" not in workflow_ids


def test_gdal_catalogue_is_folded_into_generic_workflows() -> None:
    registry = WorkflowRegistry(repository_root=PROJECT_ROOT)

    listing = registry.list_workflows(domain="gdal")
    workflow_ids = {workflow["id"] for workflow in listing["workflows"]}
    assert workflow_ids == EXPECTED_GDAL_WORKFLOWS


def test_privileged_profile_does_not_add_uncatalogued_gdal_wrappers() -> None:
    registry = WorkflowRegistry(
        configured_profile=CapabilityProfile.PRIVILEGED,
        repository_root=PROJECT_ROOT,
    )

    listing = registry.list_workflows(domain="gdal")
    workflow_ids = {workflow["id"] for workflow in listing["workflows"]}
    assert workflow_ids == EXPECTED_GDAL_WORKFLOWS


def test_repository_workflow_is_unavailable_without_repository_checkout() -> None:
    registry = WorkflowRegistry(discover_repository=False)

    workflow = registry.get_workflow("tuflow_log_summary")

    assert workflow["available"] is False
    assert workflow["execution_kind"] == "script"
    assert "command_prefix" not in workflow
    assert "--no-pause" in workflow["required_headless_arguments"]


def test_repository_script_fallback_resolves_current_relocated_path() -> None:
    registry = WorkflowRegistry(repository_root=PROJECT_ROOT)

    workflow = registry.get_workflow("tuflow_log_summary")

    expected_path = PROJECT_ROOT / "ryan-scripts/TUFLOW-python/log_processing/create_log_summary_report.py"
    assert workflow["available"] is True
    assert workflow["execution_kind"] == "script"
    assert workflow["command_prefix"] == [sys.executable, str(expected_path)]
    assert Path(workflow["script_path"]).samefile(expected_path)


def test_configured_profile_cannot_be_escalated_by_tool_input() -> None:
    registry = WorkflowRegistry(
        configured_profile=CapabilityProfile.ANALYSIS,
        repository_root=PROJECT_ROOT,
    )

    with pytest.raises(WorkflowRegistryError, match="exceeds configured profile"):
        registry.list_workflows(maximum_profile=CapabilityProfile.CREATE)


def test_packaged_workflow_paths_exist_in_checkout() -> None:
    registry = WorkflowRegistry(
        configured_profile=CapabilityProfile.PRIVILEGED,
        repository_root=PROJECT_ROOT,
    )

    listing = registry.list_workflows(include_unavailable=True)
    missing = [workflow["id"] for workflow in listing["workflows"] if not workflow["available"]]
    assert missing == []


def test_missing_repository_script_does_not_hide_working_module() -> None:
    registry = WorkflowRegistry(discover_repository=False)

    listing = registry.list_workflows(include_unavailable=True)
    missing_details = registry.get_workflow("tuflow_log_summary")

    assert listing["workflow_count"] > 0
    assert missing_details["available"] is False
    assert "Repository CLI script is unavailable" in missing_details["error"]


def test_packaged_gdal_catalogue_is_available_without_repository_checkout() -> None:
    registry = WorkflowRegistry(discover_repository=False)

    listing = registry.list_workflows(domain="gdal", include_unavailable=True)
    workflows = {workflow["id"]: workflow for workflow in listing["workflows"]}

    assert set(workflows) == EXPECTED_GDAL_WORKFLOWS
    assert workflows["translate_rasters_to_geotiff"]["available"] is False
    assert workflows["translate_rasters_to_geotiff"]["execution_kind"] == "script"


def test_profile_filtering_is_identical_for_module_and_script_targets() -> None:
    registry = WorkflowRegistry(
        configured_profile=CapabilityProfile.CREATE,
        repository_root=PROJECT_ROOT,
    )

    visible = {workflow["id"] for workflow in registry.list_workflows()["workflows"]}

    assert "tuflow_log_summary" in visible
    assert "tuflow_pomm_combine" in visible
    assert "tuflow_log_summary_append_master" not in visible
    assert "set_raster_nodata" not in visible


def test_workflow_requires_exactly_one_explicit_execution_target() -> None:
    raw = {
        "id": "ambiguous",
        "title": "Ambiguous",
        "domain": "test",
        "purpose": "Exercise target validation.",
        "module": "package.module",
        "script": "script.py",
        "profile": "analysis",
        "mutation": "read_only",
    }

    with pytest.raises(ValueError, match="exactly one execution target"):
        WorkflowSpec.from_mapping(raw)
