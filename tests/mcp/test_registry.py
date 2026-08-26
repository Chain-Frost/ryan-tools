"""Tests for staged MCP workflow discovery."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from ryan_library.mcp.models import CapabilityProfile
from ryan_library.mcp.registry import WorkflowRegistry, WorkflowRegistryError

PROJECT_ROOT = Path(__file__).resolve().parents[2]


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
    assert "translate_rasters_to_geotiff" in workflow_ids
    assert "set_raster_nodata" not in workflow_ids


def test_privileged_profile_exposes_in_place_gdal_workflows() -> None:
    registry = WorkflowRegistry(
        configured_profile=CapabilityProfile.PRIVILEGED,
        repository_root=PROJECT_ROOT,
    )

    listing = registry.list_workflows(domain="gdal")
    workflow_ids = {workflow["id"] for workflow in listing["workflows"]}
    assert "set_raster_nodata" in workflow_ids


def test_get_workflow_resolves_current_python_and_script() -> None:
    registry = WorkflowRegistry(repository_root=PROJECT_ROOT)

    workflow = registry.get_workflow("tuflow_log_summary")

    assert workflow["available"] is True
    assert workflow["command_prefix"][0] == sys.executable
    assert Path(workflow["script_path"]).samefile(PROJECT_ROOT / "ryan-scripts/TUFLOW-python/LogSummary.py")
    assert workflow["help_command"][-1] == "--help"
    assert "--no-pause" in workflow["required_headless_arguments"]


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
