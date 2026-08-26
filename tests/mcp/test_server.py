"""Tests for MCP server helper responses."""

from __future__ import annotations

import json

from ryan_library.mcp.server import check_repo_health, get_workflow, list_workflows, workflow_catalogue_resource


def test_health_reports_installed_distribution_and_profile() -> None:
    health = check_repo_health()

    assert health["ryan_library_version"] != "0.1.0"
    assert health["configured_profile"] == "create"
    assert health["catalogue_schema_version"] == "1.0"
    assert health["workflow_count_visible"] > 0


def test_generic_discovery_returns_cli_metadata() -> None:
    listing = list_workflows(domain="tuflow")
    assert "error" not in listing
    assert listing["workflow_count"] > 0

    workflow = get_workflow("tuflow_log_summary")
    assert workflow["available"] is True
    assert workflow["help_command"][-1] == "--help"
    assert "execution_note" in workflow


def test_catalogue_resource_is_valid_json() -> None:
    catalogue = json.loads(workflow_catalogue_resource())

    assert catalogue["configured_profile"] == "create"
    assert catalogue["schema_version"] == "1.0"
