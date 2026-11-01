"""Tests for staged MCP workflow discovery."""

from __future__ import annotations

import json
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from ryan_library.mcp import registry as registry_module
from ryan_library.mcp.models import CapabilityProfile, WorkflowSpec
from ryan_library.mcp.registry import WorkflowRegistry, WorkflowRegistryError

PROJECT_ROOT = Path(__file__).resolve().parents[2]
EXPECTED_GDAL_WORKFLOWS = {
    "audit_and_recompress_geotiffs",
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
    # The recompression catalogue includes --replace --yes: it is privileged,
    # even though invoking the wrapper without that flag performs an audit.
    assert workflow_ids == EXPECTED_GDAL_WORKFLOWS - {"audit_and_recompress_geotiffs", "batch_vector_clip"}


def test_privileged_profile_does_not_add_uncatalogued_gdal_wrappers() -> None:
    registry = WorkflowRegistry(
        configured_profile=CapabilityProfile.PRIVILEGED,
        repository_root=PROJECT_ROOT,
    )

    listing = registry.list_workflows(domain="gdal")
    workflow_ids = {workflow["id"] for workflow in listing["workflows"]}
    assert workflow_ids == EXPECTED_GDAL_WORKFLOWS
    clipping = registry.get_workflow("batch_vector_clip")
    assert clipping["profile"] == "privileged"
    assert clipping["mutation"] == "executes_external"
    assert clipping["requires_explicit_approval"] is True
    assert clipping["external_prerequisites"]["status"] == "not_verified"
    assert clipping["external_prerequisites"]["declared_program"] == "ogr2ogr"


def test_repository_workflow_is_unavailable_without_repository_checkout() -> None:
    registry = WorkflowRegistry(discover_repository=False)

    workflow = registry.get_workflow("tuflow_log_summary")

    assert workflow["available"] is False
    assert workflow["execution_kind"] == "script"
    assert "command_prefix" not in workflow
    assert "--no-pause" in workflow["required_headless_arguments"]


def test_culvert_workflow_is_catalogued_with_headless_scenarios() -> None:
    registry = WorkflowRegistry(repository_root=PROJECT_ROOT)

    workflow = registry.get_workflow("culvert_analysis_design")

    assert workflow["available"] is True
    assert workflow["mutation"] == "creates_outputs"
    assert workflow["script_relative_path"] == "ryan-scripts/culvert.py"
    assert {scenario["name"] for scenario in workflow["resolved_scenarios"]} == {
        "analyse_project",
        "compare_project",
        "design_crossing",
        "rating_curve",
        "render_saved_report",
        "solve_scenario",
        "uncertainty_study",
    }
    assert all("--no-pause" in scenario["command"] for scenario in workflow["resolved_scenarios"])


def test_floodway_workflow_is_catalogued_with_headless_assessment() -> None:
    registry = WorkflowRegistry(repository_root=PROJECT_ROOT)

    workflow = registry.get_workflow("floodway_design_assessment")

    assert workflow["available"] is True
    assert workflow["mutation"] == "creates_outputs"
    assert workflow["script_relative_path"] == "ryan-scripts/floodway.py"
    assert {scenario["name"] for scenario in workflow["resolved_scenarios"]} == {"assess_floodway"}
    assert "--no-pause" in workflow["resolved_scenarios"][0]["command"]


def test_tuflow_culvert_native_workflows_are_catalogued_under_create_profile() -> None:
    registry = WorkflowRegistry(repository_root=PROJECT_ROOT)

    expected = {
        "tuflow_culvert_evaluate_maximums": "ryan-scripts/tuflow/tuflow_culvert_from_maximums.py",
        "tuflow_culvert_evaluate_1d_nwk": "ryan-scripts/tuflow/tuflow_culvert_from_1d_nwk.py",
    }
    for workflow_id, script_path in expected.items():
        workflow = registry.get_workflow(workflow_id)

        assert workflow["available"] is True
        assert workflow["profile"] == "create"
        assert workflow["mutation"] == "creates_or_replaces"
        assert workflow["requires_explicit_approval"] is False
        assert workflow["script_relative_path"] == script_path
        assert workflow["metadata"]["overwrite_flag"] == "--overwrite"
        assert workflow["metadata"]["overwrite_default"] is False
        assert workflow["metadata"]["engine_modes"] == ["ryan-culverts"]
        scenarios = {scenario["name"]: scenario["command"] for scenario in workflow["resolved_scenarios"]}
        assert set(scenarios) == {"ryan_culverts"}
        assert "--engine" in scenarios["ryan_culverts"]
        assert "ryan-culverts" in scenarios["ryan_culverts"]

    visible = {workflow["id"] for workflow in registry.list_workflows(domain="tuflow")["workflows"]}
    assert "tuflow_culvert_evaluate_maximums_hy8" not in visible
    assert "tuflow_culvert_evaluate_1d_nwk_hy8" not in visible


def test_tuflow_culvert_hy8_workflows_require_privileged_profile_and_approval() -> None:
    registry = WorkflowRegistry(
        configured_profile=CapabilityProfile.PRIVILEGED,
        repository_root=PROJECT_ROOT,
    )

    expected = {
        "tuflow_culvert_evaluate_maximums_hy8": "ryan-scripts/tuflow/tuflow_culvert_from_maximums.py",
        "tuflow_culvert_evaluate_1d_nwk_hy8": "ryan-scripts/tuflow/tuflow_culvert_from_1d_nwk.py",
    }
    for workflow_id, script_path in expected.items():
        workflow = registry.get_workflow(workflow_id)

        assert workflow["available"] is True
        assert workflow["profile"] == "privileged"
        assert workflow["mutation"] == "executes_external"
        assert workflow["requires_explicit_approval"] is True
        assert workflow["script_relative_path"] == script_path
        assert workflow["metadata"]["overwrite_flag"] == "--overwrite"
        assert workflow["metadata"]["overwrite_default"] is False
        assert workflow["metadata"]["engine_modes"] == ["hy8"]
        assert workflow["metadata"]["output_mutation"] == "creates_or_replaces"
        assert workflow["metadata"]["external_program"] == "HY-8"
        scenarios = {scenario["name"]: scenario["command"] for scenario in workflow["resolved_scenarios"]}
        assert set(scenarios) == {"hy8"}
        assert "--engine" in scenarios["hy8"]
        assert "hy8" in scenarios["hy8"]
        assert "--hy8-exe" in scenarios["hy8"]


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
    assert "CLI script is unavailable" in missing_details["error"]


def test_packaged_gdal_catalogue_is_available_without_repository_checkout() -> None:
    registry = WorkflowRegistry(discover_repository=False)

    listing = registry.list_workflows(domain="gdal", include_unavailable=True)
    workflows = {workflow["id"]: workflow for workflow in listing["workflows"]}

    assert set(workflows) == EXPECTED_GDAL_WORKFLOWS - {"audit_and_recompress_geotiffs", "batch_vector_clip"}
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
        "script": "ryan-scripts/script.py",
        "profile": "analysis",
        "mutation": "read_only",
    }

    with pytest.raises(ValueError, match="exactly one execution target"):
        WorkflowSpec.from_mapping(raw)


def test_installed_scripts_work_without_checkout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from ryan_library.mcp import registry as registry_module

    scripts = tmp_path / "ryan-scripts"
    wrapper = scripts / "TUFLOW-python" / "log_processing" / "create_log_summary_report.py"
    wrapper.parent.mkdir(parents=True)
    wrapper.write_text('WRAPPER_VERSION = "installed"\n', encoding="utf-8")
    monkeypatch.setattr(registry_module, "installed_scripts_directory", lambda: scripts)
    registry = WorkflowRegistry(discover_repository=False)
    workflow = registry.get_workflow("tuflow_log_summary")
    assert registry.repository_root is None
    assert workflow["available"] is True
    assert workflow["command_prefix"] == [sys.executable, str(wrapper)]
    assert workflow["wrapper_version"] == "installed"
    assert registry.list_workflows()["scripts_directory"] == str(scripts)
    assert not registry.warnings


def test_explicit_checkout_precedes_installed_scripts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from ryan_library.mcp import registry as registry_module

    scripts = tmp_path / "ryan-scripts"
    scripts.mkdir()
    monkeypatch.setattr(registry_module, "installed_scripts_directory", lambda: scripts)
    registry = WorkflowRegistry(repository_root=PROJECT_ROOT, discover_repository=False)
    workflow = registry.get_workflow("culvert_analysis_design")
    assert workflow["script_path"] == str(PROJECT_ROOT / "ryan-scripts" / "culvert.py")


def test_culvert_workflow_relies_on_installed_distribution_dependency(monkeypatch: pytest.MonkeyPatch) -> None:
    from importlib.machinery import ModuleSpec

    from ryan_library.mcp import registry as registry_module

    original = registry_module.find_spec

    def without_solver(module: str) -> ModuleSpec | None:
        if module == "culvert_solver":
            pytest.fail("Script discovery must not probe the mandatory culvert dependency")
        return original(module)

    monkeypatch.setattr(registry_module, "find_spec", without_solver)
    registry = WorkflowRegistry(repository_root=PROJECT_ROOT)
    detail = registry.get_workflow("culvert_analysis_design")
    assert detail["available"] is True
    assert "required_modules" not in detail
    assert "missing_modules" not in detail
    assert "command_prefix" in detail


def test_workflow_reports_provenance_and_declared_hy8_prerequisite() -> None:
    registry = WorkflowRegistry(configured_profile=CapabilityProfile.PRIVILEGED, repository_root=PROJECT_ROOT)
    workflow = registry.get_workflow("tuflow_culvert_evaluate_1d_nwk_hy8")
    assert workflow["script_source"] == "repository_checkout"
    assert workflow["availability_scope"] == "entry_point_only"
    assert workflow["external_prerequisites"]["status"] == "not_verified"
    assert workflow["external_prerequisites"]["declared_program"] == "HY-8"
    assert workflow["requires_explicit_approval"] is True


def test_tuflow_external_prerequisites_are_declared_without_claiming_readiness() -> None:
    registry = WorkflowRegistry(configured_profile=CapabilityProfile.PRIVILEGED, repository_root=PROJECT_ROOT)
    for workflow_id, program in (
        ("run_tuflow_batch", "TUFLOW executable configured by wrapper"),
        ("tuflow_asc2asc_groups", "asc_to_asc_w64.exe"),
    ):
        prerequisite = registry.get_workflow(workflow_id)["external_prerequisites"]
        assert prerequisite["status"] == "not_verified"
        assert prerequisite["declared_program"] == program


def test_script_provenance_reports_missing_checkout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from ryan_library.mcp import registry as registry_module

    scripts = tmp_path / "ryan-scripts"
    scripts.mkdir()
    monkeypatch.setattr(registry_module, "installed_scripts_directory", lambda: scripts)
    registry = WorkflowRegistry(discover_repository=False)
    detail = registry.get_workflow("culvert_analysis_design")
    assert detail["script_source"] == "installed_distribution"
    assert detail["availability_scope"] == "entry_point_only"
    assert detail["available"] is False
    assert detail["installed_distribution_version"] is None or isinstance(detail["installed_distribution_version"], str)


def _keyword_workflow_mapping() -> dict[str, object]:
    return {
        "id": "curated_example",
        "title": "Curated Example",
        "domain": "test",
        "purpose": "Exercise literal search terms.",
        "script": "ryan-scripts/example.py",
        "profile": "analysis",
        "mutation": "read_only",
    }


def test_keyword_model_default_normalization_and_metadata() -> None:
    raw = _keyword_workflow_mapping()
    default = WorkflowSpec.from_mapping(raw)
    assert default.keywords == ()
    assert "keywords" not in default.metadata

    raw["keywords"] = [" Crop ", "CROP", "cropping", "two words", " Two Words "]
    workflow = WorkflowSpec.from_mapping(raw)
    assert workflow.keywords == ("Crop", "cropping", "two words")
    assert "keywords" not in workflow.metadata
    assert replace(workflow, keywords=("  Pyramids ", "PYRAMIDS")).keywords == ("Pyramids",)


def test_workflow_spec_preserves_legacy_positional_metadata_argument() -> None:
    """Adding curated keywords must not displace the existing metadata position."""
    reference = WorkflowSpec.from_mapping(_keyword_workflow_mapping())
    metadata = {"legacy_setting": True}
    workflow = WorkflowSpec(
        reference.workflow_id,
        reference.title,
        reference.domain,
        reference.purpose,
        reference.execution,
        reference.profile,
        reference.mutation,
        reference.lifecycle,
        reference.requires_explicit_approval,
        reference.headless_arguments,
        metadata,
    )
    assert workflow.metadata is metadata
    assert workflow.keywords == ()


@pytest.mark.parametrize("bad_keywords", [None, "crop", 123, ["  "], ["crop", None], ["crop", 2], ("crop",)])
def test_generic_keyword_catalogue_validation_rejects_bad_values(bad_keywords: object) -> None:
    raw = _keyword_workflow_mapping()
    raw["keywords"] = bad_keywords
    with pytest.raises(ValueError, match=r"curated_example.*keywords"):
        WorkflowSpec.from_mapping(raw)


@pytest.mark.parametrize("bad_keywords", [None, "crop", 123, ["  "], ["crop", None], ["crop", 2]])
def test_gdal_keyword_catalogue_validation_rejects_bad_values(
    bad_keywords: object, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "workflows.json").write_text(json.dumps({"schema_version": "1.0", "workflows": []}), encoding="utf-8")
    (tmp_path / "gdal_cli_tools.json").write_text(
        json.dumps(
            {
                "tools": [
                    {
                        "id": "gdal_test_example",
                        "script": "example.py",
                        "purpose": "Exercise invalid GDAL keyword loading.",
                        "keywords": bad_keywords,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    def catalogue_files(_package: str) -> Path:
        return tmp_path

    monkeypatch.setattr(registry_module, "files", catalogue_files)
    with pytest.raises(ValueError, match=r"gdal_test_example.*keywords"):
        WorkflowRegistry(discover_repository=False)


def test_gdal_catalogue_normalizes_nonempty_json_arrays(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Both catalogue loaders use the same normalization after JSON decoding."""
    (tmp_path / "workflows.json").write_text(json.dumps({"schema_version": "1.0", "workflows": []}), encoding="utf-8")
    (tmp_path / "gdal_cli_tools.json").write_text(
        json.dumps(
            {
                "tools": [
                    {
                        "id": "gdal_test_example",
                        "script": "example.py",
                        "purpose": "Exercise GDAL normalization.",
                        "keywords": [" Crop ", "CROP", "cropping", " two words ", "Two Words"],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    def catalogue_files(_package: str) -> Path:
        return tmp_path

    monkeypatch.setattr(registry_module, "files", catalogue_files)
    registry = WorkflowRegistry(discover_repository=False)
    assert registry.get_workflow("gdal_test_example")["keywords"] == ["Crop", "cropping", "two words"]
    matches = registry.list_workflows(query="cropping", include_unavailable=True)
    assert [item["id"] for item in matches["workflows"]] == ["gdal_test_example"]


def test_gdal_and_generic_keyword_discovery_preserves_canonical_ids() -> None:
    registry = WorkflowRegistry(repository_root=PROJECT_ROOT)
    expected_queries = (
        ("gdal", "CrOp", "clip_rasters_to_polygon"),
        ("GDAL", "CROPPING", "clip_rasters_to_polygon"),
        ("gdal", "pyramids", "build_external_overviews"),
        ("tuflow", "LoGs", "tuflow_log_summary"),
    )
    for domain, query, expected_id in expected_queries:
        result = registry.list_workflows(domain=domain, query=query, include_unavailable=True)
        assert expected_id in {item["id"] for item in result["workflows"]}
        assert len(result["workflows"]) == len({item["id"] for item in result["workflows"]})

    result = registry.list_workflows(domain="gdal", query="  CrOp   RASTER ", include_unavailable=True)
    assert [item["id"] for item in result["workflows"]] == ["clip_rasters_to_polygon"]
    assert registry.list_workflows(domain="gd", query="crop", include_unavailable=True)["workflow_count"] == 0
    assert (
        registry.list_workflows(domain="gdal", query="crop translate", include_unavailable=True)["workflow_count"] == 0
    )
    assert registry.list_workflows(query="cropp", include_unavailable=True)["workflow_count"] >= 1
    assert registry.list_workflows(query="nonexistent-phrase-xyz", include_unavailable=True)["workflow_count"] == 0
    assert "error" in registry.get_workflow("crop")

    detail = registry.get_workflow("clip_rasters_to_polygon")
    assert detail["keywords"] == ["crop", "cropping"]
    assert detail["metadata"].get("keywords") is None
    assert registry.get_workflow("build_external_overviews")["keywords"] == ["pyramids"]
    assert registry.get_workflow("tuflow_log_summary")["keywords"] == ["logs"]
    normal = registry.list_workflows(domain="gdal", query="crop", include_unavailable=True)["workflows"][0]
    assert "keywords" not in normal
    assert "command_prefix" not in normal
    full = registry.catalogue()
    assert len(full["workflows"]) == full["workflow_count"]
    assert full["workflow_count"] == registry.list_workflows(include_unavailable=True)["workflow_count"]
    assert next(item for item in full["workflows"] if item["id"] == "clip_rasters_to_polygon")["keywords"] == [
        "crop",
        "cropping",
    ]


def test_registry_overview_aggregates_filtered_full_set_and_bounded_examples() -> None:
    registry = WorkflowRegistry(configured_profile=CapabilityProfile.PRIVILEGED, repository_root=PROJECT_ROOT)
    for profile in CapabilityProfile:
        full = registry.list_workflows(maximum_profile=profile, include_unavailable=True)
        overview = registry.list_workflows(maximum_profile=profile, include_unavailable=True, overview=True)
        assert overview["overview"] is True
        assert overview["workflow_count"] == full["workflow_count"]
        assert overview["domain_count"] == len(overview["domains"])
        assert overview["domains"] == sorted(overview["domains"])
        assert "workflows" not in overview
        assert [row["domain"] for row in overview["domain_overview"]] == overview["domains"]
        assert sum(row["workflow_count"] for row in overview["domain_overview"]) == full["workflow_count"]
        for row in overview["domain_overview"]:
            matching = [item for item in full["workflows"] if item["domain"] == row["domain"]]
            assert row["workflow_count"] == len(matching)
            assert row["examples"] == [
                {"id": item["id"], "title": item["title"], "available": item["available"]} for item in matching[:3]
            ]
            assert all(set(example) == {"id", "title", "available"} for example in row["examples"])

    create = registry.list_workflows(maximum_profile="create", include_unavailable=True, overview=True)
    assert "gdal" in create["domains"]
    assert "run_tuflow_batch" not in {example["id"] for row in create["domain_overview"] for example in row["examples"]}
    assert (
        registry.list_workflows(maximum_profile="core", overview=True, include_unavailable=True)["workflow_count"] == 0
    )
    with pytest.raises(WorkflowRegistryError, match="overview must be a boolean"):
        registry.list_workflows(overview=1)  # type: ignore[arg-type]


def test_registry_overview_filters_query_domain_and_availability_before_counting() -> None:
    registry = WorkflowRegistry(discover_repository=False)
    full = registry.list_workflows(domain="GDAL", query="CROP", include_unavailable=True)
    overview = registry.list_workflows(domain="GDAL", query="CROP", include_unavailable=True, overview=True)
    assert overview["workflow_count"] == full["workflow_count"] == 1
    assert overview["domains"] == ["gdal"]
    assert overview["domain_overview"][0]["examples"][0]["id"] == "clip_rasters_to_polygon"

    unavailable = registry.list_workflows(domain="gdal", query="crop", include_unavailable=False, overview=True)
    assert unavailable["workflow_count"] == 0
    assert unavailable["domains"] == []
    assert unavailable["domain_overview"] == []
    assert unavailable["domain_count"] == 0
    unknown = registry.list_workflows(query="not-found-anywhere-xyz", overview=True, include_unavailable=True)
    assert unknown["workflow_count"] == 0
    assert unknown["domain_overview"] == []
