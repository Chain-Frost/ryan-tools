"""Tests for MCP server helper responses."""

from __future__ import annotations

import json
from pathlib import Path

from ryan_library.mcp.server import (
    MAX_COLLECTION_FILES,
    check_repo_health,
    get_workflow,
    inspect_tuflow_collection,
    inspect_tuflow_result,
    list_workflows,
    tuflow_processor_guidance_resource,
    workflow_catalogue_resource,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MODULE_11_RESULTS = PROJECT_ROOT / "tests/test_data/tuflow/tutorials/Module_11/results"
MODULE_11_LOGS = PROJECT_ROOT / "tests/test_data/tuflow/tutorials/Module_11/runs/log"


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


def test_inspect_tuflow_result_uses_processor_factory_and_bounds_sample() -> None:
    result_path = MODULE_11_RESULTS / "plot/csv/M11_5m_001_1d_Q.csv"

    result = inspect_tuflow_result(file_path=str(result_path), sample_rows=3)

    assert "error" not in result
    assert result["processor"] == "QProcessor"
    assert result["data_type"] == "Q"
    assert result["dataformat"] == "Timeseries"
    assert result["processed"] is True
    assert result["row_count"] > 3
    assert result["sample_row_count"] == 3
    assert result["sample_truncated"] is True
    assert len(result["sample"]) == 3
    assert "Time" in result["columns"]


def test_inspect_tuflow_result_filters_location_and_processed_time() -> None:
    result_path = MODULE_11_RESULTS / "plot/csv/M11_5m_001_1d_Q.csv"

    result = inspect_tuflow_result(
        file_path=str(result_path),
        locations=["ds1"],
        minimum_time=1.0,
        maximum_time=2.0,
        sample_rows=3,
    )

    assert "error" not in result
    assert result["applied_entity_filter"] == ["ds1"]
    assert result["time_filter"] == {
        "minimum_time": 1.0,
        "maximum_time": 2.0,
        "applied": True,
        "row_count_before": 181,
        "row_count_after": 61,
    }
    assert {row["Chan ID"] for row in result["sample"]} == {"ds1"}
    assert all(1.0 <= row["Time"] <= 2.0 for row in result["sample"])


def test_inspect_tuflow_result_supports_tlf_processor() -> None:
    log_path = MODULE_11_LOGS / "M11_5m_001.tlf"

    result = inspect_tuflow_result(file_path=str(log_path), sample_rows=1)

    assert "error" not in result
    assert result["processor"] == "TLFProcessor"
    assert result["data_type"] == "TLF"
    assert result["row_count"] == 1
    assert result["sample"][0]["EndStatus"] == "Simulation FINISHED"


def test_inspect_tuflow_collection_summarizes_processors_and_combined_sample() -> None:
    file_paths = [
        str(MODULE_11_LOGS / "M11_5m_001.tlf"),
        str(MODULE_11_LOGS / "M11_5m_002.tlf"),
    ]

    result = inspect_tuflow_collection(
        file_paths=file_paths,
        sample_rows=1,
        include_combined_sample=True,
    )

    assert "error" not in result
    assert result["requested_file_count"] == 2
    assert result["processed_file_count"] == 2
    assert result["collection_processor_count"] == 2
    assert result["data_type_counts"] == {"TLF": 2}
    assert result["dataformat_counts"] == {"TLF": 2}
    assert result["duplicate_groups"] == []
    assert result["errors"] == []
    assert result["combined"]["row_count"] == 2
    assert result["combined"]["sample_row_count"] == 1


def test_inspect_tuflow_collection_reports_per_file_errors() -> None:
    valid_path = MODULE_11_LOGS / "M11_5m_001.tlf"
    missing_path = MODULE_11_LOGS / "missing.tlf"

    result = inspect_tuflow_collection(file_paths=[str(valid_path), str(missing_path)])

    assert "error" not in result
    assert result["requested_file_count"] == 2
    assert result["collection_processor_count"] == 1
    assert result["errors"] == [{"file_path": str(missing_path), "error": f"File not found: {missing_path}"}]


def test_inspect_tuflow_collection_filters_data_types_case_insensitively() -> None:
    csv_directory = MODULE_11_RESULTS / "plot/csv"
    file_paths = [
        str(csv_directory / "M11_5m_001_1d_Q.csv"),
        str(csv_directory / "M11_5m_001_1d_H.csv"),
    ]

    result = inspect_tuflow_collection(file_paths=file_paths, data_types=["q", "missing"])

    assert "error" not in result
    assert result["processed_file_count"] == 2
    assert result["collection_processor_count"] == 1
    assert result["available_data_types"] == ["H", "Q"]
    assert result["requested_data_types"] == ["missing", "q"]
    assert result["unknown_data_types"] == ["missing"]
    assert result["data_type_counts"] == {"Q": 1}
    assert result["processors"][0]["processor"] == "QProcessor"


def test_tuflow_inspection_limits_are_enforced() -> None:
    assert inspect_tuflow_result(file_path="unused.tlf", sample_rows=21) == {
        "error": "sample_rows must be between 0 and 20."
    }
    assert inspect_tuflow_collection(file_paths=[]) == {"error": "file_paths must contain at least one file."}
    too_many_paths = [f"file-{index}.tlf" for index in range(MAX_COLLECTION_FILES + 1)]
    assert inspect_tuflow_collection(file_paths=too_many_paths) == {
        "error": f"file_paths is limited to {MAX_COLLECTION_FILES} files per call."
    }
    assert inspect_tuflow_result(file_path="unused.tlf", minimum_time=2.0, maximum_time=1.0) == {
        "error": "minimum_time must be less than or equal to maximum_time."
    }


def test_tuflow_processor_guidance_resource_is_valid_json() -> None:
    guidance = json.loads(tuflow_processor_guidance_resource())

    assert guidance["mcp_tools"]["inspect_tuflow_collection"]["filters"] == [
        "locations",
        "data_types",
        "minimum_time",
        "maximum_time",
    ]
    assert "BaseProcessor.from_file" in guidance["python_api"]["single_file_pattern"]
    assert "get_processors_by_data_type" in guidance["python_api"]["collection_methods"]
