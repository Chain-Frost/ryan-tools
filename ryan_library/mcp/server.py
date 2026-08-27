# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
"""MCP discovery server for ryan-tools CLI workflows and focused helpers."""

from __future__ import annotations

import json
import sys
from collections import Counter
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, cast

from pandas import DataFrame, to_numeric

try:
    from mcp.server import MCPServer
except ImportError:
    MCPServer = None  # type: ignore[assignment, misc]

from ryan_library.classes.tuflow_string_classes import TuflowStringParser
from ryan_library.mcp.registry import WorkflowRegistry
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection

REGISTRY = WorkflowRegistry()
MAX_SAMPLE_ROWS = 20
MAX_COLLECTION_FILES = 50
TUFLOW_PROCESSOR_GUIDANCE_URI = "ryan-tools://guidance/tuflow-processors"

if MCPServer is not None:
    mcp = MCPServer(
        "ryan-tools",
        instructions=(
            "Use list_workflows to discover enabled ryan-tools CLI workflows, then call get_workflow for the selected "
            "id. Review its help_command, mutation metadata, paths, and approval requirement before running the returned "
            "CLI command through the client shell. Use inspect_tuflow_result or inspect_tuflow_collection for bounded, "
            "read-only result queries, and consult the TUFLOW processor guidance resource for advanced Python analysis. "
            "This MCP server discovers scripts; it does not execute them."
        ),
    )
else:
    mcp = None


def _distribution_version(distribution_name: str) -> str:
    try:
        return version(distribution_name)
    except PackageNotFoundError:
        return "not installed"


def parse_tuflow_filename(filename: str) -> dict[str, Any]:
    """Parse a TUFLOW output filename into AEP, duration, temporal pattern, and data type fields."""
    parser = TuflowStringParser(file_path=Path(filename))
    return {
        "file_name": parser.file_name,
        "data_type": parser.data_type,
        "raw_run_code": parser.raw_run_code,
        "clean_run_code": parser.clean_run_code,
        "aep": str(parser.aep) if parser.aep else None,
        "duration": str(parser.duration) if parser.duration else None,
        "temporal_pattern": str(parser.tp) if parser.tp else None,
    }


def _validate_sample_rows(sample_rows: int) -> str | None:
    if 0 <= sample_rows <= MAX_SAMPLE_ROWS:
        return None
    return f"sample_rows must be between 0 and {MAX_SAMPLE_ROWS}."


def _validate_time_bounds(minimum_time: float | None, maximum_time: float | None) -> str | None:
    if minimum_time is not None and maximum_time is not None and minimum_time > maximum_time:
        return "minimum_time must be less than or equal to maximum_time."
    return None


def _dataframe_summary(data_frame: DataFrame, *, sample_rows: int) -> dict[str, Any]:
    sample_json: str = data_frame.head(sample_rows).to_json(orient="records", date_format="iso")
    sample = cast(list[dict[str, Any]], json.loads(sample_json))
    return {
        "row_count": len(data_frame),
        "columns": [str(column) for column in data_frame.columns],
        "dtypes": {str(column): str(dtype) for column, dtype in data_frame.dtypes.items()},
        "sample": sample,
        "sample_row_count": len(sample),
        "sample_truncated": len(data_frame) > len(sample),
    }


def _process_tuflow_file(file_path: str, *, locations: list[str] | None = None) -> BaseProcessor:
    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"File not found: {file_path}")
    processor: BaseProcessor = BaseProcessor.from_file(
        file_path=path,
        entity_filter=locations,
        include_path_columns=False,
    )
    processor.process()
    return processor


def _apply_time_bounds(
    processor: BaseProcessor,
    *,
    minimum_time: float | None,
    maximum_time: float | None,
) -> dict[str, Any] | None:
    if minimum_time is None and maximum_time is None:
        return None

    before_count = len(processor.df)
    if "Time" not in processor.df.columns:
        return {
            "minimum_time": minimum_time,
            "maximum_time": maximum_time,
            "applied": False,
            "reason": "The processed result has no Time column.",
            "row_count_before": before_count,
            "row_count_after": before_count,
        }

    numeric_time = to_numeric(processor.df["Time"], errors="coerce")
    mask = numeric_time.notna()
    if minimum_time is not None:
        mask &= numeric_time >= minimum_time
    if maximum_time is not None:
        mask &= numeric_time <= maximum_time
    processor.df = processor.df.loc[mask].copy()
    return {
        "minimum_time": minimum_time,
        "maximum_time": maximum_time,
        "applied": True,
        "row_count_before": before_count,
        "row_count_after": len(processor.df),
    }


def _processor_summary(
    processor: BaseProcessor,
    *,
    sample_rows: int,
    time_filter: dict[str, Any] | None = None,
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "file": processor.file_name,
        "file_path": str(processor.resolved_file_path),
        "processor": type(processor).__name__,
        "data_type": processor.data_type,
        "dataformat": processor.dataformat,
        "raw_run_code": processor.name_parser.raw_run_code,
        "processed": processor.processed,
        "applied_entity_filter": sorted(processor.applied_entity_filter or ()),
        **_dataframe_summary(processor.df, sample_rows=sample_rows),
    }
    if time_filter is not None:
        summary["time_filter"] = time_filter
    return summary


def inspect_tuflow_result(
    file_path: str,
    locations: list[str] | None = None,
    minimum_time: float | None = None,
    maximum_time: float | None = None,
    sample_rows: int = 5,
) -> dict[str, Any]:
    """Query one supported TUFLOW result by location and processed Time bounds, returning a bounded sample."""
    sample_error: str | None = _validate_sample_rows(sample_rows)
    if sample_error is not None:
        return {"error": sample_error}
    time_error: str | None = _validate_time_bounds(minimum_time, maximum_time)
    if time_error is not None:
        return {"error": time_error}
    try:
        processor: BaseProcessor = _process_tuflow_file(file_path=file_path, locations=locations)
    except Exception as error:
        return {"file_path": file_path, "error": str(error)}
    time_filter = _apply_time_bounds(
        processor,
        minimum_time=minimum_time,
        maximum_time=maximum_time,
    )
    result = _processor_summary(processor, sample_rows=sample_rows, time_filter=time_filter)
    result["guidance_resource"] = TUFLOW_PROCESSOR_GUIDANCE_URI
    return result


def inspect_tuflow_collection(
    file_paths: list[str],
    locations: list[str] | None = None,
    data_types: list[str] | None = None,
    minimum_time: float | None = None,
    maximum_time: float | None = None,
    sample_rows: int = 5,
    include_combined_sample: bool = False,
) -> dict[str, Any]:
    """Query a bounded TUFLOW file set by location, data type, and processed Time bounds without writing outputs."""
    sample_error: str | None = _validate_sample_rows(sample_rows)
    if sample_error is not None:
        return {"error": sample_error}
    time_error: str | None = _validate_time_bounds(minimum_time, maximum_time)
    if time_error is not None:
        return {"error": time_error}
    if not file_paths:
        return {"error": "file_paths must contain at least one file."}
    if len(file_paths) > MAX_COLLECTION_FILES:
        return {"error": f"file_paths is limited to {MAX_COLLECTION_FILES} files per call."}

    collection = ProcessorCollection()
    processed_file_count = 0
    processed_data_types: set[str] = set()
    time_filters: dict[int, dict[str, Any] | None] = {}
    errors: list[dict[str, str]] = []
    for file_path in file_paths:
        try:
            processor: BaseProcessor = _process_tuflow_file(file_path=file_path, locations=locations)
        except Exception as error:
            errors.append({"file_path": file_path, "error": str(error)})
            continue
        processed_file_count += 1
        processed_data_types.add(processor.data_type)
        time_filters[id(processor)] = _apply_time_bounds(
            processor,
            minimum_time=minimum_time,
            maximum_time=maximum_time,
        )
        collection.add_processor(processor)

    available_data_types = sorted(processed_data_types)
    requested_data_types = sorted({value.strip() for value in data_types or [] if value.strip()})
    unknown_data_types: list[str] = []
    if requested_data_types:
        canonical_by_casefold = {value.casefold(): value for value in available_data_types}
        selected_data_types = sorted(
            {
                canonical_by_casefold[value.casefold()]
                for value in requested_data_types
                if value.casefold() in canonical_by_casefold
            }
        )
        unknown_data_types = [value for value in requested_data_types if value.casefold() not in canonical_by_casefold]
        collection = collection.get_processors_by_data_type(selected_data_types)

    processor_summaries = [
        _processor_summary(processor, sample_rows=0, time_filter=time_filters[id(processor)])
        for processor in collection.processors
    ]

    data_type_counts: Counter[str] = Counter(processor.data_type for processor in collection.processors)
    dataformat_counts: Counter[str] = Counter(processor.dataformat for processor in collection.processors)
    duplicate_groups: list[dict[str, Any]] = []
    for (run_code, data_type), processors in sorted(collection.check_duplicates().items()):
        duplicate_groups.append(
            {
                "raw_run_code": run_code,
                "data_type": data_type,
                "files": [processor.file_name for processor in processors],
            }
        )

    result: dict[str, Any] = {
        "requested_file_count": len(file_paths),
        "processed_file_count": processed_file_count,
        "collection_processor_count": len(collection.processors),
        "total_row_count": sum(len(processor.df) for processor in collection.processors),
        "data_type_counts": dict(sorted(data_type_counts.items())),
        "dataformat_counts": dict(sorted(dataformat_counts.items())),
        "processors": processor_summaries,
        "duplicate_groups": duplicate_groups,
        "errors": errors,
        "applied_locations": sorted(BaseProcessor.normalize_locations(locations)),
        "requested_data_types": requested_data_types,
        "available_data_types": available_data_types,
        "unknown_data_types": unknown_data_types,
        "guidance_resource": TUFLOW_PROCESSOR_GUIDANCE_URI,
    }
    if include_combined_sample:
        result["combined"] = _dataframe_summary(collection.combine_raw(), sample_rows=sample_rows)
    return result


def inspect_raster_metadata(raster_path: str) -> dict[str, Any]:
    """Inspect raster dimensions, resolution, CRS, transform, bounds, bands, and NoData metadata."""
    path = Path(raster_path)
    if not path.is_file():
        return {"error": f"File not found: {raster_path}"}

    try:
        import rasterio

        with rasterio.open(path) as source:
            return {
                "file_name": path.name,
                "driver": source.driver,
                "width": source.width,
                "height": source.height,
                "count": source.count,
                "dtypes": list(source.dtypes),
                "crs": str(source.crs),
                "resolution": {"x": float(source.res[0]), "y": float(source.res[1])},
                "transform": list(source.transform),
                "bounds": {
                    "left": float(source.bounds.left),
                    "bottom": float(source.bounds.bottom),
                    "right": float(source.bounds.right),
                    "top": float(source.bounds.top),
                },
                "nodata": source.nodata,
            }
    except Exception as error:
        return {"file_name": path.name, "error": str(error)}


def list_workflows(
    domain: str | None = None,
    profile: str | None = None,
    include_unavailable: bool = False,
) -> dict[str, Any]:
    """List CLI workflows enabled by the configured MCP profile; optionally filter by domain or lower profile."""
    try:
        return REGISTRY.list_workflows(
            domain=domain,
            maximum_profile=profile,
            include_unavailable=include_unavailable,
        )
    except Exception as error:
        return {"error": str(error)}


def get_workflow(workflow_id: str) -> dict[str, Any]:
    """Return one enabled workflow's script path, help command, scenarios, and safety metadata."""
    return REGISTRY.get_workflow(workflow_id)


def check_repo_health() -> dict[str, Any]:
    """Check package versions, workflow profile, checkout discovery, and catalogue availability."""
    import ryan_library

    visible: dict[str, Any] = REGISTRY.list_workflows(include_unavailable=True)
    available: dict[str, Any] = REGISTRY.list_workflows(include_unavailable=False)
    package_directory: Path = Path(ryan_library.__file__).resolve().parent
    return {
        "python_version": sys.version,
        "python_executable": sys.executable,
        "ryan_library_version": _distribution_version("ryan_functions"),
        "mcp_sdk_version": _distribution_version("mcp"),
        "package_directory": str(package_directory),
        "root_directory": str(package_directory),
        "repository_root": str(REGISTRY.repository_root) if REGISTRY.repository_root else None,
        "configured_profile": REGISTRY.configured_profile.value,
        "catalogue_schema_version": REGISTRY.schema_version,
        "catalogue_updated": REGISTRY.catalogue_updated,
        "workflow_count_total": REGISTRY.total_workflow_count,
        "workflow_count_visible": visible["workflow_count"],
        "workflow_count_available": available["workflow_count"],
        "warnings": list(REGISTRY.warnings),
    }


def workflow_catalogue_resource() -> str:
    """Return the complete workflow catalogue visible under the configured profile."""
    return json.dumps(REGISTRY.catalogue(), indent=2)


def tuflow_processor_guidance_resource() -> str:
    """Describe the supported MCP queries and Python extension points for advanced TUFLOW analysis."""
    return json.dumps(
        {
            "purpose": (
                "Use the MCP inspection tools for bounded, read-only queries. Use the Python APIs for richer "
                "aggregations, joins, specialised combines, or query logic that is not part of the stable MCP surface."
            ),
            "mcp_tools": {
                "inspect_tuflow_result": {
                    "filters": ["locations", "minimum_time", "maximum_time"],
                    "limits": {"maximum_sample_rows": MAX_SAMPLE_ROWS},
                },
                "inspect_tuflow_collection": {
                    "filters": ["locations", "data_types", "minimum_time", "maximum_time"],
                    "limits": {
                        "maximum_files": MAX_COLLECTION_FILES,
                        "maximum_sample_rows": MAX_SAMPLE_ROWS,
                    },
                },
            },
            "time_semantics": (
                "minimum_time and maximum_time apply to the numeric Time column produced by a processor, normally "
                "simulation time in hours. Filename duration is separate metadata returned by parse_tuflow_filename."
            ),
            "python_api": {
                "single_file_import": ("from ryan_library.processors.tuflow.base_processor import BaseProcessor"),
                "single_file_pattern": (
                    "processor = BaseProcessor.from_file(Path(file_path), entity_filter=locations); processor.process()"
                ),
                "collection_import": (
                    "from ryan_library.processors.tuflow.processor_collection import ProcessorCollection"
                ),
                "collection_methods": [
                    "add_processor",
                    "filter_locations",
                    "get_processors_by_data_type",
                    "check_duplicates",
                    "combine_raw",
                    "combine_1d_timeseries",
                    "combine_1d_maximums",
                ],
                "source_paths": [
                    "ryan_library/processors/tuflow/base_processor.py",
                    "ryan_library/processors/tuflow/processor_collection.py",
                    "ryan_library/processors/tuflow/README.md",
                    "ryan_library/classes/tuflow_results_validation_and_datatypes.json",
                ],
            },
            "safety": "Processing is read-only until the caller explicitly invokes an export or write method.",
        },
        indent=2,
    )


if mcp is not None:
    mcp.tool()(parse_tuflow_filename)
    mcp.tool()(inspect_tuflow_result)
    mcp.tool()(inspect_tuflow_collection)
    mcp.tool()(inspect_raster_metadata)
    mcp.tool()(check_repo_health)
    mcp.tool()(list_workflows)
    mcp.tool()(get_workflow)
    mcp.resource(
        "ryan-tools://workflows/catalogue",
        name="workflow_catalogue",
        description="CLI workflow catalogue visible under the configured ryan-tools MCP profile.",
        mime_type="application/json",
    )(workflow_catalogue_resource)
    mcp.resource(
        TUFLOW_PROCESSOR_GUIDANCE_URI,
        name="tuflow_processor_guidance",
        description="Supported MCP queries and Python extension points for TUFLOW processors and collections.",
        mime_type="application/json",
    )(tuflow_processor_guidance_resource)


def main() -> None:
    """Run the ryan-tools MCP server over stdio."""
    if mcp is not None:
        mcp.run()
        return

    print("mcp package is not installed. Install with: pip install .[mcp]", file=sys.stderr)
    raise SystemExit(1)


if __name__ == "__main__":
    main()
