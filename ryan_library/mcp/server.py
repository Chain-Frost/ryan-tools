# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
"""MCP discovery server for ryan-tools CLI workflows and focused helpers."""

from __future__ import annotations

import json
import sys
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, cast

try:
    from mcp.server import MCPServer
except ImportError:
    MCPServer = None  # type: ignore[assignment, misc]

from ryan_library.classes.tuflow_string_classes import TuflowStringParser
from ryan_library.mcp.registry import WorkflowRegistry
from ryan_library.processors.tuflow.other_processors.TLFProcessor import TLFProcessor

REGISTRY = WorkflowRegistry()

if MCPServer is not None:
    mcp = MCPServer(
        "ryan-tools",
        instructions=(
            "Use list_workflows to discover enabled ryan-tools CLI workflows, then call get_workflow for the selected "
            "id. Review its help_command, mutation metadata, paths, and approval requirement before running the returned "
            "CLI command through the client shell. This MCP server discovers scripts; it does not execute them."
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


def inspect_tlf_log(tlf_path: str) -> dict[str, Any]:
    """Parse one TUFLOW .tlf log and return its available simulation summary fields."""
    path = Path(tlf_path)
    if not path.is_file():
        return {"error": f"File not found: {tlf_path}"}

    processor = TLFProcessor(file_path=path)
    processor.process()
    if not processor.processed or processor.df.empty:
        return {"file": path.name, "status": "Failed to parse or incomplete log file"}

    records = cast(list[dict[str, Any]], processor.df.to_dict(orient="records"))
    return {"file": path.name, "parsed_data": records[0] if records else {}}


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


if mcp is not None:
    mcp.tool()(parse_tuflow_filename)
    mcp.tool()(inspect_tlf_log)
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


def main() -> None:
    """Run the ryan-tools MCP server over stdio."""
    if mcp is not None:
        mcp.run()
        return

    print("mcp package is not installed. Install with: pip install .[mcp]", file=sys.stderr)
    raise SystemExit(1)


if __name__ == "__main__":
    main()
