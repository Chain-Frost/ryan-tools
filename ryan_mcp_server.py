# pyright: reportMissingImports=false, reportUnknownVariableType=false, reportUnknownMemberType=false, reportUntypedFunctionDecorator=false, reportUnknownParameterType=false, reportUnknownArgumentType=false, reportMissingTypeStubs=false, reportAttributeAccessIssue=false, reportAssignmentType=false
"""MCP server for ryan-tools geospatial, hydraulic, and data-processing utilities."""

import json
from pathlib import Path
from typing import Any

try:
    from mcp.server import MCPServer as FastMCP  # mcp SDK v2.0+
except ImportError:
    try:
        from mcp.server.fastmcp import FastMCP  # mcp SDK v1.x
    except ImportError:
        FastMCP = None  # type: ignore[assignment, misc]

from ryan_library.classes.tuflow_string_classes import TuflowStringParser
from ryan_library.processors.tuflow.other_processors.TLFProcessor import TLFProcessor
from ryan_library.functions.gdal.asc2asc_logic import (
    compute_max as run_asc_to_asc_max,
    compute_diff as run_asc_to_asc_diff,
    compute_stat as run_asc_to_asc_stat,
)

REPOSITORY_ROOT: Path = Path(__file__).resolve().parent
GDAL_CATALOGUE_PATH: Path = REPOSITORY_ROOT / "ryan-scripts" / "gdal-python" / "gdal_cli_tools.json"

if FastMCP is not None:
    mcp = FastMCP(
        "ryan-tools",
        instructions=(
            "Use list_gdal_cli_tools to discover supported GDAL jobs, then get_gdal_cli_tool for the selected "
            "job's current command scenarios. Execute the returned Python wrapper through the client's shell."
        ),
    )
else:
    mcp = None


def _load_gdal_cli_catalogue() -> dict[str, Any]:
    """Load and validate the repository's machine-readable GDAL catalogue."""
    try:
        raw_catalogue: Any = json.loads(GDAL_CATALOGUE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {"error": f"Unable to load GDAL catalogue at {GDAL_CATALOGUE_PATH}: {exc}"}

    if not isinstance(raw_catalogue, dict):
        return {"error": f"GDAL catalogue must contain a JSON object: {GDAL_CATALOGUE_PATH}"}
    return raw_catalogue


def list_gdal_cli_tools() -> dict[str, Any]:
    """List supported GDAL jobs; call this when choosing how to process geospatial data."""
    catalogue: dict[str, Any] = _load_gdal_cli_catalogue()
    if "error" in catalogue:
        return catalogue

    summaries: list[dict[str, Any]] = []
    raw_tools: Any = catalogue.get("tools", [])
    if isinstance(raw_tools, list):
        for raw_tool in raw_tools:
            if not isinstance(raw_tool, dict):
                continue
            summaries.append(
                {
                    "id": raw_tool.get("id"),
                    "purpose": raw_tool.get("purpose"),
                    "mutation": raw_tool.get("mutation"),
                    "requires_explicit_approval": raw_tool.get("requires_explicit_approval", False),
                }
            )

    return {
        "catalogue_updated": catalogue.get("catalogue_updated"),
        "script_directory": str(GDAL_CATALOGUE_PATH.parent),
        "default_raster_profile": (
            catalogue.get("agent_guidance", {}).get("default_raster_profile")
            if isinstance(catalogue.get("agent_guidance"), dict)
            else None
        ),
        "default_vector_format": (
            catalogue.get("agent_guidance", {}).get("default_vector_format")
            if isinstance(catalogue.get("agent_guidance"), dict)
            else None
        ),
        "tools": summaries,
        "next_step": "Call get_gdal_cli_tool with the selected id before constructing a command.",
    }


def get_gdal_cli_tool(tool_id: str) -> dict[str, Any]:
    """Return one GDAL job's defaults, safety metadata, scenarios, and absolute wrapper path."""
    catalogue: dict[str, Any] = _load_gdal_cli_catalogue()
    if "error" in catalogue:
        return catalogue

    raw_tools: Any = catalogue.get("tools", [])
    if not isinstance(raw_tools, list):
        return {"error": "The GDAL catalogue has no valid tools list."}

    for raw_tool in raw_tools:
        if not isinstance(raw_tool, dict) or raw_tool.get("id") != tool_id:
            continue
        tool: dict[str, Any] = dict(raw_tool)
        script_name: Any = tool.get("script")
        script_path: str | None = None
        if isinstance(script_name, str):
            script_path = str((GDAL_CATALOGUE_PATH.parent / script_name).resolve())
            tool["script_path"] = script_path

        command_prefix: Any = catalogue.get("command_prefix", ["py", "-3.14"])
        tool["command_prefix"] = command_prefix
        if isinstance(command_prefix, list) and script_path is not None:
            tool["help_command"] = [*command_prefix, script_path, "--help"]
            resolved_scenarios: list[dict[str, Any]] = []
            raw_scenarios: Any = tool.get("scenarios", [])
            if isinstance(raw_scenarios, list):
                for scenario in raw_scenarios:
                    if not isinstance(scenario, dict):
                        continue
                    scenario_arguments: Any = scenario.get("arguments", [])
                    if not isinstance(scenario_arguments, list):
                        continue
                    wrapper_arguments: list[Any] = scenario_arguments
                    if wrapper_arguments and wrapper_arguments[0] == script_name:
                        wrapper_arguments = wrapper_arguments[1:]
                    resolved_scenarios.append(
                        {
                            "name": scenario.get("name"),
                            "command": [*command_prefix, script_path, *wrapper_arguments],
                            "note": scenario.get("note"),
                        }
                    )
            tool["resolved_scenarios"] = resolved_scenarios
        tool["agent_guidance"] = catalogue.get("agent_guidance", {})
        return tool

    available_ids: list[Any] = [tool.get("id") for tool in raw_tools if isinstance(tool, dict)]
    return {"error": f"Unknown GDAL tool id: {tool_id}", "available_ids": available_ids}


def gdal_cli_catalogue_resource() -> str:
    """Return the complete GDAL CLI catalogue as JSON."""
    return GDAL_CATALOGUE_PATH.read_text(encoding="utf-8")


def parse_tuflow_filename(filename: str) -> dict[str, Any]:
    """Parse a TUFLOW output filename into structured components (AEP, duration, temporal pattern, data type)."""
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
    """Analyze a TUFLOW .tlf log file for simulation status, warnings, errors, and missing runs."""
    path = Path(tlf_path)
    if not path.is_file():
        return {"error": f"File not found: {tlf_path}"}

    processor = TLFProcessor(file_path=path)
    processor.process()

    if not processor.processed or processor.df.empty:
        return {"file": path.name, "status": "Failed to parse or incomplete log file"}

    records: list[dict[str, Any]] = processor.df.to_dict(orient="records")
    return {"file": path.name, "parsed_data": records[0] if records else {}}


def inspect_raster_metadata(raster_path: str) -> dict[str, Any]:
    """Inspect spatial metadata for a GeoTIFF or DEM raster file."""
    path = Path(raster_path)
    if not path.is_file():
        return {"error": f"File not found: {raster_path}"}

    try:
        import rasterio

        with rasterio.open(path) as src:
            return {
                "file_name": path.name,
                "width": src.width,
                "height": src.height,
                "count": src.count,
                "crs": str(src.crs),
                "bounds": {
                    "left": float(src.bounds.left),
                    "bottom": float(src.bounds.bottom),
                    "right": float(src.bounds.right),
                    "top": float(src.bounds.top),
                },
                "nodata": src.nodata,
            }
    except Exception as err:
        return {"file_name": path.name, "error": str(err)}


def check_repo_health() -> dict[str, str]:
    """Check the ryan-tools environment and package installation status."""
    import sys

    import ryan_library

    return {
        "python_version": sys.version,
        "ryan_library_version": getattr(ryan_library, "__version__", "0.1.0"),
        "root_directory": str(Path(__file__).parent),
    }


def tuflow_asc_to_asc_max(
    input_files: list[str],
    output_file: str,
    extra_args: list[str] | None = None,
) -> dict[str, Any]:
    """
    Run TUFLOW asc_to_asc natively in Python to find the maximum across multiple grids.
    
    Args:
        input_files: List of input grids to process.
        output_file: The path to save the output grid.
        extra_args: Any additional arguments to pass (e.g. ["-co", "COMPRESS=DEFLATE"]).
                    See https://wiki.tuflow.com/ASC_to_ASC for all options.
    """
    run_asc_to_asc_max(input_files=input_files, output_file=output_file, extra_args=extra_args)
    return {"status": "success", "output_file": output_file}


def tuflow_asc_to_asc_diff(
    file1: str,
    file2: str,
    output_file: str,
    change: bool = False,
    nowetdry: bool = False,
    extra_args: list[str] | None = None,
) -> dict[str, Any]:
    """
    Run TUFLOW asc_to_asc natively in Python to subtract file2 from file1 (file1 - file2).
    
    Args:
        file1: The first input grid (the "after" or "developed" case).
        file2: The second input grid (the "before" or "existing" case) which is subtracted from file1.
        output_file: The path to save the output grid.
        change: Optional flag to treat nodata as 0 and compute difference everywhere.
        nowetdry: Optional flag to skip generating the _wd (wet/dry) output grid.
        extra_args: Any additional arguments to pass (e.g. ["-co", "COMPRESS=DEFLATE"]).
                    See https://wiki.tuflow.com/ASC_to_ASC for all options.
    """
    run_asc_to_asc_diff(
        file1=file1, file2=file2, output_file=output_file, change=change, nowetdry=nowetdry, extra_args=extra_args
    )
    return {"status": "success", "output_file": output_file}


def tuflow_asc_to_asc_stat(
    stat_type: str,
    input_files: list[str],
    output_file: str,
    extra_args: list[str] | None = None,
) -> dict[str, Any]:
    """
    Run TUFLOW asc_to_asc natively in Python to compute statistics across grids.
    
    Args:
        stat_type: The statistic to calculate (e.g. "Median", "Mean", "Min", "Max").
        input_files: List of input grids to process.
        output_file: The path to save the output grid.
        extra_args: Any additional arguments to pass (e.g. ["-co", "COMPRESS=DEFLATE"]).
                    See https://wiki.tuflow.com/ASC_to_ASC for all options.
    """
    run_asc_to_asc_stat(
        stat_type=stat_type, input_files=input_files, output_file=output_file, extra_args=extra_args
    )
    return {"status": "success", "output_file": output_file}


if mcp is not None:
    mcp.tool()(parse_tuflow_filename)
    mcp.tool()(inspect_tlf_log)
    mcp.tool()(inspect_raster_metadata)
    mcp.tool()(check_repo_health)
    mcp.tool()(list_gdal_cli_tools)
    mcp.tool()(get_gdal_cli_tool)
    mcp.tool()(tuflow_asc_to_asc_max)
    mcp.tool()(tuflow_asc_to_asc_diff)
    mcp.tool()(tuflow_asc_to_asc_stat)
    mcp.resource(
        "ryan-tools://gdal/cli-catalogue",
        name="gdal_cli_catalogue",
        description="Complete machine-readable catalogue of supported GDAL Python wrappers and command scenarios.",
        mime_type="application/json",
    )(gdal_cli_catalogue_resource)


def main() -> None:
    """Entry point for the ``ryan-mcp`` console script."""
    if mcp is not None:
        mcp.run()
    else:
        import sys

        print("mcp package is not installed. Install with: pip install .[mcp]", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
