"""Exercise overview boolean validation through the registered MCP tool."""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest

from ryan_library.mcp import server


@pytest.mark.parametrize("invalid", [0, 1, "true", "false", None])
def test_registered_overview_rejects_non_booleans(invalid: object) -> None:
    exceptions = pytest.importorskip("mcp.server.mcpserver.exceptions")
    if server.mcp is None:
        pytest.skip("MCP SDK is not installed")
    mcp = cast("Any", server.mcp)

    async def exercise() -> None:
        with pytest.raises(exceptions.ToolError, match="overview"):
            await mcp.call_tool("list_workflows", {"overview": invalid})

    asyncio.run(exercise())


@pytest.mark.parametrize("arguments", [{}, {"overview": False}, {"overview": True}])
def test_registered_overview_retains_boolean_schema_and_valid_modes(arguments: dict[str, bool]) -> None:
    if server.mcp is None:
        pytest.skip("MCP SDK is not installed")
    mcp = cast("Any", server.mcp)

    async def exercise() -> None:
        tools: list[Any] = await mcp.list_tools()
        tool = next(tool for tool in tools if tool.name == "list_workflows")
        assert tool.input_schema["properties"]["overview"]["type"] == "boolean"
        assert tool.input_schema["properties"]["overview"]["default"] is False
        response = await mcp.call_tool("list_workflows", {"domain": "gdal", "query": "crop", **arguments})
        result = response.structured_content
        assert result["workflow_count"] == 1
        if arguments.get("overview", False):
            assert result["overview"] is True
            assert result["domain_count"] == 1
            assert result["domain_overview"][0]["examples"][0]["id"] == "clip_rasters_to_polygon"
            assert "workflows" not in result
        else:
            assert result["workflows"][0]["id"] == "clip_rasters_to_polygon"
            assert "overview" not in result

    asyncio.run(exercise())
