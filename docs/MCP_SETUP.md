# MCP Server Setup — ryan-tools

The **ryan-tools** repository includes a lightweight [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server that exposes geospatial and TUFLOW utilities as tools for AI agents (Antigravity, VS Code Copilot, Claude Desktop, etc.).

---

## Available Tools

| Tool | Description |
| --- | --- |
| `parse_tuflow_filename` | Parse a TUFLOW output filename into structured components (AEP, duration, temporal pattern, data type). |
| `inspect_tlf_log` | Analyze a TUFLOW `.tlf` log file for simulation status, warnings, errors, and missing runs. |
| `inspect_raster_metadata` | Inspect spatial metadata (CRS, bounds, resolution) for a GeoTIFF or DEM raster file. |
| `check_repo_health` | Check the ryan-tools environment: Python version, library version, and root directory. |
| `list_gdal_cli_tools` | Discover supported GDAL processing jobs without loading the full catalogue. |
| `get_gdal_cli_tool` | Retrieve one GDAL job's wrapper path, defaults, safety metadata, and command scenarios. |

## GDAL discovery

The server exposes the full JSON catalogue as the read-only resource
`ryan-tools://gdal/cli-catalogue`. It also exposes the two discovery tools above
because tool descriptions are more consistently visible to AI models than MCP
resources alone.

An AI should call `list_gdal_cli_tools`, choose the matching job, and then call
`get_gdal_cli_tool` with its `id`. The result contains an absolute `script_path`
plus ready-to-run `help_command` and `resolved_scenarios` arrays derived from
`gdal_cli_tools.json`. The AI can run one of those arrays through its normal
shell capability; catalogue scenarios already include `--no-pause`. The MCP
server discovers commands but does not execute or silently approve
file-changing GDAL operations.

---

## Prerequisites

1. **Python 3.14+** with the `ryan_functions` package installed (editable or wheel).
2. Install the MCP extra:

   ```bash
   pip install .[mcp]
   ```

   This pulls in the `mcp[cli]` package, which includes the FastMCP SDK and `stdio` transport.

---

## Running the Server

### Option A: Console entrypoint (after `pip install`)

```bash
ryan-mcp
```

### Option B: Direct invocation

```bash
python ryan_mcp_server.py
```

Both start the server using MCP's default **stdio** transport.

---

## Client Configuration

### VS Code (Antigravity / Copilot)

The repo ships a `.vscode/mcp.json` that auto-registers the server when you open the workspace:

```jsonc
// .vscode/mcp.json
{
  "servers": {
    "ryan-tools": {
      "command": "python",
      "args": ["${workspaceFolder}/ryan_mcp_server.py"],
      "env": {
        "PYTHONPATH": "${workspaceFolder}",
        "PYTHONUTF8": "1"
      }
    }
  }
}
```

No additional setup needed — the server appears in the MCP panel automatically.

### Claude Desktop

Add this to your `claude_desktop_config.json` (adjust paths as needed):

```jsonc
{
  "mcpServers": {
    "ryan-tools": {
      "command": "python",
      "args": ["E:/Library/Automation/ryan-tools/ryan_mcp_server.py"],
      "env": {
        "PYTHONPATH": "E:/Library/Automation/ryan-tools",
        "PYTHONUTF8": "1"
      }
    }
  }
}
```

> **Note:** Claude Desktop uses `mcpServers` (camelCase), while VS Code uses `servers`.

---

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| `mcp package is not installed` | Run `pip install .[mcp]` from the repo root. |
| Server starts but no tools appear | Check that `ryan_library` is importable (`python -c "import ryan_library"`). |
| `Property mcpServers is not allowed` in VS Code | Use `"servers"` as the top-level key in `.vscode/mcp.json`, not `"mcpServers"`. |
| Tools fail with `ModuleNotFoundError` | Ensure `PYTHONPATH` includes the repo root, or install the package in editable mode. |
