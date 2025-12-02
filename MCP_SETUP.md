# MCP Setup for ryan-tools

This repository includes a Model Context Protocol (MCP) server that allows AI agents (like Claude, Codex, Antigravity) to directly interact with the repository's tools.

## Prerequisites

You need to install the `mcp` python package:

```bash
pip install mcp
```

## Running the Server

You can run the server directly using Python:

```bash
python ryan_mcp_server.py
```

The server runs over stdio (standard input/output), so it is designed to be called by an MCP client, not run interactively by a human.

## Configuration

### Claude Desktop

To use this with Claude Desktop, add the following to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "ryan-tools": {
      "command": "python",
      "args": [
        "E:\\Library\\Automation\\ryan-tools\\ryan_mcp_server.py"
      ]
    }
  }
}
```

Make sure to update the path to `ryan_mcp_server.py` if it is different on your machine.

### VS Code (Antigravity / Codex)

If you are using the Antigravity or Codex extension in VS Code, you typically need to add the MCP server configuration to your VS Code `settings.json` (User or Workspace) or the extension's specific configuration file.

Add the following configuration:

```json
"mcpServers": {
  "ryan-tools": {
    "command": "python",
    "args": [
      "e:\\Library\\Automation\\ryan-tools\\ryan_mcp_server.py"
    ]
  }
}
```

> [!NOTE]
> Ensure that `python` is in your system PATH, or use the full path to your Python executable (e.g., `e:\\Library\\Automation\\ryan-tools\\.venv\\Scripts\\python.exe`).

### Standalone Antigravity

If you are running Antigravity as a standalone application, you need to edit the `mcp_config.json` file.

**Location**: `C:\Users\Ryan\.gemini\antigravity\mcp_config.json`

Add the server configuration to the JSON object:

```json
{
  "mcpServers": {
    "ryan-tools": {
      "command": "python",
      "args": [
        "e:\\Library\\Automation\\ryan-tools\\ryan_mcp_server.py"
      ]
    }
  }
}
```

If the file already exists, merge the `ryan-tools` entry into the existing `mcpServers` list.

### Other Clients

Configure your MCP client to run the command: `python path/to/ryan_mcp_server.py`.

## Available Tools

Currently, the server exposes:

- `search_files`: Fast file search using Python. Finds files matching a pattern in a directory.

More tools can be added by modifying `ryan_mcp_server.py`.
