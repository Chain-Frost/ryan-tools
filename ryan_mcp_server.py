import sys
import os
import io
import contextlib
import traceback
import json
from pathlib import Path
from typing import Any, Sequence

try:
    from mcp.server import Server
    from mcp.server.stdio import stdio_server
    import mcp.types as types
except ImportError:
    print("Error: 'mcp' package not found. Please install it with: pip install mcp", file=sys.stderr)
    sys.exit(1)



# Initialize Server
server = Server("ryan-tools-mcp")

@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="search_files",
            description="Fast file search using Python. Finds files matching a pattern in a directory.",
            inputSchema={
                "type": "object",
                "properties": {
                    "pattern": {
                        "type": "string",
                        "description": "The search pattern (substring match). Case insensitive.",
                    },
                    "directory": {
                        "type": "string",
                        "description": "The root directory to search in. Defaults to current directory.",
                    },
                    "max_results": {
                        "type": "integer",
                        "description": "Maximum number of results to return. Defaults to 50.",
                    }
                },
                "required": ["pattern"]
            },
        )
    ]

@server.call_tool()
async def handle_call_tool(
    name: str, arguments: dict[str, Any] | None
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    if name == "search_files":
        return await execute_search_files(arguments)
    
    raise ValueError(f"Unknown tool: {name}")

async def execute_search_files(arguments: dict[str, Any] | None) -> list[types.TextContent]:
    if not arguments:
        return [types.TextContent(type="text", text="Error: Missing arguments.")]
        
    pattern = arguments.get("pattern", "").lower()
    directory = arguments.get("directory")
    max_results = arguments.get("max_results", 50)
    
    root_dir = Path(directory) if directory else Path.cwd()
    
    if not root_dir.exists():
         return [types.TextContent(type="text", text=f"Error: Directory not found: {root_dir}")]

    matches = []
    count = 0
    
    try:
        for root, dirs, files in os.walk(root_dir):
            # Skip common junk directories
            dirs[:] = [d for d in dirs if d not in {'.git', '.venv', '__pycache__', 'node_modules', '.vscode'}]
            
            for file in files:
                if pattern in file.lower():
                    full_path = str(Path(root) / file)
                    matches.append(full_path)
                    count += 1
                    if count >= max_results:
                        break
            if count >= max_results:
                break
    except Exception as e:
        return [types.TextContent(type="text", text=f"Error searching files: {e}")]

    if not matches:
        return [types.TextContent(type="text", text=f"No files found matching '{pattern}' in {root_dir}")]

    return [types.TextContent(type="text", text="\n".join(matches))]

async def main():
    # Run the server using stdin/stdout streams
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options()
        )

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
