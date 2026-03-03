#!/usr/bin/env python3
"""
MCP Server for Metadata Lookup.

Exposes tools to Claude for looking up title metadata from Databricks.
Supports stdio (local) and Streamable HTTP (hosted) transports.
"""

import json
import os

from mcp.server.fastmcp import FastMCP

from metadata_lookup import lookup_title, enrich_csv, enrich_csv_content

# Create the MCP server
mcp = FastMCP(
    "metadata-lookup",
    host=os.getenv("HOST", "0.0.0.0"),
    port=int(os.getenv("PORT", "8080")),
)


@mcp.tool()
def lookup_title_metadata(imdb_id: str) -> str:
    """Look up metadata for a title given its IMDB ID.
    Returns genre, release year, program_id, import_id (studio),
    and whether the title is Fresh (new) or Returning (existing in catalog).
    """
    imdb_id = imdb_id.strip()
    if not imdb_id:
        return "Error: imdb_id is required"
    if not imdb_id.startswith("tt"):
        imdb_id = f"tt{imdb_id}"
    try:
        result = lookup_title(imdb_id)
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool()
def enrich_metadata_csv(
    input_path: str,
    output_path: str | None = None,
    overwrite_existing: bool = False,
) -> str:
    """Process a CSV file and enrich it with metadata from Databricks.
    Fills in Genre, Release Year, and Fresh vs. Returning for each title.
    The CSV must have an 'IMDB ID' column.
    Works with local file paths (stdio or mounted volumes).
    """
    input_path = input_path.strip()
    if not input_path:
        return "Error: input_path is required"
    try:
        result_path = enrich_csv(
            input_path=input_path,
            output_path=output_path,
            overwrite_existing=overwrite_existing,
        )
        return f"Successfully enriched CSV. Output written to: {result_path}"
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool()
def enrich_metadata_csv_content(
    csv_content: str,
    overwrite_existing: bool = False,
) -> str:
    """Process CSV content in-memory and return enriched CSV.
    For hosted deployment where file paths are not available.
    Fills in Genre, Release Year, and Fresh vs. Returning for each title.
    The CSV must have an 'IMDB ID' column.
    Pass the raw CSV string; returns the enriched CSV string.
    """
    if not csv_content or not csv_content.strip():
        return "Error: csv_content is required"
    try:
        return enrich_csv_content(
            csv_content=csv_content,
            overwrite_existing=overwrite_existing,
        )
    except Exception as e:
        return f"Error: {str(e)}"


# Expose Starlette app for uvicorn (Cloud Run / Docker)
app = mcp.streamable_http_app()


if __name__ == "__main__":
    if os.getenv("MCP_TRANSPORT") == "http" or os.getenv("PORT"):
        mcp.run(
            transport="streamable-http",
            host=os.getenv("HOST", "0.0.0.0"),
            port=int(os.getenv("PORT", "8080")),
        )
    else:
        mcp.run(transport="stdio")
