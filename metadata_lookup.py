#!/usr/bin/env python3
"""
Metadata Lookup Library for enriching title metadata from Databricks.

POC Scope:
- Genre (from IMDB)
- Release Year (from IMDB)
- Fresh vs. Returning (based on content_info program_id existence)
"""

from __future__ import annotations

import io
import json
import os
import time
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.service import sql as sql_service

if TYPE_CHECKING:
    import pandas as pd


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Credentials: set via environment variables or secrets.py
DATABRICKS_HOST_OVERRIDE: Optional[str] = None
DATABRICKS_TOKEN_OVERRIDE: Optional[str] = None


def _load_local_secrets() -> Tuple[Optional[str], Optional[str]]:
    """Load credentials from local secrets.py if it exists."""
    secrets_path = os.path.join(BASE_DIR, "secrets.py")
    if not os.path.exists(secrets_path):
        return None, None
    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location("local_secrets", secrets_path)
        if spec is None or spec.loader is None:
            return None, None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        host = getattr(module, "DATABRICKS_HOST", None)
        token = getattr(module, "DATABRICKS_TOKEN", None)
        return host, token
    except Exception:
        return None, None


_SECRETS_HOST, _SECRETS_TOKEN = _load_local_secrets()


def get_connection_details() -> Tuple[str, str]:
    """Get Databricks host and token from various sources (PAT auth)."""
    host = (
        DATABRICKS_HOST_OVERRIDE
        or _SECRETS_HOST
        or os.getenv("DATABRICKS_HOST")
        or os.getenv("DATABRICKS_SERVER_HOSTNAME")
    )
    token = (
        DATABRICKS_TOKEN_OVERRIDE
        or _SECRETS_TOKEN
        or os.getenv("DATABRICKS_TOKEN")
        or os.getenv("DATABRICKS_ACCESS_TOKEN")
    )

    if host and not host.startswith("http"):
        host = f"https://{host}"

    if not host or not token:
        raise ValueError(
            "Missing Databricks credentials. Set DATABRICKS_HOST and DATABRICKS_TOKEN "
            "environment variables, or create a secrets.py file."
        )
    return host, token


def get_databricks_host() -> str:
    """Get Databricks host from various sources."""
    host = (
        DATABRICKS_HOST_OVERRIDE
        or _SECRETS_HOST
        or os.getenv("DATABRICKS_HOST")
        or os.getenv("DATABRICKS_SERVER_HOSTNAME")
    )
    if not host:
        raise ValueError(
            "Missing DATABRICKS_HOST. Set the environment variable or create a secrets.py file."
        )
    if not host.startswith("http"):
        host = f"https://{host}"
    return host


def get_databricks_client() -> WorkspaceClient:
    """Create a Databricks WorkspaceClient.
    Supports two auth modes:
    - OAuth M2M (service principal): DATABRICKS_HOST, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET
    - PAT: DATABRICKS_HOST, DATABRICKS_TOKEN (or secrets.py)
    """
    host = get_databricks_host()
    client_id = os.getenv("DATABRICKS_CLIENT_ID")
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")

    if client_id and client_secret:
        config = Config(
            host=host,
            client_id=client_id,
            client_secret=client_secret,
        )
    else:
        _, token = get_connection_details()
        config = Config(host=host, token=token)

    return WorkspaceClient(config=config)


def get_sql_warehouse_id(client: WorkspaceClient) -> str:
    """Get SQL warehouse ID from env or first available warehouse."""
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    if warehouse_id:
        return warehouse_id

    warehouses = list(client.warehouses.list())
    if not warehouses:
        raise ValueError("No SQL warehouses found. Set DATABRICKS_WAREHOUSE_ID.")
    return warehouses[0].id


def execute_sql_query(
    client: WorkspaceClient,
    warehouse_id: str,
    query: str,
) -> List[Dict]:
    """Execute a SQL query and return results as list of dicts."""
    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=query,
        disposition=sql_service.Disposition.INLINE,
        format=sql_service.Format.JSON_ARRAY,
    )

    statement_id = response.statement_id

    # Poll until complete
    while True:
        status = client.statement_execution.get_statement(statement_id)
        state = status.status.state.value
        if state == "SUCCEEDED":
            break
        if state in ["FAILED", "CANCELED"]:
            error_msg = "Unknown error"
            if hasattr(status.status, "error") and status.status.error:
                if hasattr(status.status.error, "message"):
                    error_msg = status.status.error.message
            raise Exception(f"Query failed: {state}. Error: {error_msg}")
        time.sleep(1)

    statement = client.statement_execution.get_statement(statement_id)
    if not statement.manifest or not statement.manifest.schema:
        return []

    columns = [col.name for col in statement.manifest.schema.columns]
    rows: List[Dict] = []

    chunk_index = 0
    while True:
        chunk = client.statement_execution.get_statement_result_chunk_n(statement_id, chunk_index)
        if chunk.data_array:
            for row in chunk.data_array:
                row_dict = {}
                for i, col in enumerate(columns):
                    if i < len(row):
                        row_dict[col] = row[i]
                rows.append(row_dict)
        else:
            break
        if not chunk.next_chunk_index or chunk.next_chunk_index == -1:
            break
        chunk_index = chunk.next_chunk_index

    return rows


def get_imdb_metadata(
    client: WorkspaceClient,
    warehouse_id: str,
    imdb_ids: List[str],
    title_table: Optional[str] = None,
) -> Dict[str, Dict]:
    """
    Fetch genre, release year, and company (studio) for a list of IMDB IDs.
    
    Returns: {imdb_id: {"genre": "...", "release_year": int|None, "company": str|None}}
    """
    if not imdb_ids:
        return {}

    title_tbl = title_table or os.getenv(
        "DATABRICKS_IMDB_TITLE_TABLE", 
        "core_dev.imdb.imdb_title_essential_v2"
    )

    # Filter to valid IMDB IDs
    valid_ids = [iid for iid in imdb_ids if iid and iid.startswith("tt")]
    if not valid_ids:
        return {}

    results: Dict[str, Dict] = {}
    batch_size = 100

    # Column variations to try (year, company columns may vary)
    column_sets = [
        ("genres", "year", "companies"),
        ("genres", "release_year", "companies"),
        ("genres", "year", None),  # fallback without company
        ("genres", "release_year", None),
    ]

    for i in range(0, len(valid_ids), batch_size):
        batch = valid_ids[i:i + batch_size]
        imdb_ids_str = ",".join([f"'{iid}'" for iid in batch])

        rows = None
        year_col_used = None
        company_col_used = None

        for genre_col, year_col, company_col in column_sets:
            company_select = f", {company_col} as company" if company_col else ""
            for title_col in ("originalTitle", "title", None):
                title_select = f", {title_col} as title" if title_col else ""
                query = f"""
                SELECT
                    titleId,
                    {genre_col} as genres,
                    {year_col} as year
                    {company_select}
                    {title_select}
                FROM {title_tbl}
                WHERE titleId IN ({imdb_ids_str})
                """
                try:
                    rows = execute_sql_query(client, warehouse_id, query)
                    year_col_used = year_col
                    company_col_used = company_col
                    break
                except Exception as e:
                    msg = str(e).lower()
                    if "column" in msg or "unresolved" in msg or "cannot be resolved" in msg:
                        continue
                    raise
            if rows is not None:
                break

        if not rows:
            continue

        for row in rows:
            title_id = row.get("titleId")
            if not title_id:
                continue

            genre_raw = row.get("genres")
            year_raw = row.get("year")
            company_raw = row.get("company")

            # Parse genre (may be JSON array or comma-separated string)
            genre = None
            if genre_raw:
                try:
                    if isinstance(genre_raw, str):
                        if genre_raw.startswith("["):
                            genre_list = json.loads(genre_raw)
                            genre = ", ".join(genre_list) if genre_list else None
                        else:
                            genre = genre_raw
                    elif isinstance(genre_raw, list):
                        genre = ", ".join(genre_raw)
                except Exception:
                    genre = str(genre_raw)

            # Parse year
            release_year = None
            if year_raw:
                try:
                    release_year = int(year_raw)
                except (ValueError, TypeError):
                    pass

            # Parse company/studio (may be complex JSON structure)
            company = None
            if company_raw:
                try:
                    if isinstance(company_raw, str):
                        if company_raw.startswith("{") or company_raw.startswith("["):
                            company_data = json.loads(company_raw)
                        else:
                            company = company_raw
                            company_data = None
                    else:
                        company_data = company_raw
                    
                    if company_data and not company:
                        # Handle nested structure like {"production": [...], "distribution": [...]}
                        if isinstance(company_data, dict):
                            # Look for production companies first
                            prod_list = company_data.get("production", [])
                            if prod_list and isinstance(prod_list, list):
                                for prod in prod_list:
                                    if isinstance(prod, dict):
                                        comp = prod.get("company", {})
                                        if isinstance(comp, dict):
                                            company = comp.get("name")
                                            if company:
                                                break
                        elif isinstance(company_data, list) and company_data:
                            # Simple list of companies
                            if isinstance(company_data[0], dict):
                                company = company_data[0].get("name") or company_data[0].get("companyName")
                            else:
                                company = str(company_data[0])
                except Exception:
                    pass  # Leave company as None if parsing fails

            results[title_id] = {
                "genre": genre,
                "release_year": release_year,
                "company": company,
                "title": row.get("title"),
            }

    return results


def check_content_info(
    client: WorkspaceClient,
    warehouse_id: str,
    imdb_ids: List[str],
    content_table: Optional[str] = None,
) -> Dict[str, Dict]:
    """
    Check which IMDB IDs have a program_id in content_info.
    
    Returns: {imdb_id: {"program_id": int|None, "import_id": str|None, "primary_genre": str|None}}
    - If program_id exists -> title is "Returning"
    - If program_id is None -> title is "Fresh"
    - import_id contains studio info for disambiguation
    - primary_genre is the CMS genre classification
    """
    if not imdb_ids:
        return {}

    content_tbl = content_table or os.getenv(
        "DATABRICKS_CONTENT_TABLE",
        "core_prod.content.content_info"
    )

    valid_ids = [iid for iid in imdb_ids if iid and iid.startswith("tt")]
    if not valid_ids:
        return {}

    results: Dict[str, Dict] = {
        iid: {"program_id": None, "import_id": None, "primary_genre": None, "logline": None}
        for iid in valid_ids
    }
    batch_size = 200

    for i in range(0, len(valid_ids), batch_size):
        batch = valid_ids[i:i + batch_size]
        imdb_ids_str = ",".join([f"'{iid}'" for iid in batch])

        # Try different column combinations for IMDB ID, genre, and logline
        base_options = [
            ("program_imdb_id", "program_id", "import_id", "primary_genre"),
            ("program_imdb_id", "program_id", "import_id", "genre"),
            ("imdb_id", "program_id", "import_id", "primary_genre"),
            ("imdb_id", "program_id", "import_id", "genre"),
        ]
        # content_info has program_description, description, title_description; no "logline"
        logline_cols = ("program_description", "description", "title_description", None)

        rows = None
        for imdb_col, prog_col, import_col, genre_col in base_options:
            for logline_col in logline_cols:
                logline_select = f", {logline_col} as logline" if logline_col else ""
                query = f"""
                SELECT
                    {imdb_col} as imdb_id,
                    {prog_col} as program_id,
                    {import_col} as import_id,
                    {genre_col} as primary_genre
                    {logline_select}
                FROM {content_tbl}
                WHERE {imdb_col} IN ({imdb_ids_str})
                """
                try:
                    rows = execute_sql_query(client, warehouse_id, query)
                    break
                except Exception as e:
                    if "column" in str(e).lower() or "unresolved" in str(e).lower():
                        continue
                    raise
            if rows is not None:
                break

        if rows:
            for row in rows:
                imdb_id = row.get("imdb_id")
                program_id = row.get("program_id")
                import_id = row.get("import_id")
                primary_genre = row.get("primary_genre")
                logline_val = row.get("logline")
                if imdb_id:
                    if program_id:
                        try:
                            results[imdb_id]["program_id"] = int(program_id)
                        except (ValueError, TypeError):
                            pass
                    if import_id:
                        results[imdb_id]["import_id"] = str(import_id)
                    if primary_genre:
                        results[imdb_id]["primary_genre"] = str(primary_genre)
                    if logline_val:
                        results[imdb_id]["logline"] = str(logline_val)

    return results


def search_by_title(
    client: WorkspaceClient,
    warehouse_id: str,
    title_query: str,
    year: Optional[int] = None,
    title_table: Optional[str] = None,
    limit: int = 10,
) -> List[Dict]:
    """
    Search IMDB by title name.
    
    Returns: List of matching titles with basic metadata.
    """
    title_tbl = title_table or os.getenv(
        "DATABRICKS_IMDB_TITLE_TABLE",
        "core_dev.imdb.imdb_title_essential_v2"
    )
    
    # Escape single quotes in search term
    search_term = title_query.replace("'", "''")
    
    # Build query with optional year filter
    year_filter = f"AND year = {year}" if year else ""
    
    # Try different column name variations
    column_options = [
        ("originalTitle", "titleType"),
        ("title", "titleType"),
        ("originalTitle", "type"),
        ("title", "type"),
    ]
    
    rows = None
    title_col_used = None
    
    for title_col, type_col in column_options:
        query = f"""
        SELECT
            titleId,
            {title_col} as title,
            year,
            genres,
            {type_col} as titleType
        FROM {title_tbl}
        WHERE LOWER({title_col}) LIKE LOWER('%{search_term}%')
        {year_filter}
        ORDER BY 
            CASE WHEN LOWER({title_col}) = LOWER('{search_term}') THEN 0
                 WHEN LOWER({title_col}) LIKE LOWER('{search_term}%') THEN 1
                 ELSE 2 END,
            year DESC
        LIMIT {limit}
        """
        try:
            rows = execute_sql_query(client, warehouse_id, query)
            title_col_used = title_col
            break
        except Exception as e:
            msg = str(e).lower()
            if "column" in msg or "unresolved" in msg:
                continue
            raise
    
    if rows is None:
        return []
    
    results = []
    for row in rows:
        title_id = row.get("titleId")
        if not title_id:
            continue
        
        genre_raw = row.get("genres")
        genre = None
        if genre_raw:
            try:
                if isinstance(genre_raw, str) and genre_raw.startswith("["):
                    genre = ", ".join(json.loads(genre_raw))
                else:
                    genre = str(genre_raw)
            except Exception:
                genre = str(genre_raw)
        
        year_val = row.get("year")
        try:
            year_val = int(year_val) if year_val else None
        except (ValueError, TypeError):
            year_val = None
        
        results.append({
            "imdb_id": title_id,
            "title": row.get("title"),
            "year": year_val,
            "genre": genre,
            "type": row.get("titleType"),
        })
    
    return results


def lookup_by_program_id(
    client: WorkspaceClient,
    warehouse_id: str,
    program_id: int,
    content_table: Optional[str] = None,
) -> Optional[str]:
    """
    Look up IMDB ID by program_id from content_info.
    
    Returns: IMDB ID or None
    """
    content_tbl = content_table or os.getenv(
        "DATABRICKS_CONTENT_TABLE",
        "core_prod.content.content_info"
    )
    
    # Try different column names
    column_options = [
        ("program_imdb_id", "program_id"),
        ("imdb_id", "program_id"),
    ]
    
    for imdb_col, prog_col in column_options:
        query = f"""
        SELECT {imdb_col} as imdb_id
        FROM {content_tbl}
        WHERE {prog_col} = {program_id}
        LIMIT 1
        """
        try:
            rows = execute_sql_query(client, warehouse_id, query)
            if rows:
                imdb_id = rows[0].get("imdb_id")
                if imdb_id and str(imdb_id).startswith("tt"):
                    return str(imdb_id)
        except Exception as e:
            if "column" in str(e).lower() or "unresolved" in str(e).lower():
                continue
            raise
    
    return None


def lookup_metadata_by_program_id(program_id: int) -> Dict:
    """
    Look up metadata by program_id from content_info.
    Resolves IMDB ID first, then fetches full metadata.
    """
    client = get_databricks_client()
    warehouse_id = get_sql_warehouse_id(client)
    imdb_id = lookup_by_program_id(client, warehouse_id, program_id)
    if not imdb_id:
        return {
            "program_id": program_id,
            "imdb_id": None,
            "error": "No IMDB ID found for this program_id",
        }
    result = lookup_title(imdb_id)
    result["program_id"] = program_id
    return result


def lookup_title(imdb_id: str) -> Dict:
    """
    Look up metadata for a single IMDB ID.
    
    Returns: {
        "imdb_id": str,
        "primary_genre": str or None (from CMS, preferred),
        "imdb_genre": str or None (from IMDB, fallback),
        "release_year": int or None,
        "company": str or None (studio from IMDB),
        "program_id": int or None,
        "import_id": str or None (studio from content_info),
        "fresh_vs_returning": "Fresh" or "Returning"
    }
    """
    client = get_databricks_client()
    warehouse_id = get_sql_warehouse_id(client)

    imdb_meta = get_imdb_metadata(client, warehouse_id, [imdb_id])
    content_info = check_content_info(client, warehouse_id, [imdb_id])

    meta = imdb_meta.get(imdb_id, {})
    content = content_info.get(imdb_id, {})
    program_id = content.get("program_id")
    import_id = content.get("import_id")
    primary_genre = content.get("primary_genre")
    imdb_genre = meta.get("genre")

    return {
        "imdb_id": imdb_id,
        "title": meta.get("title"),
        "logline": content.get("logline"),
        "primary_genre": primary_genre,  # CMS genre (main)
        "imdb_genre": imdb_genre,         # IMDB genre (fallback)
        "release_year": meta.get("release_year"),
        "company": meta.get("company"),
        "program_id": program_id,
        "import_id": import_id,
        "fresh_vs_returning": "Returning" if program_id else "Fresh",
    }


def enrich_csv(
    input_path: str,
    output_path: Optional[str] = None,
    overwrite_existing: bool = False,
) -> str:
    """
    Enrich a CSV file with metadata from Databricks.
    
    Fills in: Genre(s), Release Year, Fresh vs. Returning
    
    Args:
        input_path: Path to input CSV
        output_path: Path for output CSV (default: adds '_enriched' suffix)
        overwrite_existing: If True, overwrite existing values; if False, only fill blanks
    
    Returns: Path to enriched CSV
    """
    import pandas as pd

    df = pd.read_csv(input_path, low_memory=False)

    # Get unique IMDB IDs
    imdb_col = "IMDB ID"
    if imdb_col not in df.columns:
        raise ValueError(f"CSV must have '{imdb_col}' column")

    imdb_ids = df[imdb_col].dropna().unique().tolist()
    imdb_ids = [str(iid).strip() for iid in imdb_ids if str(iid).strip().startswith("tt")]

    if not imdb_ids:
        print("No valid IMDB IDs found in CSV")
        return input_path

    print(f"Looking up metadata for {len(imdb_ids)} unique IMDB IDs...")

    client = get_databricks_client()
    warehouse_id = get_sql_warehouse_id(client)

    # Fetch metadata
    print("Fetching IMDB metadata (genre, year)...")
    imdb_meta = get_imdb_metadata(client, warehouse_id, imdb_ids)

    print("Checking content_info for program IDs...")
    content_info = check_content_info(client, warehouse_id, imdb_ids)

    # Enrich the dataframe
    genre_col = "Genre(s)" if "Genre(s)" in df.columns else "Primary Genre"
    year_col = "Release Year"
    fresh_col = "Fresh vs. Returning"
    studio_col = "Studio"

    enriched_count = {"genre": 0, "year": 0, "fresh": 0}

    for idx, row in df.iterrows():
        imdb_id = str(row.get(imdb_col, "")).strip()
        if not imdb_id.startswith("tt"):
            continue

        meta = imdb_meta.get(imdb_id, {})
        content = content_info.get(imdb_id, {})
        program_id = content.get("program_id")
        import_id = content.get("import_id")
        
        # Use studio from CSV to help verify match (for logging/debugging)
        csv_studio = str(row.get(studio_col, "")).strip() if studio_col in df.columns else ""
        if import_id and csv_studio and import_id.lower() != csv_studio.lower():
            # Log potential mismatch for review (studio in DB doesn't match CSV)
            print(f"  Note: {imdb_id} - CSV studio '{csv_studio}' vs DB import_id '{import_id}'")

        # Genre
        if genre_col in df.columns:
            current_genre = row.get(genre_col)
            if overwrite_existing or pd.isna(current_genre) or str(current_genre).strip() == "":
                if meta.get("genre"):
                    df.at[idx, genre_col] = meta["genre"]
                    enriched_count["genre"] += 1

        # Release Year
        if year_col in df.columns:
            current_year = row.get(year_col)
            if overwrite_existing or pd.isna(current_year) or str(current_year).strip() == "":
                if meta.get("release_year"):
                    df.at[idx, year_col] = meta["release_year"]
                    enriched_count["year"] += 1

        # Fresh vs. Returning
        if fresh_col in df.columns:
            current_fresh = row.get(fresh_col)
            if overwrite_existing or pd.isna(current_fresh) or str(current_fresh).strip() == "":
                fresh_value = "Returning" if program_id else "Fresh"
                df.at[idx, fresh_col] = fresh_value
                enriched_count["fresh"] += 1

    print(f"Enriched: {enriched_count['genre']} genres, "
          f"{enriched_count['year']} years, "
          f"{enriched_count['fresh']} fresh/returning values")

    # Write output
    if output_path is None:
        base, ext = os.path.splitext(input_path)
        output_path = f"{base}_enriched{ext}"

    df.to_csv(output_path, index=False)
    print(f"Wrote enriched CSV to: {output_path}")

    return output_path


def enrich_csv_content(
    csv_content: str,
    overwrite_existing: bool = False,
) -> str:
    """
    Enrich CSV content in-memory and return enriched CSV as a string.
    For hosted/stateless deployment where file paths are not available.

    Fills in: Genre(s), Release Year, Fresh vs. Returning

    Args:
        csv_content: Raw CSV content (must have 'IMDB ID' column)
        overwrite_existing: If True, overwrite existing values; if False, only fill blanks

    Returns: Enriched CSV content as string
    """
    import pandas as pd

    df = pd.read_csv(io.StringIO(csv_content), low_memory=False)

    imdb_col = "IMDB ID"
    if imdb_col not in df.columns:
        raise ValueError(f"CSV must have '{imdb_col}' column")

    imdb_ids = df[imdb_col].dropna().unique().tolist()
    imdb_ids = [str(iid).strip() for iid in imdb_ids if str(iid).strip().startswith("tt")]

    if not imdb_ids:
        return csv_content  # No valid IMDB IDs, return unchanged

    client = get_databricks_client()
    warehouse_id = get_sql_warehouse_id(client)
    imdb_meta = get_imdb_metadata(client, warehouse_id, imdb_ids)
    content_info = check_content_info(client, warehouse_id, imdb_ids)

    genre_col = "Genre(s)" if "Genre(s)" in df.columns else "Primary Genre"
    year_col = "Release Year"
    fresh_col = "Fresh vs. Returning"
    studio_col = "Studio"

    for idx, row in df.iterrows():
        imdb_id = str(row.get(imdb_col, "")).strip()
        if not imdb_id.startswith("tt"):
            continue

        meta = imdb_meta.get(imdb_id, {})
        content = content_info.get(imdb_id, {})
        program_id = content.get("program_id")

        if genre_col in df.columns:
            current_genre = row.get(genre_col)
            if overwrite_existing or pd.isna(current_genre) or str(current_genre).strip() == "":
                if meta.get("genre"):
                    df.at[idx, genre_col] = meta["genre"]

        if year_col in df.columns:
            current_year = row.get(year_col)
            if overwrite_existing or pd.isna(current_year) or str(current_year).strip() == "":
                if meta.get("release_year"):
                    df.at[idx, year_col] = meta["release_year"]

        if fresh_col in df.columns:
            current_fresh = row.get(fresh_col)
            if overwrite_existing or pd.isna(current_fresh) or str(current_fresh).strip() == "":
                fresh_value = "Returning" if program_id else "Fresh"
                df.at[idx, fresh_col] = fresh_value

    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue()


if __name__ == "__main__":
    # Quick test with a sample IMDB ID
    import sys
    
    if len(sys.argv) > 1:
        test_id = sys.argv[1]
    else:
        test_id = "tt0139134"  # Cruel Intentions
    
    print(f"Looking up: {test_id}")
    result = lookup_title(test_id)
    print(json.dumps(result, indent=2))
