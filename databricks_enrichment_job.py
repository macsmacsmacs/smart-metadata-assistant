#!/usr/bin/env python3
"""
Databricks job: fetch records from Airtable, enrich via metadata_lookup, PATCH back.

Job parameters (e.g. python_params or notebook_params):
  - baseId (str): Airtable base ID
  - tableId (str): Airtable table ID
  - recordIds (str): JSON array of record IDs, e.g. '["recXXX","recYYY"]'; or "view" if using viewId
  - viewId (str, optional): View ID when recordIds == "view" (enrich all records in view)

Environment / secrets:
  - AIRTABLE_PAT: Personal Access Token for Airtable API (or use dbutils.secrets.get)
  - DATABRICKS_HOST, DATABRICKS_TOKEN: for metadata_lookup (or secrets.py / job config)
"""

from __future__ import annotations

import json
import logging
import os
import sys
from typing import Any, Dict, List, Optional

import requests

from metadata_lookup import (
    check_content_info,
    get_databricks_client,
    get_imdb_metadata,
    get_sql_warehouse_id,
    lookup_by_program_id,
    search_by_title,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Airtable field names (must match base schema)
FIELD_IMDB_ID = "IMDB ID"
FIELD_PROGRAM_ID = "Program ID"
FIELD_TITLE = "Title"
FIELD_PRIMARY_GENRE = "Primary Genre"
FIELD_GENRES = "Genre(s)"
FIELD_RELEASE_YEAR = "Release Year"
FIELD_FRESH_RETURNING = "Fresh vs. Returning"
FIELD_STUDIO = "Studio"
FIELD_STATUS = "Status"

AIRTABLE_API_BASE = "https://api.airtable.com/v0"
PAGE_SIZE = 100
RECORD_ID_CHUNK = 10  # Airtable formula length limit when filtering by record IDs


def get_airtable_pat() -> str:
    """Get Airtable PAT from environment or Databricks secrets."""
    try:
        from dbutils.secrets import get
        pat = get(scope="airtable", key="pat")
        return pat
    except Exception:
        pass
    pat = os.getenv("AIRTABLE_PAT")
    if not pat:
        raise ValueError("Set AIRTABLE_PAT or dbutils.secrets(scope='airtable', key='pat')")
    return pat


def airtable_get(
    pat: str,
    base_id: str,
    table_id: str,
    *,
    view_id: Optional[str] = None,
    record_ids: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Fetch all records from Airtable (by view with pagination, or by record IDs in chunks)."""
    records: List[Dict[str, Any]] = []
    headers = {"Authorization": f"Bearer {pat}", "Content-Type": "application/json"}

    if view_id:
        offset = None
        while True:
            url = f"{AIRTABLE_API_BASE}/{base_id}/{table_id}"
            params = {"view": view_id, "pageSize": PAGE_SIZE}
            if offset:
                params["offset"] = offset
            resp = requests.get(url, headers=headers, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            records.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
        return records

    if record_ids:
        for i in range(0, len(record_ids), RECORD_ID_CHUNK):
            chunk = record_ids[i : i + RECORD_ID_CHUNK]
            formula = ",".join(f"RECORD_ID()='{rid}'" for rid in chunk)
            formula = f"OR({formula})"
            url = f"{AIRTABLE_API_BASE}/{base_id}/{table_id}"
            params = {"filterByFormula": formula, "pageSize": PAGE_SIZE}
            resp = requests.get(url, headers=headers, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            records.extend(data.get("records", []))
        return records

    raise ValueError("Provide either view_id or record_ids")


def airtable_patch(
    pat: str,
    base_id: str,
    table_id: str,
    record_id: str,
    fields: Dict[str, Any],
) -> None:
    """PATCH a single record in Airtable."""
    url = f"{AIRTABLE_API_BASE}/{base_id}/{table_id}/{record_id}"
    headers = {"Authorization": f"Bearer {pat}", "Content-Type": "application/json"}
    resp = requests.patch(url, headers=headers, json={"fields": fields}, timeout=30)
    resp.raise_for_status()


def resolve_imdb_id(
    client,
    warehouse_id: str,
    fields: Dict[str, Any],
    use_title_fallback: bool = True,
) -> Optional[str]:
    """Resolve IMDB ID from record fields: IMDB ID, else Program ID, else optional search_by_title."""
    imdb_raw = (fields.get(FIELD_IMDB_ID) or "").strip()
    if imdb_raw:
        s = str(imdb_raw).strip()
        if s.lower().startswith("tt"):
            return s
        if s.isdigit():
            return f"tt{s}"
        return s

    try:
        prog_raw = fields.get(FIELD_PROGRAM_ID)
        if prog_raw is not None and str(prog_raw).strip():
            prog_id = int(prog_raw) if isinstance(prog_raw, (int, float)) else int(str(prog_raw).strip())
            imdb_id = lookup_by_program_id(client, warehouse_id, prog_id)
            if imdb_id:
                return imdb_id
    except (ValueError, TypeError):
        pass

    if use_title_fallback:
        title = (fields.get(FIELD_TITLE) or "").strip()
        if title:
            results = search_by_title(client, warehouse_id, title, limit=1)
            if results:
                return results[0].get("imdb_id")
    return None


def run_job(
    base_id: str,
    table_id: str,
    record_ids: Optional[List[str]] = None,
    view_id: Optional[str] = None,
) -> None:
    """Fetch records from Airtable, enrich with metadata_lookup, PATCH back."""
    pat = get_airtable_pat()
    if view_id:
        record_ids = None

    records = airtable_get(pat, base_id, table_id, view_id=view_id, record_ids=record_ids)
    if not records:
        logger.info("No records to enrich.")
        return

    logger.info("Fetched %d records from Airtable.", len(records))

    client = get_databricks_client()
    warehouse_id = get_sql_warehouse_id(client)

    # Resolve IMDB ID for each record
    for rec in records:
        rid = rec.get("id")
        fields = rec.get("fields") or {}
        imdb_id = resolve_imdb_id(client, warehouse_id, fields)
        rec["_resolved_imdb_id"] = imdb_id
        rec["_fields"] = fields

    # Optional: PATCH Status = Enriching for each
    status_field = {FIELD_STATUS: "Enriching"}
    for rec in records:
        try:
            airtable_patch(pat, base_id, table_id, rec["id"], status_field)
        except Exception as e:
            logger.warning("Could not set Enriching for %s: %s", rec["id"], e)

    # Batch enrichment
    imdb_ids = [r["_resolved_imdb_id"] for r in records if r.get("_resolved_imdb_id")]
    imdb_meta = get_imdb_metadata(client, warehouse_id, imdb_ids) if imdb_ids else {}
    content_info = check_content_info(client, warehouse_id, imdb_ids) if imdb_ids else {}

    # Determine genre column (same as metadata_lookup.enrich_csv)
    first_rec_fields = records[0].get("_fields") or {}
    genre_col = FIELD_GENRES if FIELD_GENRES in first_rec_fields else FIELD_PRIMARY_GENRE

    for rec in records:
        record_id = rec["id"]
        fields = rec.get("_fields") or {}
        imdb_id = rec.get("_resolved_imdb_id")

        try:
            if not imdb_id:
                airtable_patch(
                    pat, base_id, table_id, record_id,
                    {FIELD_STATUS: "Enrichment failed"},
                )
                logger.warning("No IMDB ID for record %s; skipped.", record_id)
                continue

            meta = imdb_meta.get(imdb_id, {})
            content = content_info.get(imdb_id, {})
            program_id = content.get("program_id")
            primary_genre = content.get("primary_genre")
            imdb_genre = meta.get("genre")
            genre = primary_genre or imdb_genre
            release_year = meta.get("release_year")
            fresh = "Returning" if program_id else "Fresh"
            studio = meta.get("company") or content.get("import_id")

            update: Dict[str, Any] = {FIELD_STATUS: "Enriched - pending review"}
            if genre is not None:
                update[genre_col] = genre
            if release_year is not None:
                update[FIELD_RELEASE_YEAR] = release_year
            update[FIELD_FRESH_RETURNING] = fresh
            if program_id is not None:
                update[FIELD_PROGRAM_ID] = program_id
            if studio:
                update[FIELD_STUDIO] = studio

            airtable_patch(pat, base_id, table_id, record_id, update)
            logger.info("Enriched record %s (%s).", record_id, imdb_id)

        except Exception as e:
            logger.exception("Enrichment failed for %s: %s", record_id, e)
            try:
                airtable_patch(
                    pat, base_id, table_id, record_id,
                    {FIELD_STATUS: "Enrichment failed"},
                )
            except Exception:
                pass


def main() -> None:
    """Parse job params and run. Databricks passes python_params as sys.argv[1], [2], ..."""
    # Support both: (baseId, tableId, recordIdsJson) or (baseId, tableId, "view", viewId)
    if len(sys.argv) < 4:
        logger.error("Usage: databricks_enrichment_job.py <baseId> <tableId> <recordIdsJson|view> [viewId]")
        sys.exit(1)

    base_id = sys.argv[1].strip()
    table_id = sys.argv[2].strip()
    record_ids_arg = sys.argv[3].strip()
    view_id = sys.argv[4].strip() if len(sys.argv) > 4 else None

    record_ids: Optional[List[str]] = None
    if record_ids_arg.lower() != "view":
        try:
            record_ids = json.loads(record_ids_arg)
        except json.JSONDecodeError:
            logger.error("recordIds must be JSON array or 'view'.")
            sys.exit(1)
    elif view_id:
        pass
    else:
        logger.error("When using 'view', provide viewId as fourth argument.")
        sys.exit(1)

    run_job(base_id=base_id, table_id=table_id, record_ids=record_ids, view_id=view_id or None)


if __name__ == "__main__":
    main()
