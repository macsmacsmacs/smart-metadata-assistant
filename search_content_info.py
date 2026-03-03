#!/usr/bin/env python3
"""
Search IMDB for kids reality competition content, filtered to only titles
with a program_id in content_info (catalog titles).

Examples: MasterChef Junior, Guts, Kid Nation.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from typing import Any, Dict, List, Optional

from metadata_lookup import (
    check_content_info,
    execute_sql_query,
    get_databricks_client,
    get_sql_warehouse_id,
)

IMDB_TABLE = os.getenv("DATABRICKS_IMDB_TITLE_TABLE", "core_dev.imdb.imdb_title_essential_v2")
CONTENT_TABLE = os.getenv("DATABRICKS_CONTENT_TABLE", "core_prod.content.content_info")
DEFAULT_OUTPUT = "kids_reality_competition_content.csv"


def _normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a result row to consistent output format."""
    genre_raw = row.get("imdb_genres") or row.get("genres")
    if genre_raw:
        try:
            if isinstance(genre_raw, str) and genre_raw.startswith("["):
                genre_raw = ", ".join(json.loads(genre_raw))
            else:
                genre_raw = str(genre_raw)
        except Exception:
            genre_raw = str(genre_raw) if genre_raw else ""
    else:
        genre_raw = ""

    return {
        "imdb_id": row.get("imdb_id") or row.get("titleId"),
        "title": row.get("title") or row.get("originalTitle"),
        "year": row.get("year"),
        "imdb_genres": genre_raw,
        "titleType": row.get("titleType") or row.get("type"),
        "program_id": row.get("program_id"),
        "import_id": row.get("import_id"),
        "primary_genre": row.get("primary_genre"),
    }


def search_kids_reality_competition(
    client: Any,
    warehouse_id: str,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    """
    Search IMDB for kids reality competition content with program_id.
    Uses INNER JOIN with content_info; falls back to two-step if join fails.
    """
    # Query variants: try program_imdb_id first, then imdb_id
    # IMDB title col: originalTitle first, then title
    join_options = [
        ("program_imdb_id", "originalTitle", "titleType"),
        ("program_imdb_id", "title", "titleType"),
        ("program_imdb_id", "originalTitle", "type"),
        ("program_imdb_id", "title", "type"),
        ("imdb_id", "originalTitle", "titleType"),
        ("imdb_id", "title", "titleType"),
        ("imdb_id", "originalTitle", "type"),
        ("imdb_id", "title", "type"),
    ]

    for ci_imdb_col, imdb_title_col, imdb_type_col in join_options:
        query = f"""
        SELECT
            imdb.titleId AS imdb_id,
            imdb.{imdb_title_col} AS title,
            imdb.year,
            imdb.genres AS imdb_genres,
            imdb.{imdb_type_col} AS titleType,
            ci.program_id,
            ci.import_id,
            ci.primary_genre
        FROM {IMDB_TABLE} imdb
        INNER JOIN {CONTENT_TABLE} ci ON ci.{ci_imdb_col} = imdb.titleId
        WHERE ci.program_id IS NOT NULL
          AND (
            LOWER(CAST(imdb.genres AS STRING)) LIKE '%reality%'
            OR LOWER(CAST(imdb.genres AS STRING)) LIKE '%game-show%'
            OR LOWER(CAST(imdb.genres AS STRING)) LIKE '%competition%'
            OR LOWER(CAST(imdb.genres AS STRING)) LIKE '%game show%'
          )
          AND (
            LOWER(CAST(imdb.genres AS STRING)) LIKE '%kids%'
            OR LOWER(CAST(imdb.genres AS STRING)) LIKE '%family%'
            OR LOWER(CAST(imdb.{imdb_title_col} AS STRING)) LIKE '%junior%'
            OR LOWER(CAST(imdb.{imdb_title_col} AS STRING)) LIKE '%kids%'
            OR LOWER(CAST(imdb.{imdb_title_col} AS STRING)) LIKE '%guts%'
            OR LOWER(CAST(imdb.{imdb_title_col} AS STRING)) LIKE '%kid nation%'
          )
          AND imdb.{imdb_type_col} IN ('tvSeries', 'tvMiniSeries')
        ORDER BY imdb.year DESC NULLS LAST
        LIMIT {limit}
        """
        try:
            rows = execute_sql_query(client, warehouse_id, query)
            if rows is not None:
                return [_normalize_row(r) for r in rows]
        except Exception as e:
            msg = str(e).lower()
            if "column" in msg or "unresolved" in msg or "cannot be resolved" in msg:
                continue
            raise

    # Two-step fallback: query IMDB broadly, then filter by check_content_info
    return _search_two_step(client, warehouse_id, limit)


def _search_two_step(client: Any, warehouse_id: str, limit: int) -> List[Dict[str, Any]]:
    """Fallback: query IMDB for genre+title matches, then filter by program_id."""
    imdb_column_options = [
        ("originalTitle", "titleType"),
        ("title", "titleType"),
        ("originalTitle", "type"),
        ("title", "type"),
    ]

    for title_col, type_col in imdb_column_options:
        query = f"""
        SELECT titleId, {title_col} AS title, year, genres, {type_col} AS titleType
        FROM {IMDB_TABLE}
        WHERE (
            LOWER(CAST(genres AS STRING)) LIKE '%reality%'
            OR LOWER(CAST(genres AS STRING)) LIKE '%game-show%'
            OR LOWER(CAST(genres AS STRING)) LIKE '%competition%'
            OR LOWER(CAST(genres AS STRING)) LIKE '%game show%'
          )
          AND (
            LOWER(CAST(genres AS STRING)) LIKE '%kids%'
            OR LOWER(CAST(genres AS STRING)) LIKE '%family%'
            OR LOWER(CAST({title_col} AS STRING)) LIKE '%junior%'
            OR LOWER(CAST({title_col} AS STRING)) LIKE '%kids%'
            OR LOWER(CAST({title_col} AS STRING)) LIKE '%guts%'
            OR LOWER(CAST({title_col} AS STRING)) LIKE '%kid nation%'
          )
          AND {type_col} IN ('tvSeries', 'tvMiniSeries')
        ORDER BY year DESC NULLS LAST
        LIMIT {min(limit * 3, 2000)}
        """
        try:
            rows = execute_sql_query(client, warehouse_id, query)
            if not rows:
                return []
        except Exception as e:
            msg = str(e).lower()
            if "column" in msg or "unresolved" in msg or "cannot be resolved" in msg:
                continue
            raise

        imdb_ids = [str(r.get("titleId", "")).strip() for r in rows if r.get("titleId")]
        imdb_ids = [i for i in imdb_ids if i.startswith("tt")]
        if not imdb_ids:
            return []

        content_info = check_content_info(client, warehouse_id, imdb_ids)
        results = []
        seen = set()
        for row in rows:
            imdb_id = str(row.get("titleId", "")).strip()
            if not imdb_id or imdb_id in seen:
                continue
            ci = content_info.get(imdb_id, {})
            if not ci.get("program_id"):
                continue
            seen.add(imdb_id)
            merged = {
                "imdb_id": imdb_id,
                "title": row.get("title"),
                "year": row.get("year"),
                "imdb_genres": row.get("genres"),
                "titleType": row.get("titleType"),
                "program_id": ci.get("program_id"),
                "import_id": ci.get("import_id"),
                "primary_genre": ci.get("primary_genre"),
            }
            results.append(_normalize_row(merged))
            if len(results) >= limit:
                break
        return results

    return []


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Search for kids reality competition content (MasterChef Junior, Guts, Kid Nation) with program_id."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Max number of results (default: 500)",
    )
    parser.add_argument(
        "--output",
        "-o",
        default=DEFAULT_OUTPUT,
        help=f"Output CSV path (default: {DEFAULT_OUTPUT})",
    )
    args = parser.parse_args()

    print("Connecting to Databricks...")
    client = get_databricks_client()
    warehouse_id = get_sql_warehouse_id(client)
    print(f"Warehouse: {warehouse_id}")

    print("Searching for kids reality competition content (with program_id)...")
    results = search_kids_reality_competition(client, warehouse_id, limit=args.limit)

    print(f"\nFound {len(results)} titles")

    if not results:
        print("No matching titles found.")
        sys.exit(0)

    # Write CSV
    fieldnames = ["imdb_id", "title", "year", "imdb_genres", "titleType", "program_id", "import_id", "primary_genre"]
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in results:
            writer.writerow({k: row.get(k, "") for k in fieldnames})

    print(f"Wrote {args.output}")

    # Console summary (first 10)
    print("\nSample results:")
    for i, row in enumerate(results[:10]):
        title = row.get("title") or "?"
        year = row.get("year") or "?"
        imdb_id = row.get("imdb_id") or "?"
        prog_id = row.get("program_id") or "?"
        print(f"  {i + 1}. {title} ({year}) - {imdb_id} [program_id={prog_id}]")


if __name__ == "__main__":
    main()
