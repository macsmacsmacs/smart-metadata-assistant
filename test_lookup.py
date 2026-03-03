#!/usr/bin/env python3
"""
Test script for metadata lookup POC.

Tests with sample IMDB IDs from the CSV that are missing metadata.
"""

import json
from metadata_lookup import lookup_title, get_databricks_client, get_sql_warehouse_id

# Sample IMDB IDs from the CSV that need metadata:
# - Some are missing Release Year
# - Some should be "Returning" (have program_id)
# - Some should be "Fresh" (no program_id)

TEST_CASES = [
    # (imdb_id, title, expected_status, notes)
    ("tt0139134", "Cruel Intentions", None, "Missing Release Year in CSV"),
    ("tt23621682", "Bambi: the Reckoning", None, "Missing Release Year in CSV"),
    ("tt11762434", "Cosmic Sin", "Returning", "Has program_id 100024473 in CSV"),
    ("tt7886936", "Dating Amber", None, "Missing Release Year in CSV"),
    ("tt10310140", "Fatman", "Returning", "Has program_id 100024728 in CSV"),
    ("tt14555174", "Assassin", None, "Missing Release Year in CSV"),
]


def main():
    print("=" * 60)
    print("Metadata Lookup POC Test")
    print("=" * 60)
    
    # Test connection first
    print("\nTesting Databricks connection...")
    try:
        client = get_databricks_client()
        warehouse_id = get_sql_warehouse_id(client)
        print(f"  Connected! Warehouse ID: {warehouse_id}")
    except Exception as e:
        print(f"  ERROR: Could not connect to Databricks: {e}")
        print("\nMake sure you have set DATABRICKS_HOST and DATABRICKS_TOKEN")
        print("or created a secrets.py file with those values.")
        return
    
    print("\n" + "-" * 60)
    print("Testing individual lookups...")
    print("-" * 60)
    
    for imdb_id, title, expected_status, notes in TEST_CASES:
        print(f"\n{title} ({imdb_id})")
        print(f"  Notes: {notes}")
        
        try:
            result = lookup_title(imdb_id)
            print(f"  Genre: {result.get('genre')}")
            print(f"  Release Year: {result.get('release_year')}")
            print(f"  Company (IMDB): {result.get('company')}")
            print(f"  Program ID: {result.get('program_id')}")
            print(f"  Import ID (content_info): {result.get('import_id')}")
            print(f"  Fresh vs. Returning: {result.get('fresh_vs_returning')}")
            
            if expected_status and result.get('fresh_vs_returning') != expected_status:
                print(f"  WARNING: Expected '{expected_status}' but got '{result.get('fresh_vs_returning')}'")
        except Exception as e:
            print(f"  ERROR: {e}")
    
    print("\n" + "=" * 60)
    print("Test complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
