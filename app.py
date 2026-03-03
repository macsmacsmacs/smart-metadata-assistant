#!/usr/bin/env python3
"""
Smart Metadata Assistant - Web Interface

A Streamlit app for looking up title metadata from Databricks.
"""

import streamlit as st
import pandas as pd
import io
import time
from typing import Optional

from metadata_lookup import (
    lookup_title,
    get_databricks_client,
    get_sql_warehouse_id,
    get_imdb_metadata,
    check_content_info,
    search_by_title,
    lookup_by_program_id,
)


st.set_page_config(
    page_title="Smart Metadata Assistant",
    page_icon="🎬",
    layout="wide",
)

# Custom CSS for better styling
st.markdown("""
<style>
    .stAlert {
        margin-top: 1rem;
    }
    .result-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .metric-label {
        font-size: 0.8rem;
        color: #666;
    }
    .metric-value {
        font-size: 1.1rem;
        font-weight: 600;
    }
    .fresh-badge {
        background-color: #28a745;
        color: white;
        padding: 0.25rem 0.5rem;
        border-radius: 0.25rem;
        font-weight: 600;
    }
    .returning-badge {
        background-color: #007bff;
        color: white;
        padding: 0.25rem 0.5rem;
        border-radius: 0.25rem;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)


def check_connection() -> bool:
    """Check if Databricks connection is working."""
    try:
        client = get_databricks_client()
        get_sql_warehouse_id(client)
        return True
    except Exception:
        return False


def copyable_field(label: str, value, key: str):
    """Display a field with a copy button."""
    display_value = str(value) if value else "—"
    col1, col2 = st.columns([4, 1])
    with col1:
        st.markdown(f"**{label}**")
        st.code(display_value, language=None)
    with col2:
        st.markdown("&nbsp;")  # Spacer to align with label
        if value:
            # The st.code already has copy, but add explicit button too
            if st.button("📋", key=f"copy_{key}", help=f"Copy {label}"):
                st.toast(f"Copied: {display_value}")


def display_result(result: dict):
    """Display lookup result in a nice format."""
    col1, col2 = st.columns(2)
    
    with col1:
        # Primary Genre from CMS
        primary_genre = result.get("primary_genre")
        imdb_genre = result.get("imdb_genre")
        
        st.markdown("**Primary Genre (CMS)**")
        if primary_genre:
            st.code(primary_genre, language=None)
        else:
            st.caption("Not in CMS")
        
        st.markdown("**IMDB Genre** *(fallback)*")
        if imdb_genre:
            st.code(imdb_genre, language=None)
        else:
            st.caption("—")
        
        st.markdown("**Release Year**")
        year = result.get("release_year")
        if year:
            st.code(str(year), language=None)
        else:
            st.caption("—")
        
        st.markdown("**Production Company**")
        company = result.get("company")
        if company:
            st.code(company, language=None)
        else:
            st.caption("—")
    
    with col2:
        st.markdown("**Status**")
        status = result.get("fresh_vs_returning", "Unknown")
        if status == "Fresh":
            st.markdown('<span class="fresh-badge">Fresh (New Title)</span>', unsafe_allow_html=True)
        elif status == "Returning":
            st.markdown('<span class="returning-badge">Returning</span>', unsafe_allow_html=True)
        else:
            st.write(status)
        
        st.markdown("**Program ID**")
        prog_id = result.get("program_id")
        if prog_id:
            st.code(str(prog_id), language=None)
        else:
            st.caption("—")
        
        st.markdown("**Import ID (Studio)**")
        import_id = result.get("import_id")
        if import_id:
            st.code(import_id, language=None)
        else:
            st.caption("—")
        
        st.markdown("**IMDB ID**")
        imdb_id = result.get("imdb_id")
        if imdb_id:
            st.code(imdb_id, language=None)
        else:
            st.caption("—")


def enrich_dataframe(df: pd.DataFrame, progress_bar) -> pd.DataFrame:
    """Enrich a dataframe with metadata."""
    # Find IMDB ID column
    imdb_col = None
    for col in ["IMDB ID", "imdb_id", "IMDB_ID", "imdbId"]:
        if col in df.columns:
            imdb_col = col
            break
    
    if not imdb_col:
        st.error("CSV must have an 'IMDB ID' column")
        return df
    
    # Get unique IMDB IDs
    imdb_ids = df[imdb_col].dropna().unique().tolist()
    imdb_ids = [str(iid).strip() for iid in imdb_ids if str(iid).strip().startswith("tt")]
    
    if not imdb_ids:
        st.warning("No valid IMDB IDs found in CSV")
        return df
    
    # Connect to Databricks
    client = get_databricks_client()
    warehouse_id = get_sql_warehouse_id(client)
    
    # Fetch metadata in batches
    progress_bar.progress(0.1, "Fetching IMDB metadata...")
    imdb_meta = get_imdb_metadata(client, warehouse_id, imdb_ids)
    
    progress_bar.progress(0.5, "Checking content_info...")
    content_info = check_content_info(client, warehouse_id, imdb_ids)
    
    progress_bar.progress(0.7, "Enriching rows...")
    
    # Determine column names
    genre_col = "Genre(s)" if "Genre(s)" in df.columns else "Primary Genre" if "Primary Genre" in df.columns else None
    year_col = "Release Year" if "Release Year" in df.columns else None
    fresh_col = "Fresh vs. Returning" if "Fresh vs. Returning" in df.columns else None
    
    # Add columns if they don't exist
    if genre_col is None:
        genre_col = "Genre"
        df[genre_col] = ""
    if year_col is None:
        year_col = "Release Year"
        df[year_col] = ""
    if fresh_col is None:
        fresh_col = "Fresh vs. Returning"
        df[fresh_col] = ""
    
    enriched_count = {"genre": 0, "year": 0, "fresh": 0}
    
    for idx, row in df.iterrows():
        imdb_id = str(row.get(imdb_col, "")).strip()
        if not imdb_id.startswith("tt"):
            continue
        
        meta = imdb_meta.get(imdb_id, {})
        content = content_info.get(imdb_id, {})
        program_id = content.get("program_id")
        
        # Genre
        current_genre = row.get(genre_col)
        if pd.isna(current_genre) or str(current_genre).strip() == "":
            if meta.get("genre"):
                df.at[idx, genre_col] = meta["genre"]
                enriched_count["genre"] += 1
        
        # Release Year
        current_year = row.get(year_col)
        if pd.isna(current_year) or str(current_year).strip() == "":
            if meta.get("release_year"):
                df.at[idx, year_col] = meta["release_year"]
                enriched_count["year"] += 1
        
        # Fresh vs. Returning
        current_fresh = row.get(fresh_col)
        if pd.isna(current_fresh) or str(current_fresh).strip() == "":
            fresh_value = "Returning" if program_id else "Fresh"
            df.at[idx, fresh_col] = fresh_value
            enriched_count["fresh"] += 1
    
    progress_bar.progress(1.0, "Complete!")
    
    st.success(f"Enriched: {enriched_count['genre']} genres, {enriched_count['year']} years, {enriched_count['fresh']} fresh/returning values")
    
    return df


def main():
    st.title("🎬 Smart Metadata Assistant")
    st.markdown("Look up title metadata from Databricks - Genre, Release Year, and Fresh vs. Returning status")
    
    # Connection status
    with st.sidebar:
        st.markdown("### Connection Status")
        if check_connection():
            st.success("✓ Connected to Databricks")
        else:
            st.error("✗ Not connected to Databricks")
            st.markdown("""
            Make sure you have:
            - `secrets.py` with `DATABRICKS_HOST` and `DATABRICKS_TOKEN`
            - Or environment variables set
            """)
            return
    
    # Main interface with tabs
    tab1, tab2, tab3 = st.tabs(["🔍 Lookup", "📋 Bulk ID Lookup", "📊 Batch Process CSV"])
    
    # Tab 1: Single Lookup
    with tab1:
        st.markdown("### Look up a single title")
        
        # Search type selector
        search_type = st.radio(
            "Search by:",
            ["IMDB ID", "Program ID (CMS)", "Title Name"],
            horizontal=True,
            help="Choose how you want to search for the title"
        )
        
        if search_type == "IMDB ID":
            col1, col2 = st.columns([3, 1])
            with col1:
                imdb_input = st.text_input(
                    "IMDB ID",
                    placeholder="tt0139134",
                    help="Enter an IMDB ID (e.g., tt0139134 for Cruel Intentions)"
                )
            with col2:
                lookup_btn = st.button("🔍 Look Up", type="primary", use_container_width=True)
            
            if lookup_btn and imdb_input:
                imdb_id = imdb_input.strip()
                if not imdb_id.startswith("tt"):
                    imdb_id = f"tt{imdb_id}"
                
                with st.spinner(f"Looking up {imdb_id}..."):
                    try:
                        result = lookup_title(imdb_id)
                        st.markdown(f"### Results for `{imdb_id}`")
                        display_result(result)
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
        
        elif search_type == "Program ID (CMS)":
            col1, col2 = st.columns([3, 1])
            with col1:
                program_input = st.text_input(
                    "Program ID",
                    placeholder="100024473",
                    help="Enter a numeric program ID from CMS"
                )
            with col2:
                lookup_btn = st.button("🔍 Look Up", type="primary", use_container_width=True, key="prog_btn")
            
            if lookup_btn and program_input:
                try:
                    program_id = int(program_input.strip())
                    with st.spinner(f"Looking up program ID {program_id}..."):
                        client = get_databricks_client()
                        warehouse_id = get_sql_warehouse_id(client)
                        
                        imdb_id = lookup_by_program_id(client, warehouse_id, program_id)
                        
                        if imdb_id:
                            result = lookup_title(imdb_id)
                            st.markdown(f"### Results for Program ID `{program_id}`")
                            st.info(f"Found IMDB ID: `{imdb_id}`")
                            display_result(result)
                        else:
                            st.warning(f"No IMDB ID found for program ID {program_id}")
                except ValueError:
                    st.error("Please enter a valid numeric program ID")
                except Exception as e:
                    st.error(f"Error: {str(e)}")
        
        elif search_type == "Title Name":
            col1, col2, col3 = st.columns([2, 1, 1])
            with col1:
                title_input = st.text_input(
                    "Title Name",
                    placeholder="Cruel Intentions",
                    help="Enter a title name to search"
                )
            with col2:
                year_input = st.text_input(
                    "Year (optional)",
                    placeholder="1999",
                    help="Filter by release year"
                )
            with col3:
                search_btn = st.button("🔍 Search", type="primary", use_container_width=True, key="title_btn")
            
            # Initialize session state for title search
            if 'title_search_results' not in st.session_state:
                st.session_state.title_search_results = None
            if 'selected_title_imdb' not in st.session_state:
                st.session_state.selected_title_imdb = None
            
            # Handle search
            if search_btn and title_input:
                # Clear previous selection when doing new search
                st.session_state.selected_title_imdb = None
                
                year_filter = None
                if year_input:
                    try:
                        year_filter = int(year_input.strip())
                    except ValueError:
                        st.warning("Invalid year, searching without year filter")
                
                with st.spinner(f"Searching for '{title_input}'..."):
                    try:
                        client = get_databricks_client()
                        warehouse_id = get_sql_warehouse_id(client)
                        
                        results = search_by_title(client, warehouse_id, title_input, year=year_filter)
                        st.session_state.title_search_results = results
                        
                        if not results:
                            st.warning(f"No titles found matching '{title_input}'")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
                        st.session_state.title_search_results = None
            
            # Display search results if we have them
            if st.session_state.title_search_results:
                results = st.session_state.title_search_results
                
                st.markdown(f"### Found {len(results)} matching titles")
                st.caption("Select a title to view full metadata:")
                
                # Create a selectbox for choosing the title
                title_options = [
                    f"{r['title']} ({r.get('year') or '?'}) - {r['imdb_id']}"
                    for r in results
                ]
                
                selected_idx = st.selectbox(
                    "Choose a title:",
                    range(len(title_options)),
                    format_func=lambda i: title_options[i],
                    key="title_selector"
                )
                
                # Also show as a table for reference
                with st.expander("View all matches as table"):
                    table_data = []
                    for r in results:
                        table_data.append({
                            "Title": r.get("title"),
                            "Year": r.get("year") or "—",
                            "Type": r.get("type") or "—",
                            "IMDB ID": r.get("imdb_id"),
                            "Genre": r.get("genre") or "—",
                        })
                    st.dataframe(table_data, use_container_width=True)
                
                # Get full details button
                if st.button("📋 Get Full Metadata", type="primary", key="get_details_btn"):
                    st.session_state.selected_title_imdb = results[selected_idx]['imdb_id']
                
                # Show full details if selected
                if st.session_state.selected_title_imdb:
                    selected_imdb = st.session_state.selected_title_imdb
                    st.divider()
                    st.markdown(f"### Full Details for `{selected_imdb}`")
                    
                    with st.spinner("Loading full metadata..."):
                        try:
                            full_result = lookup_title(selected_imdb)
                            display_result(full_result)
                        except Exception as e:
                            st.error(f"Error loading details: {str(e)}")
    
    # Tab 2: Bulk ID Lookup
    with tab2:
        st.markdown("### Bulk ID Lookup")
        st.markdown("Paste a list of IMDB IDs or Program IDs (one per line or comma-separated)")
        
        id_type = st.radio(
            "ID Type:",
            ["IMDB IDs", "Program IDs"],
            horizontal=True,
            key="bulk_id_type"
        )
        
        ids_input = st.text_area(
            "Paste IDs here:",
            height=150,
            placeholder="tt0139134\ntt1167638\ntt7329656\n\nor\n\ntt0139134, tt1167638, tt7329656",
            help="One ID per line, or comma-separated"
        )
        
        if st.button("🔍 Look Up All", type="primary", key="bulk_lookup_btn"):
            if not ids_input.strip():
                st.warning("Please paste some IDs")
            else:
                # Parse IDs - handle newlines and commas
                raw_ids = ids_input.replace(",", "\n").split("\n")
                ids = [id.strip() for id in raw_ids if id.strip()]
                
                if not ids:
                    st.warning("No valid IDs found")
                else:
                    with st.spinner(f"Looking up {len(ids)} IDs..."):
                        try:
                            client = get_databricks_client()
                            warehouse_id = get_sql_warehouse_id(client)
                            
                            results = []
                            
                            if id_type == "IMDB IDs":
                                # Normalize IMDB IDs
                                imdb_ids = []
                                for id in ids:
                                    if not id.startswith("tt"):
                                        id = f"tt{id}"
                                    imdb_ids.append(id)
                                
                                # Batch fetch metadata
                                imdb_meta = get_imdb_metadata(client, warehouse_id, imdb_ids)
                                content_info = check_content_info(client, warehouse_id, imdb_ids)
                                
                                for imdb_id in imdb_ids:
                                    meta = imdb_meta.get(imdb_id, {})
                                    content = content_info.get(imdb_id, {})
                                    results.append({
                                        "IMDB ID": imdb_id,
                                        "Primary Genre (CMS)": content.get("primary_genre") or "",
                                        "IMDB Genre": meta.get("genre") or "",
                                        "Release Year": meta.get("release_year") or "",
                                        "Program ID": content.get("program_id") or "",
                                        "Import ID": content.get("import_id") or "",
                                        "Fresh/Returning": "Returning" if content.get("program_id") else "Fresh",
                                        "Company": meta.get("company") or "",
                                    })
                            
                            else:  # Program IDs
                                for prog_id_str in ids:
                                    try:
                                        prog_id = int(prog_id_str)
                                        imdb_id = lookup_by_program_id(client, warehouse_id, prog_id)
                                        
                                        if imdb_id:
                                            imdb_meta = get_imdb_metadata(client, warehouse_id, [imdb_id])
                                            content_info = check_content_info(client, warehouse_id, [imdb_id])
                                            meta = imdb_meta.get(imdb_id, {})
                                            content = content_info.get(imdb_id, {})
                                            
                                            results.append({
                                                "Program ID": prog_id,
                                                "IMDB ID": imdb_id,
                                                "Primary Genre (CMS)": content.get("primary_genre") or "",
                                                "IMDB Genre": meta.get("genre") or "",
                                                "Release Year": meta.get("release_year") or "",
                                                "Import ID": content.get("import_id") or "",
                                                "Fresh/Returning": "Returning",
                                                "Company": meta.get("company") or "",
                                            })
                                        else:
                                            results.append({
                                                "Program ID": prog_id,
                                                "IMDB ID": "Not found",
                                                "Primary Genre (CMS)": "",
                                                "IMDB Genre": "",
                                                "Release Year": "",
                                                "Import ID": "",
                                                "Fresh/Returning": "",
                                                "Company": "",
                                            })
                                    except ValueError:
                                        results.append({
                                            "Program ID": prog_id_str,
                                            "IMDB ID": "Invalid ID",
                                            "Primary Genre (CMS)": "",
                                            "IMDB Genre": "",
                                            "Release Year": "",
                                            "Import ID": "",
                                            "Fresh/Returning": "",
                                            "Company": "",
                                        })
                            
                            if results:
                                st.markdown(f"### Results ({len(results)} IDs)")
                                
                                # Create dataframe
                                df_results = pd.DataFrame(results)
                                
                                # Display as interactive table
                                st.dataframe(df_results, use_container_width=True)
                                
                                # Download button
                                csv_buffer = io.StringIO()
                                df_results.to_csv(csv_buffer, index=False)
                                csv_data = csv_buffer.getvalue()
                                
                                st.download_button(
                                    label="📥 Download Results as CSV",
                                    data=csv_data,
                                    file_name="bulk_lookup_results.csv",
                                    mime="text/csv",
                                    type="primary"
                                )
                            else:
                                st.warning("No results found")
                                
                        except Exception as e:
                            st.error(f"Error: {str(e)}")
    
    # Tab 3: Batch CSV Processing
    with tab3:
        st.markdown("### Process a CSV file")
        st.markdown("Upload a CSV with an `IMDB ID` column to enrich it with metadata.")
        
        uploaded_file = st.file_uploader(
            "Choose a CSV file",
            type=["csv"],
            help="CSV must have an 'IMDB ID' column"
        )
        
        if uploaded_file:
            try:
                df = pd.read_csv(uploaded_file)
                st.markdown(f"**Loaded:** {len(df)} rows")
                
                # Show preview
                with st.expander("Preview uploaded data"):
                    st.dataframe(df.head(10), use_container_width=True)
                
                if st.button("🚀 Enrich CSV", type="primary"):
                    progress_bar = st.progress(0, "Starting...")
                    
                    try:
                        enriched_df = enrich_dataframe(df.copy(), progress_bar)
                        
                        # Show preview of enriched data
                        st.markdown("### Enriched Data Preview")
                        st.dataframe(enriched_df.head(20), use_container_width=True)
                        
                        # Download button
                        csv_buffer = io.StringIO()
                        enriched_df.to_csv(csv_buffer, index=False)
                        csv_data = csv_buffer.getvalue()
                        
                        st.download_button(
                            label="📥 Download Enriched CSV",
                            data=csv_data,
                            file_name=f"enriched_{uploaded_file.name}",
                            mime="text/csv",
                            type="primary"
                        )
                    except Exception as e:
                        st.error(f"Error processing CSV: {str(e)}")
                        raise e
            except Exception as e:
                st.error(f"Error reading CSV: {str(e)}")


if __name__ == "__main__":
    main()
