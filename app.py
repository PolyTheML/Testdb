import streamlit as st
import pandas as pd
import sqlite3
import io
from datetime import datetime
import os

class SQLiteConnector:
    def __init__(self, db_path):
        self.db_path = db_path
    
    def connect(self):
        """Test connection to SQLite database"""
        try:
            if not os.path.exists(self.db_path):
                st.error(f"Database file not found: {self.db_path}")
                return False
            
            # Test connection
            conn = sqlite3.connect(self.db_path)
            conn.close()
            return True
        except Exception as e:
            st.error(f"Error connecting to SQLite database: {e}")
            return False
    
    def _get_connection(self):
        """Create a new connection for each operation to avoid threading issues"""
        return sqlite3.connect(self.db_path)
    
    def get_tables(self):
        """Get list of tables in the SQLite database"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            tables = [row[0] for row in cursor.fetchall()]
            conn.close()
            return tables
        except Exception as e:
            st.error(f"Error fetching tables: {e}")
            return []
    
    def get_table_data(self, table_name, custom_query=None, limit=None, offset=None):
        """Get data from a specific table with optional filtering"""
        try:
            conn = self._get_connection()
            
            if custom_query:
                # Use custom query (already validated)
                query = custom_query
            else:
                # Default query with optional limit and offset
                query = f"SELECT * FROM [{table_name}]"
                if limit:
                    query += f" LIMIT {limit}"
                    if offset:
                        query += f" OFFSET {offset}"
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
        except Exception as e:
            st.error(f"Error fetching data: {e}")
            return None
    
    def validate_query(self, query):
        """Validate that query is safe (basic validation)"""
        try:
            # Convert to lowercase for checking
            query_lower = query.lower().strip()
            
            # Must start with SELECT
            if not query_lower.startswith('select'):
                return False, "Query must start with SELECT"
            
            # Prohibited operations
            prohibited = ['insert', 'update', 'delete', 'drop', 'create', 'alter', 'truncate']
            for word in prohibited:
                if word in query_lower:
                    return False, f"'{word.upper()}' operations are not allowed"
            
            # Test query syntax by running EXPLAIN
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(f"EXPLAIN QUERY PLAN {query}")
            conn.close()
            
            return True, "Query is valid"
        except Exception as e:
            return False, f"Query error: {str(e)}"
    
    def get_sample_data(self, table_name, limit=5):
        """Get a small sample of data for preview"""
        return self.get_table_data(table_name, limit=limit)
    
    def get_table_info(self, table_name):
        """Get basic info about a table"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
            row_count = cursor.fetchone()[0]
            
            # Get column info
            cursor.execute(f"PRAGMA table_info([{table_name}])")
            columns = cursor.fetchall()
            
            conn.close()
            
            return {
                'rows': row_count,
                'columns': len(columns),
                'column_info': [(col[1], col[2]) for col in columns]  # (name, type)
            }
        except Exception as e:
            st.error(f"Error getting table info for {table_name}: {e}")
            return None

def convert_df_to_csv(df):
    """Convert dataframe to CSV for download"""
    return df.to_csv(index=False).encode('utf-8')

def convert_df_to_excel(df):
    """Convert dataframe to Excel for download"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Sheet1', index=False)
    return output.getvalue()

def main():
    st.set_page_config(
        page_title="SQLite Table Downloader",
        page_icon="🗃️",
        layout="wide"
    )
    
    st.title("🗃️ SQLite Table Downloader")
    st.markdown("Connect to your SQLite database and download any table with a single click!")
    
    # Sidebar for database connection
    with st.sidebar:
        st.header("📁 SQLite Database")
        
        # File uploader for SQLite database
        uploaded_file = st.file_uploader(
            "Upload SQLite Database File",
            type=['db', 'sqlite', 'sqlite3'],
            help="Upload your .db, .sqlite, or .sqlite3 file"
        )
        
        # Or enter file path
        st.markdown("**OR**")
        db_path = st.text_input(
            "Database File Path",
            placeholder="path/to/your/database.db",
            help="Enter the full path to your SQLite database file"
        )
        
        # Handle uploaded file
        if uploaded_file is not None:
            # Save uploaded file temporarily
            temp_db_path = f"temp_{uploaded_file.name}"
            with open(temp_db_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            db_path = temp_db_path
        
        # Connect button
        if st.button("🔗 Connect to Database", type="primary"):
            if db_path:
                connector = SQLiteConnector(db_path)
                if connector.connect():
                    st.session_state.db_connector = connector
                    st.session_state.connected = True
                    st.session_state.db_path = db_path
                    st.success(f"✅ Connected to: {os.path.basename(db_path)}")
                else:
                    st.session_state.connected = False
            else:
                st.error("Please provide a database file or path")
        
        # Connection status
        if hasattr(st.session_state, 'connected') and st.session_state.connected:
            st.success("🟢 Connected")
            st.info(f"Database: {os.path.basename(st.session_state.db_path)}")
            
            if st.button("🔄 Refresh Tables"):
                st.rerun()
                
            if st.button("❌ Disconnect"):
                st.session_state.connected = False
                st.rerun()
        else:
            st.error("🔴 Not Connected")
    
    # Main content area
    if hasattr(st.session_state, 'connected') and st.session_state.connected:
        
        # Get available tables
        tables = st.session_state.db_connector.get_tables()
        
        if tables:
            st.header(f"📋 Available Tables ({len(tables)} total)")
            
            # Search/filter tables
            search_term = st.text_input("🔍 Search tables:", placeholder="Enter table name...")
            if search_term:
                tables = [table for table in tables if search_term.lower() in table.lower()]
            
            if not tables and search_term:
                st.warning("No tables match your search.")
            else:
                # Display tables in expandable sections
                for table in tables:
                    with st.expander(f"📊 **{table}**", expanded=False):
                        # Get table info first
                        table_info = st.session_state.db_connector.get_table_info(table)
                        
                        if table_info:
                            # Basic table info
                            info_col1, info_col2, info_col3 = st.columns(3)
                            with info_col1:
                                st.metric("Rows", f"{table_info['rows']:,}")
                            with info_col2:
                                st.metric("Columns", table_info['columns'])
                            with info_col3:
                                if st.button(f"📋 Show Columns", key=f"cols_{table}"):
                                    st.session_state[f"show_cols_{table}"] = not st.session_state.get(f"show_cols_{table}", False)
                            
                            # Show column details
                            if st.session_state.get(f"show_cols_{table}", False):
                                st.markdown("**Columns:**")
                                cols_display = st.columns(3)
                                for i, (col_name, col_type) in enumerate(table_info['column_info']):
                                    with cols_display[i % 3]:
                                        st.write(f"• **{col_name}** ({col_type})")
                            
                            st.markdown("---")
                            
                            # Query Options Section
                            st.markdown("### 🔍 **Data Filtering Options**")
                            
                            query_type = st.radio(
                                "Choose extraction method:",
                                ["All Data", "Row Range", "Custom Query"],
                                key=f"query_type_{table}",
                                horizontal=True
                            )
                            
                            custom_query = None
                            limit = None
                            offset = None
                            
                            if query_type == "Row Range":
                                range_col1, range_col2 = st.columns(2)
                                with range_col1:
                                    limit = st.number_input(
                                        "Number of rows:", 
                                        min_value=1, 
                                        max_value=table_info['rows'], 
                                        value=min(1000, table_info['rows']),
                                        key=f"limit_{table}"
                                    )
                                with range_col2:
                                    offset = st.number_input(
                                        "Start from row:", 
                                        min_value=0, 
                                        max_value=max(0, table_info['rows']-1), 
                                        value=0,
                                        key=f"offset_{table}"
                                    )
                                st.info(f"Will extract rows {offset+1} to {min(offset+limit, table_info['rows'])}")
                            
                            elif query_type == "Custom Query":
                                st.markdown("**Write your custom SQL query:**")
                                custom_query = st.text_area(
                                    "SQL Query:",
                                    value=f"SELECT * FROM [{table}] WHERE ",
                                    height=100,
                                    key=f"custom_query_{table}",
                                    help="Only SELECT queries are allowed. Use column names shown above."
                                )
                                
                                # Validate query button
                                if st.button(f"✅ Validate Query", key=f"validate_{table}"):
                                    is_valid, message = st.session_state.db_connector.validate_query(custom_query)
                                    if is_valid:
                                        st.success(f"✅ {message}")
                                        # Show sample of results
                                        try:
                                            sample_df = st.session_state.db_connector.get_table_data(table, custom_query=f"{custom_query} LIMIT 3")
                                            if sample_df is not None and not sample_df.empty:
                                                st.write("**Query preview (first 3 rows):**")
                                                st.dataframe(sample_df, use_container_width=True)
                                            else:
                                                st.warning("Query returned no results")
                                        except:
                                            st.warning("Could not preview query results")
                                    else:
                                        st.error(f"❌ {message}")
                            
                            st.markdown("---")
                            
                            # Preview and Download Section
                            preview_col, download_col = st.columns([1, 2])
                            
                            with preview_col:
                                if st.button(f"👁️ Preview Data", key=f"preview_{table}"):
                                    st.session_state.preview_table = table
                                    st.session_state.preview_query_type = query_type
                                    st.session_state.preview_custom_query = custom_query
                                    st.session_state.preview_limit = limit
                                    st.session_state.preview_offset = offset
                            
                            with download_col:
                                st.markdown("**📥 Download Filtered Data:**")
                                dl_col1, dl_col2 = st.columns(2)
                                
                                with dl_col1:
                                    if st.button(f"📄 Download CSV", key=f"csv_{table}", use_container_width=True):
                                        with st.spinner("Preparing download..."):
                                            if query_type == "Custom Query":
                                                df = st.session_state.db_connector.get_table_data(table, custom_query=custom_query)
                                            else:
                                                df = st.session_state.db_connector.get_table_data(table, limit=limit, offset=offset)
                                            
                                            if df is not None:
                                                csv_data = convert_df_to_csv(df)
                                                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                                                filter_suffix = ""
                                                if query_type == "Row Range":
                                                    filter_suffix = f"_rows_{offset+1}to{offset+len(df)}"
                                                elif query_type == "Custom Query":
                                                    filter_suffix = "_filtered"
                                                
                                                st.download_button(
                                                    label="⬇️ Download CSV",
                                                    data=csv_data,
                                                    file_name=f"{table}{filter_suffix}_{timestamp}.csv",
                                                    mime="text/csv",
                                                    key=f"dl_csv_{table}_{timestamp}"
                                                )
                                
                                with dl_col2:
                                    if st.button(f"📊 Download Excel", key=f"excel_{table}", use_container_width=True):
                                        with st.spinner("Preparing download..."):
                                            if query_type == "Custom Query":
                                                df = st.session_state.db_connector.get_table_data(table, custom_query=custom_query)
                                            else:
                                                df = st.session_state.db_connector.get_table_data(table, limit=limit, offset=offset)
                                            
                                            if df is not None:
                                                excel_data = convert_df_to_excel(df)
                                                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                                                filter_suffix = ""
                                                if query_type == "Row Range":
                                                    filter_suffix = f"_rows_{offset+1}to{offset+len(df)}"
                                                elif query_type == "Custom Query":
                                                    filter_suffix = "_filtered"
                                                
                                                st.download_button(
                                                    label="⬇️ Download Excel",
                                                    data=excel_data,
                                                    file_name=f"{table}{filter_suffix}_{timestamp}.xlsx",
                                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                                    key=f"dl_excel_{table}_{timestamp}"
                                                )
            
            # Preview section
            if hasattr(st.session_state, 'preview_table'):
                st.markdown("---")
                preview_table = st.session_state.preview_table
                st.header(f"🔍 Preview: **{preview_table}**")
                
                # Get the data based on preview settings
                if hasattr(st.session_state, 'preview_query_type'):
                    query_type = st.session_state.preview_query_type
                    
                    if query_type == "Custom Query" and hasattr(st.session_state, 'preview_custom_query'):
                        # For preview, limit custom queries to 100 rows
                        custom_query = st.session_state.preview_custom_query
                        preview_query = f"{custom_query} LIMIT 100"
                        df = st.session_state.db_connector.get_table_data(preview_table, custom_query=preview_query)
                    elif query_type == "Row Range":
                        limit = getattr(st.session_state, 'preview_limit', None)
                        offset = getattr(st.session_state, 'preview_offset', None)
                        # For preview, limit to 100 rows max
                        preview_limit = min(limit, 100) if limit else 100
                        df = st.session_state.db_connector.get_table_data(preview_table, limit=preview_limit, offset=offset)
                    else:
                        # All Data - but limit preview to 100 rows
                        df = st.session_state.db_connector.get_table_data(preview_table, limit=100)
                else:
                    # Fallback to simple preview
                    df = st.session_state.db_connector.get_table_data(preview_table, limit=100)
                
                if df is not None:
                    # Show basic stats
                    col1, col2, col3, col4, col5 = st.columns(5)
                    with col1:
                        st.metric("Preview Rows", f"{len(df):,}")
                    with col2:
                        st.metric("Columns", len(df.columns))
                    with col3:
                        st.metric("Memory", f"{df.memory_usage(deep=True).sum() / 1024:.1f} KB")
                    with col4:
                        # Show filter type
                        filter_type = getattr(st.session_state, 'preview_query_type', 'All Data')
                        st.metric("Filter", filter_type)
                    with col5:
                        if st.button("❌ Close Preview"):
                            # Clean up preview session state
                            attrs_to_remove = ['preview_table', 'preview_query_type', 'preview_custom_query', 'preview_limit', 'preview_offset']
                            for attr in attrs_to_remove:
                                if hasattr(st.session_state, attr):
                                    delattr(st.session_state, attr)
                            st.rerun()
                    
                    # Show filter details if applicable
                    if hasattr(st.session_state, 'preview_query_type'):
                        filter_info = ""
                        if st.session_state.preview_query_type == "Row Range":
                            offset = getattr(st.session_state, 'preview_offset', 0)
                            limit = getattr(st.session_state, 'preview_limit', 100)
                            filter_info = f"Showing rows {offset+1} to {offset+len(df)}"
                        elif st.session_state.preview_query_type == "Custom Query":
                            filter_info = "Custom SQL query applied (showing first 100 results)"
                        
                        if filter_info:
                            st.info(f"ℹ️ {filter_info}")
                    
                    # Display the data
                    st.dataframe(
                        df, 
                        use_container_width=True,
                        height=400
                    )
                    
                    # Quick download buttons for previewed table
                    st.markdown("**Quick Download (Full Dataset):**")
                    download_col1, download_col2, download_col3 = st.columns([1, 1, 2])
                    
                    # Prepare full dataset based on preview settings
                    query_type = getattr(st.session_state, 'preview_query_type', 'All Data')
                    
                    with download_col1:
                        if st.button("📄 Download Full CSV", key="preview_csv_full"):
                            with st.spinner("Preparing full dataset..."):
                                if query_type == "Custom Query":
                                    full_df = st.session_state.db_connector.get_table_data(preview_table, custom_query=st.session_state.preview_custom_query)
                                elif query_type == "Row Range":
                                    full_df = st.session_state.db_connector.get_table_data(preview_table, limit=st.session_state.preview_limit, offset=st.session_state.preview_offset)
                                else:
                                    full_df = st.session_state.db_connector.get_table_data(preview_table)
                                
                                if full_df is not None:
                                    csv_data = convert_df_to_csv(full_df)
                                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                                    filter_suffix = "_filtered" if query_type != "All Data" else ""
                                    st.download_button(
                                        label=f"⬇️ CSV ({len(full_df)} rows)",
                                        data=csv_data,
                                        file_name=f"{preview_table}{filter_suffix}_{timestamp}.csv",
                                        mime="text/csv",
                                        key="preview_dl_csv"
                                    )
                    
                    with download_col2:
                        if st.button("📊 Download Full Excel", key="preview_excel_full"):
                            with st.spinner("Preparing full dataset..."):
                                if query_type == "Custom Query":
                                    full_df = st.session_state.db_connector.get_table_data(preview_table, custom_query=st.session_state.preview_custom_query)
                                elif query_type == "Row Range":
                                    full_df = st.session_state.db_connector.get_table_data(preview_table, limit=st.session_state.preview_limit, offset=st.session_state.preview_offset)
                                else:
                                    full_df = st.session_state.db_connector.get_table_data(preview_table)
                                
                                if full_df is not None:
                                    excel_data = convert_df_to_excel(full_df)
                                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                                    filter_suffix = "_filtered" if query_type != "All Data" else ""
                                    st.download_button(
                                        label=f"⬇️ Excel ({len(full_df)} rows)",
                                        data=excel_data,
                                        file_name=f"{preview_table}{filter_suffix}_{timestamp}.xlsx",
                                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                        key="preview_dl_excel"
                                    )
        else:
            st.warning("⚠️ No tables found in the database.")
    
    else:
        # Welcome screen
        st.markdown("""
        ## 🚀 Welcome to SQLite Table Downloader!
        
        This app makes it easy to browse and download tables from your SQLite database.
        
        ### ✨ Features:
        - 📁 **Upload database files** or specify file paths
        - 📊 **Browse all tables** with detailed metadata
        - 🔍 **Preview table data** before downloading
        - 📥 **Multiple extraction options:**
          - **All Data**: Download complete tables
          - **Row Range**: Specify start row and number of rows
          - **Custom SQL**: Write your own SELECT queries
        - 📥 **One-click downloads** in CSV or Excel format
        - 🔍 **Search tables** by name
        - ✅ **Query validation** for custom SQL
        - ⚡ **Fast and responsive** interface
        
        ### 🛠️ How to Use:
        1. **Upload** your SQLite database file using the sidebar
        2. **Connect** to the database
        3. **Browse** available tables and their metadata
        4. **Preview** table contents if needed
        5. **Download** any table as CSV or Excel with one click!
        
        ### 📋 Required Dependencies:
        ```bash
        pip install streamlit pandas openpyxl
        ```
        
        ### 💡 Tips:
        - File names include timestamps to avoid conflicts
        - Use the search feature to quickly find specific tables
        - Preview data before downloading large tables
        - The app handles special characters in table names automatically
        
        ---
        👈 **Get started by connecting to your SQLite database using the sidebar!**
        """)

if __name__ == "__main__":
    main()