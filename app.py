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
    
    def get_column_values(self, table_name, column_name, limit=100):
        """Get unique values for a column (for filter dropdowns)"""
        try:
            conn = self._get_connection()
            query = f"SELECT DISTINCT [{column_name}] FROM [{table_name}] WHERE [{column_name}] IS NOT NULL ORDER BY [{column_name}] LIMIT {limit}"
            cursor = conn.cursor()
            cursor.execute(query)
            values = [row[0] for row in cursor.fetchall()]
            conn.close()
            return values
        except Exception as e:
            st.error(f"Error getting column values: {e}")
            return []

def build_visual_query(table_name, table_info):
    """Build SQL query using visual interface"""
    st.markdown("### 🎯 **Visual Query Builder**")
    
    columns = [col[0] for col in table_info['column_info']]
    column_types = {col[0]: col[1] for col in table_info['column_info']}
    
    # Column Selection
    st.markdown("**1. Select Columns to Include:**")
    col_selection_type = st.radio(
        "Choose columns:",
        ["All Columns", "Specific Columns"],
        horizontal=True,
        key=f"col_type_{table_name}"
    )
    
    selected_columns = []
    if col_selection_type == "All Columns":
        selected_columns = columns
        st.info(f"✅ All {len(columns)} columns selected")
    else:
        selected_columns = st.multiselect(
            "Choose columns:",
            columns,
            default=columns[:5] if len(columns) > 5 else columns,
            key=f"cols_{table_name}"
        )
        if not selected_columns:
            st.warning("⚠️ Please select at least one column")
            return None
    
    # Filters Section
    st.markdown("**2. Add Filters (Optional):**")
    
    # Initialize filters in session state
    filter_key = f"filters_{table_name}"
    if filter_key not in st.session_state:
        st.session_state[filter_key] = []
    
    # Add new filter button
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("➕ Add Filter", key=f"add_filter_{table_name}"):
            st.session_state[filter_key].append({
                'column': columns[0],
                'operator': 'equals',
                'value': '',
                'logic': 'AND'
            })
            st.rerun()
    
    with col2:
        if st.session_state[filter_key] and st.button("🗑️ Clear All Filters", key=f"clear_filters_{table_name}"):
            st.session_state[filter_key] = []
            st.rerun()
    
    # Display existing filters
    for i, filter_item in enumerate(st.session_state[filter_key]):
        with st.container():
            st.markdown(f"**Filter {i+1}:**")
            
            filter_col1, filter_col2, filter_col3, filter_col4, filter_col5 = st.columns([2, 2, 3, 1, 1])
            
            with filter_col1:
                # Logic operator (AND/OR) - not shown for first filter
                if i > 0:
                    logic = st.selectbox(
                        "Logic:",
                        ["AND", "OR"],
                        index=0 if filter_item['logic'] == 'AND' else 1,
                        key=f"logic_{table_name}_{i}"
                    )
                    st.session_state[filter_key][i]['logic'] = logic
                else:
                    st.write("WHERE")
            
            with filter_col2:
                # Column selection
                column = st.selectbox(
                    "Column:",
                    columns,
                    index=columns.index(filter_item['column']) if filter_item['column'] in columns else 0,
                    key=f"filter_col_{table_name}_{i}"
                )
                st.session_state[filter_key][i]['column'] = column
            
            with filter_col3:
                # Operator selection
                col_type = column_types.get(column, 'TEXT').upper()
                
                if 'INT' in col_type or 'REAL' in col_type or 'NUMERIC' in col_type:
                    operators = ['equals (=)', 'not equals (!=)', 'greater than (>)', 'less than (<)', 
                               'greater or equal (>=)', 'less or equal (<=)', 'is null', 'is not null']
                else:
                    operators = ['equals (=)', 'not equals (!=)', 'contains (LIKE)', 'starts with', 
                               'ends with', 'is null', 'is not null']
                
                operator = st.selectbox(
                    "Operator:",
                    operators,
                    index=0,
                    key=f"filter_op_{table_name}_{i}"
                )
                st.session_state[filter_key][i]['operator'] = operator
            
            with filter_col4:
                # Value input (only if operator needs a value)
                if 'null' not in operator.lower():
                    if 'INT' in col_type:
                        value = st.number_input(
                            "Value:",
                            value=0,
                            key=f"filter_val_{table_name}_{i}",
                            label_visibility="collapsed"
                        )
                    elif 'REAL' in col_type or 'NUMERIC' in col_type:
                        value = st.number_input(
                            "Value:",
                            value=0.0,
                            key=f"filter_val_{table_name}_{i}",
                            label_visibility="collapsed"
                        )
                    else:
                        value = st.text_input(
                            "Value:",
                            value=filter_item.get('value', ''),
                            key=f"filter_val_{table_name}_{i}",
                            label_visibility="collapsed",
                            placeholder="Enter value..."
                        )
                    st.session_state[filter_key][i]['value'] = value
                else:
                    st.write("—")
            
            with filter_col5:
                # Remove filter button
                if st.button("❌", key=f"remove_filter_{table_name}_{i}", help="Remove this filter"):
                    st.session_state[filter_key].pop(i)
                    st.rerun()
    
    # Sorting Section
    st.markdown("**3. Sort Results (Optional):**")
    
    sort_enabled = st.checkbox("Enable sorting", key=f"sort_enabled_{table_name}")
    sort_column = None
    sort_direction = None
    
    if sort_enabled:
        sort_col1, sort_col2 = st.columns(2)
        with sort_col1:
            sort_column = st.selectbox(
                "Sort by column:",
                selected_columns,
                key=f"sort_col_{table_name}"
            )
        with sort_col2:
            sort_direction = st.selectbox(
                "Sort direction:",
                ["Ascending (A-Z)", "Descending (Z-A)"],
                key=f"sort_dir_{table_name}"
            )
    
    # Limit Section
    st.markdown("**4. Limit Results (Optional):**")
    limit_enabled = st.checkbox("Limit number of rows", key=f"limit_enabled_{table_name}")
    row_limit = None
    
    if limit_enabled:
        row_limit = st.number_input(
            "Maximum rows:",
            min_value=1,
            max_value=1000000,
            value=1000,
            key=f"row_limit_{table_name}"
        )
    
    # Build the SQL query
    if not selected_columns:
        return None
    
    # SELECT clause
    if col_selection_type == "All Columns":
        select_clause = "*"
    else:
        select_clause = ", ".join([f"[{col}]" for col in selected_columns])
    
    query = f"SELECT {select_clause} FROM [{table_name}]"
    
    # WHERE clause
    where_conditions = []
    for i, filter_item in enumerate(st.session_state[filter_key]):
        column = filter_item['column']
        operator = filter_item['operator']
        value = filter_item.get('value', '')
        logic = filter_item.get('logic', 'AND')
        
        # Build condition based on operator
        if operator == 'equals (=)':
            condition = f"[{column}] = '{value}'"
        elif operator == 'not equals (!=)':
            condition = f"[{column}] != '{value}'"
        elif operator == 'greater than (>)':
            condition = f"[{column}] > {value}"
        elif operator == 'less than (<)':
            condition = f"[{column}] < {value}"
        elif operator == 'greater or equal (>=)':
            condition = f"[{column}] >= {value}"
        elif operator == 'less or equal (<=)':
            condition = f"[{column}] <= {value}"
        elif operator == 'contains (LIKE)':
            condition = f"[{column}] LIKE '%{value}%'"
        elif operator == 'starts with':
            condition = f"[{column}] LIKE '{value}%'"
        elif operator == 'ends with':
            condition = f"[{column}] LIKE '%{value}'"
        elif operator == 'is null':
            condition = f"[{column}] IS NULL"
        elif operator == 'is not null':
            condition = f"[{column}] IS NOT NULL"
        else:
            continue
        
        # Add logic operator for subsequent conditions
        if i > 0:
            where_conditions.append(f" {logic} {condition}")
        else:
            where_conditions.append(condition)
    
    if where_conditions:
        query += " WHERE " + "".join(where_conditions)
    
    # ORDER BY clause
    if sort_enabled and sort_column:
        direction = "ASC" if sort_direction == "Ascending (A-Z)" else "DESC"
        query += f" ORDER BY [{sort_column}] {direction}"
    
    # LIMIT clause
    if limit_enabled and row_limit:
        query += f" LIMIT {row_limit}"
    
    return query

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
    st.markdown("Connect to your SQLite database and download any table with visual query builder!")
    
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
                                ["All Data", "Row Range", "Visual Query Builder", "Custom SQL"],
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
                            
                            elif query_type == "Visual Query Builder":
                                custom_query = build_visual_query(table, table_info)
                                
                                if custom_query:
                                    # Show generated query
                                    st.markdown("**Generated SQL Query:**")
                                    st.code(custom_query, language='sql')
                                    
                                    # Validate query button
                                    if st.button(f"✅ Validate Query", key=f"validate_visual_{table}"):
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
                            
                            elif query_type == "Custom SQL":
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
                                            if query_type in ["Custom SQL", "Visual Query Builder"]:
                                                df = st.session_state.db_connector.get_table_data(table, custom_query=custom_query)
                                            else:
                                                df = st.session_state.db_connector.get_table_data(table, limit=limit, offset=offset)
                                            
                                            if df is not None:
                                                csv_data = convert_df_to_csv(df)
                                                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                                                filter_suffix = ""
                                                if query_type == "Row Range":
                                                    filter_suffix = f"_rows_{offset+1}to{offset+len(df)}"
                                                elif query_type in ["Custom SQL", "Visual Query Builder"]:
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
                                            if query_type in ["Custom SQL", "Visual Query Builder"]:
                                                df = st.session_state.db_connector.get_table_data(table, custom_query=custom_query)
                                            else:
                                                df = st.session_state.db_connector.get_table_data(table, limit=limit, offset=offset)
                                            
                                            if df is not None:
                                                excel_data = convert_df_to_excel(df)
                                                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                                                filter_suffix = ""
                                                if query_type == "Row Range":
                                                    filter_suffix = f"_rows_{offset+1}to{offset+len(df)}"
                                                elif query_type in ["Custom SQL", "Visual Query Builder"]:
                                                    filter_suffix = "_filtered"
                                                
                                                st.download_button(
                                                    label="⬇️ Download Excel",
                                                    data=excel_data,
                                                    file_name=f"{table}{filter_suffix}_{timestamp}.xlsx",
                                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                                    key=f"dl_excel_{table}_{timestamp}"
                                                )
            
            # Preview section (keeping the existing preview code)
            if hasattr(st.session_state, 'preview_table'):
                st.markdown("---")
                preview_table = st.session_state.preview_table
                st.header(f"🔍 Preview: **{preview_table}**")
                
                # Get the data based on preview settings
                if hasattr(st.session_state, 'preview_query_type'):
                    query_type = st.session_state.preview_query_type
                    
                    if query_type in ["Custom SQL", "Visual Query Builder"] and hasattr(st.session_state, 'preview_custom_query'):
                        # For preview, limit custom queries to 100 rows
                        custom_query = st.session_state.preview_custom_query
                        if custom_query:
                            preview_query = f"{custom_query} LIMIT 100"
                            df = st.session_state.db_connector.get_table_data(preview_table, custom_query=preview_query)
                        else:
                            df = st.session_state.db_connector.get_table_data(preview_table, limit=100)
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
                        elif st.session_state.preview_query_type in ["Custom SQL", "Visual Query Builder"]:
                            filter_info = "Custom query applied (showing first 100 results)"
                        
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
                                if query_type in ["Custom SQL", "Visual Query Builder"]:
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
                                if query_type in ["Custom SQL", "Visual Query Builder"]:
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
          - **Visual Query Builder**: Point-and-click filtering (NEW!)
          - **Custom SQL**: Write your own SELECT queries
        - 📥 **One-click downloads** in CSV or Excel format
        - 🔍 **Search tables** by name
        - ✅ **Query validation** for custom SQL
        - ⚡ **Fast and responsive** interface
        
        ### 🛠️ How to Use:
        1. **Upload** your SQLite database file using the sidebar
        2. **Connect** to the database
        3. **Browse** available tables and their metadata
        4. **Choose** your extraction method:
           - Use the **Visual Query Builder** for easy point-and-click filtering
           - Or write **Custom SQL** for advanced queries
        5. **Preview** table contents if needed
        6. **Download** any table as CSV or Excel with one click!
        
        ### 💡 Tips:
        - The **Visual Query Builder** lets you filter without writing SQL
        - You can combine multiple filters with AND/OR logic
        - File names include timestamps to avoid conflicts
        - Use the search feature to quickly find specific tables
        - Preview data before downloading large tables
        
        ---
        👈 **Get started by connecting to your SQLite database using the sidebar!**
        """)

if __name__ == "__main__":
    main()