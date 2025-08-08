import streamlit as st
import pandas as pd
import sqlite3
import io
from datetime import datetime
import os
from abc import ABC, abstractmethod

# Try to import additional database drivers
try:
    import psycopg2
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

try:
    import mysql.connector
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False

try:
    from sqlalchemy import create_engine, text, MetaData, inspect
    import urllib.parse
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

class DatabaseConnector(ABC):
    """Abstract base class for database connectors"""
    
    def __init__(self):
        self.connection = None
        self.engine = None
    
    @abstractmethod
    def connect(self, **kwargs):
        """Test connection to database"""
        pass
    
    @abstractmethod
    def get_tables(self):
        """Get list of tables in the database"""
        pass
    
    @abstractmethod
    def get_table_data(self, table_name, custom_query=None, limit=None, offset=None):
        """Get data from a specific table"""
        pass
    
    @abstractmethod
    def validate_query(self, query):
        """Validate SQL query"""
        pass
    
    @abstractmethod
    def get_table_info(self, table_name):
        """Get basic info about a table"""
        pass
    
    def disconnect(self):
        """Disconnect from database"""
        if self.connection:
            self.connection.close()
        if self.engine:
            self.engine.dispose()

class SQLiteConnector(DatabaseConnector):
    def __init__(self, db_path):
        super().__init__()
        self.db_path = db_path
    
    def connect(self, **kwargs):
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
                query = custom_query
            else:
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
            query_lower = query.lower().strip()
            
            if not query_lower.startswith('select'):
                return False, "Query must start with SELECT"
            
            prohibited = ['insert', 'update', 'delete', 'drop', 'create', 'alter', 'truncate']
            for word in prohibited:
                if word in query_lower:
                    return False, f"'{word.upper()}' operations are not allowed"
            
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(f"EXPLAIN QUERY PLAN {query}")
            conn.close()
            
            return True, "Query is valid"
        except Exception as e:
            return False, f"Query error: {str(e)}"
    
    def get_table_info(self, table_name):
        """Get basic info about a table"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
            row_count = cursor.fetchone()[0]
            
            cursor.execute(f"PRAGMA table_info([{table_name}])")
            columns = cursor.fetchall()
            
            conn.close()
            
            return {
                'rows': row_count,
                'columns': len(columns),
                'column_info': [(col[1], col[2]) for col in columns]
            }
        except Exception as e:
            st.error(f"Error getting table info for {table_name}: {e}")
            return None

class PostgreSQLConnector(DatabaseConnector):
    def __init__(self, host, port, database, username, password):
        super().__init__()
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        
    def connect(self, **kwargs):
        """Test connection to PostgreSQL database"""
        try:
            if SQLALCHEMY_AVAILABLE:
                # Use SQLAlchemy for better compatibility
                connection_string = f"postgresql://{self.username}:{urllib.parse.quote_plus(self.password)}@{self.host}:{self.port}/{self.database}"
                self.engine = create_engine(connection_string)
                # Test connection
                with self.engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                return True
            elif POSTGRES_AVAILABLE:
                # Fallback to psycopg2
                self.connection = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user=self.username,
                    password=self.password
                )
                self.connection.close()
                return True
            else:
                st.error("PostgreSQL dependencies not installed. Please install: pip install psycopg2-binary sqlalchemy")
                return False
        except Exception as e:
            st.error(f"Error connecting to PostgreSQL database: {e}")
            return False
    
    def get_tables(self):
        """Get list of tables in the PostgreSQL database"""
        try:
            if self.engine:
                inspector = inspect(self.engine)
                return inspector.get_table_names()
            else:
                # Fallback method
                query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                ORDER BY table_name
                """
                df = pd.read_sql_query(query, self.connection)
                return df['table_name'].tolist()
        except Exception as e:
            st.error(f"Error fetching tables: {e}")
            return []
    
    def get_table_data(self, table_name, custom_query=None, limit=None, offset=None):
        """Get data from a specific table with optional filtering"""
        try:
            if custom_query:
                query = custom_query
            else:
                query = f'SELECT * FROM "{table_name}"'
                if limit:
                    query += f" LIMIT {limit}"
                    if offset:
                        query += f" OFFSET {offset}"
            
            if self.engine:
                df = pd.read_sql_query(query, self.engine)
            else:
                df = pd.read_sql_query(query, self.connection)
            return df
        except Exception as e:
            st.error(f"Error fetching data: {e}")
            return None
    
    def validate_query(self, query):
        """Validate that query is safe"""
        try:
            query_lower = query.lower().strip()
            
            if not query_lower.startswith('select'):
                return False, "Query must start with SELECT"
            
            prohibited = ['insert', 'update', 'delete', 'drop', 'create', 'alter', 'truncate']
            for word in prohibited:
                if word in query_lower:
                    return False, f"'{word.upper()}' operations are not allowed"
            
            # Test query with EXPLAIN
            if self.engine:
                with self.engine.connect() as conn:
                    conn.execute(text(f"EXPLAIN {query}"))
            return True, "Query is valid"
        except Exception as e:
            return False, f"Query error: {str(e)}"
    
    def get_table_info(self, table_name):
        """Get basic info about a table"""
        try:
            # Get row count
            count_query = f'SELECT COUNT(*) FROM "{table_name}"'
            if self.engine:
                with self.engine.connect() as conn:
                    result = conn.execute(text(count_query))
                    row_count = result.scalar()
            else:
                df = pd.read_sql_query(count_query, self.connection)
                row_count = df.iloc[0, 0]
            
            # Get column info
            info_query = """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = %s 
            ORDER BY ordinal_position
            """
            if self.engine:
                df = pd.read_sql_query(info_query, self.engine, params=[table_name])
            else:
                df = pd.read_sql_query(info_query, self.connection, params=[table_name])
            
            columns = [(row['column_name'], row['data_type']) for _, row in df.iterrows()]
            
            return {
                'rows': row_count,
                'columns': len(columns),
                'column_info': columns
            }
        except Exception as e:
            st.error(f"Error getting table info for {table_name}: {e}")
            return None

class MySQLConnector(DatabaseConnector):
    def __init__(self, host, port, database, username, password):
        super().__init__()
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        
    def connect(self, **kwargs):
        """Test connection to MySQL database"""
        try:
            if SQLALCHEMY_AVAILABLE:
                # Use SQLAlchemy for better compatibility
                connection_string = f"mysql+pymysql://{self.username}:{urllib.parse.quote_plus(self.password)}@{self.host}:{self.port}/{self.database}"
                self.engine = create_engine(connection_string)
                # Test connection
                with self.engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                return True
            elif MYSQL_AVAILABLE:
                # Fallback to mysql-connector-python
                self.connection = mysql.connector.connect(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user=self.username,
                    password=self.password
                )
                return True
            else:
                st.error("MySQL dependencies not installed. Please install: pip install mysql-connector-python sqlalchemy pymysql")
                return False
        except Exception as e:
            st.error(f"Error connecting to MySQL database: {e}")
            return False
    
    def get_tables(self):
        """Get list of tables in the MySQL database"""
        try:
            if self.engine:
                inspector = inspect(self.engine)
                return inspector.get_table_names()
            else:
                query = "SHOW TABLES"
                cursor = self.connection.cursor()
                cursor.execute(query)
                tables = [row[0] for row in cursor.fetchall()]
                cursor.close()
                return tables
        except Exception as e:
            st.error(f"Error fetching tables: {e}")
            return []
    
    def get_table_data(self, table_name, custom_query=None, limit=None, offset=None):
        """Get data from a specific table with optional filtering"""
        try:
            if custom_query:
                query = custom_query
            else:
                query = f"SELECT * FROM `{table_name}`"
                if limit:
                    query += f" LIMIT {limit}"
                    if offset:
                        query += f" OFFSET {offset}"
            
            if self.engine:
                df = pd.read_sql_query(query, self.engine)
            else:
                df = pd.read_sql_query(query, self.connection)
            return df
        except Exception as e:
            st.error(f"Error fetching data: {e}")
            return None
    
    def validate_query(self, query):
        """Validate that query is safe"""
        try:
            query_lower = query.lower().strip()
            
            if not query_lower.startswith('select'):
                return False, "Query must start with SELECT"
            
            prohibited = ['insert', 'update', 'delete', 'drop', 'create', 'alter', 'truncate']
            for word in prohibited:
                if word in query_lower:
                    return False, f"'{word.upper()}' operations are not allowed"
            
            # Test query with EXPLAIN
            if self.engine:
                with self.engine.connect() as conn:
                    conn.execute(text(f"EXPLAIN {query}"))
            return True, "Query is valid"
        except Exception as e:
            return False, f"Query error: {str(e)}"
    
    def get_table_info(self, table_name):
        """Get basic info about a table"""
        try:
            # Get row count
            count_query = f"SELECT COUNT(*) FROM `{table_name}`"
            if self.engine:
                with self.engine.connect() as conn:
                    result = conn.execute(text(count_query))
                    row_count = result.scalar()
            else:
                cursor = self.connection.cursor()
                cursor.execute(count_query)
                row_count = cursor.fetchone()[0]
                cursor.close()
            
            # Get column info
            if self.engine:
                info_query = f"DESCRIBE `{table_name}`"
                df = pd.read_sql_query(info_query, self.engine)
                columns = [(row['Field'], row['Type']) for _, row in df.iterrows()]
            else:
                cursor = self.connection.cursor()
                cursor.execute(f"DESCRIBE `{table_name}`")
                columns = [(row[0], row[1]) for row in cursor.fetchall()]
                cursor.close()
            
            return {
                'rows': row_count,
                'columns': len(columns),
                'column_info': columns
            }
        except Exception as e:
            st.error(f"Error getting table info for {table_name}: {e}")
            return None

def create_database_connector(db_type, **kwargs):
    """Factory function to create appropriate database connector"""
    if db_type == "SQLite":
        return SQLiteConnector(kwargs['db_path'])
    elif db_type == "PostgreSQL":
        return PostgreSQLConnector(
            kwargs['host'], kwargs['port'], kwargs['database'], 
            kwargs['username'], kwargs['password']
        )
    elif db_type == "MySQL":
        return MySQLConnector(
            kwargs['host'], kwargs['port'], kwargs['database'], 
            kwargs['username'], kwargs['password']
        )
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

def render_database_connection_form():
    """Render the database connection form in sidebar"""
    st.header("🔌 Database Connection")
    
    # Database type selection
    available_types = ["SQLite"]
    if POSTGRES_AVAILABLE or SQLALCHEMY_AVAILABLE:
        available_types.append("PostgreSQL")
    if MYSQL_AVAILABLE or SQLALCHEMY_AVAILABLE:
        available_types.append("MySQL")
    
    db_type = st.selectbox(
        "Database Type",
        available_types,
        help="Select your database type"
    )
    
    connector_params = {}
    
    if db_type == "SQLite":
        st.markdown("**SQLite Database**")
        
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
            temp_db_path = f"temp_{uploaded_file.name}"
            with open(temp_db_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            db_path = temp_db_path
        
        connector_params = {'db_path': db_path}
    
    elif db_type == "PostgreSQL":
        st.markdown("**PostgreSQL Connection**")
        
        col1, col2 = st.columns(2)
        with col1:
            host = st.text_input("Host", value="localhost", placeholder="localhost")
            database = st.text_input("Database", placeholder="mydb")
        with col2:
            port = st.number_input("Port", min_value=1, max_value=65535, value=5432)
            username = st.text_input("Username", placeholder="postgres")
        
        password = st.text_input("Password", type="password")
        
        connector_params = {
            'host': host,
            'port': port,
            'database': database,
            'username': username,
            'password': password
        }
    
    elif db_type == "MySQL":
        st.markdown("**MySQL Connection**")
        
        col1, col2 = st.columns(2)
        with col1:
            host = st.text_input("Host", value="localhost", placeholder="localhost", key="mysql_host")
            database = st.text_input("Database", placeholder="mydb", key="mysql_db")
        with col2:
            port = st.number_input("Port", min_value=1, max_value=65535, value=3306, key="mysql_port")
            username = st.text_input("Username", placeholder="root", key="mysql_user")
        
        password = st.text_input("Password", type="password", key="mysql_pass")
        
        connector_params = {
            'host': host,
            'port': port,
            'database': database,
            'username': username,
            'password': password
        }
    
    # Connect button
    if st.button("🔗 Connect to Database", type="primary"):
        # Validate required fields
        if db_type == "SQLite" and not connector_params.get('db_path'):
            st.error("Please provide a database file or path")
            return
        elif db_type in ["PostgreSQL", "MySQL"]:
            required_fields = ['host', 'database', 'username', 'password']
            missing_fields = [field for field in required_fields if not connector_params.get(field)]
            if missing_fields:
                st.error(f"Please fill in: {', '.join(missing_fields)}")
                return
        
        # Attempt connection
        try:
            connector = create_database_connector(db_type, **connector_params)
            if connector.connect():
                st.session_state.db_connector = connector
                st.session_state.connected = True
                st.session_state.db_type = db_type
                st.session_state.connector_params = connector_params
                
                if db_type == "SQLite":
                    db_name = os.path.basename(connector_params['db_path'])
                else:
                    db_name = f"{connector_params['database']} ({connector_params['host']})"
                
                st.success(f"✅ Connected to: {db_name}")
            else:
                st.session_state.connected = False
        except Exception as e:
            st.error(f"Connection failed: {e}")
            st.session_state.connected = False
    
    # Connection status
    if hasattr(st.session_state, 'connected') and st.session_state.connected:
        st.success("🟢 Connected")
        
        # Show connection info
        if st.session_state.db_type == "SQLite":
            db_name = os.path.basename(st.session_state.connector_params['db_path'])
            st.info(f"**{st.session_state.db_type}**: {db_name}")
        else:
            params = st.session_state.connector_params
            st.info(f"**{st.session_state.db_type}**: {params['database']} @ {params['host']}:{params['port']}")
        
        if st.button("🔄 Refresh Tables"):
            st.rerun()
            
        if st.button("❌ Disconnect"):
            if hasattr(st.session_state, 'db_connector'):
                st.session_state.db_connector.disconnect()
            st.session_state.connected = False
            st.rerun()
    else:
        st.error("🔴 Not Connected")

# Update build_visual_query function to handle different SQL dialects
def build_visual_query(table_name, table_info, db_type="SQLite"):
    """Build SQL query using visual interface (adapted for different databases)"""
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
    
    # Filters Section (same as before)
    st.markdown("**2. Add Filters (Optional):**")
    
    filter_key = f"filters_{table_name}"
    if filter_key not in st.session_state:
        st.session_state[filter_key] = []
    
    # [Rest of the filter logic remains the same...]
    # ... (keeping the existing filter code for brevity)
    
    # Build the SQL query with database-specific syntax
    if not selected_columns:
        return None
    
    # SELECT clause
    if col_selection_type == "All Columns":
        select_clause = "*"
    else:
        if db_type == "SQLite":
            select_clause = ", ".join([f"[{col}]" for col in selected_columns])
        elif db_type == "PostgreSQL":
            select_clause = ", ".join([f'"{col}"' for col in selected_columns])
        else:  # MySQL
            select_clause = ", ".join([f"`{col}`" for col in selected_columns])
    
    # FROM clause with proper quoting
    if db_type == "SQLite":
        query = f"SELECT {select_clause} FROM [{table_name}]"
    elif db_type == "PostgreSQL":
        query = f'SELECT {select_clause} FROM "{table_name}"'
    else:  # MySQL
        query = f"SELECT {select_clause} FROM `{table_name}`"
    
    # WHERE clause (adapt column quoting based on database type)
    # ... (adapt the existing WHERE clause logic with proper quoting)
    
    return query

# Keep the existing helper functions
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
        page_title="Multi-Database Table Downloader",
        page_icon="🗃️",
        layout="wide"
    )
    
    st.title("🗃️ Multi-Database Table Downloader")
    st.markdown("Connect to SQLite, PostgreSQL, or MySQL databases and download tables with visual query builder!")
    
    # Show installation instructions if dependencies are missing
    if not (POSTGRES_AVAILABLE or MYSQL_AVAILABLE or SQLALCHEMY_AVAILABLE):
        st.warning("⚠️ **Missing Dependencies**: To use PostgreSQL or MySQL, install: `pip install sqlalchemy psycopg2-binary mysql-connector-python pymysql`")
    
    # Sidebar for database connection
    with st.sidebar:
        render_database_connection_form()
    
    # Main content area (keep existing logic but update for multi-database support)
    if hasattr(st.session_state, 'connected') and st.session_state.connected:
        # [Rest of the main logic remains similar, but now uses the abstract connector interface]
        
        # Get available tables
        tables = st.session_state.db_connector.get_tables()
        
        if tables:
            st.header(f"📋 Available Tables ({len(tables)} total)")
            
            # [Keep the rest of the existing table display logic...]
            # The beauty is that all connectors implement the same interface,
            # so the rest of your code works unchanged!
            
            st.success(f"✅ Connected to {st.session_state.db_type} database with {len(tables)} tables")
        else:
            st.warning("⚠️ No tables found in the database.")
    
    else:
        # Welcome screen (update to mention multiple database support)
        st.markdown("""
        ## 🚀 Welcome to Multi-Database Table Downloader!
        
        This app makes it easy to browse and download tables from multiple database types.
        
        ### 🎯 Supported Databases:
        - **SQLite** (.db, .sqlite, .sqlite3 files)
        - **PostgreSQL** (with psycopg2 or SQLAlchemy)
        - **MySQL** (with mysql-connector-python or SQLAlchemy)
        
        ### ✨ Features:
        - 🔌 **Multi-database support** - SQLite, PostgreSQL, MySQL
        - 📁 **Flexible connections** - file upload, connection strings
        - 📊 **Browse all tables** with detailed metadata
        - 🔍 **Preview table data** before downloading
        - 📥 **Multiple extraction options**
        - 🎯 **Visual Query Builder** - point-and-click filtering
        - 📥 **One-click downloads** in CSV or Excel format
        
        👈 **Get started by connecting to your database using the sidebar!**
        """)

if __name__ == "__main__":
    main()