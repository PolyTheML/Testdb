import streamlit as st
import pandas as pd
import sqlite3
import io
from datetime import datetime
import os
from abc import ABC, abstractmethod
import time

# Try to import additional database drivers
try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
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
            try:
                self.connection.close()
            except:
                pass
        if self.engine:
            try:
                self.engine.dispose()
            except:
                pass

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

class MySQLConnector(DatabaseConnector):
    def __init__(self, host, port, database, username, password, 
                 connection_timeout=10, autocommit=True, charset='utf8mb4',
                 ssl_disabled=True, auth_plugin=None):
        super().__init__()
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.connection_timeout = connection_timeout
        self.autocommit = autocommit
        self.charset = charset
        self.ssl_disabled = ssl_disabled
        self.auth_plugin = auth_plugin
        
    def connect(self, **kwargs):
        """Test connection to MySQL database with improved error handling"""
        try:
            if MYSQL_AVAILABLE:
                # Build connection configuration
                config = {
                    'host': self.host,
                    'port': int(self.port),
                    'database': self.database,
                    'user': self.username,
                    'password': self.password,
                    'connection_timeout': self.connection_timeout,
                    'autocommit': self.autocommit,
                    'charset': self.charset,
                    'use_unicode': True,
                    'raise_on_warnings': False
                }
                
                # Add SSL configuration if disabled
                if self.ssl_disabled:
                    config['ssl_disabled'] = True
                
                # Add auth plugin if specified
                if self.auth_plugin:
                    config['auth_plugin'] = self.auth_plugin
                
                # Test connection with timeout
                try:
                    test_conn = mysql.connector.connect(**config)
                    
                    # Test if we can actually query
                    cursor = test_conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    cursor.close()
                    test_conn.close()
                    
                    # Store successful connection config for reuse
                    self.connection_config = config
                    return True
                    
                except MySQLError as mysql_err:
                    # Handle specific MySQL errors with helpful messages
                    error_code = getattr(mysql_err, 'errno', None)
                    error_msg = str(mysql_err)
                    
                    if error_code == 2003:  # Can't connect to MySQL server
                        st.error(f"🔌 Connection Failed (Error 2003)")
                        st.markdown("""
                        **Possible Solutions:**
                        - Verify MySQL server is running on `{}`
                        - Check if port `{}` is correct (standard is 3306)
                        - Ensure firewall allows connections to this port
                        - For cloud databases, check security groups/firewall rules
                        """.format(self.host, self.port))
                        
                    elif error_code == 1045:  # Access denied
                        st.error(f"🚫 Access Denied (Error 1045)")
                        st.markdown("""
                        **Possible Solutions:**
                        - Double-check username and password
                        - Ensure user has database privileges: `GRANT ALL ON {}.* TO '{}'@'%'`
                        - For remote connections, user needs host permissions
                        - Try connecting from MySQL command line first
                        """.format(self.database, self.username))
                        
                    elif error_code == 1049:  # Unknown database
                        st.error(f"🗄️ Database Not Found (Error 1049)")
                        st.markdown("""
                        **Possible Solutions:**
                        - Verify database name '{}' exists
                        - Check if you have access to this specific database
                        - List databases with: `SHOW DATABASES;`
                        """.format(self.database))
                        
                    elif error_code == 2005:  # Unknown host
                        st.error(f"🌐 Host Not Found (Error 2005)")
                        st.markdown("""
                        **Possible Solutions:**
                        - Check if hostname '{}' is correct
                        - Try using IP address instead of hostname
                        - Verify DNS resolution
                        """.format(self.host))
                        
                    elif error_code == 1251:  # Client authentication protocol issue
                        st.error(f"🔐 Authentication Protocol Issue (Error 1251)")
                        st.markdown("""
                        **Solution:**
                        - This is often caused by newer MySQL versions using `caching_sha2_password`
                        - Try enabling 'mysql_native_password' authentication below
                        - Or update user authentication: `ALTER USER '{}'@'%' IDENTIFIED WITH mysql_native_password BY 'your_password';`
                        """.format(self.username))
                        
                    else:
                        st.error(f"MySQL Error {error_code}: {error_msg}")
                    
                    return False
                    
            elif SQLALCHEMY_AVAILABLE:
                # Fallback to SQLAlchemy with PyMySQL
                try:
                    # URL encode password to handle special characters
                    encoded_password = urllib.parse.quote_plus(self.password)
                    connection_string = f"mysql+pymysql://{self.username}:{encoded_password}@{self.host}:{self.port}/{self.database}"
                    
                    # Add connection parameters for better compatibility
                    connection_string += "?charset=utf8mb4"
                    if self.ssl_disabled:
                        connection_string += "&ssl_disabled=true"
                    
                    self.engine = create_engine(
                        connection_string, 
                        connect_args={
                            'connect_timeout': self.connection_timeout,
                        }
                    )
                    
                    # Test connection
                    with self.engine.connect() as conn:
                        conn.execute(text("SELECT 1"))
                    return True
                    
                except Exception as sqlalchemy_err:
                    st.error(f"SQLAlchemy connection failed: {sqlalchemy_err}")
                    return False
            else:
                st.error("MySQL dependencies not installed. Please install: pip install mysql-connector-python sqlalchemy pymysql")
                return False
                
        except Exception as e:
            st.error(f"Unexpected error connecting to MySQL: {e}")
            return False
    
    def _get_connection(self):
        """Get a fresh MySQL connection"""
        if hasattr(self, 'connection_config'):
            return mysql.connector.connect(**self.connection_config)
        else:
            # Fallback configuration
            return mysql.connector.connect(
                host=self.host,
                port=int(self.port),
                database=self.database,
                user=self.username,
                password=self.password,
                connection_timeout=self.connection_timeout,
                autocommit=self.autocommit,
                charset=self.charset
            )
    
    def get_tables(self):
        """Get list of tables in the MySQL database"""
        try:
            if self.engine:
                inspector = inspect(self.engine)
                return inspector.get_table_names()
            else:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute("SHOW TABLES")
                tables = [row[0] for row in cursor.fetchall()]
                cursor.close()
                conn.close()
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
                if limit is not None:
                    query += f" LIMIT {limit}"
                    if offset is not None:
                        query += f" OFFSET {offset}"
            
            if self.engine:
                df = pd.read_sql_query(query, self.engine)
            else:
                conn = self._get_connection()
                df = pd.read_sql_query(query, conn)
                conn.close()
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
            else:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute(f"EXPLAIN {query}")
                cursor.fetchall()
                cursor.close()
                conn.close()
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
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute(count_query)
                row_count = cursor.fetchone()[0]
                cursor.close()
                conn.close()
            
            # Get column info
            if self.engine:
                info_query = f"DESCRIBE `{table_name}`"
                df = pd.read_sql_query(info_query, self.engine)
                columns = [(row['Field'], row['Type']) for _, row in df.iterrows()]
            else:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute(f"DESCRIBE `{table_name}`")
                columns = [(row[0], row[1]) for row in cursor.fetchall()]
                cursor.close()
                conn.close()
            
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
    elif db_type == "MySQL":
        return MySQLConnector(
            kwargs['host'], kwargs['port'], kwargs['database'], 
            kwargs['username'], kwargs['password'],
            connection_timeout=kwargs.get('connection_timeout', 10),
            ssl_disabled=kwargs.get('ssl_disabled', True),
            auth_plugin=kwargs.get('auth_plugin', None)
        )
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

def render_database_connection_form():
    """Render the database connection form in sidebar"""
    st.header("Database Connection")
    
    # Initialize session state variables if they don't exist
    if 'connected' not in st.session_state:
        st.session_state.connected = False
    if 'db_type' not in st.session_state:
        st.session_state.db_type = None
    if 'db_connector' not in st.session_state:
        st.session_state.db_connector = None
    if 'connector_params' not in st.session_state:
        st.session_state.connector_params = {}
    
    # Database type selection
    available_types = ["SQLite"]
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
            # Save the uploaded file to a temporary location
            temp_dir = "temp_db"
            if not os.path.exists(temp_dir):
                os.makedirs(temp_dir)
            temp_db_path = os.path.join(temp_dir, uploaded_file.name)
            with open(temp_db_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            db_path = temp_db_path
        
        connector_params = {'db_path': db_path}
    
    elif db_type == "MySQL":
        st.markdown("**MySQL Connection**")
        
        # Connection method selection
        connection_method = st.selectbox(
            "Connection Method",
            [
                "Manual Configuration", 
                "MySQL Workbench Connection", 
                "Common Cloud Providers"
            ],
            key="mysql_connection_method"
        )
        
        if connection_method == "MySQL Workbench Connection":
            st.info(
                "If you use MySQL Workbench, you can find these connection details in:\n"
                "• Workbench → Database → Manage Connections\n"
                "• Select your connection and view the parameters"
            )
            
            # Show example of where to find connection details
            with st.expander("How to find MySQL Workbench connection details"):
                st.markdown("""
                **Step-by-step guide:**
                1. Open MySQL Workbench
                2. Click on "Database" in the menu
                3. Select "Manage Connections..."
                4. Choose your connection from the list
                5. Copy the connection parameters:
                   - **Hostname**: Usually shown as "Hostname"
                   - **Port**: Default is 3306
                   - **Schema**: Your database name
                   - **Username**: Your MySQL username
                6. Use these values in the form below
                
                **Note**: The password is not stored in Workbench for security reasons.
                """)
        
        elif connection_method == "Common Cloud Providers":
            cloud_provider = st.selectbox(
                "Cloud Provider",
                ["AWS RDS", "Google Cloud SQL", "Azure Database", "Other"],
                key="cloud_provider"
            )
            
            if cloud_provider == "AWS RDS":
                st.info("For AWS RDS, use your RDS endpoint as hostname (e.g., mydb.xyz.rds.amazonaws.com)")
            elif cloud_provider == "Google Cloud SQL":
                st.info("For Google Cloud SQL, use the public IP or connection name")
            elif cloud_provider == "Azure Database":
                st.info("For Azure, use your server name (e.g., myserver.mysql.database.azure.com)")
        
        # Connection form (same for all methods)
        col1, col2 = st.columns(2)
        with col1:
            host = st.text_input(
                "Host/Hostname", 
                value="localhost", 
                placeholder="localhost or server IP/domain",
                key="mysql_host",
                help="Server address (localhost for local MySQL, or remote server address)"
            )
            database = st.text_input(
                "Database/Schema", 
                placeholder="database_name",
                key="mysql_db",
                help="Name of the database/schema you want to connect to"
            )
        with col2:
            port = st.number_input(
                "Port", 
                min_value=1, 
                max_value=65535, 
                value=3306,
                key="mysql_port",
                help="MySQL server port (default: 3306)"
            )
            username = st.text_input(
                "Username", 
                placeholder="root or your username",
                key="mysql_user",
                help="MySQL username with access to the database"
            )
        
        password = st.text_input(
            "Password", 
            type="password",
            key="mysql_pass",
            help="MySQL password for the username above"
        )
        
        # Advanced connection options
        with st.expander("🔧 Advanced Connection Options"):
            col1_adv, col2_adv = st.columns(2)
            
            with col1_adv:
                connection_timeout = st.number_input(
                    "Connection Timeout (seconds)", 
                    min_value=5, 
                    max_value=60, 
                    value=10,
                    help="How long to wait for connection before timeout"
                )
                
                ssl_disabled = st.checkbox(
                    "Disable SSL", 
                    value=True,
                    help="Check this if you're getting SSL errors (common for local MySQL)"
                )
            
            with col2_adv:
                auth_plugin = st.selectbox(
                    "Authentication Plugin",
                    [None, "mysql_native_password", "caching_sha2_password"],
                    format_func=lambda x: "Auto-detect" if x is None else x,
                    help="Choose specific auth plugin if having authentication issues"
                )
                
                charset = st.selectbox(
                    "Character Set",
                    ["utf8mb4", "utf8", "latin1"],
                    help="Database character encoding (utf8mb4 recommended)"
                )
        
        # Connection testing and troubleshooting
        with st.expander("🔍 Connection Troubleshooting Guide"):
            st.markdown("""
            **Pre-connection Checklist:**
            
            ✅ **Local MySQL Setup:**
            1. Install MySQL Server (not just Workbench)
            2. Start MySQL service:
               - Windows: Services → MySQL → Start
               - macOS: System Preferences → MySQL → Start
               - Linux: `sudo systemctl start mysql`
            3. Test with command line: `mysql -u root -p`
            
            ✅ **Remote MySQL Setup:**
            1. Get correct connection details from your admin/provider
            2. Ensure your IP is whitelisted
            3. Check firewall rules (port 3306 typically)
            4. Verify user has remote connection privileges
            
            ✅ **Common MySQL Commands for Setup:**
            ```sql
            -- Create database
            CREATE DATABASE your_database_name;
            
            -- Create user with remote access
            CREATE USER 'your_username'@'%' IDENTIFIED BY 'your_password';
            
            -- Grant privileges
            GRANT ALL PRIVILEGES ON your_database_name.* TO 'your_username'@'%';
            FLUSH PRIVILEGES;
            
            -- For authentication issues, try:
            ALTER USER 'your_username'@'%' IDENTIFIED WITH mysql_native_password BY 'your_password';
            ```
            """)
        
        # Test connection button (separate from main connect)
        if st.button("Test Connection", key="test_mysql"):
            if not all([host, database, username]):
                st.error("Please fill in Host, Database, and Username to test connection")
            else:
                test_connector_params = {
                    'host': host,
                    'port': port,
                    'database': database,
                    'username': username,
                    'password': password,
                    'connection_timeout': connection_timeout,
                    'ssl_disabled': ssl_disabled,
                    'auth_plugin': auth_plugin
                }
                
                with st.spinner("Testing connection..."):
                    try:
                        test_connector = MySQLConnector(**test_connector_params)
                        if test_connector.connect():
                            st.success("Connection test successful!")
                            test_connector.disconnect()
                        else:
                            st.error("Connection test failed")
                    except Exception as e:
                        st.error(f"Connection test failed: {e}")
        
        connector_params = {
            'host': host,
            'port': port,
            'database': database,
            'username': username,
            'password': password,
            'connection_timeout': connection_timeout,
            'ssl_disabled': ssl_disabled,
            'auth_plugin': auth_plugin
        }
    
    # Connect button
    if st.button("Connect to Database", type="primary"):
        # Validate required fields
        if db_type == "SQLite" and not connector_params.get('db_path'):
            st.error("Please provide a database file or path")
            return
        elif db_type in ["MySQL"]:
            required_fields = ['host', 'database', 'username']
            missing_fields = [field for field in required_fields if not connector_params.get(field)]
            if missing_fields:
                st.error(f"Please fill in: {', '.join(missing_fields)}")
                return
        
        # Attempt connection
        try:
            with st.spinner(f"Connecting to {db_type}..."):
                connector = create_database_connector(db_type, **connector_params)
                if connector.connect():
                    st.session_state.db_connector = connector
                    st.session_state.connected = True
                    st.session_state.db_type = db_type
                    st.session_state.connector_params = connector_params
                    
                    if db_type == "SQLite":
                        db_name = os.path.basename(connector_params['db_path'])
                    else:
                        db_name = f"{connector_params['database']}@{connector_params['host']}"
                    
                    st.success(f"Connected to: {db_name}")
                    st.rerun()
                else:
                    st.session_state.connected = False
        except Exception as e:
            st.error(f"Connection failed: {e}")
            st.session_state.connected = False
    
    # Connection status
    if st.session_state.connected:
        st.success("Connected")
        
        # Show connection info
        if st.session_state.db_type == "SQLite":
            db_name = os.path.basename(st.session_state.connector_params.get('db_path', 'Unknown'))
            st.info(f"**{st.session_state.db_type}**: {db_name}")
        elif st.session_state.db_type in ["MySQL"]:
            params = st.session_state.connector_params
            st.info(f"**{st.session_state.db_type}**: {params.get('database', 'Unknown')} @ {params.get('host', 'Unknown')}:{params.get('port', 'Unknown')}")
        
        if st.button("Refresh Tables"):
            st.rerun()
            
        if st.button("Disconnect"):
            if st.session_state.db_connector:
                st.session_state.db_connector.disconnect()
            
            # Clear all session state keys
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            
            st.rerun()
    else:
        st.error("Not Connected")

def build_visual_query(table_name, table_info, db_type="SQLite"):
    """Build SQL query using visual interface (adapted for different databases)"""
    st.markdown("### Visual Query Builder")
    
    columns = [col[0] for col in table_info['column_info']]
    column_types = {col[0]: col[1] for col in table_info['column_info']}
    
    # --- UI Elements for Query Building ---

    # 1. Column Selection
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
        st.info(f"All {len(columns)} columns selected")
    else:
        selected_columns = st.multiselect(
            "Choose columns:",
            columns,
            default=columns[:5] if len(columns) > 5 else columns,
            key=f"cols_{table_name}"
        )
        if not selected_columns:
            st.warning("Please select at least one column")
            return None

    # 2. Filters Section
    st.markdown("**2. Add Filters (Optional):**")
    
    filter_key = f"filters_{table_name}"
    if filter_key not in st.session_state:
        st.session_state[filter_key] = []
    
    # Add/Clear filter buttons
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("Add Filter", key=f"add_filter_{table_name}"):
            # Initialize with proper default values based on first column type
            first_col_type = column_types.get(columns[0], 'TEXT').upper()
            if any(x in first_col_type for x in ['INT', 'SERIAL', 'BIGINT']):
                default_value = 0
            elif any(x in first_col_type for x in ['REAL', 'NUMERIC', 'DECIMAL', 'FLOAT', 'DOUBLE']):
                default_value = 0.0
            else:
                default_value = ""
            
            st.session_state[filter_key].append({
                'column': columns[0],
                'operator': 'equals (=)',
                'value': default_value,
                'logic': 'AND'
            })
            st.rerun()
    
    with col2:
        if st.session_state[filter_key] and st.button("Clear All Filters", key=f"clear_filters_{table_name}"):
            st.session_state[filter_key] = []
            st.rerun()
    
    # Display existing filter UI
    for i, filter_item in enumerate(st.session_state[filter_key]):
        with st.container(border=True):
            filter_col1, filter_col2, filter_col3, filter_col4, filter_col5 = st.columns([1.5, 3, 2.5, 3, 1])
            
            with filter_col1:
                if i > 0:
                    logic = st.selectbox(
                        "Logic",
                        ["AND", "OR"],
                        index=0 if filter_item['logic'] == 'AND' else 1,
                        key=f"logic_{table_name}_{i}",
                        label_visibility="collapsed"
                    )
                    st.session_state[filter_key][i]['logic'] = logic
                else:
                    st.write("WHERE")
            
            with filter_col2:
                column = st.selectbox(
                    "Column",
                    columns,
                    index=columns.index(filter_item['column']) if filter_item['column'] in columns else 0,
                    key=f"filter_col_{table_name}_{i}",
                    label_visibility="collapsed"
                )
                # Update the column in session state
                old_column = st.session_state[filter_key][i]['column']
                if column != old_column:
                    # Reset value when column changes to match new column type
                    new_col_type = column_types.get(column, 'TEXT').upper()
                    if any(x in new_col_type for x in ['INT', 'SERIAL', 'BIGINT']):
                        st.session_state[filter_key][i]['value'] = 0
                    elif any(x in new_col_type for x in ['REAL', 'NUMERIC', 'DECIMAL', 'FLOAT', 'DOUBLE']):
                        st.session_state[filter_key][i]['value'] = 0.0
                    else:
                        st.session_state[filter_key][i]['value'] = ""
                
                st.session_state[filter_key][i]['column'] = column
            
            with filter_col3:
                col_type = column_types.get(column, 'TEXT').upper()
                
                if any(x in col_type for x in ['INT', 'REAL', 'NUMERIC', 'DECIMAL', 'FLOAT', 'DOUBLE', 'SERIAL', 'BIGINT']):
                    operators = ['equals (=)', 'not equals (!=)', 'greater than (>)', 'less than (<)', 
                               'greater or equal (>=)', 'less or equal (<=)', 'is null', 'is not null']
                else: # Text or other types
                    operators = ['equals (=)', 'not equals (!=)', 'contains (LIKE)', 'starts with', 
                               'ends with', 'is null', 'is not null']
                
                operator = st.selectbox(
                    "Operator",
                    operators,
                    index=operators.index(filter_item['operator']) if filter_item['operator'] in operators else 0,
                    key=f"filter_op_{table_name}_{i}",
                    label_visibility="collapsed"
                )
                st.session_state[filter_key][i]['operator'] = operator
            
            with filter_col4:
                if 'null' not in operator.lower():
                    # Get current value with proper type handling
                    current_value = filter_item.get('value')
                    
                    if any(x in col_type for x in ['INT', 'SERIAL', 'BIGINT']):
                        # Handle integer columns
                        try:
                            if current_value is None or current_value == '':
                                default_val = 0
                            else:
                                default_val = int(current_value)
                        except (ValueError, TypeError):
                            default_val = 0
                        
                        value = st.number_input(
                            "Value", 
                            value=default_val, 
                            step=1, 
                            key=f"filter_val_{table_name}_{i}", 
                            label_visibility="collapsed"
                        )
                    elif any(x in col_type for x in ['REAL', 'NUMERIC', 'DECIMAL', 'FLOAT', 'DOUBLE']):
                        # Handle float columns
                        try:
                            if current_value is None or current_value == '':
                                default_val = 0.0
                            else:
                                default_val = float(current_value)
                        except (ValueError, TypeError):
                            default_val = 0.0
                        
                        value = st.number_input(
                            "Value", 
                            value=default_val, 
                            format="%.4f", 
                            key=f"filter_val_{table_name}_{i}", 
                            label_visibility="collapsed"
                        )
                    else:
                        # Handle text columns
                        if current_value is None:
                            default_val = ""
                        else:
                            default_val = str(current_value)
                        
                        value = st.text_input(
                            "Value", 
                            value=default_val, 
                            key=f"filter_val_{table_name}_{i}", 
                            label_visibility="collapsed", 
                            placeholder="Enter value..."
                        )
                    st.session_state[filter_key][i]['value'] = value
                else:
                    st.write(" ") # Placeholder for alignment

            with filter_col5:
                if st.button("X", key=f"remove_filter_{table_name}_{i}", help="Remove this filter"):
                    st.session_state[filter_key].pop(i)
                    st.rerun()

    # 3. Sorting Section
    st.markdown("**3. Sort Results (Optional):**")
    sort_enabled = st.checkbox("Enable sorting", key=f"sort_enabled_{table_name}")
    sort_column, sort_direction = None, None
    if sort_enabled:
        sort_col1, sort_col2 = st.columns(2)
        with sort_col1:
            sort_column = st.selectbox("Sort by column:", selected_columns, key=f"sort_col_{table_name}")
        with sort_col2:
            sort_direction = st.selectbox("Sort direction:", ["Ascending (A-Z)", "Descending (Z-A)"], key=f"sort_dir_{table_name}")
    
    # 4. Limit Section
    st.markdown("**4. Limit Results (Optional):**")
    limit_enabled = st.checkbox("Limit number of rows", key=f"limit_enabled_{table_name}")
    row_limit = None
    if limit_enabled:
        row_limit = st.number_input("Maximum rows:", min_value=1, max_value=1000000, value=1000, key=f"row_limit_{table_name}")

    # --- Build the SQL Query from UI state ---
    
    if not selected_columns:
        return None
    
    # SELECT clause
    if col_selection_type == "All Columns":
        select_clause = "*"
    else:
        if db_type == "SQLite":
            select_clause = ", ".join([f"[{col}]" for col in selected_columns])
        else:  # MySQL
            select_clause = ", ".join([f"`{col}`" for col in selected_columns])
            
    # FROM clause
    if db_type == "SQLite":
        query = f"SELECT {select_clause} FROM [{table_name}]"
    else:  # MySQL
        query = f"SELECT {select_clause} FROM `{table_name}`"
        
    # WHERE clause
    where_conditions = []
    for i, filter_item in enumerate(st.session_state[filter_key]):
        column = filter_item['column']
        operator = filter_item['operator']
        value = filter_item.get('value', '')
        logic = filter_item.get('logic', 'AND')
        
        # Column quoting based on database type
        if db_type == "SQLite":
            quoted_col = f"[{column}]"
        else:  # MySQL
            quoted_col = f"`{column}`"

        # Handle string vs numeric values for quoting
        col_type = column_types.get(column, 'TEXT').upper()
        is_numeric = any(x in col_type for x in ['INT', 'REAL', 'NUMERIC', 'DECIMAL', 'FLOAT', 'DOUBLE', 'SERIAL', 'BIGINT'])
        
        # Basic sanitization for string values to prevent SQL injection
        if not is_numeric and isinstance(value, str):
            safe_value = value.replace("'", "''") 
            quoted_value = f"'{safe_value}'"
        else:
            quoted_value = str(value)
            
        condition = ""
        # Build condition based on operator
        if operator == 'equals (=)':
            condition = f"{quoted_col} = {quoted_value}"
        elif operator == 'not equals (!=)':
            condition = f"{quoted_col} != {quoted_value}"
        elif operator == 'greater than (>)':
            condition = f"{quoted_col} > {quoted_value}"
        elif operator == 'less than (<)':
            condition = f"{quoted_col} < {quoted_value}"
        elif operator == 'greater or equal (>=)':
            condition = f"{quoted_col} >= {quoted_value}"
        elif operator == 'less or equal (<=)':
            condition = f"{quoted_col} <= {quoted_value}"
        elif operator == 'contains (LIKE)':
            condition = f"{quoted_col} LIKE '%{str(value).replace('%', '%%')}%'"
        elif operator == 'starts with':
            condition = f"{quoted_col} LIKE '{str(value).replace('%', '%%')}%'"
        elif operator == 'ends with':
            condition = f"{quoted_col} LIKE '%{str(value).replace('%', '%%')}'"
        elif operator == 'is null':
            condition = f"{quoted_col} IS NULL"
        elif operator == 'is not null':
            condition = f"{quoted_col} IS NOT NULL"
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
        if db_type == "SQLite":
            query += f" ORDER BY [{sort_column}] {direction}"
        else:  # MySQL
            query += f" ORDER BY `{sort_column}` {direction}"
    
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
        page_title="Multi-Database Table Downloader",
        page_icon="📊",
        layout="wide"
    )
    
    st.title("Multi-Database Table Downloader")
    st.markdown("Connect to SQLite, PostgreSQL, or MySQL databases and download tables with a visual query builder!")
    
    # Show installation instructions if dependencies are missing
    if not (MYSQL_AVAILABLE or SQLALCHEMY_AVAILABLE):
        st.warning("Missing Dependencies for MySQL: Install with `pip install sqlalchemy mysql-connector-python pymysql`")
    
    # Sidebar for database connection
    with st.sidebar:
        render_database_connection_form()
    
    # Main content area
    if st.session_state.get('connected') and st.session_state.get('db_connector'):
        
        db_connector = st.session_state.db_connector
        
        # Get available tables
        try:
            tables = db_connector.get_tables()
        except Exception as e:
            st.error(f"Error retrieving tables: {e}")
            tables = []
        
        if tables:
            st.header(f"Available Tables ({len(tables)} total)")
            
            # Search/filter tables
            search_term = st.text_input("Search tables:", placeholder="Enter table name...")
            if search_term:
                filtered_tables = [table for table in tables if search_term.lower() in table.lower()]
            else:
                filtered_tables = tables
            
            if not filtered_tables and search_term:
                st.warning("No tables match your search.")
            else:
                # Display tables in expandable sections
                for table in filtered_tables:
                    with st.expander(f"**{table}**", expanded=False):
                        try:
                            table_info = db_connector.get_table_info(table)
                            
                            if table_info:
                                # Basic table info
                                info_col1, info_col2, info_col3 = st.columns(3)
                                with info_col1:
                                    st.metric("Rows", f"{table_info['rows']:,}")
                                with info_col2:
                                    st.metric("Columns", table_info['columns'])
                                with info_col3:
                                    if st.button(f"Show Columns", key=f"show_cols_btn_{table}"):
                                        key = f"show_cols_state_{table}"
                                        st.session_state[key] = not st.session_state.get(key, False)
                                
                                # Show column details
                                if st.session_state.get(f"show_cols_state_{table}", False):
                                    st.markdown("**Columns:**")
                                    cols_display = st.columns(3)
                                    for i, (col_name, col_type) in enumerate(table_info['column_info']):
                                        with cols_display[i % 3]:
                                            st.write(f"• **{col_name}** ({col_type})")
                                
                                st.markdown("---")
                                
                                # Query Options Section
                                st.markdown("### Data Filtering Options")
                                
                                query_type = st.radio(
                                    "Choose extraction method:",
                                    ["All Data", "Row Range", "Visual Query Builder", "Custom SQL"],
                                    key=f"query_type_{table}",
                                    horizontal=True
                                )
                                
                                custom_query, limit, offset = None, None, None
                                
                                if query_type == "Row Range":
                                    range_col1, range_col2 = st.columns(2)
                                    with range_col1:
                                        limit = st.number_input("Number of rows:", min_value=1, max_value=table_info['rows'], value=min(1000, table_info['rows']), key=f"limit_{table}")
                                    with range_col2:
                                        offset = st.number_input("Start from row (0-indexed):", min_value=0, max_value=max(0, table_info['rows']-1), value=0, key=f"offset_{table}")
                                    st.info(f"Will extract rows from {offset} to {min(offset+limit-1, table_info['rows']-1)}")
                                
                                elif query_type == "Visual Query Builder":
                                    custom_query = build_visual_query(table, table_info, st.session_state.db_type)
                                    if custom_query:
                                        st.markdown("**Generated SQL Query:**")
                                        st.code(custom_query, language='sql')
                                
                                elif query_type == "Custom SQL":
                                    if st.session_state.db_type == "MySQL":
                                        default_query = f"SELECT * FROM `{table}` WHERE "
                                    else: # SQLite
                                        default_query = f"SELECT * FROM [{table}] WHERE "
                                    
                                    custom_query = st.text_area("SQL Query:", value=default_query, height=100, key=f"custom_query_{table}", help="Only SELECT queries are allowed.")
                                    if st.button(f"Validate Query", key=f"validate_{table}"):
                                        is_valid, message = db_connector.validate_query(custom_query)
                                        if is_valid: st.success(f"{message}")
                                        else: st.error(f"{message}")
                                
                                st.markdown("---")
                                
                                # Preview and Download Section
                                if st.button(f"Preview Data & Download", key=f"preview_{table}"):
                                    st.session_state.preview_table = table
                                    st.session_state.preview_query_type = query_type
                                    st.session_state.preview_custom_query = custom_query
                                    st.session_state.preview_limit = limit
                                    st.session_state.preview_offset = offset
                                    st.rerun()

                            else:
                                st.error(f"Could not get table info for {table}")
                        except Exception as e:
                            st.error(f"Error processing table {table}: {e}")
            
            # Preview section (shown outside the expander)
            if st.session_state.get('preview_table'):
                st.markdown("---")
                preview_table = st.session_state.preview_table
                st.header(f"Preview & Download: **{preview_table}**")
                
                # Fetch data for preview (limited to 100 rows)
                with st.spinner("Loading preview..."):
                    try:
                        query_type = st.session_state.get('preview_query_type', 'All Data')
                        custom_query = st.session_state.get('preview_custom_query')
                        
                        preview_df = None
                        if query_type in ["Custom SQL", "Visual Query Builder"] and custom_query:
                            preview_query = custom_query
                            if "limit" not in custom_query.lower():
                                preview_query += " LIMIT 100"
                            preview_df = db_connector.get_table_data(preview_table, custom_query=preview_query)
                        elif query_type == "Row Range":
                            limit = st.session_state.get('preview_limit')
                            offset = st.session_state.get('preview_offset')
                            preview_limit = min(limit, 100) if limit else 100
                            preview_df = db_connector.get_table_data(preview_table, limit=preview_limit, offset=offset)
                        else: # All Data
                            preview_df = db_connector.get_table_data(preview_table, limit=100)

                        if preview_df is not None:
                            if st.button("Close Preview"):
                                for attr in ['preview_table', 'preview_query_type', 'preview_custom_query', 'preview_limit', 'preview_offset']:
                                    if attr in st.session_state: del st.session_state[attr]
                                st.rerun()
                            
                            st.dataframe(preview_df, use_container_width=True, height=400)
                            
                            # Download Full Dataset
                            st.markdown("**Download Full Dataset:**")
                            if st.button("Prepare Full Dataset for Download", type="primary"):
                                with st.spinner("Preparing full dataset..."):
                                    full_df = None
                                    if query_type in ["Custom SQL", "Visual Query Builder"]:
                                        full_df = db_connector.get_table_data(preview_table, custom_query=custom_query)
                                    elif query_type == "Row Range":
                                        full_df = db_connector.get_table_data(preview_table, limit=st.session_state.get('preview_limit'), offset=st.session_state.get('preview_offset'))
                                    else: # All Data
                                        full_df = db_connector.get_table_data(preview_table)
                                    
                                    if full_df is not None:
                                        st.session_state.download_df = full_df
                                        st.session_state.download_table_name = preview_table
                                        st.success(f"Prepared {len(full_df):,} rows for download.")
                                    else:
                                        st.error("Failed to fetch full dataset.")

                            if 'download_df' in st.session_state:
                                dl_df = st.session_state.download_df
                                dl_table = st.session_state.download_table_name
                                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                                
                                dl_col1, dl_col2 = st.columns(2)
                                with dl_col1:
                                    st.download_button(
                                        label=f"Download CSV ({len(dl_df):,} rows)",
                                        data=convert_df_to_csv(dl_df),
                                        file_name=f"{dl_table}_{timestamp}.csv",
                                        mime="text/csv",
                                        use_container_width=True
                                    )
                                with dl_col2:
                                    st.download_button(
                                        label=f"Download Excel ({len(dl_df):,} rows)",
                                        data=convert_df_to_excel(dl_df),
                                        file_name=f"{dl_table}_{timestamp}.xlsx",
                                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                        use_container_width=True
                                    )
                        else:
                            st.warning("Query returned no data to preview.")
                    except Exception as e:
                        st.error(f"Error loading preview data: {e}")

        elif st.session_state.get('connected'):
            st.warning("No tables found in the database or connection lost. Please refresh or reconnect.")
    
    else:
        # Welcome screen
        st.markdown("""
        ## Welcome to the Multi-Database Downloader!
        
        This app makes it easy to browse, filter, and download tables from multiple database types.
        
        ### Features:
        - **Multi-database support**: SQLite and MySQL.
        - **Flexible connections**: Upload SQLite files or connect to remote servers.
        - **Browse tables** with detailed metadata (row counts, columns).
        - **Visual Query Builder**: Point-and-click to create complex filters without writing SQL.
        - **Custom SQL**: For power users who want to write their own queries.
        - **Data Preview**: See a sample of your data before committing to a full download.
        - **One-click downloads** in both CSV and Excel formats.
        
        ### Installation Requirements
        For full functionality, please install the required libraries:
        ```bash
        pip install streamlit pandas sqlalchemy mysql-connector-python pymysql openpyxl
        ```
        
        ### MySQL Connection Troubleshooting
        If you're having MySQL connection issues:
        
        1. **Check MySQL Server Status:**
           ```bash
           # Windows
           net start mysql80
           
           # macOS
           sudo /usr/local/mysql/support-files/mysql.server start
           
           # Linux
           sudo systemctl status mysql
           sudo systemctl start mysql
           ```
           
        2. **Test Command Line Access:**
           ```bash
           mysql -h your_host -P 3306 -u your_username -p your_database
           ```
           
        3. **Common Fixes:**
           - For "authentication plugin" errors, try the mysql_native_password option
           - For SSL errors, disable SSL in advanced options
           - For cloud databases, verify security group settings
           - Check if your user has remote connection privileges
        
        ---
        **Get started by connecting to your database using the sidebar!**
        """)

if __name__ == "__main__":
    main()