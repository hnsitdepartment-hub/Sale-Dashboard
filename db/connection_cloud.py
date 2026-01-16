import pyodbc
import streamlit as st
import os
import requests
import json
from typing import Optional, Union

# -------------------------------
# Environment Detection
# -------------------------------
def is_streamlit_cloud() -> bool:
    """Check if running on Streamlit Cloud"""
    return os.environ.get('STREAMLIT_CLOUD') == 'true'

def is_local_development() -> bool:
    """Check if running in local development"""
    return not is_streamlit_cloud()

# -------------------------------
# Cloud Configuration
# -------------------------------
class CloudConfig:
    """Configuration for cloud deployment"""
    # API Proxy URL (can be set via environment variable)
    API_PROXY_URL = os.environ.get('DB_API_PROXY', '')

    # Fallback to direct connection if no proxy configured
    USE_DIRECT_CONNECTION = not API_PROXY_URL

# -------------------------------
# API Proxy Client (for cloud deployment)
# -------------------------------
class DatabaseAPIClient:
    """HTTP client for database API proxy"""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')

    def execute_query(self, query: str, params: Optional[list] = None) -> list:
        """Execute SQL query via API proxy"""
        try:
            response = requests.post(
                f"{self.base_url}/query",
                json={
                    "query": query,
                    "params": params or []
                },
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            st.error(f"API Proxy Error: {str(e)}")
            raise

    def fetch_dataframe(self, query: str, params: Optional[list] = None) -> 'pd.DataFrame':
        """Fetch data as pandas DataFrame"""
        import pandas as pd
        data = self.execute_query(query, params)
        return pd.DataFrame(data)

# -------------------------------
# Connection Wrapper (Main Interface)
# -------------------------------
class DatabaseConnection:
    """Unified database connection interface"""

    def __init__(self, connection_type: str):
        self.connection_type = connection_type
        self._native_conn = None
        self._api_client = None

        if is_streamlit_cloud() and CloudConfig.USE_DIRECT_CONNECTION:
            # Try direct connection first on cloud
            try:
                self._native_conn = self._create_native_connection()
            except Exception:
                # Fall back to API if direct fails
                if CloudConfig.API_PROXY_URL:
                    self._api_client = DatabaseAPIClient(CloudConfig.API_PROXY_URL)
                else:
                    raise ConnectionError("No database connection available in cloud environment")
        elif is_local_development():
            # Use native connection locally
            self._native_conn = self._create_native_connection()
        else:
            # Use API client if configured
            if CloudConfig.API_PROXY_URL:
                self._api_client = DatabaseAPIClient(CloudConfig.API_PROXY_URL)
            else:
                self._native_conn = self._create_native_connection()

    def _create_native_connection(self) -> pyodbc.Connection:
        """Create native pyodbc connection"""
        if self.connection_type == "kdsdb":
            return self._connect_kdsdb()
        elif self.connection_type == "candelahns":
            return self._connect_candelahns()
        else:
            raise ValueError(f"Unknown connection type: {self.connection_type}")

    def _connect_kdsdb(self) -> pyodbc.Connection:
        """Connect to KDS_DB (Windows Auth for local, SQL Auth for cloud)"""
        if is_streamlit_cloud():
            # Cloud connection (if KDS_DB is accessible)
            conn_str = (
                "DRIVER={ODBC Driver 17 for SQL Server};"
                "SERVER=localhost;"
                "DATABASE=KDS_DB;"
                "UID=sa;"  # Would need proper credentials
                "PWD=your_password;"  # Would need proper credentials
                "Encrypt=no;"
            )
        else:
            # Original local connection
            conn_str = (
                "DRIVER={SQL Server};"
                "SERVER=localhost;"
                "DATABASE=KDS_DB;"
                "Trusted_Connection=yes;"
            )

        return pyodbc.connect(conn_str, autocommit=False)

    def _connect_candelahns(self) -> pyodbc.Connection:
        """Connect to Candelahns DB"""
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=103.86.55.183,2001;"
            "DATABASE=Candelahns;"
            "UID=ReadOnlyUser;"
            "PWD=902729@Rafy;"
            "Encrypt=no;"
            "TrustServerCertificate=yes;"
            "Connection Timeout=30;"
        )
        return pyodbc.connect(conn_str, autocommit=False)

    def cursor(self):
        """Get database cursor (compatible interface)"""
        if self._native_conn:
            return self._native_conn.cursor()
        else:
            return APIProxyCursor(self._api_client)

    def commit(self):
        """Commit transaction (no-op for API)"""
        if self._native_conn:
            self._native_conn.commit()

    def close(self):
        """Close connection"""
        if self._native_conn:
            self._native_conn.close()

    def execute(self, query: str, params: Optional[list] = None):
        """Execute SQL query"""
        if self._native_conn:
            cursor = self._native_conn.cursor()
            cursor.execute(query, params or ())
            return cursor
        else:
            return self._api_client.execute_query(query, params)

    def fetch_dataframe(self, query: str, params: Optional[list] = None) -> 'pd.DataFrame':
        """Fetch data as pandas DataFrame"""
        if self._native_conn:
            import pandas as pd
            return pd.read_sql(query, self._native_conn, params=params)
        else:
            return self._api_client.fetch_dataframe(query, params)

# -------------------------------
# API Proxy Cursor (for compatibility)
# -------------------------------
class APIProxyCursor:
    """Cursor-like interface for API client"""

    def __init__(self, api_client: DatabaseAPIClient):
        self.api_client = api_client
        self._data = []
        self._columns = []

    def execute(self, query: str, params: Optional[list] = None):
        """Execute query via API"""
        self._data = self.api_client.execute_query(query, params)
        if self._data:
            self._columns = list(self._data[0].keys())
        return self

    def fetchall(self) -> list:
        """Get all results"""
        return self._data

    def fetchone(self) -> Optional[dict]:
        """Get single result"""
        return self._data[0] if self._data else None

    @property
    def description(self) -> list:
        """Get column descriptions (for pandas compatibility)"""
        return [(col, None, None, None, None, None, None) for col in self._columns] if self._columns else []

# -------------------------------
# Wrapper Functions (Drop-in Replacements)
# -------------------------------
@st.cache_resource
def get_connection_kdsdb() -> DatabaseConnection:
    """Get KDS_DB connection (cloud-compatible)"""
    return DatabaseConnection("kdsdb")

@st.cache_resource
def get_connection_candelahns() -> DatabaseConnection:
    """Get Candelahns DB connection (cloud-compatible)"""
    return DatabaseConnection("candelahns")

def get_connection_candelahns_direct() -> pyodbc.Connection:
    """Get direct Candelahns connection (original function)"""
    conn_str = (
        "DRIVER={SQL Server};"
        "SERVER=103.86.55.183,2001;"
        "DATABASE=Candelahns;"
        "UID=ReadOnlyUser;"
        "PWD=902729@Rafy"
    )
    conn = pyodbc.connect(conn_str, autocommit=False)
    print("Connected to Candelahns DB successfully")
    return conn

# -------------------------------
# Test Function (Cloud-compatible)
# -------------------------------
@st.cache_resource
def test_connection(server: str, database: str, auth: str = "windows", uid: Optional[str] = None, pwd: Optional[str] = None) -> Union[DatabaseConnection, pyodbc.Connection]:
    """Test database connection"""
    if is_streamlit_cloud() and not CloudConfig.USE_DIRECT_CONNECTION:
        # Use API client for testing
        if CloudConfig.API_PROXY_URL:
            return DatabaseConnection("test")
        else:
            raise ConnectionError("No database connection available in cloud environment")
    else:
        # Use original test function
        if auth.lower() == "windows":
            conn_str = f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
        else:
            conn_str = f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={uid};PWD={pwd};"

        return pyodbc.connect(conn_str, autocommit=False)
