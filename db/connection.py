import pyodbc
import streamlit as st

# -------------------------------
# KDS_DB (Windows Authentication on localhost)
# -------------------------------
@st.cache_resource
def get_connection_kdsdb():
    conn_str = (
        "DRIVER={SQL Server};"
        "SERVER=localhost;"
        "DATABASE=KDS_DB;"
        "Trusted_Connection=yes;"
    )
    conn = pyodbc.connect(conn_str, autocommit=False)
    print("Connected to KDS_DB successfully")
    return conn

# -------------------------------
# Candelahns DB (Remote SQL Authentication)
# -------------------------------
def get_connection_candelahns():
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
# Generic Test Function (Optional)
# -------------------------------
@st.cache_resource
def test_connection(server, database, auth="windows", uid=None, pwd=None):
    if auth.lower() == "windows":
        conn_str = f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    else:
        conn_str = f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={uid};PWD={pwd};"
    conn = pyodbc.connect(conn_str, autocommit=False)
    print(f"Connected to {database} on {server} successfully")
    return conn
