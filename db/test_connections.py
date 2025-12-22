import pyodbc

server = "localhost"
database = "KDS_DB"

conn_str = f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes"

try:
    conn = pyodbc.connect(conn_str, timeout=5)
    print("Connection successful!")
    conn.close()
except Exception as e:
    print("Connection failed:", e)
