import pandas as pd

def get_active_branches(conn):
    # Correct table reference: dbo.branches
    query = "SELECT shop_id, branch_name FROM dbo.branches WHERE is_active = 1 ORDER BY branch_name"
    return pd.read_sql(query, conn)

def save_branch(conn, shop_id, branch_name):
    cursor = conn.cursor()
    cursor.execute("""
        MERGE dbo.branches AS t
        USING (SELECT ? AS shop_id, ? AS branch_name) AS s
        ON t.shop_id = s.shop_id
        WHEN MATCHED THEN
            UPDATE SET branch_name = s.branch_name, is_active = 1
        WHEN NOT MATCHED THEN
            INSERT (shop_id, branch_name)
            VALUES (s.shop_id, s.branch_name);
    """, shop_id, branch_name)  # <-- semicolon is added before closing triple quotes
    conn.commit()
    
def disable_branch(conn, shop_id):
    shop_id = int(shop_id)
    cursor = conn.cursor()
    cursor.execute("UPDATE dbo.branches SET is_active=0 WHERE shop_id=?", shop_id)
    conn.commit()
