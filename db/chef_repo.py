import pandas as pd

def get_chef_categories(conn):
    return pd.read_sql(
        "SELECT category_id, category_name FROM dbo.cfg_chef_categories WHERE is_active=1 ORDER BY category_name",
        conn
    )

def get_branch_chef_targets(conn):
    return pd.read_sql(
        """
        SELECT t.id, b.branch_name, c.category_name, t.monthly_target
        FROM dbo.cfg_branch_chef_targets t
        JOIN dbo.branches b ON t.shop_id = b.shop_id
        JOIN dbo.cfg_chef_categories c ON t.category_id = c.category_id
        ORDER BY b.branch_name, c.category_name
        """,
        conn
    )

def save_branch_chef_target(conn, shop_id, category_id, target):
    cursor = conn.cursor()
    cursor.execute("""
        MERGE dbo.cfg_branch_chef_targets t
        USING (SELECT ? AS s, ? AS c, ? AS t) x
        ON t.shop_id=x.s AND t.category_id=x.c
        WHEN MATCHED THEN UPDATE SET monthly_target=x.t
        WHEN NOT MATCHED THEN INSERT (shop_id, category_id, monthly_target)
        VALUES (x.s, x.c, x.t)
    """, shop_id, category_id, target)
    conn.commit()

def disable_chef_target(conn, shop_id, category_id):
    cursor = conn.cursor()
    cursor.execute("DELETE FROM dbo.cfg_branch_chef_targets WHERE shop_id=? AND category_id=?", shop_id, category_id)
    conn.commit()

def disable_branch(conn, shop_id):
    shop_id = int(shop_id)
    cursor = conn.cursor()
    cursor.execute("UPDATE dbo.branches SET is_active=0 WHERE shop_id=?", shop_id)
    conn.commit()
