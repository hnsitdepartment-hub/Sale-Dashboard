import pandas as pd
from .connection import get_connection_candelahns

def get_branch_ots(conn, shop_id):
    """
    Return all active OTs for a specific branch.
    Uses Candelahns DB for employee list.
    """
    conn_candelahns = get_connection_candelahns()
    try:
        return pd.read_sql(
            "SELECT shop_employee_id, field_name FROM dbo.tblDefShopEmployees WHERE shop_id=? ORDER BY field_name",
            conn_candelahns,
            params=[int(shop_id)]
        )
    finally:
        conn_candelahns.close()

def get_branch_ot_targets(conn):
    """
    Return all saved OT targets.
    Note: Uses employee_id for display since employees are in Candelahns DB.
    """
    return pd.read_sql(
        """
        SELECT t.id, b.branch_name, t.employee_id, t.monthly_target
        FROM dbo.ot_targets t
        JOIN dbo.branches b ON t.shop_id = b.shop_id
        ORDER BY b.branch_name, t.employee_id
        """,
        conn
    )

def save_branch_ot_target(conn, shop_id, employee_id, target):
    cursor = conn.cursor()
    cursor.execute("""
        MERGE dbo.ot_targets t
        USING (SELECT ? AS s, ? AS e, ? AS t) x
        ON t.shop_id=x.s AND t.employee_id=x.e
        WHEN MATCHED THEN UPDATE SET monthly_target=x.t
        WHEN NOT MATCHED THEN INSERT (shop_id, employee_id, monthly_target)
        VALUES (x.s, x.e, x.t)
    """, shop_id, employee_id, target)
    conn.commit()

def disable_ot_target(conn, shop_id, employee_id):
    cursor = conn.cursor()
    cursor.execute("UPDATE dbo.ot_targets SET is_active=0 WHERE shop_id=? AND employee_id=?", shop_id, employee_id)
    conn.commit()
