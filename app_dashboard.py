import pyodbc
import pandas as pd
import streamlit as st
from datetime import date, timedelta
from typing import List
import altair as alt
from db.connection import get_connection_candelahns, get_connection_kdsdb

# -------------------------------
# UI CONFIG
# -------------------------------
st.set_page_config(page_title="Sales Dashboard", layout="wide")
st.title("📊 Sales Dashboard")

# -------------------------------
# DATABASE CONNECTION
# -------------------------------
def get_connection():
    conn_str = (
        "DRIVER={SQL Server};"
        "SERVER=103.86.55.183,2001;"
        "DATABASE=Candelahns;"
        "UID=ReadOnlyUser;"
        "PWD=902729@Rafy"
    )
    return pyodbc.connect(conn_str, autocommit=False)

conn = get_connection()
conn_kds = get_connection_kdsdb()

# -------------------------------
# FRESH PICK DATABASE CONNECTION
# -------------------------------
def get_connection_fresh_pick():
    conn_str = (
        "DRIVER={SQL Server};"
        "SERVER=103.86.55.183,2002;"
        "DATABASE=CandelaFP;"
        "UID=ReadOnlyUser;"
        "PWD=902729@Rafy"
    )
    return pyodbc.connect(conn_str, autocommit=False)

conn_fresh = get_connection_fresh_pick()

# -------------------------------
# SIDEBAR FILTERS
# -------------------------------
st.sidebar.header("🔍 Filters")

# Date filter
start_date = st.sidebar.date_input("Start Date", date(2026, 1, 1))
end_date = st.sidebar.date_input("End Date", date(2026, 1, 31))
if start_date > end_date:
    st.error("Start Date cannot be after End Date")
    st.stop()
start_date_str = start_date.strftime("%Y-%m-%d")
end_date_str = end_date.strftime("%Y-%m-%d")

# Branch selection fixed to 7 branches
selected_branch_ids = [2, 3, 4, 6, 8, 10, 14]

# Filter Mode: Filter blocked names/comments
data_mode = st.sidebar.radio("Mode", ["Filtered", "Unfiltered"])

# -------------------------------
# BLOCKED CUSTOMERS / COMMENTS
# -------------------------------
blocked_names = [
    "Wali Jaan Personal Orders", "Raza Khan M.D", "Customer Discount 100%",
    "Daraksha Mobile 100%", "DHA Police Discount 100%",
    "HNS Product Marketing 100%", "Home Food Order (Madam)", "Home Food Orders",
    "Home Food Orders (Raza Khan)", "Home Food Orders (Shehryar Khan)",
    "Home Food Orders (Umair Sb)", "Rangers mobile 100%",
    "Return N Cancellation (Aftert Preperation)",
    "Return N Cancellation (without preperation)"
]

blocked_comments = [
    "Wali Jaan Personal Orders", "100% Wali bhai",
    "Return N Cancellation (Aftert Preperation)",
    "Return N Cancellation (without preperation)",
    "100% Discount Wali Bhai Personal Order",
    "Customer Order Change Then Return", "marketing order in day",
    "HNS Product Marketing 100%", "Mistake"
]

# -------------------------------
# ORDER TYPES FOR ANALYSIS
# -------------------------------
order_types = {
    'Food Panda': 'Food Panda',
    'Takeaway': 'Takeaway',
    'Web Online Paid Order': 'Web Online Paid Order',
    'Dine IN': 'Dine IN',
    'Credit Card South': 'Credit Card South',
    'HNS Credit Card': 'HNS Credit Card',
    'Delivery': 'Delivery',
    'Cash Web Online Order': 'Cash Web Online Order',
    'Others': None  # For all other customer names
}

# -------------------------------
# FRESH PICK PRODUCTS
# -------------------------------
fresh_pick_products = [
    'Chicken Breast Boneless',
    'Chicken Broast',
    'Chicken Gut & Liver',
    'Chicken Karahi Cut',
    'Chicken Leg Boneless',
    'Chicken Liver',
    'Chicken Neck & Rib Cage',
    'Chicken Tikka Cut',
    'Chicken Wings',
    'Whole Chicken',
    'Whole Chicken Skin-on'
]

# -------------------------------
# BRANCH TARGETS
# -------------------------------
branch_targets = {
    2: 34100000,  # Khadda Market Outlet
    4: 22475000,  # Rahat Commercial Outlet
    8: 18600000,  # North Nazimabad Outlet
    10: 13175000,  # Jinnah Avenue (Malir ) Outlet
    6: 18600000,  # ii chundrigar Outlet
    3: 2000000     # Festival
}

# -------------------------------
# HELPER FUNCTIONS
# -------------------------------
def placeholders(n: int) -> str:
    return ", ".join(["?"] * n) if n > 0 else ""

def build_ot_report(df: pd.DataFrame, df_targets: pd.DataFrame) -> pd.DataFrame:
    """Nested OT / Employee report: Branch -> Employee -> Products with totals."""
    rows = []
    for (shop_id, shop_name), df_shop in df.groupby(['shop_id','shop_name']):
        rows.append({'Branch': f"{shop_name} (ID:{shop_id})", 'Employee': '', 'Product':'', 'Qty':'','Allocated_Nt':'', 'Target': ''})
        for (emp_id, emp_name), df_emp in df_shop.groupby(['employee_id','employee_name']):
            target = df_targets[(df_targets['employee_id'] == emp_id) & (df_targets['shop_id'] == shop_id)]['target_amount']
            target_val = target.iloc[0] if not target.empty else 0
            rows.append({'Branch':'', 'Employee':f"{emp_name} (OT:{emp_id})", 'Product':'', 'Qty':'','Allocated_Nt':'', 'Target': str(target_val)})
            for _, r in df_emp.iterrows():
                rows.append({
                    'Branch':'',
                    'Employee':'',
                    'Product': r['product'] if 'product' in r else '',
                    'Qty': str(int(r['total_qty'])) if 'total_qty' in r else '',
                    'Allocated_Nt': str(float(r['total_sale'])) if 'total_sale' in r else '',
                    'Target': ''
                })
            total_allocated = df_emp['total_sale'].sum()
            rows.append({'Branch':'','Employee':'','Product':'TOTAL','Qty':'','Allocated_Nt':str(total_allocated), 'Target': ''})
            rows.append({'Branch':'','Employee':'','Product':'','Qty':'','Allocated_Nt':'', 'Target': ''})
        rows.append({'Branch':'','Employee':'','Product':'','Qty':'','Allocated_Nt':'', 'Target': ''})
    return pd.DataFrame(rows)

# -------------------------------
# SQL QUERIES
# -------------------------------
# 1️⃣ Branch Summary
branch_query = f"""
SELECT sh.shop_id, sh.shop_name,
       COUNT(DISTINCT s.sale_id) AS total_sales,
       COALESCE(SUM(s.Nt_amount), 0) AS total_Nt_amount
FROM tblDefShops sh
LEFT JOIN tblSales s ON sh.shop_id = s.shop_id AND s.sale_date BETWEEN ? AND ?
WHERE sh.shop_id IN ({placeholders(len(selected_branch_ids))})
"""
branch_params = [start_date_str, end_date_str] + selected_branch_ids

if data_mode == "Filtered":
    if blocked_names:
        branch_query += f" AND s.Cust_name NOT IN ({placeholders(len(blocked_names))})"
        branch_params.extend(blocked_names)
    if blocked_comments:
        branch_query += f" AND (s.Additional_Comments NOT IN ({placeholders(len(blocked_comments))}) OR s.Additional_Comments IS NULL)"
        branch_params.extend(blocked_comments)

branch_query += " GROUP BY sh.shop_id, sh.shop_name ORDER BY sh.shop_id"

# Global sales for all tabs
df_sales_query = f"""
SELECT shop_id, SUM(Nt_amount) AS total_sales
FROM tblSales
WHERE sale_date BETWEEN ? AND ?
AND shop_id IN ({placeholders(len(selected_branch_ids))})
"""
df_sales_params = [start_date_str, end_date_str] + selected_branch_ids

if data_mode == "Filtered":
    if blocked_names:
        df_sales_query += f" AND Cust_name NOT IN ({placeholders(len(blocked_names))})"
        df_sales_params.extend(blocked_names)
    if blocked_comments:
        df_sales_query += f" AND (Additional_Comments NOT IN ({placeholders(len(blocked_comments))}) OR Additional_Comments IS NULL)"
        df_sales_params.extend(blocked_comments)

df_sales_query += " GROUP BY shop_id"

# 2️⃣ OT / Employee Report
ot_query = f"""
SELECT s.shop_id, sh.shop_name,
       COALESCE(e.shop_employee_id,0) AS employee_id,
       COALESCE(e.field_name,'Online/Unassigned') AS employee_name,
       SUM(s.Nt_amount) AS total_sale
FROM tblSales s
LEFT JOIN tblDefShopEmployees e ON s.employee_id=e.shop_employee_id
LEFT JOIN tblDefShops sh ON s.shop_id=sh.shop_id
WHERE s.sale_date BETWEEN ? AND ?
AND s.shop_id IN ({placeholders(len(selected_branch_ids))})
"""
ot_params = [start_date_str, end_date_str] + selected_branch_ids
if data_mode == "Filtered":
    if blocked_names:
        ot_query += f" AND s.Cust_name NOT IN ({placeholders(len(blocked_names))})"
        ot_params.extend(blocked_names)
    if blocked_comments:
        ot_query += f" AND (s.Additional_Comments NOT IN ({placeholders(len(blocked_comments))}) OR s.Additional_Comments IS NULL)"
        ot_params.extend(blocked_comments)
ot_query += " GROUP BY s.shop_id, sh.shop_name, COALESCE(e.shop_employee_id,0), COALESCE(e.field_name,'Online/Unassigned') ORDER BY s.shop_id, employee_name"

# 3️⃣ Line Item / Chef Report
line_item_query = f"""
SELECT s.shop_id, sh.shop_name, t.field_name AS product,
       SUM(li.qty) AS total_qty,
       SUM((li.qty*li.Unit_price)/NULLIF(st.line_total,0)*s.Nt_amount) AS total_line_value_incl_tax
FROM tblSales s
JOIN tblSalesLineItems li ON s.sale_id=li.sale_id
JOIN TempProductBarcode t ON li.Product_Item_ID=t.Product_Item_ID AND li.Product_code=t.Product_code
LEFT JOIN tblDefShops sh ON s.shop_id=sh.shop_id
JOIN (
    SELECT sale_id, SUM(qty*Unit_price) AS line_total
    FROM tblSalesLineItems
    GROUP BY sale_id
) st ON st.sale_id=s.sale_id
WHERE s.sale_date BETWEEN ? AND ?
AND s.shop_id IN ({placeholders(len(selected_branch_ids))})
"""
line_item_params = [start_date_str, end_date_str] + selected_branch_ids
if data_mode == "Filtered":
    if blocked_names:
        line_item_query += f" AND s.Cust_name NOT IN ({placeholders(len(blocked_names))})"
        line_item_params.extend(blocked_names)
    if blocked_comments:
        line_item_query += f" AND (s.Additional_Comments NOT IN ({placeholders(len(blocked_comments))}) OR s.Additional_Comments IS NULL)"
        line_item_params.extend(blocked_comments)
line_item_query += " GROUP BY s.shop_id, sh.shop_name, t.field_name ORDER BY s.shop_id, t.field_name"

# 5️⃣ Order Type Analysis
order_type_query = f"""
SELECT
    CASE
        WHEN s.Cust_name = 'Food Panda' THEN 'Food Panda'
        WHEN s.Cust_name = 'Takeaway' THEN 'Takeaway'
        WHEN s.Cust_name = 'Web Online Paid Order' THEN 'Web Online Paid Order'
        WHEN s.Cust_name = 'Cash Web Online Order' THEN 'Cash Web Online Order'
        WHEN s.Cust_name = 'Dine IN' THEN 'Dine IN'
        WHEN s.Cust_name = 'Credit Card South' THEN 'Credit Card South'
        WHEN s.Cust_name = 'HNS Credit Card' THEN 'HNS Credit Card'
        WHEN s.Cust_name = 'Delivery' THEN 'Delivery'
        ELSE 'Others'
    END AS order_type,
    COUNT(DISTINCT s.sale_id) AS total_orders,
    SUM(s.Nt_amount) AS total_sales
FROM tblSales s
WHERE s.sale_date BETWEEN ? AND ?
AND s.shop_id IN ({placeholders(len(selected_branch_ids))})
"""
order_type_params = [start_date_str, end_date_str] + selected_branch_ids

if data_mode == "Filtered":
    if blocked_names:
        order_type_query += f" AND s.Cust_name NOT IN ({placeholders(len(blocked_names))})"
        order_type_params.extend(blocked_names)
    if blocked_comments:
        order_type_query += f" AND (s.Additional_Comments NOT IN ({placeholders(len(blocked_comments))}) OR s.Additional_Comments IS NULL)"
        order_type_params.extend(blocked_comments)

order_type_query += " GROUP BY CASE WHEN s.Cust_name = 'Food Panda' THEN 'Food Panda' WHEN s.Cust_name = 'Takeaway' THEN 'Takeaway' WHEN s.Cust_name = 'Web Online Paid Order' THEN 'Web Online Paid Order' WHEN s.Cust_name = 'Cash Web Online Order' THEN 'Cash Web Online Order' WHEN s.Cust_name = 'Dine IN' THEN 'Dine IN' WHEN s.Cust_name = 'Credit Card South' THEN 'Credit Card South' WHEN s.Cust_name = 'HNS Credit Card' THEN 'HNS Credit Card' WHEN s.Cust_name = 'Delivery' THEN 'Delivery' ELSE 'Others' END ORDER BY total_sales DESC"

# 6️⃣ Fresh Pick Sales Data
fresh_pick_sales_query = f"""
SELECT s.Cust_name AS Customer,
       p.item_name AS Product,
       SUM(li.qty) AS TotalQuantitySold,
       SUM(li.qty * li.Unit_price) AS TotalRevenue,
       SUM(DISTINCT s.NT_amount) AS TotalSaleAmount,
       COUNT(DISTINCT s.sale_id) AS NumberOfSales
FROM tblSales s
INNER JOIN tblSalesLineItems li ON s.sale_id = li.sale_id AND s.shop_id = li.shop_id
INNER JOIN tblProductItem pi ON li.Product_Item_ID = pi.Product_Item_ID
INNER JOIN tblDefProducts p ON pi.Product_ID = p.product_id
WHERE p.item_name IN ({placeholders(len(fresh_pick_products))})
AND s.sale_date BETWEEN ? AND ?
AND s.Cust_name IS NOT NULL
"""

# 7️⃣ QR Sales Data (Blinkco orders)
qr_sales_query = f"""
SELECT
    s.shop_id,
    sh.shop_name AS shop_name,
    COALESCE(e.shop_employee_id, 0) AS employee_id,
    COALESCE(e.field_name, 'Online/Unassigned') AS employee_name,
    SUM(s.NT_amount) AS total_sale,
    s.external_ref_id,
    s.external_ref_type
FROM
    tblSales s
LEFT JOIN
    tblDefShopEmployees e ON s.employee_id = e.shop_employee_id
LEFT JOIN
    tblDefShops sh ON s.shop_id = sh.shop_id
WHERE
    s.sale_date BETWEEN ? AND ?
    AND s.shop_id IN ({placeholders(len(selected_branch_ids))})
    AND s.external_ref_type = 'Blinkco order'
"""

qr_sales_params = [start_date_str, end_date_str] + selected_branch_ids

if data_mode == "Filtered":
    if blocked_names:
        qr_sales_query += f" AND s.Cust_name NOT IN ({placeholders(len(blocked_names))})"
        qr_sales_params.extend(blocked_names)
    if blocked_comments:
        qr_sales_query += f" AND (s.Additional_Comments NOT IN ({placeholders(len(blocked_comments))}) OR s.Additional_Comments IS NULL)"
        qr_sales_params.extend(blocked_comments)

qr_sales_query += " GROUP BY s.shop_id, sh.shop_name, COALESCE(e.shop_employee_id, 0), COALESCE(e.field_name, 'Online/Unassigned'), s.external_ref_id, s.external_ref_type ORDER BY total_sale DESC"

# 8️⃣ Employee-wise QR and Normal Sales Data
employee_sales_query = f"""
SELECT
    COALESCE(e.shop_employee_id, 0) AS employee_id,
    COALESCE(e.field_name, 'Online/Unassigned') AS employee_name,
    COUNT(DISTINCT s.sale_id) AS total_transactions,
    SUM(s.NT_amount) AS total_sales,
    SUM(CASE WHEN s.external_ref_type = 'Blinkco order' THEN s.NT_amount ELSE 0 END) AS qr_sales,
    SUM(CASE WHEN s.external_ref_type != 'Blinkco order' OR s.external_ref_type IS NULL THEN s.NT_amount ELSE 0 END) AS normal_sales,
    COUNT(CASE WHEN s.external_ref_type = 'Blinkco order' THEN 1 END) AS qr_transactions,
    COUNT(CASE WHEN s.external_ref_type != 'Blinkco order' OR s.external_ref_type IS NULL THEN 1 END) AS normal_transactions,
    CASE
        WHEN SUM(s.NT_amount) > 0
        THEN (SUM(CASE WHEN s.external_ref_type = 'Blinkco order' THEN s.NT_amount ELSE 0 END) * 100.0 / SUM(s.NT_amount))
        ELSE 0
    END AS qr_percentage
FROM
    tblSales s
LEFT JOIN
    tblDefShopEmployees e ON s.employee_id = e.shop_employee_id
LEFT JOIN
    tblDefShops sh ON s.shop_id = sh.shop_id
WHERE
    s.sale_date BETWEEN ? AND ?
    AND s.shop_id IN ({placeholders(len(selected_branch_ids))})
GROUP BY
    COALESCE(e.shop_employee_id, 0), COALESCE(e.field_name, 'Online/Unassigned')
ORDER BY
    total_sales DESC
"""
employee_sales_params = [start_date_str, end_date_str] + selected_branch_ids

fresh_pick_sales_params = fresh_pick_products + [start_date_str, end_date_str]

if data_mode == "Filtered":
    if blocked_names:
        fresh_pick_sales_query += f" AND s.Cust_name NOT IN ({placeholders(len(blocked_names))})"
        fresh_pick_sales_params.extend(blocked_names)
    if blocked_comments:
        fresh_pick_sales_query += f" AND (s.Additional_Comments NOT IN ({placeholders(len(blocked_comments))}) OR s.Additional_Comments IS NULL)"
        fresh_pick_sales_params.extend(blocked_comments)

fresh_pick_sales_query += " GROUP BY s.Cust_name, p.item_name ORDER BY Customer, TotalQuantitySold DESC"

# -------------------------------
# FETCH DATA
# -------------------------------
try:
    # Branch Summary
    cursor1 = conn.cursor()
    cursor1.execute(branch_query, branch_params)
    df_branch = pd.DataFrame.from_records(cursor1.fetchall(), columns=[desc[0] for desc in cursor1.description])
    cursor1.close()

    # OT / Employee
    cursor2 = conn.cursor()
    cursor2.execute(ot_query, ot_params)
    df_ot = pd.DataFrame.from_records(cursor2.fetchall(), columns=[desc[0] for desc in cursor2.description])
    df_ot['total_sale'] = df_ot['total_sale'].apply(lambda x: round(float(x) if x is not None else 0.0))
    cursor2.close()

    # Line Item / Chef
    cursor3 = conn.cursor()
    cursor3.execute(line_item_query, line_item_params)
    df_line_item = pd.DataFrame.from_records(cursor3.fetchall(), columns=[desc[0] for desc in cursor3.description])
    df_line_item['total_line_value_incl_tax'] = df_line_item['total_line_value_incl_tax'].apply(lambda x: float(x) if x is not None else 0.0)
    cursor3.close()

    # Order Types
    cursor5 = conn.cursor()
    cursor5.execute(order_type_query, order_type_params)
    df_order_types = pd.DataFrame.from_records(cursor5.fetchall(), columns=[desc[0] for desc in cursor5.description])
    cursor5.close()

    # Fresh Pick Sales Data
    try:
        cursor6 = conn_fresh.cursor()
        cursor6.execute(fresh_pick_sales_query, fresh_pick_sales_params)
        df_fresh_pick_sales = pd.DataFrame.from_records(cursor6.fetchall(), columns=[desc[0] for desc in cursor6.description])
        cursor6.close()
    except Exception as e:
        st.error(f"Error fetching Fresh Pick sales data: {e}")
        df_fresh_pick_sales = pd.DataFrame()

    # QR Sales Data
    try:
        cursor7 = conn.cursor()
        cursor7.execute(qr_sales_query, qr_sales_params)
        df_qr_sales_data = pd.DataFrame.from_records(cursor7.fetchall(), columns=[desc[0] for desc in cursor7.description])
        df_qr_sales_data['total_sale'] = df_qr_sales_data['total_sale'].apply(lambda x: round(float(x) if x is not None else 0.0))
        cursor7.close()
    except Exception as e:
        st.error(f"Error fetching QR sales data: {e}")
        df_qr_sales_data = pd.DataFrame()

    # Employee-wise Sales Data
    try:
        cursor8 = conn.cursor()
        cursor8.execute(employee_sales_query, employee_sales_params)
        df_employee_sales = pd.DataFrame.from_records(cursor8.fetchall(), columns=[desc[0] for desc in cursor8.description])
        cursor8.close()
    except Exception as e:
        st.error(f"Error fetching employee sales data: {e}")
        df_employee_sales = pd.DataFrame()

finally:
    pass

# -------------------------------
# FETCH TARGETS FROM KDS_DB
# -------------------------------
# Chef targets
df_chef_targets = pd.read_sql("SELECT shop_id, category_id, monthly_target as target_amount, target_type FROM dbo.branch_chef_targets", conn_kds)
categories = pd.read_sql("SELECT * FROM dbo.chef_sale", conn_kds)

# OT targets
df_ot_targets = pd.read_sql("SELECT shop_id, employee_id, monthly_target as target_amount FROM dbo.ot_targets", conn_kds)

# Fresh Pick targets
df_fresh_targets = pd.read_sql("SELECT customer_name, product_name, target_qty as target_amount FROM dbo.fresh_pick_targets", conn_kds)
# Fetch employee names from Candelahns
conn_candelahns_temp = pyodbc.connect("DRIVER={SQL Server};SERVER=103.86.55.183,2001;DATABASE=Candelahns;UID=ReadOnlyUser;PWD=902729@Rafy")
df_ot_employees = pd.read_sql("SELECT shop_employee_id, field_name FROM dbo.tblDefShopEmployees", conn_candelahns_temp)
df_ot_employees.rename(columns={'shop_employee_id': 'employee_id', 'field_name': 'employee_name'}, inplace=True)
conn_candelahns_temp.close()

# -------------------------------
# Add Targets & Remaining
# -------------------------------
df_branch['Monthly_Target'] = df_branch['shop_id'].map(branch_targets).fillna(0).astype(float)
df_branch['total_Nt_amount'] = df_branch['total_Nt_amount'].astype(float)
df_branch['Remaining_Target'] = df_branch['Monthly_Target'] - df_branch['total_Nt_amount']

# Nested OT report
df_ot_report = build_ot_report(df_ot, df_ot_targets)

# -------------------------------
# STREAMLIT TABS
# -------------------------------
tab1, tab2, tab3, tab4, tab5, tab6, tab_qr = st.tabs(["Main Sale", "Order Taker Sale", "Chef Sale", "Chef Targets", "OT Targets", "Fresh Pick Target", "QR Commission"])

# --- Branch Summary ---
with tab1:
    st.subheader("🏪 Branch Performance Overview")
    if df_branch.empty:
        st.info("No sales data for selected filters.")
    else:
        # Calculate total
        total_current = df_branch['total_Nt_amount'].sum()
        total_target = df_branch['Monthly_Target'].sum()
        total_remaining = total_target - total_current
        total_achieved_pct = (total_current / total_target * 100) if total_target > 0 else 0

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Sales", f"PKR {total_current:,.0f}")
        col2.metric("Total Target", f"PKR {total_target:,.0f}")
        col3.metric("Remaining", f"PKR {total_remaining:,.0f}")
        col4.metric("Achieved", f"{total_achieved_pct:,.1f}%")

        # Individual branch cards
        for i, row in df_branch.iterrows():
            with st.expander(f"{row['shop_name']} (ID: {row['shop_id']})"):
                achievd_pct = (row['total_Nt_amount'] / row['Monthly_Target'] * 100) if row['Monthly_Target'] > 0 else 0
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Current Sales", f"PKR {row['total_Nt_amount']:,.0f}")
                col2.metric("Monthly Target", f"PKR {row['Monthly_Target']:,.0f}")
                col3.metric("Remaining", f"PKR {row['Remaining_Target']:,.0f}")
                col4.metric("Achieved", f"{achievd_pct:,.1f}%")

        # Download
        st.download_button(
            "Download Branch CSV",
            df_branch.to_csv(index=False).encode("utf-8"),
            "branch_report.csv",
            "text/csv"
        )

        # Chart: Achievement %
        df_br_perf = df_branch.copy()
        df_br_perf['Achieved_%'] = df_br_perf.apply(lambda r: (r['total_Nt_amount'] / r['Monthly_Target'] * 100) if r['Monthly_Target'] > 0 else 0, axis=1)
        chart = alt.Chart(df_br_perf).mark_bar().encode(
            y=alt.Y('shop_name:N', sort='-x'),
            x='Achieved_%:Q',
            color=alt.value("#1f77b4")
        ).properties(width=600, height=400, title="Branch Achievement %")
        st.altair_chart(chart)

        # Order Type Analysis Section
        st.markdown("---")
        st.subheader("📊 Order Type Analysis")

        if not df_order_types.empty:
            # Ensure numeric types for calculations
            df_order_types['total_orders'] = pd.to_numeric(df_order_types['total_orders'], errors='coerce').fillna(0)
            df_order_types['total_sales'] = pd.to_numeric(df_order_types['total_sales'], errors='coerce').fillna(0)

            # Calculate percentages
            total_orders_all = df_order_types['total_orders'].sum()
            total_sales_all = df_order_types['total_sales'].sum()

            df_order_types_display = df_order_types.copy()
            df_order_types_display['orders_percentage'] = (df_order_types_display['total_orders'] / total_orders_all * 100).round(1)
            df_order_types_display['sales_percentage'] = (df_order_types_display['total_sales'] / total_sales_all * 100).round(1)

            # Format numbers
            df_order_types_display['total_orders'] = df_order_types_display['total_orders'].apply(lambda x: f"{x:,}")
            df_order_types_display['total_sales'] = df_order_types_display['total_sales'].apply(lambda x: f"PKR {x:,.0f}")

            # Display table
            st.dataframe(df_order_types_display[['order_type', 'total_orders', 'orders_percentage', 'total_sales', 'sales_percentage']],
                        column_config={
                            "order_type": "Order Type",
                            "total_orders": "Orders",
                            "orders_percentage": "Orders %",
                            "total_sales": "Sales Amount",
                            "sales_percentage": "Sales %"
                        }, width='stretch')

            # Pie Chart for Sales Distribution
            pie_chart = alt.Chart(df_order_types).mark_arc(innerRadius=50).encode(
                theta=alt.Theta('total_sales:Q'),
                color=alt.Color('order_type:N', legend=alt.Legend(title="Order Types")),
                tooltip=['order_type:N', 'total_sales:Q', 'total_orders:Q']
            ).properties(
                width=400,
                height=400,
                title="Sales Distribution by Order Type"
            )
            st.altair_chart(pie_chart)

            # Summary metrics for key order types
            col1, col2, col3 = st.columns(3)

            # Food Panda stats
            fp_data = df_order_types[df_order_types['order_type'] == 'Food Panda']
            if not fp_data.empty:
                fp_sales = fp_data['total_sales'].iloc[0]
                fp_orders = fp_data['total_orders'].iloc[0]
                col1.metric("🍕 Food Panda", f"PKR {fp_sales:,.0f}", f"{fp_orders} orders")

            # Delivery stats
            del_data = df_order_types[df_order_types['order_type'] == 'Delivery']
            if not del_data.empty:
                del_sales = del_data['total_sales'].iloc[0]
                del_orders = del_data['total_orders'].iloc[0]
                col2.metric("🚚 Delivery", f"PKR {del_sales:,.0f}", f"{del_orders} orders")

            # Dine In stats
            dine_data = df_order_types[df_order_types['order_type'] == 'Dine IN']
            if not dine_data.empty:
                dine_sales = dine_data['total_sales'].iloc[0]
                dine_orders = dine_data['total_orders'].iloc[0]
                col3.metric("🍽️ Dine In", f"PKR {dine_sales:,.0f}", f"{dine_orders} orders")

        else:
            st.info("No order type data available for selected filters.")

# --- OT / Employee Report ---
with tab2:
    st.subheader("👨‍💼 OT / Employee Report")

    if df_ot.empty:
        st.info("No OT / Employee data for selected filters.")
    else:
        st.dataframe(df_ot, width='stretch')
        st.download_button("Download OT/Employee CSV", df_ot.to_csv(index=False).encode("utf-8"), "ot_employee_report.csv", "text/csv")

# --- Line Item / Chef Report ---
with tab3:
    st.subheader("📦 Line Item Sum Branch Wise")
    if df_line_item.empty:
        st.info("No Line Item data for selected filters.")
    else:
        st.dataframe(df_line_item, width='stretch')
        st.download_button("Download Line Item CSV", df_line_item.to_csv(index=False).encode("utf-8"), "line_item_sum.csv", "text/csv")

        # Graph: Top 10 products by total_line_value_incl_tax
        df_top_prod = df_line_item.groupby('product')['total_line_value_incl_tax'].sum().sort_values(ascending=False).head(10).reset_index()
        chart = alt.Chart(df_top_prod).mark_bar().encode(
            y=alt.Y('product:N', sort='-x'),
            x='total_line_value_incl_tax:Q',
            tooltip=['product','total_line_value_incl_tax']
        ).properties(width=700, height=400)
        st.altair_chart(chart)

# --- Chef Targets ---
with tab4:
    st.subheader("Product Sales vs Targets")

    # Branch selection for filtering
    branch_options = df_branch['shop_name'].tolist() if not df_branch.empty else []
    if branch_options:
        selected_branch_chef = st.selectbox("Select Branch", branch_options, key="chef_branch_select")
        branch_match = df_branch.loc[df_branch['shop_name'] == selected_branch_chef, 'shop_id']
        selected_shop_id_chef = branch_match.values[0] if not branch_match.empty else None
    else:
        st.error("No branch data available. Please check your filters.")
        selected_shop_id_chef = None

    # Get targets for this branch
    df_chef_filtered = df_chef_targets[df_chef_targets['shop_id'] == selected_shop_id_chef]

    if df_chef_filtered.empty:
        st.info("No target data for selected branch.")
    else:
        # Merge targets with categories
        df_targets_with_categories = df_chef_filtered.merge(categories, on='category_id', how='left')

        # Filter to only show specified categories
        sale_categories = [
            "SALES - BAR B Q",
            "SALES - CHINESE",
            "SALES - FAST FOOD",
            "SALES - HANDI",
            "SALES - JUICES SHAKES & DESSERTS",
            "SALES - KARAHI",
            "SALES - TANDOOR",
            "SALES - ROLL",
            "SALES - NASHTA"
        ]
        qty_categories = [
            "SALES - BEVERAGES",
            "SALES - SIDE ORDER"
        ]
        allowed_categories = sale_categories + qty_categories
        df_targets_with_categories = df_targets_with_categories[df_targets_with_categories['category_name'].isin(allowed_categories)]

        # Set correct target_type based on category
        def get_target_type(cat):
            if cat in sale_categories:
                return 'Sale'
            elif cat in qty_categories:
                return 'Quantity'
            else:
                return 'Sale'
        df_targets_with_categories['target_type'] = df_targets_with_categories['category_name'].apply(get_target_type)

        # Get sales data for this branch
        df_sales_this_branch = df_line_item[df_line_item['shop_id'] == selected_shop_id_chef].copy()

        # Filter out unwanted products
        products_to_hide = ['Sales - Employee Food', 'Deals', 'Modifiers']
        df_sales_this_branch = df_sales_this_branch[~df_sales_this_branch['product'].isin(products_to_hide)]

        # Clean names for merging
        def clean_name(name):
            # Convert to uppercase first
            name = name.upper()
            # Remove prefixes (both "SALES -" and "SALES")
            name = name.replace("SALES -", "").replace("SALES", "").strip()
            # Handle special cases
            name = name.replace("SIDE ORDERS", "SIDE ORDER")
            # Convert remaining dashes to spaces
            name = name.replace("-", " ")
            # Split into words, singularize, rejoin
            words = name.split()
            cleaned_words = []
            for word in words:
                # Remove trailing 's' for plurals
                if word in ['ROLLS', 'SIDES', 'ORDERS']:
                    if word == 'ROLLS':
                        word = 'ROLL'
                    elif word == 'ORDERS':
                        word = 'ORDER'
                    elif word == 'SIDES':
                        word = 'SIDE'
                cleaned_words.append(word)
            return ' '.join(cleaned_words)

        df_sales_this_branch['product_clean'] = df_sales_this_branch['product'].apply(clean_name)
        df_targets_with_categories['category_clean'] = df_targets_with_categories['category_name'].apply(clean_name)

        # Left join targets with sales data
        df_products_with_targets = df_targets_with_categories.merge(
            df_sales_this_branch, left_on='category_clean', right_on='product_clean', how='left'
        ).fillna({'total_line_value_incl_tax': 0, 'total_qty': 0})

        # Set product name: use sales product if available, else category name
        df_products_with_targets['product'] = df_products_with_targets['product'].fillna(df_products_with_targets['category_name'])

        # Format table - use appropriate current value based on target_type
        product_table = df_products_with_targets[['product', 'target_amount', 'target_type']].copy()

        # For Sale targets, use sales amount; for quantity targets, use quantity
        product_table['Current'] = df_products_with_targets.apply(
            lambda row: row['total_line_value_incl_tax'] if row['target_type'] == 'Sale' else row['total_qty'], axis=1
        )

        product_table.columns = ['Product', 'Target', 'Type', 'Current Sale']
        # Ensure numeric types for calculations
        product_table['Target'] = product_table['Target'].astype(float)
        product_table['Current Sale'] = product_table['Current Sale'].astype(float)
        product_table['Remaining'] = product_table['Target'] - product_table['Current Sale']
        product_table['Achievement %'] = (product_table['Current Sale'] / product_table['Target'] * 100).fillna(0)

        # Calculate bonus for achieved targets (Achievement >= 100%)
        product_table['Bonus'] = product_table.apply(
            lambda row: row['Current Sale'] * 0.5 if row['Achievement %'] >= 100 else 0, axis=1
        )

        # Format numbers (handle NaN values)
        product_table['Target'] = product_table['Target'].fillna(0).apply(lambda x: f"{int(x):,}")
        product_table['Current Sale'] = product_table['Current Sale'].fillna(0).apply(lambda x: f"{int(x):,}")
        product_table['Remaining'] = product_table['Remaining'].fillna(0).apply(lambda x: f"{int(x):,}")
        product_table['Achievement %'] = product_table['Achievement %'].fillna(0).apply(lambda x: f"{x:.1f}%")

        # For quantity targets, append "qty" to Target
        product_table['Target'] = product_table.apply(lambda row: f"{row['Target']} qty" if row['Type'] == 'Quantity' else row['Target'], axis=1)

        st.dataframe(product_table[['Product', 'Target', 'Current Sale', 'Remaining', 'Achievement %', 'Bonus']], width='stretch')

# --- OT Targets ---
with tab5:
    st.subheader("👨 OT Targets Performance")

    # Branch selection for filtering
    branch_options = df_branch['shop_name'].tolist() if not df_branch.empty else []
    if branch_options:
        selected_branch_ot = st.selectbox("Select Branch", branch_options, key="ot_branch_select")
        branch_match = df_branch.loc[df_branch['shop_name'] == selected_branch_ot, 'shop_id']
        selected_shop_id_ot = branch_match.values[0] if not branch_match.empty else None
    else:
        st.error("No branch data available. Please check your filters.")
        selected_shop_id_ot = None

    # Option to hide rows with 0 sales
    hide_zero_sales = st.checkbox("Hide employees with 0 current sales", value=False)
    hide_zero_target = st.checkbox("Hide employees with 0 target", value=False)

    # Get sales data for the selected branch (all OTs who made sales)
    df_ot_sales_branch = df_ot[df_ot['shop_id'] == selected_shop_id_ot] if selected_shop_id_ot else pd.DataFrame()

    if df_ot_sales_branch.empty:
        st.info("No OT sales data for selected branch.")
    else:
        # Get targets for this branch
        df_ot_targets_branch = df_ot_targets[df_ot_targets['shop_id'] == selected_shop_id_ot]

        # Left join sales data with targets (show all OTs with sales, targets if available)
        df_ot_perf = df_ot_sales_branch.merge(
            df_ot_targets_branch, on=['shop_id', 'employee_id'], how='left'
        ).fillna({'target_amount': 0})

        # For employees without names, use 'ID: employee_id'
        if 'employee_name' in df_ot_perf.columns:
            df_ot_perf['employee_name'] = df_ot_perf['employee_name'].fillna(df_ot_perf['employee_id'].astype(str).apply(lambda x: f'ID: {x}'))
        else:
            df_ot_perf['employee_name'] = df_ot_perf['employee_id'].astype(str).apply(lambda x: f'ID: {x}')

        # Filter out zero sales if option is selected
        if hide_zero_sales:
            df_ot_perf = df_ot_perf[df_ot_perf['total_sale'] > 0]

        # Filter out zero target if option is selected
        if hide_zero_target:
            df_ot_perf = df_ot_perf[df_ot_perf['target_amount'] > 0]

        # Format table
        ot_table = df_ot_perf[['employee_name', 'target_amount', 'total_sale']].copy()
        ot_table.columns = ['OT Name', 'Target', 'Current Sale']
        ot_table['Remaining'] = ot_table['Target'] - ot_table['Current Sale']
        ot_table['Achievement %'] = (ot_table['Current Sale'] / ot_table['Target'] * 100).fillna(0)

        # Format numbers (handle NaN values)
        ot_table['Target'] = ot_table['Target'].fillna(0).apply(lambda x: f"{int(x):,}")
        ot_table['Current Sale'] = ot_table['Current Sale'].fillna(0).apply(lambda x: f"{int(round(x)):,}")
        ot_table['Remaining'] = ot_table['Remaining'].fillna(0).apply(lambda x: f"{int(round(x)):,}")
        ot_table['Achievement %'] = ot_table['Achievement %'].fillna(0).apply(lambda x: f"{x:.1f}%")

        st.dataframe(ot_table[['OT Name', 'Target', 'Current Sale', 'Remaining', 'Achievement %']], width='stretch')

# --- Fresh Pick Target ---
with tab6:
    st.subheader("🥩 Fresh Pick Targets")

    # Customer selection
    customer_options = df_fresh_targets['customer_name'].unique().tolist() if not df_fresh_targets.empty else []
    if customer_options:
        selected_customer = st.selectbox("Select Customer", customer_options, key="fresh_customer_select")

        # Get targets for this customer
        df_targets_customer = df_fresh_targets[df_fresh_targets['customer_name'] == selected_customer]

        if df_targets_customer.empty:
            st.info("No Fresh Pick targets found for selected customer.")
        else:
            # Create normalized versions for better matching
            df_targets_normalized = df_targets_customer.copy()
            df_sales_normalized = df_fresh_pick_sales.copy()

            # Normalize customer names: remove 'Qty' suffix and make uppercase
            df_targets_normalized['customer_match'] = df_targets_normalized['customer_name'].str.replace(' Qty', '', regex=False).str.upper().str.strip()
            df_sales_normalized['customer_match'] = df_sales_normalized['Customer'].str.upper().str.strip()

            # Normalize product names: uppercase and strip
            df_targets_normalized['product_match'] = df_targets_normalized['product_name'].str.upper().str.strip()
            df_sales_normalized['product_match'] = df_sales_normalized['Product'].str.upper().str.strip()

            # Merge using normalized names
            df_with_targets = df_targets_normalized.merge(
                df_sales_normalized,
                left_on=['customer_match', 'product_match'],
                right_on=['customer_match', 'product_match'],
                how='left'
            ).fillna({'TotalQuantitySold': 0, 'TotalSaleAmount': 0})

            # Calculate summary metrics from raw data first (convert to float to avoid Decimal/float error)
            total_target = float(df_with_targets['target_amount'].sum())
            total_sold = float(df_with_targets['TotalQuantitySold'].sum())
            total_sold_amount = float(df_with_targets['TotalSaleAmount'].sum())
            overall_achievement = (total_sold / total_target * 100) if total_target > 0 else 0

            # Format table
            fresh_table = df_with_targets[['customer_name', 'product_name', 'target_amount', 'TotalQuantitySold', 'TotalSaleAmount']].copy()
            fresh_table['target_amount'] = fresh_table['target_amount'].astype(float)
            fresh_table['TotalQuantitySold'] = fresh_table['TotalQuantitySold'].astype(float)
            fresh_table['TotalSaleAmount'] = fresh_table['TotalSaleAmount'].astype(float)
            fresh_table.columns = ['Customer Name', 'Product Name', 'Target Qty', 'Sold Qty', 'Sold Amount']
            fresh_table['Remaining Qty'] = fresh_table['Target Qty'] - fresh_table['Sold Qty']
            fresh_table['Achievement %'] = (fresh_table['Sold Qty'] / fresh_table['Target Qty'] * 100).fillna(0)

            # Format numbers
            fresh_table['Target Qty'] = fresh_table['Target Qty'].fillna(0).apply(lambda x: f"{float(x):.1f}")
            fresh_table['Sold Qty'] = fresh_table['Sold Qty'].fillna(0).apply(lambda x: f"{float(x):.1f}")
            fresh_table['Sold Amount'] = fresh_table['Sold Amount'].fillna(0).apply(lambda x: f"PKR {x:,.0f}")
            fresh_table['Remaining Qty'] = fresh_table['Remaining Qty'].fillna(0).apply(lambda x: f"{float(x):.1f}")
            fresh_table['Achievement %'] = fresh_table['Achievement %'].fillna(0).apply(lambda x: f"{x:.1f}%")

            st.write(f"**Targets vs Actuals for {selected_customer}:**")
            st.dataframe(fresh_table[['Customer Name', 'Product Name', 'Target Qty', 'Sold Qty', 'Remaining Qty', 'Sold Amount', 'Achievement %']], width='stretch')

            # Summary metrics
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Target Qty", f"{total_target:.1f}")
            col2.metric("Total Sold Qty", f"{total_sold:.1f}")
            col3.metric("Overall Achievement", f"{overall_achievement:.1f}%")

            st.metric("Total Sales Amount", f"PKR {total_sold_amount:,.0f}")
    else:
        st.error("No Fresh Pick target data available.")

# QR Commission Employees List
qr_employees_list = [
    # Khadda Main Branch (Shop ID: 2)
    {'id': 119, 'name': 'M. Aqib (OT) 3415', 'branch': 'Khadda Market Outlet', 'shop_id': 2},
    {'id': 332, 'name': 'Javed', 'branch': 'Khadda Market Outlet', 'shop_id': 2},
    {'id': 257, 'name': 'Kiran (Sup)', 'branch': 'Khadda Market Outlet', 'shop_id': 2},
    {'id': 298, 'name': 'M. Safdar 3423', 'branch': 'Khadda Market Outlet', 'shop_id': 2},
    {'id': 367, 'name': 'Mohsin Cashier Main Counter', 'branch': 'Khadda Market Outlet', 'shop_id': 2},
    {'id': 129, 'name': 'MUSHTAQ KASHMIRI (ST) 3070', 'branch': 'Khadda Market Outlet', 'shop_id': 2},
    {'id': 304, 'name': 'shahzad OT Khadda Old', 'branch': 'Khadda Market Outlet', 'shop_id': 2},
    {'id': 357, 'name': 'Yasir Ahmed(OT)', 'branch': 'Khadda Market Outlet', 'shop_id': 2},
    {'id': 341, 'name': 'Zakir Khadda Old 3921', 'branch': 'Khadda Market Outlet', 'shop_id': 2},
    {'id': 338, 'name': 'M. Zohaib OT 3716', 'branch': 'Khadda Market Outlet', 'shop_id': 2},
    {'id': 369, 'name': 'Amjad Cashier', 'branch': 'Khadda Market Outlet', 'shop_id': 2},
    {'id': 362, 'name': 'Zia ur Rehman (OT)', 'branch': 'Khadda Market Outlet', 'shop_id': 2},
    {'id': 245, 'name': 'MURAD(OT)', 'branch': 'Khadda Market Outlet', 'shop_id': 2},

    # MALIR (Shop ID: 10)
    {'id': 312, 'name': 'Adeel Ahmed (ST)', 'branch': 'Jinnah Avenue (Malir ) Outlet', 'shop_id': 10},
    {'id': 722, 'name': 'M. Ishaque ST', 'branch': 'Jinnah Avenue (Malir ) Outlet', 'shop_id': 10},
    {'id': 313, 'name': 'Junaid Abdullah (OT)', 'branch': 'Jinnah Avenue (Malir ) Outlet', 'shop_id': 10},
    {'id': 346, 'name': 'M. Hussain (OT)', 'branch': 'Jinnah Avenue (Malir ) Outlet', 'shop_id': 10},
    {'id': 347, 'name': 'Umar Hayat (OT)', 'branch': 'Jinnah Avenue (Malir ) Outlet', 'shop_id': 10},

    # North Nazimabad (Shop ID: 8)
    {'id': 273, 'name': 'Ammar ST', 'branch': 'North Nazimabad Outlet', 'shop_id': 8},
    {'id': 16, 'name': 'Bheem  (OT)', 'branch': 'North Nazimabad Outlet', 'shop_id': 8},
    {'id': 336, 'name': 'Rehan (OT)', 'branch': 'North Nazimabad Outlet', 'shop_id': 8},
    {'id': 297, 'name': 'Saad ST', 'branch': 'North Nazimabad Outlet', 'shop_id': 8},
    {'id': 320, 'name': 'Shakeel', 'branch': 'North Nazimabad Outlet', 'shop_id': 8},
    {'id': 181, 'name': 'Siraj Alam (OT)', 'branch': 'North Nazimabad Outlet', 'shop_id': 8},

    # Rahat Commercial (Shop ID: 4)
    {'id': 285, 'name': 'M. Noman OT', 'branch': 'Rahat Commercial', 'shop_id': 4},
    {'id': 47, 'name': 'UMER (ST)', 'branch': 'Rahat Commercial', 'shop_id': 4},
    {'id': 358, 'name': 'Waheed Dine Out (OT)', 'branch': 'Rahat Commercial', 'shop_id': 4},

    # TOWER (Shop ID: 6)
    {'id': 350, 'name': 'Anwar ul Haq (OT)', 'branch': 'ii chundrigar Outlet', 'shop_id': 6},
    {'id': 203, 'name': 'Bilal (OT)', 'branch': 'ii chundrigar Outlet', 'shop_id': 6},
    {'id': 45, 'name': 'Fabii(OT)', 'branch': 'ii chundrigar Outlet', 'shop_id': 6},
    {'id': 296, 'name': 'Karan OT', 'branch': 'ii chundrigar Outlet', 'shop_id': 6},
    {'id': 339, 'name': 'Syed Salman Shah', 'branch': 'ii chundrigar Outlet', 'shop_id': 6}
]

# --- QR Commission ---
with tab_qr:
    st.subheader("📱 QR Commission (2% on Sales)")

    # Branch selection for filtering
    branch_options = df_branch['shop_name'].tolist() if not df_branch.empty else []
    if branch_options:
        selected_branch_qr = st.selectbox("Select Branch", branch_options, key="qr_branch_select")
        branch_match = df_branch.loc[df_branch['shop_name'] == selected_branch_qr, 'shop_id']
        selected_shop_id_qr = branch_match.values[0] if not branch_match.empty else None
    else:
        st.error("No branch data available. Please check your filters.")
        selected_shop_id_qr = None

    # Get total sales for the selected branch
    branch_total_sales = df_branch[df_branch['shop_id'] == selected_shop_id_qr]['total_Nt_amount'].sum() if selected_shop_id_qr else 0

    # Get QR sales for the selected branch
    qr_branch_sales = df_qr_sales_data[df_qr_sales_data['shop_id'] == selected_shop_id_qr]['total_sale'].sum() if selected_shop_id_qr else 0

    # Show summary metrics
    st.markdown("### 📊 Sales Summary")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Branch Sales", f"PKR {branch_total_sales:,.0f}")
    col2.metric("QR Sales (Blinkco)", f"PKR {qr_branch_sales:,.0f}")
    if branch_total_sales > 0:
        qr_percentage = (qr_branch_sales / branch_total_sales * 100)
        col3.metric("QR Sales %", f"{qr_percentage:.1f}%")

    st.markdown("---")

    # Filter QR employees for selected branch using shop_id
    branch_qr_employees = [emp for emp in qr_employees_list if emp.get('shop_id') == selected_shop_id_qr]

    if not branch_qr_employees:
        st.info(f"No QR employees defined for {selected_branch_qr}.")
    else:
        # Create dataframe for QR employees
        df_qr_emp = pd.DataFrame(branch_qr_employees)
        # Only use the first 3 columns (id, name, branch) and ignore shop_id for the dataframe
        df_qr_emp = df_qr_emp[['id', 'name', 'branch']]
        df_qr_emp.columns = ['Employee ID', 'Employee Name', 'Branch']

        # Get sales data for these employees
        qr_employee_ids = df_qr_emp['Employee ID'].tolist()
        qr_employee_names = df_qr_emp['Employee Name'].tolist()

        # Filter sales data by employee_id OR employee_name
        df_qr_sales = df_ot[
            (df_ot['employee_id'].isin(qr_employee_ids)) |
            (df_ot['employee_name'].isin(qr_employee_names))
        ].copy()

        if df_qr_sales.empty:
            st.info(f"No sales data found for QR employees in {selected_branch_qr}.")
            # Show what employees exist in the sales data
            st.write("**Employees in sales data for this branch:**")
            branch_sales = df_ot[df_ot['shop_id'] == selected_shop_id_qr]
            unique_emps = branch_sales[['employee_id', 'employee_name']].drop_duplicates()
            for _, emp in unique_emps.head(10).iterrows():
                st.write(f"  ID: {emp['employee_id']}, Name: {emp['employee_name']}")
        else:
            # Merge with employee list to ensure all employees are shown
            df_qr_commission = df_qr_emp.merge(
                df_qr_sales[['employee_id', 'employee_name', 'total_sale']],
                left_on=['Employee ID', 'Employee Name'],
                right_on=['employee_id', 'employee_name'],
                how='left'
            ).fillna({'total_sale': 0})

            # Calculate 2% commission
            df_qr_commission['commission'] = df_qr_commission['total_sale'] * 0.02

            # Format table
            qr_table = df_qr_commission[['Employee Name', 'Employee ID', 'total_sale', 'commission']].copy()
            qr_table.columns = ['Employee Name', 'Employee ID', 'Current Sale', 'Commission (2%)']

            # Format numbers
            qr_table['Current Sale'] = qr_table['Current Sale'].fillna(0).apply(lambda x: f"PKR {int(round(x)):,}")
            qr_table['Commission (2%)'] = qr_table['Commission (2%)'].fillna(0).apply(lambda x: f"PKR {int(round(x)):,}")

            # Show summary
            total_sales_qr = df_qr_commission['total_sale'].sum()
            total_commission_qr = df_qr_commission['commission'].sum()

            col1, col2 = st.columns(2)
            col1.metric("Total Sales (QR Employees)", f"PKR {total_sales_qr:,.0f}")
            col2.metric("Total Commission (2%)", f"PKR {total_commission_qr:,.0f}")

            st.dataframe(qr_table[['Employee Name', 'Employee ID', 'Current Sale', 'Commission (2%)']], width='stretch')

            st.download_button("Download QR Commission CSV", qr_table.to_csv(index=False), "qr_commission.csv", "text/csv")

    st.markdown("---")

    # Employee-wise QR and Normal Sales Section
    st.subheader("👥 Employee-wise QR vs Normal Sales Performance")

    if not df_employee_sales.empty:
        # Filter employee sales for selected branch
        df_branch_employee_sales = df_employee_sales[df_employee_sales['employee_id'] != 0].copy()  # Exclude Online/Unassigned

        if not df_branch_employee_sales.empty:
            # Format numbers for display
            df_branch_employee_sales['total_sales'] = df_branch_employee_sales['total_sales'].apply(lambda x: f"PKR {int(x):,}")
            df_branch_employee_sales['qr_sales'] = df_branch_employee_sales['qr_sales'].apply(lambda x: f"PKR {int(x):,}")
            df_branch_employee_sales['normal_sales'] = df_branch_employee_sales['normal_sales'].apply(lambda x: f"PKR {int(x):,}")
            df_branch_employee_sales['qr_percentage'] = df_branch_employee_sales['qr_percentage'].apply(lambda x: f"{x:.1f}%")

            # Calculate totals
            total_sales_all = df_branch_employee_sales['total_sales'].str.replace('PKR ', '').str.replace(',', '').astype(float).sum()
            total_qr_sales_all = df_branch_employee_sales['qr_sales'].str.replace('PKR ', '').str.replace(',', '').astype(float).sum()
            total_normal_sales_all = df_branch_employee_sales['normal_sales'].str.replace('PKR ', '').str.replace(',', '').astype(float).sum()

            # Show summary metrics
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Employee Sales", f"PKR {int(total_sales_all):,}")
            col2.metric("Total QR Sales", f"PKR {int(total_qr_sales_all):,}")
            col3.metric("Total Normal Sales", f"PKR {int(total_normal_sales_all):,}")

            # Display employee-wise data
            st.dataframe(
                df_branch_employee_sales[
                    ['employee_name', 'total_transactions', 'total_sales', 'qr_sales', 'normal_sales',
                     'qr_transactions', 'normal_transactions', 'qr_percentage']
                ],
                column_config={
                    "employee_name": "Employee Name",
                    "total_transactions": "Total Transactions",
                    "total_sales": "Total Sales",
                    "qr_sales": "QR Sales",
                    "normal_sales": "Normal Sales",
                    "qr_transactions": "QR Transactions",
                    "normal_transactions": "Normal Transactions",
                    "qr_percentage": "QR %"
                },
                width='stretch'
            )

            # Download button for employee sales data
            st.download_button(
                "Download Employee Sales CSV",
                df_branch_employee_sales.to_csv(index=False),
                "employee_sales.csv",
                "text/csv"
            )
        else:
            st.info("No employee sales data available for the selected branch.")
    else:
        st.info("No employee sales data available.")
