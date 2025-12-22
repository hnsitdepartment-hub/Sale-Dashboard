import pyodbc
import pandas as pd
import streamlit as st
from datetime import date
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
@st.cache_resource
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
# SIDEBAR FILTERS
# -------------------------------
st.sidebar.header("🔍 Filters")

# Date filter
start_date = st.sidebar.date_input("Start Date", date(2025, 12, 1))
end_date = st.sidebar.date_input("End Date", date(2025, 12, 5))
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
# BRANCH TARGETS
# -------------------------------
branch_targets = {
    2: 34100000,  # Khadda Market Outlet
    4: 22475000,  # Rahat Commercial Outlet
    8: 18600000,  # North Nazimabad Outlet
    10: 10075000,  # Jinnah Avenue (Malir ) Outlet
    6: 18600000,  # ii chundrigar Outlet
    3: 1999999    # Festival
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
SELECT s.shop_id, sh.shop_name,
       COUNT(DISTINCT s.sale_id) AS total_sales,
       SUM(s.Nt_amount) AS total_Nt_amount
FROM tblSales s
LEFT JOIN tblDefShops sh ON s.shop_id = sh.shop_id
WHERE s.sale_date BETWEEN ? AND ?
AND s.shop_id IN ({placeholders(len(selected_branch_ids))})
"""
branch_params = [start_date_str, end_date_str] + selected_branch_ids

if data_mode == "Filtered":
    if blocked_names:
        branch_query += f" AND s.Cust_name NOT IN ({placeholders(len(blocked_names))})"
        branch_params.extend(blocked_names)
    if blocked_comments:
        branch_query += f" AND (s.Additional_Comments NOT IN ({placeholders(len(blocked_comments))}) OR s.Additional_Comments IS NULL)"
        branch_params.extend(blocked_comments)

branch_query += " GROUP BY s.shop_id, sh.shop_name ORDER BY s.shop_id"

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
tab1, tab2, tab3, tab4, tab5 = st.tabs(["Main Sale", "Order Taker Sale", "Chef Sale", "Chef Targets", "OT Targets"])

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
    branch_options = df_branch['shop_name'].tolist()
    selected_branch_chef = st.selectbox("Select Branch", branch_options, key="chef_branch_select")
    selected_shop_id_chef = df_branch.loc[df_branch['shop_name'] == selected_branch_chef, 'shop_id'].values[0]

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

        # Format numbers (handle NaN values)
        product_table['Target'] = product_table['Target'].fillna(0).apply(lambda x: f"{int(x):,}")
        product_table['Current Sale'] = product_table['Current Sale'].fillna(0).apply(lambda x: f"{int(x):,}")
        product_table['Remaining'] = product_table['Remaining'].fillna(0).apply(lambda x: f"{int(x):,}")
        product_table['Achievement %'] = product_table['Achievement %'].fillna(0).apply(lambda x: f"{x:.1f}%")

        # For quantity targets, append "qty" to Target
        product_table['Target'] = product_table.apply(lambda row: f"{row['Target']} qty" if row['Type'] == 'Quantity' else row['Target'], axis=1)

        st.dataframe(product_table[['Product', 'Target', 'Current Sale', 'Remaining', 'Achievement %']], width='stretch')

# --- OT Targets ---
with tab5:
    st.subheader("👨 OT Targets Performance")

    # Branch selection for filtering
    branch_options = df_branch['shop_name'].tolist()
    selected_branch_ot = st.selectbox("Select Branch", branch_options, key="ot_branch_select")
    selected_shop_id_ot = df_branch.loc[df_branch['shop_name'] == selected_branch_ot, 'shop_id'].values[0]

    # Option to hide rows with 0 sales
    hide_zero_sales = st.checkbox("Hide employees with 0 current sales", value=False)

    # Get targets for this branch
    df_ot_targets_branch = df_ot_targets[df_ot_targets['shop_id'] == selected_shop_id_ot]

    if df_ot_targets_branch.empty:
        st.info("No OT target data for selected branch.")
    else:
        # Get sales data for the date range (all branches, we'll filter later)
        df_ot_sales_all = df_ot.groupby(['shop_id', 'employee_id'])['total_sale'].sum().reset_index()

        # Left join targets with sales data
        df_ot_perf = df_ot_targets_branch.merge(
            df_ot_sales_all, on=['shop_id', 'employee_id'], how='left'
        ).fillna(0)

        # Merge with employee names
        df_ot_perf = df_ot_perf.merge(df_ot_employees, on='employee_id', how='left')

        # For employees without names, use 'ID: employee_id'
        df_ot_perf['employee_name'] = df_ot_perf['employee_name'].fillna(df_ot_perf['employee_id'].astype(str).apply(lambda x: f'ID: {x}'))

        # Final verification: ensure all records are for the selected branch
        df_ot_perf = df_ot_perf[df_ot_perf['shop_id'] == selected_shop_id_ot]

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
