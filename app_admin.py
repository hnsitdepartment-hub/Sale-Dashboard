import streamlit as st
import pandas as pd
import altair as alt
from db.connection_cloud import get_connection_candelahns, get_connection_kdsdb

# -------------------------------
# AUTHENTICATION
# -------------------------------
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False

if not st.session_state.logged_in:
    st.title("Admin Login")
    username = st.text_input("Username")
    password = st.text_input("Password", type='password')
    if st.button("Login"):
        if username == 'admin' and password == '902729':
            st.session_state.logged_in = True
            st.rerun()
        else:
            st.error("Invalid credentials")
    st.stop()

# -------------------------------
# PAGE CONFIG
# -------------------------------
st.set_page_config(page_title="Admin Dashboard", layout="wide")

# -------------------------------
# DB CONNECTIONS
# -------------------------------
conn_sales = get_connection_candelahns()
conn_kds = get_connection_kdsdb()

# -------------------------------
# TABS
# -------------------------------
tab_overview, tab_branches, tab_employees, tab_chefs, tab_targets, tab_sales_reports, tab_inventory_reports, tab_customer_reports, tab_financial_reports, tab_operational_reports, tab_audit_reports, tab_analytics_reports = st.tabs([
    "Dashboard Overview", "Branch Management", "Employee Management", "Chef Management", "Target Management", "Sales Reports", "Inventory Reports", "Customer Reports", "Financial Reports", "Operational Reports", "Audit Reports", "Analytics Reports"
])

# -------------------------------
# FETCH DATA
# -------------------------------
# Active branches
branches = pd.read_sql("SELECT shop_id, branch_name FROM dbo.branches WHERE is_active=1", conn_kds)

# Chef targets
df_chef_targets = pd.read_sql("SELECT shop_id, category_id, monthly_target as target_amount FROM dbo.cfg_branch_chef_targets", conn_kds)
categories = pd.read_sql("SELECT category_id, category_name FROM dbo.cfg_chef_categories", conn_kds)

# OT targets
df_ot_targets = pd.read_sql("SELECT shop_id, employee_id, monthly_target as target_amount FROM dbo.ot_targets", conn_kds)
# Fetch employee names from Candelahns
import pyodbc
conn_candelahns_temp = pyodbc.connect("DRIVER={SQL Server};SERVER=103.86.55.183,2001;DATABASE=Candelahns;UID=ReadOnlyUser;PWD=902729@Rafy")
df_ot_employees = pd.read_sql("SELECT shop_employee_id, field_name FROM dbo.tblDefShopEmployees", conn_candelahns_temp)
df_ot_employees.rename(columns={'shop_employee_id': 'employee_id', 'field_name': 'employee_name'}, inplace=True)
conn_candelahns_temp.close()

with tab_overview:
    st.subheader("Dashboard Overview")

    # Date inputs
    start_date = st.date_input("Start Date", pd.to_datetime("2025-12-01"))
    end_date = st.date_input("End Date", pd.to_datetime("2025-12-18"))

    # Fetch sales data
    df_sales = pd.read_sql(
        f"""
        SELECT shop_id, SUM(Nt_amount) AS total_sales
        FROM tblSales
        WHERE sale_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY shop_id
        """, conn_sales
    )

    # Merge with branches
    df_branch_sales = branches.merge(df_sales, on="shop_id", how="left").fillna(0)

    # Employee sales
    df_employee_sales = df_ot_targets.merge(df_ot_employees, on='employee_id', how='left').merge(df_sales, on='shop_id', how='left').fillna(0)[['employee_name', 'total_sales']].groupby('employee_name')['total_sales'].sum().reset_index().sort_values('total_sales', ascending=False)

    # Product sales
    df_product_sales = pd.read_sql(
        f"""
        SELECT t.field_name AS product, SUM((li.qty*li.Unit_price)/NULLIF(st.line_total,0)*s.Nt_amount) AS total_sales
        FROM tblSales s
        JOIN tblSalesLineItems li ON s.sale_id=li.sale_id
        JOIN TempProductBarcode t ON li.Product_Item_ID=t.Product_Item_ID AND li.Product_code=t.Product_code
        JOIN (
            SELECT sale_id, SUM(qty*Unit_price) AS line_total
            FROM tblSalesLineItems
            GROUP BY sale_id
        ) st ON st.sale_id=s.sale_id
        WHERE s.sale_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY t.field_name
        ORDER BY total_sales DESC
        """, conn_sales
    )

    # Chef sales
    df_chef_sales = df_chef_targets.merge(categories, on='category_id', how='left').merge(df_sales, on='shop_id', how='left').fillna(0)[['category_name', 'total_sales']].groupby('category_name')['total_sales'].sum().reset_index().sort_values('total_sales', ascending=False)

    # Layout
    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("Branch Sales")
        st.dataframe(df_branch_sales[['branch_name', 'total_sales']].sort_values('total_sales', ascending=False), use_container_width=True)

    with col2:
        st.subheader("Top 10 Employees")
        st.dataframe(df_employee_sales.head(10), use_container_width=True)

    col3, col4 = st.columns(2)

    with col3:
        st.subheader("Top 10 Products")
        st.dataframe(df_product_sales.head(10), use_container_width=True)

    with col4:
        st.subheader("Top 10 Chefs")
        st.dataframe(df_chef_sales.head(10), use_container_width=True)

with tab_branches:
    st.subheader("Branch Management")

    # Refresh branches
    branches = pd.read_sql("SELECT shop_id, branch_name FROM dbo.branches WHERE is_active=1", conn_kds)
    branch_targets_df = pd.read_sql("SELECT shop_id, monthly_target FROM dbo.branch_targets", conn_kds)

    # Merge
    branches_full = branches.merge(branch_targets_df, on='shop_id', how='left').fillna(0)

    st.subheader("Current Branches")
    st.dataframe(branches_full, use_container_width=True)

    # Add Branch
    st.subheader("Add New Branch")
    with st.form("add_branch"):
        shop_id = st.number_input("Shop ID", min_value=1, step=1)
        branch_name = st.text_input("Branch Name")
        monthly_target = st.number_input("Monthly Target", min_value=0.0)
        if st.form_submit_button("Add Branch"):
            try:
                cursor = conn_kds.cursor()
                cursor.execute("INSERT INTO dbo.branches (shop_id, branch_name, is_active) VALUES (?, ?, 1)", (shop_id, branch_name))
                cursor.execute("INSERT INTO dbo.branch_targets (shop_id, monthly_target) VALUES (?, ?)", (shop_id, monthly_target))
                conn_kds.commit()
                st.success("Branch added successfully!")
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")

    # Edit Branch
    st.subheader("Edit Branch")
    if not branches_full.empty:
        edit_options = [f"{row['branch_name']} (ID: {row['shop_id']})" for _, row in branches_full.iterrows()]
        selected_edit = st.selectbox("Select Branch to Edit", edit_options)
        if selected_edit:
            selected_shop_id = int(selected_edit.split("(ID: ")[1].strip(")"))
            current_row = branches_full[branches_full['shop_id'] == selected_shop_id].iloc[0]
            with st.form("edit_branch"):
                new_name = st.text_input("Branch Name", value=current_row['branch_name'])
                new_target = st.number_input("Monthly Target", value=current_row['monthly_target'])
                if st.form_submit_button("Update Branch"):
                    try:
                        cursor = conn_kds.cursor()
                        cursor.execute("UPDATE dbo.branches SET branch_name = ? WHERE shop_id = ?", (new_name, selected_shop_id))
                        cursor.execute("UPDATE dbo.branch_targets SET monthly_target = ? WHERE shop_id = ?", (new_target, selected_shop_id))
                        conn_kds.commit()
                        st.success("Branch updated successfully!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")

    # Delete Branch
    st.subheader("Delete Branch")
    if not branches_full.empty:
        delete_options = [f"{row['branch_name']} (ID: {row['shop_id']})" for _, row in branches_full.iterrows()]
        selected_delete = st.selectbox("Select Branch to Delete", delete_options)
        if selected_delete and st.button("Delete Branch"):
            selected_shop_id = int(selected_delete.split("(ID: ")[1].strip(")"))
            try:
                cursor = conn_kds.cursor()
                cursor.execute("DELETE FROM dbo.branch_targets WHERE shop_id = ?", (selected_shop_id,))
                cursor.execute("UPDATE dbo.branches SET is_active = 0 WHERE shop_id = ?", (selected_shop_id,))
                conn_kds.commit()
                st.success("Branch deleted successfully!")
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")

with tab_sales_reports:
    st.subheader("Sales Reports")

    # Date inputs for reports
    start_date_sales = st.date_input("Start Date", pd.to_datetime("2025-12-01"), key="sales_start")
    end_date_sales = st.date_input("End Date", pd.to_datetime("2025-12-18"), key="sales_end")

    sales_tabs = st.tabs([
        "Sales by Date/Time", "Sales by Employee", "Sales by Shop", "Sales by Product/Category",
        "Sales by Customer", "Voided/Cancelled Sales"
    ])

    with sales_tabs[0]:
        st.subheader("Sales by Date/Time (Daily)")
        try:
            df = pd.read_sql(f"""
                SELECT CONVERT(DATE, sale_date) AS sale_date, SUM(Nt_amount) AS total_sales
                FROM tblSales
                WHERE sale_date BETWEEN '{start_date_sales}' AND '{end_date_sales}'
                GROUP BY CONVERT(DATE, sale_date)
                ORDER BY sale_date
            """, conn_sales)
            st.dataframe(df, use_container_width=True)
            st.download_button("Download CSV", df.to_csv(index=False), "sales_by_date.csv", "text/csv")
        except Exception as e:
            st.error(f"Error: {e}")

    with sales_tabs[1]:
        st.subheader("Sales by Employee")
        try:
            df = pd.read_sql(f"""
                SELECT e.field_name AS employee_name, SUM(s.Nt_amount) AS total_sales, COUNT(s.sale_id) AS total_orders
                FROM tblSales s
                LEFT JOIN tblDefShopEmployees e ON s.employee_id = e.shop_employee_id
                WHERE s.sale_date BETWEEN '{start_date_sales}' AND '{end_date_sales}'
                GROUP BY e.field_name
                ORDER BY total_sales DESC
            """, conn_sales)
            st.dataframe(df, use_container_width=True)
            st.download_button("Download CSV", df.to_csv(index=False), "sales_by_employee.csv", "text/csv")
        except Exception as e:
            st.error(f"Error: {e}")

    with sales_tabs[2]:
        st.subheader("Sales by Shop")
        try:
            df = pd.read_sql(f"""
                SELECT sh.shop_name, SUM(s.Nt_amount) AS total_sales
                FROM tblSales s
                LEFT JOIN tblDefShops sh ON s.shop_id = sh.shop_id
                WHERE s.sale_date BETWEEN '{start_date_sales}' AND '{end_date_sales}'
                GROUP BY sh.shop_name
                ORDER BY total_sales DESC
            """, conn_sales)
            st.dataframe(df, use_container_width=True)
            st.download_button("Download CSV", df.to_csv(index=False), "sales_by_shop.csv", "text/csv")
        except Exception as e:
            st.error(f"Error: {e}")

    with sales_tabs[3]:
        st.subheader("Sales by Product/Category")
        try:
            df = pd.read_sql(f"""
                SELECT t.field_name AS product, SUM((li.qty*li.Unit_price)) AS total_sales
                FROM tblSalesLineItems li
                JOIN tblSales s ON li.sale_id = s.sale_id
                JOIN TempProductBarcode t ON li.Product_Item_ID = t.Product_Item_ID
                WHERE s.sale_date BETWEEN '{start_date_sales}' AND '{end_date_sales}'
                GROUP BY t.field_name
                ORDER BY total_sales DESC
            """, conn_sales)
            st.dataframe(df, use_container_width=True)
            st.download_button("Download CSV", df.to_csv(index=False), "sales_by_product.csv", "text/csv")
        except Exception as e:
            st.error(f"Error: {e}")

    with sales_tabs[4]:
        st.subheader("Sales by Customer")
        try:
            df = pd.read_sql(f"""
                SELECT Cust_name, SUM(Nt_amount) AS total_sales, COUNT(sale_id) AS total_orders
                FROM tblSales
                WHERE sale_date BETWEEN '{start_date_sales}' AND '{end_date_sales}' AND Cust_name IS NOT NULL
                GROUP BY Cust_name
                ORDER BY total_sales DESC
            """, conn_sales)
            st.dataframe(df, use_container_width=True)
            st.download_button("Download CSV", df.to_csv(index=False), "sales_by_customer.csv", "text/csv")
        except Exception as e:
            st.error(f"Error: {e}")

    with sales_tabs[5]:
        st.subheader("Voided/Cancelled Sales")
        try:
            df = pd.read_sql(f"""
                SELECT sale_date, Cust_name, Nt_amount, Additional_Comments
                FROM tblSales
                WHERE sale_date BETWEEN '{start_date_sales}' AND '{end_date_sales}' AND (Additional_Comments LIKE '%cancel%' OR Additional_Comments LIKE '%void%')
            """, conn_sales)
            st.dataframe(df, use_container_width=True)
            st.download_button("Download CSV", df.to_csv(index=False), "voided_sales.csv", "text/csv")
        except Exception as e:
            st.error(f"Error: {e}")

with tab_inventory_reports:
    st.subheader("Inventory Reports")

    # Date inputs for reports
    start_date_inventory = st.date_input("Start Date", pd.to_datetime("2025-12-01"), key="inventory_start")
    end_date_inventory = st.date_input("End Date", pd.to_datetime("2025-12-18"), key="inventory_end")

    inventory_tabs = st.tabs([
        "Stock Levels", "Low Stock Alerts", "Stock Movement", "Product Performance", "Wastage Tracking"
    ])

    with inventory_tabs[0]:
        st.subheader("Stock Levels")
        st.info("Showing current stock levels by product")
        try:
            df = pd.read_sql("""
                SELECT si.Product_Item_ID, p.item_name, si.quantity, si.closing_inv
                FROM tblShopProductInventory si
                JOIN tblDefProducts p ON si.Product_Item_ID = p.product_id
                ORDER BY si.quantity DESC
            """, conn_sales)
            st.dataframe(df, use_container_width=True)
            st.download_button("Download CSV", df.to_csv(index=False), "stock_levels.csv", "text/csv")
        except Exception as e:
            st.error(f"Error: {e}")

    with inventory_tabs[1]:
        st.subheader("Low Stock Alerts")
        st.info("Items where current quantity is below closing inventory level")
        try:
            df = pd.read_sql("""
                SELECT si.Product_Item_ID, p.item_name, si.quantity, si.closing_inv
                FROM tblShopProductInventory si
                JOIN tblDefProducts p ON si.Product_Item_ID = p.product_id
                WHERE si.quantity <= si.closing_inv
                ORDER BY si.quantity ASC
            """, conn_sales)
            if df.empty:
                st.success("No items are currently below reorder level!")
            else:
                st.warning(f"Found {len(df)} items below reorder level:")
                st.dataframe(df, use_container_width=True)
                st.download_button("Download CSV", df.to_csv(index=False), "low_stock_alerts.csv", "text/csv")
        except Exception as e:
            st.error(f"Error: {e}")

    with inventory_tabs[2]:
        st.subheader("Stock Movement")
        st.info("Stock transfer requests and movements between shops")
        try:
            # Join STR master with items and products
            df = pd.read_sql("""
                SELECT TOP 50
                    sm.str_master_id as str_id,
                    sm.str_name,
                    sm.str_date,
                    si.Product_Item_ID,
                    p.item_name as product_name,
                    si.str_quantity as requested_qty,
                    si.dispatch_quantity as sent_qty,
                    si.receive_quantity as received_qty,
                    si.Dispatch_Shop_ID as from_shop,
                    si.Receive_Shop_ID as to_shop,
                    si.product_price,
                    si.cost_price
                FROM tblStrMaster sm
                JOIN tblStrItems si ON sm.str_master_id = si.str_id
                LEFT JOIN tblDefProducts p ON si.Product_Item_ID = p.product_id
                ORDER BY sm.str_date DESC, sm.str_master_id DESC
            """, conn_sales)
            if df.empty:
                st.info("No stock transfer records found.")
            else:
                st.dataframe(df, use_container_width=True)
                st.download_button("Download CSV", df.to_csv(index=False), "stock_movement.csv", "text/csv")
        except Exception as e:
            st.error(f"Error: {e}")

    with inventory_tabs[3]:
        st.subheader("Product Performance")
        st.info("Using sales data for inventory performance")
        try:
            df = pd.read_sql(f"""
                SELECT t.field_name AS product, SUM(li.qty) AS total_qty_sold, SUM((li.qty*li.Unit_price)) AS total_revenue
                FROM tblSalesLineItems li
                JOIN tblSales s ON li.sale_id = s.sale_id
                JOIN TempProductBarcode t ON li.Product_Item_ID = t.Product_Item_ID
                WHERE s.sale_date BETWEEN '{start_date_inventory}' AND '{end_date_inventory}'
                GROUP BY t.field_name
                ORDER BY total_qty_sold DESC
            """, conn_sales)
            st.dataframe(df, use_container_width=True)
            st.download_button("Download CSV", df.to_csv(index=False), "product_performance.csv", "text/csv")
        except Exception as e:
            st.error(f"Error: {e}")

    with inventory_tabs[4]:
        st.subheader("Wastage Tracking")
        st.info("Using tblShopWastage - please verify table structure")
        try:
            df = pd.read_sql("SELECT TOP 10 * FROM tblShopWastage ORDER BY shop_Wastage_id DESC", conn_sales)
            st.dataframe(df)
        except Exception as e:
            st.error(f"Table not found or error: {e}")

with tab_customer_reports:
    st.subheader("Customer Reports")

    # Date inputs for reports
    start_date_customer = st.date_input("Start Date", pd.to_datetime("2025-12-01"), key="customer_start")
    end_date_customer = st.date_input("End Date", pd.to_datetime("2025-12-18"), key="customer_end")

    customer_tabs = st.tabs([
        "Demographics", "Loyalty Points", "Membership Status", "Complaints"
    ])

    with customer_tabs[0]:
        st.subheader("Customer Demographics")
        st.info("Using tblMemberInfo - please verify table structure")
        try:
            df = pd.read_sql("SELECT TOP 10 * FROM tblMemberInfo", conn_sales)
            st.dataframe(df)
        except Exception as e:
            st.error(f"Table not found or error: {e}")

    with customer_tabs[1]:
        st.subheader("Loyalty Points")
        st.info("Using tblMemberNetPoints - please verify table structure")
        try:
            df = pd.read_sql("SELECT TOP 10 * FROM tblMemberNetPoints", conn_sales)
            st.dataframe(df)
        except Exception as e:
            st.error(f"Table not found or error: {e}")

    with customer_tabs[2]:
        st.subheader("Membership Status")
        st.info("Using tblMemberCardStatus - please verify table structure")
        try:
            df = pd.read_sql("SELECT TOP 10 * FROM tblMemberCardStatus", conn_sales)
            st.dataframe(df)
        except Exception as e:
            st.error(f"Table not found or error: {e}")

    with customer_tabs[3]:
        st.subheader("Customer Complaints")
        st.info("Using tblMemberComplaint - please verify table structure")
        try:
            df = pd.read_sql(f"SELECT TOP 10 * FROM tblMemberComplaint WHERE complaint_date BETWEEN '{start_date_customer}' AND '{end_date_customer}'", conn_sales)
            st.dataframe(df)
        except Exception as e:
            st.error(f"Table not found or error: {e}")

with tab_financial_reports:
    st.subheader("Financial Reports")

    # Date inputs for reports
    start_date_financial = st.date_input("Start Date", pd.to_datetime("2025-12-01"), key="financial_start")
    end_date_financial = st.date_input("End Date", pd.to_datetime("2025-12-18"), key="financial_end")

    financial_tabs = st.tabs([
        "Revenue Analysis", "Expense Tracking", "Profit & Loss", "Cash Flow", "Supplier Payments"
    ])

    with financial_tabs[0]:
        st.subheader("Revenue Analysis")
        st.info("Using sales data for revenue analysis")
        try:
            df = pd.read_sql(f"""
                SELECT CONVERT(DATE, sale_date) AS date, SUM(Nt_amount) AS total_revenue
                FROM tblSales
                WHERE sale_date BETWEEN '{start_date_financial}' AND '{end_date_financial}'
                GROUP BY CONVERT(DATE, sale_date)
                ORDER BY date
            """, conn_sales)
            st.dataframe(df, use_container_width=True)
            st.download_button("Download CSV", df.to_csv(index=False), "revenue_analysis.csv", "text/csv")
        except Exception as e:
            st.error(f"Error: {e}")

    with financial_tabs[1]:
        st.subheader("Expense Tracking")
        st.info("Using tblGLSupplier_Payments - please verify table structure")
        try:
            df = pd.read_sql("SELECT TOP 10 * FROM tblGLSupplier_Payments", conn_sales)
            st.dataframe(df)
        except Exception as e:
            st.error(f"Table not found or error: {e}")

    with financial_tabs[2]:
        st.subheader("Profit & Loss")
        st.info("Using tblGlCOAMain for chart of accounts - please verify table structure")
        try:
            df = pd.read_sql("SELECT TOP 10 * FROM tblGlCOAMain", conn_sales)
            st.dataframe(df)
        except Exception as e:
            st.error(f"Table not found or error: {e}")

    with financial_tabs[3]:
        st.subheader("Cash Flow")
        st.info("Using tblCashBankClosing - please verify table structure")
        try:
            df = pd.read_sql(f"SELECT TOP 10 * FROM tblCashBankClosing WHERE closing_date BETWEEN '{start_date_financial}' AND '{end_date_financial}'", conn_sales)
            st.dataframe(df)
        except Exception as e:
            st.error(f"Table not found or error: {e}")

    with financial_tabs[4]:
        st.subheader("Supplier Payments")
        st.info("Using tblSupplierPayments - please verify table structure")
        try:
            df = pd.read_sql("SELECT TOP 10 * FROM tblSupplierPayments", conn_sales)
            st.dataframe(df)
        except Exception as e:
            st.error(f"Table not found or error: {e}")

with tab_operational_reports:
    st.subheader("Operational Reports")

    # Date inputs for reports
    start_date_operational = st.date_input("Start Date", pd.to_datetime("2025-12-01"), key="operational_start")
    end_date_operational = st.date_input("End Date", pd.to_datetime("2025-12-18"), key="operational_end")

    operational_tabs = st.tabs([
        "Employee Attendance", "Purchase Orders", "STR", "Kitchen/Order Management"
    ])

    with operational_tabs[0]:
        st.subheader("Employee Attendance")
        st.info("Using tblEmployeeAttendance - please verify table structure")
        try:
            df = pd.read_sql(f"SELECT TOP 10 * FROM tblEmployeeAttendance WHERE attendance_date BETWEEN '{start_date_operational}' AND '{end_date_operational}'", conn_sales)
            st.dataframe(df)
        except Exception as e:
            st.error(f"Table not found or error: {e}")

    with operational_tabs[1]:
        st.subheader("Purchase Orders")
        st.info("Using tblPurchaseOrders - please verify table structure")
        try:
            df = pd.read_sql("SELECT TOP 10 * FROM tblPurchaseOrders", conn_sales)
            st.dataframe(df)
        except Exception as e:
            st.error(f"Table not found or error: {e}")

    with operational_tabs[2]:
        st.subheader("STR (Stock Transfer Requests)")
        st.info("Using tblStrMaster - please verify table structure")
        try:
            df = pd.read_sql(f"SELECT TOP 10 * FROM tblStrMaster WHERE str_date BETWEEN '{start_date_operational}' AND '{end_date_operational}'", conn_sales)
            st.dataframe(df)
        except Exception as e:
            st.error(f"Table not found or error: {e}")

    with operational_tabs[3]:
        st.subheader("Kitchen/Order Management")
        st.info("Using tblSalesRestaurant - please verify table structure")
        try:
            df = pd.read_sql("SELECT TOP 10 * FROM tblSalesRestaurant", conn_sales)
            st.dataframe(df)
        except Exception as e:
            st.error(f"Table not found or error: {e}")

with tab_audit_reports:
    st.subheader("Audit Reports")

    # Date inputs for reports
    start_date_audit = st.date_input("Start Date", pd.to_datetime("2025-12-01"), key="audit_start")
    end_date_audit = st.date_input("End Date", pd.to_datetime("2025-12-18"), key="audit_end")

    audit_tabs = st.tabs([
        "User Activity Logs", "Data Synchronization", "Error Logs"
    ])

    with audit_tabs[0]:
        st.subheader("User Activity Logs")
        st.info("Using tblActivityLog - please verify table structure")
        try:
            df = pd.read_sql("SELECT TOP 10 * FROM tblActivityLog", conn_sales)
            st.dataframe(df)
        except Exception as e:
            st.error(f"Table not found or error: {e}")

    with audit_tabs[1]:
        st.subheader("Data Synchronization")
        st.info("Using tblDataLog - please verify table structure")
        try:
            df = pd.read_sql("SELECT TOP 10 * FROM tblDataLog", conn_sales)
            st.dataframe(df)
        except Exception as e:
            st.error(f"Table not found or error: {e}")

    with audit_tabs[2]:
        st.subheader("Error Logs")
        st.info("Using tblEventLog - please verify table structure")
        try:
            df = pd.read_sql("SELECT TOP 10 * FROM tblEventLog", conn_sales)
            st.dataframe(df)
        except Exception as e:
            st.error(f"Table not found or error: {e}")

with tab_analytics_reports:
    st.subheader("Analytics Reports")

    # Date inputs for reports
    start_date_analytics = st.date_input("Start Date", pd.to_datetime("2025-01-01"), key="analytics_start")
    end_date_analytics = st.date_input("End Date", pd.to_datetime("2025-12-31"), key="analytics_end")

    analytics_tabs = st.tabs([
        "Trend Analysis"
    ])

    with analytics_tabs[0]:
        st.subheader("Trend Analysis (Year-over-Year)")
        try:
            df = pd.read_sql(f"""
                SELECT YEAR(sale_date) AS year, MONTH(sale_date) AS month, SUM(Nt_amount) AS monthly_sales
                FROM tblSales
                WHERE sale_date BETWEEN '{start_date_analytics}' AND '{end_date_analytics}'
                GROUP BY YEAR(sale_date), MONTH(sale_date)
                ORDER BY year, month
            """, conn_sales)
            st.dataframe(df, use_container_width=True)
            st.download_button("Download CSV", df.to_csv(index=False), "trend_analysis.csv", "text/csv")
        except Exception as e:
            st.error(f"Error: {e}")

with tab_chefs:
    st.subheader("Chef Management")

    # Refresh data
    categories = pd.read_sql("SELECT category_id, category_name FROM dbo.cfg_chef_categories", conn_kds)
    df_chef_targets = pd.read_sql("SELECT shop_id, category_id, monthly_target FROM dbo.cfg_branch_chef_targets", conn_kds)

    # Merge
    df_chef_full = df_chef_targets.merge(categories, on='category_id', how='left').merge(branches[['shop_id', 'branch_name']], on='shop_id', how='left')

    st.subheader("Current Chef Categories and Targets")
    st.dataframe(df_chef_full, use_container_width=True)

    # Add Category
    st.subheader("Add New Category")
    with st.form("add_category"):
        category_name = st.text_input("Category Name")
        targets = {}
        for _, branch in branches.iterrows():
            targets[branch['shop_id']] = st.number_input(f"Target for {branch['branch_name']}", min_value=0.0, key=f"target_{branch['shop_id']}")
        if st.form_submit_button("Add Category"):
            try:
                cursor = conn_kds.cursor()
                # Insert category
                cursor.execute("INSERT INTO dbo.cfg_chef_categories (category_name) VALUES (?)", (category_name,))
                category_id = cursor.execute("SELECT SCOPE_IDENTITY()").fetchone()[0]
                # Insert targets
                for shop_id, target in targets.items():
                    cursor.execute("INSERT INTO dbo.cfg_branch_chef_targets (shop_id, category_id, monthly_target) VALUES (?, ?, ?)", (shop_id, category_id, target))
                conn_kds.commit()
                st.success("Category added successfully!")
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")

    # Edit Category
    st.subheader("Edit Category")
    if not categories.empty:
        edit_options = [f"{row['category_name']} (ID: {row['category_id']})" for _, row in categories.iterrows()]
        selected_edit_cat = st.selectbox("Select Category to Edit", edit_options)
        if selected_edit_cat:
            cat_id = int(selected_edit_cat.split("(ID: ")[1].strip(")"))
            current_cat = categories[categories['category_id'] == cat_id].iloc[0]
            with st.form("edit_category"):
                new_name = st.text_input("Category Name", value=current_cat['category_name'])
                if st.form_submit_button("Update Category"):
                    try:
                        cursor = conn_kds.cursor()
                        cursor.execute("UPDATE dbo.cfg_chef_categories SET category_name = ? WHERE category_id = ?", (new_name, cat_id))
                        conn_kds.commit()
                        st.success("Category updated successfully!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")

    # Delete Category
    st.subheader("Delete Category")
    if not categories.empty:
        delete_options = [f"{row['category_name']} (ID: {row['category_id']})" for _, row in categories.iterrows()]
        selected_delete_cat = st.selectbox("Select Category to Delete", delete_options)
        if selected_delete_cat and st.button("Delete Category"):
            cat_id = int(selected_delete_cat.split("(ID: ")[1].strip(")"))
            try:
                cursor = conn_kds.cursor()
                cursor.execute("DELETE FROM dbo.cfg_branch_chef_targets WHERE category_id = ?", (cat_id,))
                cursor.execute("DELETE FROM dbo.cfg_chef_categories WHERE category_id = ?", (cat_id,))
                conn_kds.commit()
                st.success("Category deleted successfully!")
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")

with tab_targets:
    st.subheader("Target Management")

    # Refresh data
    df_ot_targets = pd.read_sql("SELECT shop_id, employee_id, monthly_target FROM dbo.ot_targets", conn_kds)

    st.subheader("Current OT Targets")
    ot_full = df_ot_targets.merge(df_ot_employees, on='employee_id', how='left').merge(branches[['shop_id', 'branch_name']], on='shop_id', how='left')
    st.dataframe(ot_full, use_container_width=True)

    # Add Target
    st.subheader("Add New OT Target")
    with st.form("add_ot_target"):
        selected_branch_ot = st.selectbox("Select Branch", branches['branch_name'].tolist(), key="add_ot_branch")
        selected_employee = st.selectbox("Select Employee", df_ot_employees['employee_name'].tolist(), key="add_ot_emp")
        monthly_target = st.number_input("Monthly Target", min_value=0.0)
        if st.form_submit_button("Add Target"):
            shop_id = branches[branches['branch_name'] == selected_branch_ot]['shop_id'].iloc[0]
            emp_id = df_ot_employees[df_ot_employees['employee_name'] == selected_employee]['employee_id'].iloc[0]
            try:
                cursor = conn_kds.cursor()
                cursor.execute("INSERT INTO dbo.ot_targets (shop_id, employee_id, monthly_target) VALUES (?, ?, ?)", (shop_id, emp_id, monthly_target))
                conn_kds.commit()
                st.success("OT target added successfully!")
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")

    # Edit Target
    st.subheader("Edit OT Target")
    if not ot_full.empty:
        edit_options_ot = [f"{row['employee_name']} - {row['branch_name']} (Target: {row['monthly_target']})" for _, row in ot_full.iterrows()]
        selected_edit_ot = st.selectbox("Select Target to Edit", edit_options_ot)
        if selected_edit_ot:
            # Parse to get employee and branch
            emp_name = selected_edit_ot.split(" - ")[0]
            branch_name = selected_edit_ot.split(" - ")[1].split(" (")[0]
            shop_id = branches[branches['branch_name'] == branch_name]['shop_id'].iloc[0]
            emp_id = df_ot_employees[df_ot_employees['employee_name'] == emp_name]['employee_id'].iloc[0]
            current_target = ot_full[(ot_full['employee_id'] == emp_id) & (ot_full['shop_id'] == shop_id)]['monthly_target'].iloc[0]
            with st.form("edit_ot_target"):
                new_target = st.number_input("Monthly Target", value=current_target)
                if st.form_submit_button("Update Target"):
                    try:
                        cursor = conn_kds.cursor()
                        cursor.execute("UPDATE dbo.ot_targets SET monthly_target = ? WHERE shop_id = ? AND employee_id = ?", (new_target, shop_id, emp_id))
                        conn_kds.commit()
                        st.success("OT target updated successfully!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")

    # Delete Target
    st.subheader("Delete OT Target")
    if not ot_full.empty:
        delete_options_ot = [f"{row['employee_name']} - {row['branch_name']} (Target: {row['monthly_target']})" for _, row in ot_full.iterrows()]
        selected_delete_ot = st.selectbox("Select Target to Delete", delete_options_ot)
        if selected_delete_ot and st.button("Delete Target"):
            emp_name = selected_delete_ot.split(" - ")[0]
            branch_name = selected_delete_ot.split(" - ")[1].split(" (")[0]
            shop_id = branches[branches['branch_name'] == branch_name]['shop_id'].iloc[0]
            emp_id = df_ot_employees[df_ot_employees['employee_name'] == emp_name]['employee_id'].iloc[0]
            try:
                cursor = conn_kds.cursor()
                cursor.execute("DELETE FROM dbo.ot_targets WHERE shop_id = ? AND employee_id = ?", (shop_id, emp_id))
                conn_kds.commit()
                st.success("OT target deleted successfully!")
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")
