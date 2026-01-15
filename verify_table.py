import pyodbc

def verify_tblShopWastage():
    # Connect to the Candelahns database
    conn_str = "DRIVER={SQL Server};SERVER=103.86.55.183,2001;DATABASE=Candelahns;UID=ReadOnlyUser;PWD=902729@Rafy"
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Check if table exists
        cursor.execute("""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = 'tblShopWastage'
        """)

        if cursor.fetchone():
            print("[OK] Table tblShopWastage exists")

            # Get table structure
            cursor.execute("""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = 'tblShopWastage'
                ORDER BY ORDINAL_POSITION
            """)

            columns = cursor.fetchall()
            print("\nTable Structure:")
            print("-" * 50)
            for col in columns:
                print(f"{col[0]:<20} {col[1]:<15} {col[2]:<10} {str(col[3]):<10}")

            # Check expected columns
            expected_columns = ['shop_Wastage_id', 'Shop_id', 'wastage_id', 'Qty', 'Product_Item_ID']
            actual_columns = [col[0] for col in columns]

            print(f"\nExpected columns: {expected_columns}")
            print(f"Actual columns: {actual_columns}")

            missing = set(expected_columns) - set(actual_columns)
            extra = set(actual_columns) - set(expected_columns)

            if not missing and not extra:
                print("[OK] Column structure matches expected!")
            else:
                if missing:
                    print(f"[ERROR] Missing columns: {list(missing)}")
                if extra:
                    print(f"[INFO] Extra columns: {list(extra)}")

            # Get sample data
            print("\nSample data (TOP 5):")
            cursor.execute("SELECT TOP 5 * FROM tblShopWastage ORDER BY shop_Wastage_id")
            sample_rows = cursor.fetchall()

            if sample_rows:
                # Print column headers
                column_names = [column[0] for column in cursor.description]
                print(" | ".join(f"{name:<15}" for name in column_names))
                print("-" * (len(column_names) * 16))

                for row in sample_rows:
                    print(" | ".join(f"{str(val):<15}" for val in row))
            else:
                print("No data found in table")

        else:
            print("[ERROR] Table tblShopWastage does not exist")

        conn.close()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    verify_tblShopWastage()
