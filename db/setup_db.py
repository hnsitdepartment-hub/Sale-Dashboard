import pyodbc

def setup_database():
    conn_str = (
        "DRIVER={SQL Server};"
        "SERVER=localhost;"
        "DATABASE=KDS_DB;"
        "Trusted_Connection=yes;"
    )

    try:
        conn = pyodbc.connect(conn_str, autocommit=True)
        cursor = conn.cursor()

        # Create Branches table
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='branches' AND xtype='U')
            CREATE TABLE dbo.branches (
                shop_id INT PRIMARY KEY,
                branch_name NVARCHAR(255) NOT NULL UNIQUE,
                is_active BIT DEFAULT 1 NOT NULL
            )
        """)

        # Create Chef Categories table
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='cfg_chef_categories' AND xtype='U')
            CREATE TABLE dbo.cfg_chef_categories (
                category_id INT IDENTITY(1,1) PRIMARY KEY,
                category_name NVARCHAR(255) NOT NULL UNIQUE,
                is_active BIT DEFAULT 1 NOT NULL
            )
        """)

        # Create Branch Chef Targets table
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='branch_chef_targets' AND xtype='U')
            CREATE TABLE dbo.branch_chef_targets (
                id INT IDENTITY(1,1) PRIMARY KEY,
                shop_id INT NOT NULL,
                category_id INT NOT NULL,
                monthly_target DECIMAL(18,2) NOT NULL,
                created_at DATETIME2 DEFAULT GETDATE(),
                FOREIGN KEY (shop_id) REFERENCES dbo.branches(shop_id),
                FOREIGN KEY (category_id) REFERENCES dbo.cfg_chef_categories(category_id),
                UNIQUE (shop_id, category_id)
            )
        """)

        # Create OT Targets table
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='ot_targets' AND xtype='U')
            CREATE TABLE dbo.ot_targets (
                id INT IDENTITY(1,1) PRIMARY KEY,
                shop_id INT NOT NULL,
                employee_id INT NOT NULL,
                monthly_target DECIMAL(18,2) NOT NULL,
                FOREIGN KEY (shop_id) REFERENCES dbo.branches(shop_id),
                UNIQUE (shop_id, employee_id)
            )
        """)

        print("Database setup completed successfully")
        conn.close()

    except Exception as e:
        print(f"Database setup failed: {e}")

if __name__ == "__main__":
    setup_database()
