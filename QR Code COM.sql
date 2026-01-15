-------------------------------------------------------------
-----------------------Master Query---------------------------

SELECT 
    s.sale_id,
    s.sale_date,
    s.cust_name,
    s.adjustment_comments,
    LEFT(s.adjustment_comments, 6) AS first_6_chars,
    b.BlinkOrderId,
    -- NEW COLUMN: Match/Unmatch Status
    CASE 
        WHEN b.BlinkOrderId IS NOT NULL THEN 'Match'
        ELSE 'Unmatch'
    END AS blink_match_status,
    -- Shop Details
    s.shop_id,
    sh.shop_name,
    sh.shop_code,
    sh.address AS shop_address,
    -- Employee Details
    s.employee_id,
    e.field_name AS employee_name,
    e.employee_type_id,
    -- Sale Amount
    s.NT_amount,
    s.GT_amount
FROM 
    tblSales s
-- CHANGE: LEFT JOIN instead of INNER JOIN
LEFT JOIN 
    tblInitialRawBlinkOrder b 
    ON LEFT(s.adjustment_comments, 6) = b.BlinkOrderId
LEFT JOIN 
    tblDefShops sh 
    ON s.shop_id = sh.shop_id
LEFT JOIN 
    tblDefShopEmployees e 
    ON s.employee_id = e.shop_employee_id
WHERE 
    s.sale_date BETWEEN '2026-01-01' AND '2026-01-30'
ORDER BY 
    s.sale_id;


------------------------------------------------------------------------------

-------------------------------Count QR MATCH OR UNMATCH ----------------------

-- Total sales count
SELECT COUNT(*) AS total_sales FROM tblSales 
WHERE sale_date BETWEEN '2026-01-01' AND '2026-01-30';

-- Blink-matched sales
SELECT COUNT(*) AS matched_sales FROM tblSales s
LEFT JOIN tblInitialRawBlinkOrder b 
ON LEFT(s.adjustment_comments, 6) = b.BlinkOrderId
WHERE s.sale_date BETWEEN '2026-01-01' AND '2026-01-30'
AND b.BlinkOrderId IS NOT NULL;

-- Unmatched sales (yeh 0 hona chahiye)
SELECT COUNT(*) AS unmatched_sales FROM tblSales s
LEFT JOIN tblInitialRawBlinkOrder b 
ON LEFT(s.adjustment_comments, 6) = b.BlinkOrderId
WHERE s.sale_date BETWEEN '2026-01-01' AND '2026-01-30'
AND b.BlinkOrderId IS NULL;

---------------------------------------------------------------------------------
--------------------------MASTER WITH ORDR COUNT MATCH OR UNMATCH----------------
SELECT 
    s.sale_id,
    s.sale_date,
    s.cust_name,
    s.adjustment_comments,
    LEFT(s.adjustment_comments, 6) AS first_6_chars,
    b.BlinkOrderId,
    CASE 
        WHEN b.BlinkOrderId IS NOT NULL THEN 'Match'
        ELSE 'Unmatch'
    END AS blink_match_status,
    s.shop_id,
    sh.shop_name,
    sh.shop_code,
    sh.address AS shop_address,
    s.employee_id,
    e.field_name AS employee_name,
    e.employee_type_id,
    s.NT_amount,
    s.GT_amount
FROM 
    tblSales s
LEFT JOIN 
    tblInitialRawBlinkOrder b 
    ON LEFT(s.adjustment_comments, 6) = b.BlinkOrderId
LEFT JOIN 
    tblDefShops sh 
    ON s.shop_id = sh.shop_id
LEFT JOIN 
    tblDefShopEmployees e 
    ON s.employee_id = e.shop_employee_id
WHERE 
    s.sale_date BETWEEN '2026-01-01' AND '2026-01-30'
ORDER BY 
    CASE WHEN b.BlinkOrderId IS NOT NULL THEN 0 ELSE 1 END,
    s.sale_id;


/* SUMMARY COUNTS */
SELECT 
    'All Sales' AS category,
    COUNT(*) AS total_count
FROM tblSales s
WHERE s.sale_date BETWEEN '2026-01-01' AND '2026-01-30'

UNION ALL

SELECT 
    'Matched (Blink Orders)' AS category,
    COUNT(*) AS total_count
FROM tblSales s
LEFT JOIN tblInitialRawBlinkOrder b 
ON LEFT(s.adjustment_comments, 6) = b.BlinkOrderId
WHERE s.sale_date BETWEEN '2026-01-01' AND '2026-01-30'
AND b.BlinkOrderId IS NOT NULL

UNION ALL

SELECT 
    'Unmatched (Normal Sales)' AS category,
    COUNT(*) AS total_count
FROM tblSales s
LEFT JOIN tblInitialRawBlinkOrder b 
ON LEFT(s.adjustment_comments, 6) = b.BlinkOrderId
WHERE s.sale_date BETWEEN '2026-01-01' AND '2026-01-30'
AND b.BlinkOrderId IS NULL;

-----------------------------------------------------------------------------------------------
------------------------------OT TOTAL SALE WITH MATCH OR UNMATCH------------------------------

/* EMPLOYEE WISE SALES SUMMARY - WITH SHOP/BRANCH */
SELECT 
    -- Shop/Branch Details
    s.shop_id,
    sh.shop_name AS branch_name,
    sh.shop_code AS branch_code,
    
    -- Employee Details
    e.shop_employee_id AS employee_id,
    e.field_name AS employee_name,
    e.employee_type_id,
    
    -- Matched Sales (Blink Orders with employee)
    SUM(CASE WHEN b.BlinkOrderId IS NOT NULL THEN 1 ELSE 0 END) AS matched_sales_count,
    SUM(CASE WHEN b.BlinkOrderId IS NOT NULL THEN s.NT_amount ELSE 0 END) AS matched_sales_amount,
    
    -- Unmatched Sales (Normal sales)
    SUM(CASE WHEN b.BlinkOrderId IS NULL THEN 1 ELSE 0 END) AS unmatched_sales_count,
    SUM(CASE WHEN b.BlinkOrderId IS NULL THEN s.NT_amount ELSE 0 END) AS unmatched_sales_amount,
    
    -- Total
    COUNT(*) AS total_sales_count,
    SUM(s.NT_amount) AS total_sales_amount
    
FROM 
    tblSales s
LEFT JOIN 
    tblInitialRawBlinkOrder b 
    ON LEFT(s.adjustment_comments, 6) = b.BlinkOrderId
LEFT JOIN 
    tblDefShops sh 
    ON s.shop_id = sh.shop_id
LEFT JOIN 
    tblDefShopEmployees e 
    ON s.employee_id = e.shop_employee_id
WHERE 
    s.sale_date BETWEEN '2026-01-01' AND '2026-01-02'
    AND s.employee_id IS NOT NULL
GROUP BY 
    s.shop_id, sh.shop_name, sh.shop_code,
    e.shop_employee_id, e.field_name, e.employee_type_id
ORDER BY 
    s.shop_id, total_sales_amount DESC;

----------------------------------------------------------------------------
------------------EMPLOYEE WISE SALES SUMMARY - WITH 2% COMMISSION ---------
SELECT 
    -- Shop/Branch Details
    s.shop_id,
    sh.shop_name AS branch_name,
    sh.shop_code AS branch_code,
    
    -- Employee Details
    e.shop_employee_id AS employee_id,
    e.field_name AS employee_name,
    e.employee_type_id,
    
    -- Matched Sales (Blink Orders with employee)
    SUM(CASE WHEN b.BlinkOrderId IS NOT NULL THEN 1 ELSE 0 END) AS matched_sales_count,
    SUM(CASE WHEN b.BlinkOrderId IS NOT NULL THEN s.NT_amount ELSE 0 END) AS matched_sales_amount,
    
    -- NEW COLUMN: 2% Commission on Matched Sales
    (SUM(CASE WHEN b.BlinkOrderId IS NOT NULL THEN s.NT_amount ELSE 0 END) * 0.02) AS matched_commission_2pct,
    
    -- Unmatched Sales (Normal sales)
    SUM(CASE WHEN b.BlinkOrderId IS NULL THEN 1 ELSE 0 END) AS unmatched_sales_count,
    SUM(CASE WHEN b.BlinkOrderId IS NULL THEN s.NT_amount ELSE 0 END) AS unmatched_sales_amount,
    
    -- Total
    COUNT(*) AS total_sales_count,
    SUM(s.NT_amount) AS total_sales_amount
    
FROM 
    tblSales s
LEFT JOIN 
    tblInitialRawBlinkOrder b 
    ON LEFT(s.adjustment_comments, 6) = b.BlinkOrderId
LEFT JOIN 
    tblDefShops sh 
    ON s.shop_id = sh.shop_id
LEFT JOIN 
    tblDefShopEmployees e 
    ON s.employee_id = e.shop_employee_id
WHERE 
    s.sale_date BETWEEN '2026-01-10' AND '2026-01-11'
    AND s.employee_id IS NOT NULL
GROUP BY 
    s.shop_id, sh.shop_name, sh.shop_code,
    e.shop_employee_id, e.field_name, e.employee_type_id
ORDER BY 
    s.shop_id, total_sales_amount DESC;
