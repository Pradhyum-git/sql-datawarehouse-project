/*
-------------------------------------------------------------------------------
DDL Script : Create Gold Views
-------------------------------------------------------------------------------
Script Purpose:
      This script creates views for the Gold Layer in the data warehouse.
      The Gold Layer represents for the final dimensions and fact tables(star schema)

*/


-- ===================================================================
--       Create dimension:gold.dim_customers
-- ===================================================================
DROP VIEW IF EXISTS gold.dim_customers;
CREATE VIEW gold.dim_customers AS
(
SELECT
ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key, -- Surrogate key
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname  AS firstname,
ci.cst_lastname AS lastname,
ci.cst_marital_status  AS marital_status,
CASE 
	WHEN ci.cst_gndr<>'N/A' THEN ci.cst_gndr -- CRM IS THE PRIMARY SOURCE FOR gender
    ELSE COALESCE(ca.gen,'N/A')
END AS gender,
ca.bdate AS birthdate,
la.cntry AS country  ,
ci.cst_create_date AS create_date
FROM
silver.crm_cust_info ci
LEFT JOIN
silver.erp_cust_az12 ca
ON ci.cst_key=ca.cid
LEFT JOIN
silver.erp_loc_a101 la
ON ci.cst_key=la.cid
);



-- ===================================================================
--         Create dimension:gold.dim_products
-- ===================================================================
DROP VIEW IF EXISTS gold.dim_products;
CREATE VIEW gold.dim_products AS
(
SELECT
ROW_NUMBER() OVER(ORDER BY prd_start_dt,prd_key) AS product_key,
pi.prd_id AS product_id,
pi.prd_key AS product_number,
pi.prd_nm AS product_name,
pi.cat_id AS category_id,
px.cat AS category,
px.subcat AS sub_category,
px.maintenance,
pi.prd_cost AS cost,
pi.prd_line AS product_line,
pi.prd_start_dt AS start_date
FROM crm_prd_info pi
LEFT JOIN
silver.erp_px_cat_g1v2 px
ON pi.cat_id=px.id
WHERE pi.prd_end_dt IS NULL  -- Filters all historical data
) ;



-- ===================================================================
--           Create Fact Table:gold.fact_sales
-- ===================================================================
DROP VIEW IF EXISTS gold.fact_sales;
CREATE VIEW gold.fact_sales AS
(
SELECT 
sd.sls_ord_num AS order_number,
cu.customer_key AS customer_key,
pr.product_key AS product_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN
gold.dim_customers cu
ON sd.sls_cust_id=cu.customer_id
LEFT JOIN
gold.dim_products pr
ON sd.sls_prd_key=pr.product_number
);
