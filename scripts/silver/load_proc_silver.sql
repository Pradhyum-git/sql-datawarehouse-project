/*
--------------------------------------------------------------------
Stored Procedure : Load silver Layer (Bronze->Silver)
--------------------------------------------------------------------
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    load data into the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
*/
DELIMITER $$
DROP PROCEDURE silver.load_silver;
CREATE PROCEDURE silver.load_silver()
BEGIN
	DECLARE procedure_start_time,procedure_end_time DATETIME;
    DECLARE cust_time,prd_time,sales_time,az12_time,a101_time,g1v2_time DATETIME;
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        SELECT 'Error occurred during execution' AS msg;
        RESIGNAL;
    END;
    START TRANSACTION;
		
		SELECT 'Loading silver Layer' AS msg;
        SELECT 'Loading CRM Tables' AS msg;
		SET procedure_start_time=NOW();
        SET cust_time=NOW();
		SELECT 'Truncating Table silver.crm_cust_info' AS msg;
		TRUNCATE TABLE silver.crm_cust_info;
		SELECT 'Inserting Data into the silver.crm_cust_info' as msg;
		-- Data Transformation
		INSERT INTO silver.crm_cust_info(cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)(SELECT
		cst_id,cst_key,
		TRIM(cst_firstname) AS cst_firstname,TRIM(cst_lastname) AS cst_lastname, -- Removing Unwanted Spaces
		CASE 
			WHEN  cst_marital_status='M' THEN 'Married'
			WHEN cst_marital_status='S' THEN 'single'
			ELSE 'N/A'
		END AS cst_marital_status, -- Data normalization / standardization
		CASE 
			WHEN cst_gndr='M' THEN 'Male'
			WHEN cst_gndr='F' THEN 'Female'
			ELSE 'N/A'
		END AS cst_gndr -- Data Normalization / Standardization
		,cst_create_date
		FROM
		( -- Removing duplicate values
			SELECT *,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn 
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		)t
		WHERE rn=1
		);
		SELECT CONCAT('Step 1 executed in ',TIMESTAMPDIFF(SECOND,cust_time,NOW())) AS msg;
        
        
		-- clean and load crm_prd_info
        SET prd_time=NOW();
		SELECT 'Truncate the silver.crm_prd_info table' AS msg;
		TRUNCATE TABLE silver.crm_prd_info;
		SELECT 'Insert into the silver.crm_prd_info table' AS msg;
		-- Data Transformation of products table
		Insert INTO silver.crm_prd_info(prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
		(
		SELECT
		prd_id,
		REPLACE(SUBSTR(prd_key,1,5),'-','_') AS cat_id, -- Extract category id
		SUBSTR(prd_key,7,LENGTH(prd_key)) AS prd_key, -- Extract product key
		prd_nm,
		COALESCE(prd_cost,0) AS prd_cost,  -- Handling missing values
		CASE TRIM(UPPER(prd_line))
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other sales'
			WHEN 'M' THEN 'Mountain'
			WHEN 'T' THEN 'Touring'
			ELSE 'N/A'
		END AS prd_line, -- Data normalization
		prd_start_dt,
		LEAD(prd_start_dt) OVER(PARTITION BY prd_key Order by prd_start_dt)-INTERVAL 1 DAY AS prd_end_date -- deriving new column
		FROM
		bronze.crm_prd_info
		);
		SELECT CONCAT('Step 2 executed in ',TIMESTAMPDIFF(SECOND,prd_time,NOW())) AS msg;

-- Clean and load sales details
		SET sales_time=NOW();
		SELECT 'Truncate the silver.crm_sales_details table' AS msg;
		TRUNCATE TABLE silver.crm_sales_details;
		SELECT 'Insert into the silver.crm_sales_details table' AS msg;
		-- Data cleaning and loading into silver's crm_sales_details
		INSERT INTO silver.crm_sales_details(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,
		sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)(
		SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt<=0 OR LENGTH(sls_order_dt)<>8 THEN null
			 ELSE CAST(sls_order_dt AS DATE)
		END sls_order_dt, -- casting into date and if the date is invalid  then replacing with null
		CASE WHEN sls_ship_dt<=0 OR LENGTH(sls_ship_dt)<>8 THEN null
			 ELSE CAST(sls_ship_dt AS DATE)
		END sls_ship_dt, -- if the ship date is invalid replacing with null and casting into date data type
		CASE WHEN sls_due_dt<=0 OR LENGTH(sls_due_dt)<>8 THEN null
			 ELSE CAST(sls_due_dt AS DATE)
		END sls_due_dt, -- if the ship date is invalid replacing with null and casting into date data type
		CASE WHEN sls_sales<>sls_quantity*ABS(sls_price) OR sls_sales<=0 OR sls_sales IS NULL 
			THEN ABS(sls_price)*sls_quantity
			ELSE sls_sales
		END AS sls_sales, -- Maintaing data consistency i.e., sls_sales=sls_price*sls_quantity 
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price<=0 
			THEN sls_sales/NULLIF(sls_quantity,0)
			ELSE sls_price
		END AS sls_price -- Derive price if original value is invalid
		FROM
		bronze.crm_sales_details
		);
		SELECT CONCAT('Step 3 executed in ',TIMESTAMPDIFF(SECOND,sales_time,NOW())) AS msg;
        SELECT 'Loading ERP Tables' AS msg;
		SET az12_time=NOW();
		SELECT 'Truncate the silver.erp_cust_az12 table' AS msg;
		TRUNCATE TABLE silver.erp_cust_az12;
		SELECT 'Insert into the silver.erp_cust_az12 table' AS msg;
		--  Clean and load the erp_cust_az12 table
		INSERT INTO silver.erp_cust_az12(cid,bdate,gen)
		( 
			SELECT 
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTR(cid,4,LENGTH(cid))  -- Remove NAS prefix if exists
				 ELSE cid
			END AS cid,
			CASE WHEN bdate>NOW() THEN null  
				 ELSE bdate
			END AS bdate,  -- makes null when the date of birth is not valid
			CASE WHEN UPPER(TRIM(gen)) IN ('M','Male') THEN  'Male' 
				WHEN UPPER(TRIM(gen)) IN ('F','Female') THEN 'Female'
				ELSE 'N/A'
			END as gen -- Normalize gender values and handle null values
			FROM bronze.erp_cust_az12
		);
        SELECT CONCAT('Step 4 executed in ',TIMESTAMPDIFF(SECOND,az12_time,NOW())) AS msg;
        
        SET a101_time=NOW();
		SELECT 'Truncate the silver.erp_loc_a101 table' AS msg;
		TRUNCATE TABLE silver.erp_loc_a101;
		SELECT 'Insert into the silver.erp_loc_a101 table' AS msg;
		-- Clean and load erp_loc_a101
		INSERT INTO silver.erp_loc_a101(cid,cntry)
		(
			SELECT 
			REPLACE(cid,'-','') AS cid, -- Removes - from customer id
			CASE 
				WHEN TRIM(cntry) IN ('US','USA','United States') THEN 'United States'
				WHEN TRIM(cntry)='Australia' THEN 'Australia'
				WHEN TRIM(cntry) IN ('DE','Germany') THEN 'Germany'
				WHEN TRIM(cntry)='Canada' THEN 'Canada'
				WHEN TRIM(cntry)='France' THEN 'France'
				WHEN TRIM(cntry)='United Kingdom' THEN 'United Kingdom'
				ELSE 'N/A'
			END AS cntry -- Normalize the country and handle missing values
			FROM
			bronze.erp_loc_a101
		);
		SELECT CONCAT('Step 5 executed in ',TIMESTAMPDIFF(SECOND,a101_time,NOW())) AS msg;

		SET g1v2_time=NOW();
		SELECT 'Truncate the silver.erp_px_cat_g1v2 table' AS msg;
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		SELECT 'Insert into the silver.erp_px_cat_g1v2 table' AS msg;
		-- Clean and load erp_px_cat_g1v2
		INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
		(	SELECT
			id,
			cat,
			subcat,
			maintenance
			FROM
			bronze.erp_px_cat_g1v2
		);
        SELECT CONCAT('Step 6 executed in ',TIMESTAMPDIFF(SECOND,g1v2_time,NOW())) AS msg;
	COMMIT;
    SELECT 'Procedure Completed Successfully' as msg;
    SELECT CONCAT('Whole Procedure executed in ',TIMESTAMPDIFF(SECOND,procedure_start_time,NOW()),'seconds') AS msg;
END
$$
DELIMITER ;
CALL silver.load_silver;  -- Execution
