/*
===========================================================================================
Stored Procedure: Load Silver Layer (Bronze to Silver)
===========================================================================================
Script Purpose:
	This script performs ETL (Extract,Transform, Load) process to load data from
	bronze schema tables into silver schema tables.
Actions:
	Truncates silver schema tables
	Loads transformed and cleansed data from bronze schema tables into silver schema tables
Parameters:
	None
	This stored procedure does not accept any parameters
Use Example:
	EXEC silver.load_silver
===========================================================================================
*/
	
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	BEGIN TRY
		DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
		SET @batch_start_time = GETDATE();
		PRINT '=============================================================================';
		PRINT 'Loading Silver Layer';
		PRINT '=============================================================================';
		PRINT '-----------------------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>Truncating table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT 'Inserting data into silver.crm_cus_info';
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)

		SELECT
			cst_id,
			TRIM(cst_key) cst_key,
			TRIM(cst_firstname) cst_firstname,
			TRIM(cst_lastname) cst_lastname,
			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				 ELSE 'n/a'
			END cst_marital_status,
			CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				 ELSE 'n/a'
			END cst_gndr,
			cst_create_date
		FROM (
		SELECT *,
		ROW_NUMBER()OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) rank_flag
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL)t
		WHERE rank_flag = 1
		SET @end_time = GETDATE();
		PRINT 'Loading duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>---------------------------';

		SET @start_time = GETDATE();
		PRINT '>>Truncating table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT 'Inserting data into silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt)
		SELECT
		prd_id,
		TRIM(REPLACE(SUBSTRING(prd_key, 1,5), '-', '_')) AS cat_id,
		TRIM(SUBSTRING(prd_key, 7,LEN(prd_key))) prd_key,
		TRIM(prd_nm) prd_nm,
		ISNULL(prd_cost, 0) prd_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN 'R' THEN 'Roads'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'M' THEN 'Mountain'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END prd_line,
		CAST(prd_start_dt AS DATE) prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info
		SET @end_time = GETDATE();
		PRINT 'Loading duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>--------------------------';

		SET @start_time = GETDATE();
		PRINT '>>Truncating table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT 'Inserting data into silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price)

		SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE)
		END AS sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * sls_price
				THEN sls_quantity * ABS(sls_price)
			 ELSE sls_sales
		END sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <= 0
				THEN sls_sales / NULLIF(sls_quantity, 0)
			 ELSE sls_price
		END sls_price
		FROM bronze.crm_sales_details
		SET @end_time = GETDATE();
		PRINT 'Loading duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>--------------------------';

		PRINT '-----------------------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-----------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>Truncating table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT 'Inserting data into silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (
			cust_key,
			bdate,
			gen
			)
		SELECT
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
				 ELSE cid
			END AS cust_key,
			bdate,
			CASE WHEN UPPER(TRIM(gen)) LIKE '%F%' THEN 'Female'
				 WHEN UPPER(TRIM(gen)) LIKE '%M%' THEN 'Male'
				 ELSE 'n/a'
			END gen
		FROM bronze.erp_cust_az12
		WHERE bdate < GETDATE()
		SET @end_time = GETDATE();
		PRINT 'Loading duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>--------------------------';
	
		SET @start_time = GETDATE();
		PRINT '>>Truncating table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT 'Inserting data into silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (
			cid,
			cntry)
		SELECT
			REPLACE(cid, '-', '') cid,
			CASE WHEN cntry LIKE '%DE%' THEN 'Germany'
				 WHEN cntry LIKE '%US%' THEN 'United States'
				 WHEN cntry IS NULL OR cntry = '' THEN 'n/a'
				 ELSE cntry
			END cntry
		FROM bronze.erp_loc_a101
		SET @end_time = GETDATE();
		PRINT 'Loading duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>--------------------------';

		SET @start_time = GETDATE();
		PRINT '>>Truncating table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT 'Inserting data into silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance)
		SELECT
			id,
			CASE WHEN TRIM(cat) IS NOT NULL THEN TRIM(cat)
			ELSE 'n/a'
			END AS cat,
			CASE WHEN TRIM(subcat) IS NOT NULL THEN TRIM(subcat)
			ELSE 'n/a'
			END AS subcat,
			CASE WHEN TRIM(maintenance) IS NOT NULL THEN TRIM(maintenance)
			ELSE 'n/a'
			END AS maintenance
		FROM bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE();
		PRINT 'Loading duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>--------------------------';
		PRINT 'Total Loading duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
	END TRY

	BEGIN CATCH
		PRINT '==================================================================';
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==================================================================';
	END CATCH
END