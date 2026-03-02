
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
		 ELSE COALESCE(cu.gen, 'n/a')
	END gender,
	ca.cntry AS country,
	cu.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 cu
ON		  cu.cust_key = ci.cst_key
LEFT JOIN silver.erp_loc_a101 ca
ON		  ca.cid = ci.cst_key;
GO

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
	DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER (ORDER BY prd_id) AS product_key,
	pr.prd_id AS product_id,
	pr.prd_key AS product_number,
	pr.prd_nm AS product_name,
	pr.cat_id AS category_id,
	px.cat AS category_name,
	px.subcat AS sub_category,
	px.maintenance,
	pr.prd_cost AS cost,
	pr.prd_line AS product_line,
	pr.prd_start_dt AS start_date
FROM silver.crm_prd_info pr
LEFT JOIN Silver.erp_px_cat_g1v2 px
ON		  pr.cat_id = px.id
WHERE pr.prd_end_dt IS NULL;
GO

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
	DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
	f.sls_ord_num AS order_number,
	p.product_key,
	c.customer_key,
	f.sls_order_dt AS order_date,
	f.sls_ship_dt AS shipping_date,
	f.sls_due_dt AS due_date,
	f.sls_sales AS sales_amount,
	f.sls_quantity AS quantity,
	f.sls_price AS price
FROM silver.crm_sales_details f
LEFT JOIN gold.dim_products p
ON		  f.sls_prd_key = p.product_number
LEFT JOIN gold.dim_customers c
ON		  f.sls_cust_id = c.customer_id;
GO