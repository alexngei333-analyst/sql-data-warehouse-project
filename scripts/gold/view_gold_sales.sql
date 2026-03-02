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
ON		  f.sls_cust_id = c.customer_id