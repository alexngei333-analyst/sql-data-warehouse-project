/*
===================================================================================
Data Analysis -> For Reporting
===================================================================================
Script Purpose:
	This script analyses data by combining dufferent tables and aggregating to 
	show trend over time analysis, quantitative analysis among other aggregations.
	Run each query to get different trends in the grocery sales database.
	These results can be used in decision making for future investments by the 
	management.
*/

-- category by total_sales
SELECT
	p.category_id,
	p.category_name,
	ROUND(SUM(f.total_sales), 2) AS total_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON		  f.product_key = p.product_key
GROUP BY
	p.category_id,
	p.category_name
ORDER BY ROUND(SUM(f.total_sales), 2) DESC

-- employee by total_sales
SELECT
	e.employee_key,
	e.full_name,
	ROUND(SUM(f.total_sales), 2) AS total_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_employees e
ON		  f.employee_key = e.employee_key
GROUP BY e.full_name
ORDER BY 2,3 DESC

-- customer by total_sales
SELECT
	c.customer_key,
	c.full_name,
	ROUND(SUM(f.total_sales), 2) AS total_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON		  f.customer_key = c.customer_key
GROUP BY
	c.customer_key,
	c.full_name
ORDER BY 2,3 DESC

-- gender by sales_count
-- ordered to show the top performing cities downwards
SELECT
	c.city_id,
	c.city_name,
	COUNT(f.sales_id) AS sales_count
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON		  f.customer_key = c.customer_key
GROUP BY
	c.city_id,
	c.city_name
ORDER BY 3 DESC

-- class by total_sales
SELECT
	p.class,
	ROUND(SUM(f.total_sales), 2) AS total_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON		  f.product_key = p.product_key
GROUP BY
	p.class
ORDER BY 2 DESC