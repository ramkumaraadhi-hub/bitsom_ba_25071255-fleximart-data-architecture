-- analytics_queries.sql

SELECT
  SUM(amount) AS total_revenue,
  COUNT(DISTINCT sale_id) AS total_orders,
  COUNT(DISTINCT customer_id) AS unique_customers,
  COUNT(DISTINCT product_id) AS unique_products
FROM fact_sales;

SELECT
  fs.customer_id,
  dc.full_name,
  SUM(fs.amount) AS total_spent
FROM fact_sales fs
LEFT JOIN dim_customer dc ON dc.customer_id = fs.customer_id
GROUP BY fs.customer_id, dc.full_name
ORDER BY total_spent DESC
LIMIT 10;

SELECT
  dp.category,
  COUNT(DISTINCT dp.product_id) AS num_products,
  SUM(fs.quantity) AS total_qty,
  SUM(fs.amount) AS total_revenue
FROM fact_sales fs
LEFT JOIN dim_product dp ON dp.product_id = fs.product_id
GROUP BY dp.category
ORDER BY total_revenue DESC;

WITH month_rev AS (
  SELECT
    dd.year,
    dd.month,
    dd.month_name,
    SUM(fs.amount) AS monthly_revenue
  FROM fact_sales fs
  JOIN dim_date dd ON dd.date_id = fs.date_id
  GROUP BY dd.year, dd.month, dd.month_name
)
SELECT
  year,
  month_name,
  monthly_revenue,
  SUM(monthly_revenue) OVER (PARTITION BY year ORDER BY month
      ROWS UNBOUNDED PRECEDING) AS cumulative_revenue
FROM month_rev
ORDER BY year, month;

SELECT
  CASE
    WHEN dp.price < 1000 THEN '< 1k'
    WHEN dp.price < 5000 THEN '1k–5k'
    WHEN dp.price < 10000 THEN '5k–10k'
    ELSE '>= 10k'
  END AS price_band,
  COUNT(DISTINCT dp.product_id) AS num_products,
  SUM(fs.amount) AS revenue
FROM fact_sales fs
JOIN dim_product dp ON dp.product_id = fs.product_id
GROUP BY price_band
ORDER BY revenue DESC;
