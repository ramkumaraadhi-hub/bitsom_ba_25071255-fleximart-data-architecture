-- warehouse_data.sql (MySQL)

INSERT INTO dim_customer (customer_id, full_name, email, phone, city)
SELECT
  c.customer_id,
  TRIM(CONCAT(IFNULL(c.first_name,''),' ',IFNULL(c.last_name,''))) AS full_name,
  c.email, c.phone, c.city
FROM customers_clean c
WHERE c.customer_id IS NOT NULL
ON DUPLICATE KEY UPDATE
  full_name = VALUES(full_name),
  email = VALUES(email),
  phone = VALUES(phone),
  city = VALUES(city);

INSERT INTO dim_product (product_id, name, category, price, currency, active)
SELECT
  p.product_id, p.name, p.category,
  IFNULL(p.price, 0), IFNULL(p.currency, 'INR'), IFNULL(p.active, TRUE)
FROM products_clean p
WHERE p.product_id IS NOT NULL
ON DUPLICATE KEY UPDATE
  name = VALUES(name),
  category = VALUES(category),
  price = VALUES(price),
  currency = VALUES(currency),
  active = VALUES(active);

INSERT INTO dim_date (date_id, day, month, year, quarter, month_name)
SELECT DISTINCT
  DATE(s.sale_ts) AS date_id,
  DAY(DATE(s.sale_ts)) AS day,
  MONTH(DATE(s.sale_ts)) AS month,
  YEAR(DATE(s.sale_ts)) AS year,
  QUARTER(DATE(s.sale_ts)) AS quarter,
  DATE_FORMAT(DATE(s.sale_ts), '%M') AS month_name
FROM sales_clean s
WHERE s.sale_ts IS NOT NULL
ON DUPLICATE KEY UPDATE
  day = VALUES(day),
  month = VALUES(month),
  year = VALUES(year),
  quarter = VALUES(quarter),
  month_name = VALUES(month_name);

INSERT INTO fact_sales (sale_id, customer_id, product_id, date_id, quantity, amount)
SELECT
  s.sale_id, s.customer_id, s.product_id,
  DATE(s.sale_ts) AS date_id,
  IFNULL(s.quantity, 1),
  IFNULL(s.amount, 0)
FROM sales_clean s
WHERE s.sale_id IS NOT NULL
ON DUPLICATE KEY UPDATE
  customer_id = VALUES(customer_id),
  product_id = VALUES(product_id),
  date_id = VALUES(date_id),
  quantity = VALUES(quantity),
  amount = VALUES(amount);
