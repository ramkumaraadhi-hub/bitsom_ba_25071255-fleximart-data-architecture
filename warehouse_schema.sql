-- warehouse_schema.sql (MySQL)

DROP TABLE IF EXISTS dim_customer;
CREATE TABLE dim_customer (
  customer_sk INT AUTO_INCREMENT PRIMARY KEY,
  customer_id VARCHAR(50) NOT NULL UNIQUE,
  full_name VARCHAR(200),
  email VARCHAR(200),
  phone VARCHAR(50),
  city VARCHAR(100)
);

DROP TABLE IF EXISTS dim_product;
CREATE TABLE dim_product (
  product_sk INT AUTO_INCREMENT PRIMARY KEY,
  product_id VARCHAR(50) NOT NULL UNIQUE,
  name VARCHAR(200),
  category VARCHAR(100),
  price DECIMAL(12,2),
  currency VARCHAR(10),
  active BOOLEAN
);

DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
  date_sk INT AUTO_INCREMENT PRIMARY KEY,
  date_id DATE NOT NULL UNIQUE,
  day INT,
  month INT,
  year INT,
  quarter INT,
  month_name VARCHAR(20)
);

DROP TABLE IF EXISTS fact_sales;
CREATE TABLE fact_sales (
  sales_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
  sale_id VARCHAR(50) NOT NULL UNIQUE,
  customer_id VARCHAR(50) NOT NULL,
  product_id VARCHAR(50) NOT NULL,
  date_id DATE NOT NULL,
  quantity DECIMAL(12,2),
  amount DECIMAL(12,2),
  INDEX idx_customer (customer_id),
  INDEX idx_product (product_id),
  INDEX idx_date (date_id)
);
