# Star Schema Design (Part 3)

## Fact Table: fact_sales
- **Grain:** One row per sale event
- **Measures:** quantity, amount
- **Keys:** customer_id, product_id, date_id

## Dimensions
### dim_customer
- Attributes: customer_id, full_name, email, phone, city

### dim_product
- Attributes: product_id, name, category, price, currency, active

### dim_date
- Attributes: date_id (DATE), day, month, year, quarter, month_name

## Rationale
A star schema simplifies analytics by centralizing numeric measures in `fact_sales` and linking them to descriptive attributes in dimensions. This supports revenue trends, category analysis, and customer segmentation while keeping queries simple and performant.
