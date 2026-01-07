# Part 2: Data Cleaning

## Overview
This section focuses on cleaning and preparing data for analysis. The cleaning process ensures data consistency and accuracy.

## Cleaning Steps
- **Handle Missing Values**
  - Missing emails replaced with `unknown@example.com`.
  - Missing product descriptions filled with `Not Available`.
- **Remove Duplicates**
  - Exact duplicates removed from all tables.
- **Data Type Standardization**
  - Converted date columns to `YYYY-MM-DD`.
  - Ensured numeric columns (price, quantity) are integers/floats.

## How to Run
- Cleaning logic is integrated in `etl_pipeline.py`.
- After running ETL, verify cleaned data in SQLite:
  ```sql
  SELECT * FROM customers WHERE email = 'unknown@example.com';
