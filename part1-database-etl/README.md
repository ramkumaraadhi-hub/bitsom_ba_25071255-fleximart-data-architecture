# Part 1: Database ETL

## Overview
This section covers the Extract, Transform, and Load (ETL) process for the FlexiMart dataset. The goal is to load raw CSV files into a SQLite database and prepare them for analysis.

## Steps Implemented
1. **Extract**
   - Source files: `customers_raw.csv`, `products_raw.csv`, `sales_raw.csv`
   - Loaded into Pandas DataFrames in Google Colab.

2. **Transform**
   - Removed duplicates.
   - Handled missing values (e.g., missing emails replaced with placeholder).
   - Standardized column names and data types.

3. **Load**
   - Created SQLite database: `fleximart.db`
   - Tables: `customers`, `products`, `sales`
   - Inserted cleaned data into respective tables.

## How to Run
- Open `etl_pipeline.py` in Google Colab.
- Ensure raw CSV files are in `/content/data/`.
- Run:
  ```bash
  python3 etl_pipeline.py
