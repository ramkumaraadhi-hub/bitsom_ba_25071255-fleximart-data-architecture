# Part 3: Data Quality Report

## Overview
This section provides a summary of data quality checks performed after ETL and cleaning.

## Report Details
- **Duplicates Removed**
  - Customers: 1
  - Products: 0
  - Sales: 2
- **Missing Values Handled**
  - Emails: 10
  - Product descriptions: 5
- **Final Record Counts**
  - Customers: 26
  - Products: 15
  - Sales: 100

## How to Generate Report
- Run the final cell in Colab (`generate_report()` function).
- Output saved in `data_quality_report.txt`.

## Files in this Part
- `data_quality_report.txt` – Summary of data quality checks
