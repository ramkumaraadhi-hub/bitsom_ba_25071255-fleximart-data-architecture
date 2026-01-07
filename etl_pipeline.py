# -*- coding: utf-8 -*-
"""
FlexiMart - ETL Pipeline (Task 1.1)
- Reads 3 CSVs (customers, products, sales) from ./data/
- Cleans per assignment:
  * Dedupe rows
  * Handle missing values (emails, prices, stock)
  * Standardize phone to +91-XXXXXXXXXX
  * Normalize categories (Electronics, Fashion, Groceries)
  * Convert dates to YYYY-MM-DD
- Loads into SQLite (portable in Colab)
- Generates ./reports/data_quality_report.txt
"""

import os
import re
import math
from datetime import datetime
import argparse

import pandas as pd
import sqlite3

# ----------------------------
# Helpers
# ----------------------------
PHONE_RE = re.compile(r"\D+")

def normalize_phone(phone):
    if pd.isna(phone): return None
    digits = PHONE_RE.sub("", str(phone).strip())
    if digits.startswith("91") and len(digits) == 12:
        digits = digits[-10:]
    if digits.startswith("0"):
        digits = digits[1:]
    return f"+91-{digits}" if len(digits) == 10 else None

DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y", "%m-%d-%Y")

def parse_date(s):
    if pd.isna(s) or str(s).strip() == "":
        return None
    t = str(s).strip()
    try:
        return pd.to_datetime(t, infer_datetime_format=True, dayfirst=False, errors="raise").strftime("%Y-%m-%d")
    except Exception:
        pass
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(t, fmt).strftime("%Y-%m-%d")
        except Exception:
            continue
    return None

def normalize_category(cat):
    if pd.isna(cat) or str(cat).strip() == "":
        return "Unknown"
    mapping = {"electronics": "Electronics", "fashion": "Fashion", "groceries": "Groceries"}
    return mapping.get(str(cat).strip().lower(), str(cat).strip().title())

def gen_placeholder_email(raw_id, idx):
    base = str(raw_id).strip() if pd.notna(raw_id) and str(raw_id).strip() != "" else f"row{idx}"
    return f"unknown+{base}@fleximart.com"

# ----------------------------
# Transform: Customers (PATCHED)
# ----------------------------
def etl_customers(path):
    df = pd.read_csv(path)
    original = len(df)

    # Trim safe text fields first (do NOT touch 'email' yet)
    for c in ["first_name", "last_name", "phone", "city", "registration_date"]:
        df[c] = df[c].astype(str).str.strip()

    # Normalize phones, dates
    df["phone"] = df["phone"].apply(normalize_phone)
    df["registration_date"] = df["registration_date"].apply(parse_date)

    # Exact dedupe
    df = df.drop_duplicates()
    after_exact = len(df)

    # Detect missing emails BEFORE casting to str
    missing_email_before = int(df["email"].isna().sum()) + int((df["email"].fillna("").str.strip() == "").sum())

    # Fill placeholders for missing emails
    df["email"] = df.apply(
        lambda r: r["email"] if (pd.notna(r["email"]) and str(r["email"]).strip() != "")
        else gen_placeholder_email(r.get("customer_id"), r.name),
        axis=1
    )
    # Now trim emails
    df["email"] = df["email"].astype(str).str.strip()

    # Deduplicate by email (keep first)
    df = df.drop_duplicates(subset=["email"], keep="first")
    final = len(df)

    metrics = {
        "customers_processed": original,
        "customers_exact_dupes_removed": original - after_exact,
        "customers_missing_emails_filled": missing_email_before,
        "customers_loaded": final
    }
    return df, metrics

# ----------------------------
# Transform: Products
# ----------------------------
def etl_products(path):
    df = pd.read_csv(path)
    original = len(df)
    df["product_name"] = df["product_name"].astype(str).str.strip()
    df["category"] = df["category"].apply(normalize_category)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["stock_quantity"] = pd.to_numeric(df["stock_quantity"], errors="coerce")

    missing_price_before = int(df["price"].isna().sum())
    cat_median = df.groupby("category")["price"].median()
    global_median = df["price"].median()

    def impute_price(row):
        if pd.notna(row["price"]): return row["price"]
        med = cat_median.get(row["category"])
        return global_median if (med is None or (isinstance(med, float) and math.isnan(med))) else med
    df["price"] = df.apply(impute_price, axis=1)

    missing_stock_before = int(df["stock_quantity"].isna().sum())
    df["stock_quantity"] = df["stock_quantity"].fillna(0).astype(int)

    df = df.drop_duplicates(subset=["product_name", "category"], keep="first")
    final = len(df)

    metrics = {
        "products_processed": original,
        "products_missing_price_imputed": missing_price_before,
        "products_missing_stock_defaulted": missing_stock_before,
        "products_loaded": final
    }
    return df, metrics

# ----------------------------
# Transform: Sales → Orders + Order Items (PATCHED validation)
# ----------------------------
def etl_sales(path, customers_df, products_df):
    df = pd.read_csv(path)
    original = len(df)

    # Only strip certain string columns
    df["transaction_id"] = df["transaction_id"].astype(str).str.strip()
    df["status"] = df["status"].astype(str).str.strip()
    df["customer_id"] = df["customer_id"].astype(str).str.strip()
    df["product_id"]  = df["product_id"].astype(str).str.strip()

    df["quantity"]   = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")

    df = df.drop_duplicates()
    df = df.drop_duplicates(subset=["transaction_id"], keep="first")
    after_dupes = len(df)

    df["transaction_date"] = df["transaction_date"].apply(parse_date)

    valid_customers = set(customers_df["customer_id"].astype(str))
    valid_products  = set(products_df["product_id"].astype(str))

    def row_valid(r):
        return (
            r["customer_id"] in valid_customers and
            r["product_id"]  in valid_products  and
            pd.notna(r["transaction_date"])    and
            pd.notna(r["quantity"])            and
            pd.notna(r["unit_price"])
        )

    df_valid = df[df.apply(row_valid, axis=1)].copy()
    dropped = original - len(df_valid)

    df_valid["subtotal"] = (df_valid["quantity"] * df_valid["unit_price"]).round(2)

    orders = (df_valid[["transaction_id", "customer_id", "transaction_date", "status", "subtotal"]]
              .groupby(["transaction_id", "customer_id", "transaction_date", "status"], as_index=False)
              .agg(total_amount=("subtotal", "sum")))

    order_items = df_valid.rename(columns={
        "transaction_id": "order_id_external",
        "product_id": "product_id_external"
    })[["order_id_external", "product_id_external", "quantity", "unit_price", "subtotal"]]

    metrics = {
        "sales_processed": original,
        "sales_dupes_removed": original - after_dupes,
        "sales_fk_missing_dropped": dropped,
        "orders_to_load": len(orders),
        "order_items_to_load": len(order_items)
    }
    return orders, order_items, metrics

# ----------------------------
# DB (SQLite) & Schema (PATCHED with unique index)
# ----------------------------
def get_sqlite_conn(db_path):
    return sqlite3.connect(db_path)

def ensure_schema(conn):
    cur = conn.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS customers (
        customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL,
        phone TEXT,
        city TEXT,
        registration_date DATE
    );

    CREATE TABLE IF NOT EXISTS products (
        product_id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_name TEXT NOT NULL,
        category TEXT NOT NULL,
        price REAL NOT NULL,
        stock_quantity INTEGER DEFAULT 0
    );

    -- unique index to allow upsert by name+category
    CREATE UNIQUE INDEX IF NOT EXISTS ux_products_name_cat
    ON products (product_name, category);

    CREATE TABLE IF NOT EXISTS orders (
        order_id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER NOT NULL,
        order_date DATE NOT NULL,
        total_amount REAL NOT NULL,
        status TEXT DEFAULT 'Pending',
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
    );

    CREATE TABLE IF NOT EXISTS order_items (
        order_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id INTEGER NOT NULL,
        product_id INTEGER NOT NULL,
        quantity INTEGER NOT NULL,
        unit_price REAL NOT NULL,
        subtotal REAL NOT NULL,
        FOREIGN KEY (order_id) REFERENCES orders(order_id),
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    );
    """)
    conn.commit()
    cur.close()

# ----------------------------
# Loaders (PATCHED to UPSERT)
# ----------------------------
def load_customers(conn, df):
    cur = conn.cursor()
    for _, r in df.iterrows():
        cur.execute("""
        INSERT INTO customers (first_name, last_name, email, phone, city, registration_date)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(email) DO UPDATE SET
          phone=excluded.phone,
          city=excluded.city,
          registration_date=excluded.registration_date
        """, (r["first_name"], r["last_name"], r["email"], r["phone"], r["city"], r["registration_date"]))
    conn.commit(); cur.close()

def load_products(conn, df):
    cur = conn.cursor()
    for _, r in df.iterrows():
        cur.execute("""
        INSERT INTO products (product_name, category, price, stock_quantity)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(product_name, category) DO UPDATE SET
          price=excluded.price,
          stock_quantity=excluded.stock_quantity
        """, (r["product_name"], r["category"], float(r["price"]), int(r["stock_quantity"])))
    conn.commit(); cur.close()

def load_orders_and_items(conn, orders_df, items_df, customers_df, products_df):
    cur = conn.cursor()

    # Lookup maps AFTER insert
    cur.execute("SELECT customer_id, email FROM customers")
    cust_map = {email: cid for (cid, email) in cur.fetchall()}

    cur.execute("SELECT product_id, product_name, category FROM products")
    prod_map = {(name, category): pid for (pid, name, category) in cur.fetchall()}

    # external product code -> (name, category)
    ext_prod_to_name_cat = {r["product_id"]: (r["product_name"], r["category"]) for _, r in products_df.iterrows()}

    # Insert orders, map external txn -> real order_id
    ext_to_orderid = {}
    for _, r in orders_df.iterrows():
        raw_customer_id = r["customer_id"]
        cust_email = customers_df.loc[customers_df["customer_id"] == raw_customer_id, "email"].values[0]
        real_cust_id = cust_map.get(cust_email)
        if real_cust_id is None:
            continue
        cur.execute("""
        INSERT INTO orders (customer_id, order_date, total_amount, status)
        VALUES (?, ?, ?, ?)
        """, (real_cust_id, r["transaction_date"], float(r["total_amount"]), r["status"]))
        ext_to_orderid[r["transaction_id"]] = cur.lastrowid

    # Insert order items
    for _, r in items_df.iterrows():
        order_id = ext_to_orderid.get(r["order_id_external"])
        name_cat = ext_prod_to_name_cat.get(r["product_id_external"])
        if order_id is None or name_cat is None:
            continue
        product_id = prod_map.get(name_cat)
        if product_id is None:
            continue
        cur.execute("""
        INSERT INTO order_items (order_id, product_id, quantity, unit_price, subtotal)
        VALUES (?, ?, ?, ?, ?)
        """, (order_id, product_id, int(r["quantity"]), float(r["unit_price"]), float(r["subtotal"])))
    conn.commit(); cur.close()

# ----------------------------
# Report
# ----------------------------
def write_report(metrics, out_path):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    lines = [
        "FlexiMart Data Quality Report",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "Customers:",
        f"  Records processed: {metrics.get('customers_processed', 0)}",
        f"  Exact duplicates removed: {metrics.get('customers_exact_dupes_removed', 0)}",
        f"  Missing emails handled: {metrics.get('customers_missing_emails_filled', 0)}",
        f"  Final records loaded: {metrics.get('customers_loaded', 0)}",
        "",
        "Products:",
        f"  Records processed: {metrics.get('products_processed', 0)}",
        f"  Missing prices imputed: {metrics.get('products_missing_price_imputed', 0)}",
        f"  Missing stock defaulted: {metrics.get('products_missing_stock_defaulted', 0)}",
        f"  Final records loaded: {metrics.get('products_loaded', 0)}",
        "",
        "Sales:",
        f"  Records processed: {metrics.get('sales_processed', 0)}",
        f"  Duplicate transactions removed: {metrics.get('sales_dupes_removed', 0)}",
        f"  Rows dropped due to missing/invalid FKs or dates: {metrics.get('sales_fk_missing_dropped', 0)}",
        f"  Orders loaded: {metrics.get('orders_to_load', 0)}",
        f"  Order items loaded: {metrics.get('order_items_to_load', 0)}",
    ]
    with open(out_path, "w") as f:
        f.write("\n".join(lines))
    print(f"[INFO] Report written -> {out_path}")

# ----------------------------
# Main
# ----------------------------
def main(data_dir="./data", db_path="./fleximart.db", report_path="./reports/data_quality_report.txt"):
    cust_path = os.path.join(data_dir, "customers_raw.csv")
    prod_path = os.path.join(data_dir, "products_raw.csv")
    sales_path = os.path.join(data_dir, "sales_raw.csv")
    for p in [cust_path, prod_path, sales_path]:
        if not os.path.exists(p):
            raise FileNotFoundError(f"Missing data file: {p}")

    customers_df, m_cust = etl_customers(cust_path)
    products_df, m_prod = etl_products(prod_path)
    orders_df, items_df, m_sales = etl_sales(sales_path, customers_df, products_df)

    conn = get_sqlite_conn(db_path)
    ensure_schema(conn)
    load_customers(conn, customers_df)
    load_products(conn, products_df)
    load_orders_and_items(conn, orders_df, items_df, customers_df, products_df)

    metrics = {**m_cust, **m_prod, **m_sales}
    write_report(metrics, report_path)
    conn.close()
    print("[SUCCESS] ETL completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FlexiMart ETL Pipeline")
    parser.add_argument("--data_dir", default="./data", help="Directory containing raw CSVs")
    parser.add_argument("--db_path", default="./fleximart.db", help="SQLite DB file path")
    parser.add_argument("--report_path", default="./reports/data_quality_report.txt", help="Report output path")
    args = parser.parse_args()
    main(data_dir=args.data_dir, db_path=args.db_path, report_path=args.report_path)
