# FlexiMart Relational Schema Documentation (Part 1.2)

## Entity–Relationship Description (Text Format)

### ENTITY: customers
**Purpose:** Stores customer master information.

**Attributes:**
- `customer_id` (PK): Unique identifier for each customer.
- `first_name`: Customer's first name.
- `last_name`: Customer's last name.
- `email`: Contact email (unique where available).
- `phone`: Contact number.
- `city`: City of residence.
- `created_at`: When the customer record was created.

**Relationships:**
- One **customer** can place **MANY** orders — 1:M with `orders(customer_id)`.

---

### ENTITY: orders
**Purpose:** Captures each purchase event by a customer.

**Attributes:**
- `order_id` (PK): Unique identifier for each order.
- `customer_id` (FK → customers.customer_id): Who placed the order.
- `order_date`: Timestamp/date of the order.
- `status`: Lifecycle state (e.g., Pending, Completed, Cancelled).
- `payment_method`: Channel used (e.g., UPI, Card, NetBanking).
- *(Optional)* `order_total`: Derived from sum of order_items; typically not stored in 3NF.

**Relationships:**
- One **order** contains **MANY** line items — 1:M with `order_items(order_id)`.

---

### ENTITY: order_items
**Purpose:** Line-level details within an order.

**Attributes:**
- `order_item_id` (PK): Surrogate key for the line item.
- `order_id` (FK → orders.order_id): Parent order reference.
- `product_id` (FK → products.product_id): SKU purchased.
- `quantity`: Units purchased.
- `unit_price`: Unit selling price at time of order (₹).
- `line_amount`: `quantity * unit_price` (stored for performance; otherwise derived).

**Relationships:**
- Each line item references **ONE** order and **ONE** product (M:1 to both).

---

### ENTITY: products
**Purpose:** Product catalog master.

**Attributes:**
- `product_id` (PK): Unique product identifier.
- `name`: Product name.
- `category`: Category (e.g., Accessories, Electronics).
- `price`: Current list price (₹).
- `currency`: ISO currency code (e.g., INR).
- `active`: Boolean indicating if product is currently sold.
- `created_at`: When the product record was created.

**Relationships:**
- One **product** can appear in **MANY** order_items — 1:M with `order_items(product_id)`.

---

## Normalization Explanation (3NF)
This design conforms to **Third Normal Form (3NF)** because every non-key attribute depends on the key, the whole key, and nothing but the key in its respective table. In `customers`, attributes such as `first_name`, `last_name`, `email`, `phone`, and `city` are fully functionally dependent on `customer_id`. In `orders`, `customer_id`, `order_date`, `status`, and `payment_method` depend solely on `order_id`. In `order_items`, all descriptive columns (`order_id`, `product_id`, `quantity`, `unit_price`, `line_amount`) depend on the surrogate key `order_item_id` (or on the natural composite key `(order_id, product_id)` if chosen). In `products`, `name`, `category`, `price`, `currency`, `active`, and `created_at` depend on `product_id`.

By separating masters (`customers`, `products`) from transactions (`orders`, `order_items`), we avoid **update anomalies** (e.g., changing a product price in one place), **insert anomalies** (e.g., adding a product before any order exists), and **delete anomalies** (e.g., deleting an order does not remove product details). Derived values like `order_total` and `line_amount` can be computed from `order_items`, ensuring attributes are not dependent on non-key columns. Any candidate transitive dependencies (e.g., `city` → `state`) are not present in this schema; if introduced, they should be factored into separate lookup tables. This structure supports consistent analytics and simplifies constraints, indexing, and query performance.

### Functional Dependencies
- `customers`: `customer_id → first_name, last_name, email, phone, city, created_at`
- `orders`: `order_id → customer_id, order_date, status, payment_method`
- `order_items` (surrogate key design): `order_item_id → order_id, product_id, quantity, unit_price, line_amount`
- `order_items` (composite key alternative): `(order_id, product_id) → quantity, unit_price, line_amount`
- `products`: `product_id → name, category, price, currency, active, created_at`

### How anomalies are avoided
- **Update anomaly:** Product details and customer attributes exist only in their master tables; changes occur in one place.
- **Insert anomaly:** New products/customers can be inserted without requiring an order; transactional tables reference existing masters via FKs.
- **Delete anomaly:** Deleting an order does not delete product or customer master data; cascades are controlled via FK rules.

---

## Sample Data Representation
*(All amounts in ₹ INR; dates in ISO `YYYY-MM-DD`)*

### customers (sample)
| customer_id | first_name | last_name | email                     | phone      | city   | created_at  |
|-------------|------------|-----------|---------------------------|------------|--------|-------------|
| C001        | Ananya     | Sharma    | ananya.sharma@example.com | 9876501234 | Pune   | 2024-01-05  |
| C002        | Ravi       | Kumar     | ravi.kumar@example.com    | 9898012345 | Chennai| 2024-02-02  |
| C003        | Priya      | Iyer      | priya.iyer@example.com    | 9723412345 | Mumbai | 2024-03-10  |

### products (sample)
| product_id | name                | category    | price | currency | active | created_at  |
|------------|---------------------|-------------|-------|----------|--------|-------------|
| P100       | Wireless Mouse      | Accessories | 799   | INR      | TRUE   | 2024-01-01  |
| P200       | Mechanical Keyboard | Accessories | 3499  | INR      | TRUE   | 2024-01-01  |
| P300       | 24" Monitor         | Electronics | 10999 | INR      | TRUE   | 2024-01-01  |

### orders (sample)
| order_id | customer_id | order_date  | status    | payment_method |
|----------|-------------|-------------|-----------|----------------|
| O5001    | C001        | 2024-04-12  | Completed | UPI            |
| O5002    | C001        | 2024-06-18  | Completed | CreditCard     |
| O5003    | C002        | 2024-07-03  | Completed | NetBanking     |

### order_items (sample)
| order_item_id | order_id | product_id | quantity | unit_price | line_amount |
|---------------|----------|------------|----------|------------|-------------|
| OI9001        | O5001    | P100       | 2        | 799        | 1598        |
| OI9002        | O5001    | P200       | 1        | 3499       | 3499        |
| OI9003        | O5002    | P300       | 1        | 10999      | 10999       |
| OI9004        | O5003    | P100       | 3        | 799        | 2397        |
| OI9005        | O5003    | P200       | 2        | 3499       | 6998        |
