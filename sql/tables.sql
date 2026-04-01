CREATE TABLE IF NOT EXISTS stores (
    store_id SERIAL PRIMARY KEY,
    store_name VARCHAR(50),
    store_address VARCHAR(100),
    store_city VARCHAR(50),
    store_state VARCHAR(10),
    store_zip VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS vendors (
    vendor_id SERIAL PRIMARY KEY,
    vendor_name VARCHAR(100) UNIQUE,
    vendor_phone VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS products (
    product_upc VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(100),
    brand VARCHAR(50),
    department_name VARCHAR(50),
    category_name VARCHAR(50),
    size INT,
    unit VARCHAR(10),
    pack_size INT,
    regular_price DECIMAL(10,2),
    unit_cost DECIMAL(10,2),
    vendor_id INT,

    FOREIGN KEY (vendor_id) REFERENCES vendors(vendor_id)
);

CREATE TABLE IF NOT EXISTS transactions (
    receipt_id VARCHAR(50) PRIMARY KEY,
    store_id INT,
    sale_datetime TIMESTAMP,
    cashier_name VARCHAR(50),
    tender_type VARCHAR(50),
    customer_segment VARCHAR(50),

    FOREIGN KEY (store_id) REFERENCES stores(store_id)
);

CREATE TABLE IF NOT EXISTS line_items (
    line_id BIGSERIAL PRIMARY KEY,
    receipt_id VARCHAR(50),
    product_upc VARCHAR(20),
    line_number INT,
    quantity INT,
    unit_price_effective DECIMAL(10,2),
    line_subtotal DECIMAL(10,2),
    tax_amount DECIMAL(10,2),

    FOREIGN KEY (receipt_id) REFERENCES transactions(receipt_id),
    FOREIGN KEY (product_upc) REFERENCES products(product_upc)
);

CREATE TABLE IF NOT EXISTS promotions (
    promo_id VARCHAR(50) PRIMARY KEY,
    product_upc VARCHAR(20),
    promo_type VARCHAR(50),
    discount_percent DECIMAL(5,2),
    start_date DATE,
    end_date DATE,
    duration INT,

    FOREIGN KEY (product_upc) REFERENCES products(product_upc)
);

CREATE TABLE IF NOT EXISTS inventory_snapshots (
    snapshot_id BIGSERIAL PRIMARY KEY,
    snapshot_date DATE,
    store_id INT,
    product_upc VARCHAR(20),
    on_hand_qty INT,
    inventory_cost_value DECIMAL(12,2),

    FOREIGN KEY (store_id) REFERENCES stores(store_id),
    FOREIGN KEY (product_upc) REFERENCES products(product_upc)
);