
-- -----------------------------------------------------
-- Step 1: Create the Database
-- -----------------------------------------------------
DROP DATABASE IF EXISTS aethermart_db;
CREATE DATABASE IF NOT EXISTS aethermart_db;

-- Use the newly created database
USE aethermart_db;

-- -----------------------------------------------------
-- Step 2: Define Table Schemas
-- -----------------------------------------------------

-- Table `categories`
CREATE TABLE IF NOT EXISTS categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL UNIQUE
); 

-- Table `suppliers`
CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id SERIAL PRIMARY KEY,
    supplier_name VARCHAR(150) NOT NULL,
    contact_email VARCHAR(100)
);

-- Table `customers`
CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100),
    registration_date DATE,
    city VARCHAR(100),
    state VARCHAR(50),
    zipcode VARCHAR(20)
);

-- Table `products`
CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    category_id BIGINT UNSIGNED,
    supplier_id BIGINT UNSIGNED,
    CONSTRAINT fk_products_categories FOREIGN KEY (category_id) REFERENCES categories(category_id),
    CONSTRAINT fk_products_suppliers FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
);



-- Table `orders`
CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    customer_id BIGINT UNSIGNED,
    order_date VARCHAR(50), -- To accommodate mixed date formats for later cleansing
    total_amount DECIMAL(12, 2),
    CONSTRAINT fk_orders_customers FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Table `order_items`
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id BIGINT UNSIGNED,
    product_id BIGINT UNSIGNED,
    quantity INT NOT NULL,
    price_per_unit DECIMAL(10, 2) NOT NULL,
    CONSTRAINT fk_order_items_orders FOREIGN KEY (order_id) REFERENCES orders(order_id),
    CONSTRAINT fk_order_items_products FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- Table `reviews`
CREATE TABLE IF NOT EXISTS reviews (
    review_id SERIAL PRIMARY KEY,
    product_id BIGINT UNSIGNED,
    customer_id BIGINT UNSIGNED,
    rating VARCHAR(10), -- To accommodate invalid ratings for later cleansing
    review_text TEXT,
    review_date DATE,
    CONSTRAINT fk_reviews_products FOREIGN KEY (product_id) REFERENCES products(product_id),
    CONSTRAINT fk_reviews_customers FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);