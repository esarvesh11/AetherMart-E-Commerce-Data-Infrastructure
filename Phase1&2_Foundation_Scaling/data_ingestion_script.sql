USE aethermart_db1;

-- -----------------------------------------------------
-- Load data for `categories` table
-- -----------------------------------------------------
LOAD DATA LOCAL INFILE 'categories.csv'
INTO TABLE categories
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(category_id, category_name);

-- -----------------------------------------------------
-- Load data for `suppliers` table
-- -----------------------------------------------------
LOAD DATA LOCAL INFILE 'suppliers.csv'
INTO TABLE suppliers
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(supplier_id, supplier_name, contact_email);

-- -----------------------------------------------------
-- Load data for `products` table
-- -----------------------------------------------------
LOAD DATA LOCAL INFILE 'products.csv'
INTO TABLE products
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(product_id, product_name, price, category_id, supplier_id);

-- -----------------------------------------------------
-- Load data for `customers` table
-- -----------------------------------------------------
LOAD DATA LOCAL INFILE 'customers.csv'
INTO TABLE customers
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(customer_id, first_name, last_name, email, registration_date, city, state, zipcode);

-- -----------------------------------------------------
-- Load data for `orders` table
-- -----------------------------------------------------
LOAD DATA LOCAL INFILE 'orders.csv'
INTO TABLE orders
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(order_id, customer_id, order_date, total_amount);

-- -----------------------------------------------------
-- Load data for `order_items` table
-- -----------------------------------------------------
LOAD DATA LOCAL INFILE 'order_items.csv'
INTO TABLE order_items
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(order_item_id, order_id, product_id, quantity, price_per_unit);

-- -----------------------------------------------------
-- Load data for `reviews` table
-- -----------------------------------------------------
LOAD DATA LOCAL INFILE 'reviews.csv'
INTO TABLE reviews
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(review_id, product_id, customer_id, rating, review_text, review_date);
