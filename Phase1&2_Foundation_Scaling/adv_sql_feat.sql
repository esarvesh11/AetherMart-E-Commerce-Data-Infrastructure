-- This script fulfills the "Advanced SQL Features" requirement.
-- It implements a virtual column, views, stored procedures, and a user-defined function.

USE AetherMart;

-- -------------------------------------
-- VIRTUAL COLUMN
-- -------------------------------------
-- A virtual column automatically computes its value from other columns.
-- Here, we'll add a 'total_price' to the order_items table.
ALTER TABLE order_items
ADD COLUMN total_price DECIMAL(12, 2) AS (quantity * price_per_unit) VIRTUAL;


-- -------------------------------------
-- VIEWS
-- -------------------------------------
-- Views simplify complex queries, providing a secure and simple interface for users.

-- View 1: Simplified Product Information
-- This view joins products with their categories and suppliers for easy browsing.
CREATE OR REPLACE VIEW Vw_ProductDetails AS
SELECT
    p.product_id,
    p.product_name,
    c.category_name,
    s.supplier_name,
    p.price
FROM products p
JOIN categories c ON p.category_id = c.category_id
JOIN suppliers s ON p.supplier_id = s.supplier_id;


-- View 2: Customer Lifetime Value (LTV)
-- This view provides a pre-calculated summary of each customer's total spending.
CREATE OR REPLACE VIEW Vw_CustomerLTV AS
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    SUM(oi.total_price) AS lifetime_value
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email;


-- -------------------------------------
-- STORED PROCEDURES
-- -------------------------------------
-- Stored procedures encapsulate business logic on the database server.

-- Procedure 1: Get Order History for a Customer
-- This procedure takes a customer_id and returns their complete order history.
DELIMITER $$
CREATE PROCEDURE Sp_GetCustomerOrders(IN p_customer_id INT)
BEGIN
    SELECT
        o.order_id,
        o.order_date_clean,
        p.product_name,
        oi.quantity,
        oi.price_per_unit,
        oi.total_price
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    WHERE o.customer_id = p_customer_id
    ORDER BY o.order_date_clean DESC;
END$$
DELIMITER ;


-- Procedure 2: Add a New Product
-- This procedure safely adds a new product to the catalog.
DELIMITER $$
CREATE PROCEDURE Sp_AddNewProduct(
    IN p_product_name VARCHAR(255),
    IN p_price DECIMAL(10, 2),
    IN p_category_id INT,
    IN p_supplier_id INT
)
BEGIN
    INSERT INTO products (product_name, price, category_id, supplier_id)
    VALUES (p_product_name, p_price, p_category_id, p_supplier_id);
END$$
DELIMITER ;


-- -------------------------------------
-- USER-DEFINED FUNCTION (UDF)
-- -------------------------------------
-- UDFs allow for reusable custom logic within queries.

-- Function: Calculate Customer Loyalty Status
-- This function takes a registration date and returns a loyalty tier.
DELIMITER $$
CREATE FUNCTION Fn_GetCustomerLoyalty(p_reg_date DATE)
RETURNS VARCHAR(20)
DETERMINISTIC
BEGIN
    DECLARE days_registered INT;
    SET days_registered = DATEDIFF(CURDATE(), p_reg_date);

    IF days_registered > 730 THEN
        RETURN 'Gold Tier';
    ELSEIF days_registered > 365 THEN
        RETURN 'Silver Tier';
    ELSE
        RETURN 'Bronze Tier';
    END IF;
END$$
DELIMITER ;