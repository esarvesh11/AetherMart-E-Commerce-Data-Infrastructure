-- =====================================================
-- AetherMart Triggers Implementation
-- Phase 1: Critical Business Logic Validation
-- Phase 2: Audit Trail Implementation
-- Database: MariaDB 11.8.3
-- =====================================================

USE AetherMart;

-- =====================================================
-- PHASE 1: CRITICAL BUSINESS LOGIC TRIGGERS
-- =====================================================

-- 1.3 Product Validation Trigger
-- Ensures products have valid prices and references
-- -----------------------------------------------------
DROP TRIGGER IF EXISTS validate_product_before_insert;

DELIMITER $$
CREATE TRIGGER validate_product_before_insert
    BEFORE INSERT ON products
    FOR EACH ROW
BEGIN
    -- Validate price is positive
    IF NEW.price <= 0 THEN
        SIGNAL SQLSTATE '45000' 
        SET MESSAGE_TEXT = 'Product price must be greater than zero';
    END IF;
    
    -- Validate price is reasonable (not excessive)
    IF NEW.price > 50000.00 THEN
        SIGNAL SQLSTATE '45000' 
        SET MESSAGE_TEXT = 'Product price exceeds maximum allowed limit of $50,000';
    END IF;
    
    -- Validate product name is not empty
    IF TRIM(NEW.product_name) = '' OR NEW.product_name IS NULL THEN
        SIGNAL SQLSTATE '45000' 
        SET MESSAGE_TEXT = 'Product name cannot be empty';
    END IF;
END$$
DELIMITER ;



-- =====================================================
-- PHASE 2: AUDIT TRAIL IMPLEMENTATION
-- =====================================================

-- -----------------------------------------------------
-- 2.1 Create Audit Tables
-- -----------------------------------------------------

-- Price change history table
CREATE TABLE IF NOT EXISTS price_history (
    history_id SERIAL PRIMARY KEY,
    product_id BIGINT UNSIGNED NOT NULL,
    old_price DECIMAL(10, 2),
    new_price DECIMAL(10, 2) NOT NULL,
    change_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    change_user VARCHAR(100) DEFAULT USER(),
    change_reason VARCHAR(255),
    CONSTRAINT fk_price_history_products FOREIGN KEY (product_id) REFERENCES products(product_id)
) ENGINE=InnoDB;


-- -----------------------------------------------------
-- 2.2 Product Price Change Audit Trigger
-- -----------------------------------------------------
DROP TRIGGER IF EXISTS audit_product_price_changes;

DELIMITER $$
CREATE TRIGGER audit_product_price_changes
    AFTER UPDATE ON products
    FOR EACH ROW
BEGIN
    -- Only log if price actually changed
    IF OLD.price != NEW.price THEN
        INSERT INTO price_history (
            product_id, 
            old_price, 
            new_price, 
            change_date,
            change_user
        )
        VALUES (
            NEW.product_id, 
            OLD.price, 
            NEW.price, 
            NOW(),
            USER()
        );
    END IF;
END$$
DELIMITER ;


-- Customer data change history
CREATE TABLE IF NOT EXISTS customer_audit (
    audit_id SERIAL PRIMARY KEY,
    customer_id BIGINT UNSIGNED NOT NULL,
    field_changed VARCHAR(50) NOT NULL,
    old_value TEXT,
    new_value TEXT,
    change_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    change_user VARCHAR(100) DEFAULT USER(),
    operation_type ENUM('INSERT', 'UPDATE', 'DELETE') NOT NULL,
    CONSTRAINT fk_customer_audit_customers FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
) ENGINE=InnoDB;

-- -----------------------------------------------------
-- 2.3 Customer Data Change Audit Triggers
-- -----------------------------------------------------

-- Customer INSERT audit
DROP TRIGGER IF EXISTS audit_customer_insert;

DELIMITER $$
CREATE TRIGGER audit_customer_insert
    AFTER INSERT ON customers
    FOR EACH ROW
BEGIN
    INSERT INTO customer_audit (customer_id, field_changed, old_value, new_value, operation_type)
    VALUES (NEW.customer_id, 'RECORD_CREATED', NULL, 
            CONCAT('first_name:', NEW.first_name, ', last_name:', NEW.last_name, ', email:', COALESCE(NEW.email, 'NULL')), 
            'INSERT');
END$$
DELIMITER ;

-- Customer UPDATE audit
DROP TRIGGER IF EXISTS audit_customer_update;

DELIMITER $$
CREATE TRIGGER audit_customer_update
    AFTER UPDATE ON customers
    FOR EACH ROW
BEGIN
    -- Log email changes
    IF OLD.email != NEW.email OR (OLD.email IS NULL AND NEW.email IS NOT NULL) OR (OLD.email IS NOT NULL AND NEW.email IS NULL) THEN
        INSERT INTO customer_audit (customer_id, field_changed, old_value, new_value, operation_type)
        VALUES (NEW.customer_id, 'email', OLD.email, NEW.email, 'UPDATE');
    END IF;
    
    -- Log address changes
    IF OLD.city != NEW.city OR OLD.state != NEW.state OR OLD.zipcode != NEW.zipcode THEN
        INSERT INTO customer_audit (customer_id, field_changed, old_value, new_value, operation_type)
        VALUES (NEW.customer_id, 'address', 
                CONCAT(COALESCE(OLD.city,''), ', ', COALESCE(OLD.state,''), ' ', COALESCE(OLD.zipcode,'')),
                CONCAT(COALESCE(NEW.city,''), ', ', COALESCE(NEW.state,''), ' ', COALESCE(NEW.zipcode,'')),
                'UPDATE');
    END IF;
END$$
DELIMITER ;