-- This script handles the final 'Transform' and 'Verify' phases for Milestone 1.

USE aethermart_db1;

-- ----------------------------------------------------
-- Transformation Step 1: Clean Empty Emails in 'customers'
-- ----------------------------------------------------
-- Convert empty strings ('') for emails into proper NULL values.
UPDATE customers
SET email = NULL
WHERE email = '';


-- ----------------------------------------------------
-- Transformation Step 2: Standardize 'orders.order_date' Formats
-- ----------------------------------------------------
-- Directly transform 'order_date' to standard DATE format.

UPDATE orders
SET order_date =
    CASE
        -- YYYY-MM-DD
        WHEN order_date LIKE '____-__-__' THEN STR_TO_DATE(order_date, '%Y-%m-%d')
        
        -- MM-DD-YYYY
        WHEN order_date LIKE '__-__-____' THEN STR_TO_DATE(order_date, '%m-%d-%Y')
        
        -- MM/DD/YYYY
        WHEN order_date LIKE '__/__/____' THEN STR_TO_DATE(order_date, '%m/%d/%Y')
        
        ELSE NULL
    END;


-- ----------------------------------------------------
-- Transformation Step 3: Clean and Convert 'reviews.rating'
-- ----------------------------------------------------
-- Convert valid numeric ratings, set all invalid/malformed values to NULL.

UPDATE reviews
SET rating =
    CASE
        WHEN rating RLIKE '^[0-9]+$' THEN CAST(rating AS UNSIGNED)
        ELSE NULL
    END;

