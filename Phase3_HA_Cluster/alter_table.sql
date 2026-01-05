-- =====================================================
-- Milestone 3: Add Vector Capabilities to aethermart_db1
-- Add vector columns for semantic search
-- =====================================================


CREATE USER 'student_user'@'54.224.4.94' IDENTIFIED BY 'StudentPass123!';

GRANT ALL PRIVILEGES ON aethermart_db1.* TO 'student_user'@'54.224.4.94';

FLUSH PRIVILEGES;





USE aethermart_db1;

-- =====================================================
ADD VECTOR COLUMNS TO PRODUCTS TABLE
-- =====================================================

-- Add description and embedding columns to products
ALTER TABLE products 
ADD COLUMN IF NOT EXISTS product_description TEXT;

ALTER TABLE products 
ADD COLUMN IF NOT EXISTS product_embedding VECTOR(768);

-- Verify columns added
DESCRIBE products;

-- =====================================================
ALTER TABLE products MODIFY product_embedding VECTOR(768) NOT NULL;
ALTER TABLE products ADD VECTOR INDEX idx_prod_embedding (product_embedding) M=16 DISTANCE=cosine;