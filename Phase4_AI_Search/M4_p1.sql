-- =====================================================
-- MILESTONE 4: COMPREHENSIVE PERFORMANCE ANALYSIS
-- MariaDB Cluster Troubleshooting & Optimization
-- Covers: Products, Customers, Orders, Order_Items, Reviews
-- =====================================================

-- =====================================================
-- SECTION 1: CLUSTER HEALTH CHECK (AWS EC2 Ubuntu)
-- =====================================================

-- Run on ALL nodes (Node1, Node2, Node3)
SHOW STATUS LIKE 'wsrep_cluster_size';
SHOW STATUS LIKE 'wsrep_cluster_status';
SHOW STATUS LIKE 'wsrep_ready';
SHOW STATUS LIKE 'wsrep_local_state_comment';
SHOW STATUS LIKE 'wsrep_connected';

-- Check replication lag
SHOW STATUS LIKE 'wsrep_flow_control_paused';
SHOW STATUS LIKE 'wsrep_local_recv_queue_avg';
SHOW STATUS LIKE 'wsrep_local_send_queue_avg';

-- =====================================================
-- SECTION 2: PERFORMANCE ANALYSIS - PRODUCT OPERATIONS
-- =====================================================

-- Query 1: Product Search with Category and Supplier (Common E-commerce Query)
EXPLAIN
SELECT 
    p.product_id,
    p.product_name,
    p.price,
    c.category_name,
    s.supplier_name,
    COUNT(r.review_id) as review_count,
    AVG(r.rating) as avg_rating
FROM products p
JOIN categories c ON p.category_id = c.category_id
JOIN suppliers s ON p.supplier_id = s.supplier_id
LEFT JOIN reviews r ON p.product_id = r.product_id
WHERE p.price BETWEEN 500 AND 2000
GROUP BY p.product_id, p.product_name, p.price, c.category_name, s.supplier_name
ORDER BY avg_rating DESC
LIMIT 20;

-- Optimization: Create composite indexes
CREATE INDEX idx_product_price ON products(price);
CREATE INDEX idx_review_product ON reviews(product_id, rating);

-- Query 2: Vector Similarity Search on Products
-- Find similar products using embeddings
EXPLAIN
SELECT 
    p.product_id,
    p.product_name,
    VEC_DISTANCE(p.product_embedding, (
        SELECT product_embedding 
        FROM products 
        WHERE product_id = 1
    )) as similarity_distance
FROM products p
WHERE p.product_embedding IS NOT NULL
ORDER BY similarity_distance ASC
LIMIT 10;

-- =====================================================
-- SECTION 3: PERFORMANCE ANALYSIS - CUSTOMER OPERATIONS
-- =====================================================

-- Query 3: Customer Order History with Product Details
EXPLAIN
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(o.total_amount) as lifetime_value,
    COUNT(DISTINCT oi.product_id) as unique_products_purchased,
    AVG(r.rating) as avg_review_rating
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
LEFT JOIN order_items oi ON o.order_id = oi.order_id
LEFT JOIN reviews r ON c.customer_id = r.customer_id
WHERE o.order_date >= DATE_SUB(NOW(), INTERVAL 1 YEAR)
GROUP BY c.customer_id, c.first_name, c.last_name, c.email
HAVING total_orders > 5
ORDER BY lifetime_value DESC
LIMIT 50;

-- Optimization: Create indexes for customer analytics
CREATE INDEX idx_order_customer_date ON orders(customer_id, order_date);
CREATE INDEX idx_orderitem_order ON order_items(order_id, product_id);
CREATE INDEX idx_review_customer ON reviews(customer_id, rating);

-- Query 4: Customer Segmentation by Purchase Behavior
EXPLAIN ANALYZE
SELECT 
    CASE 
        WHEN total_orders >= 10 THEN 'VIP'
        WHEN total_orders >= 5 THEN 'Regular'
        ELSE 'New'
    END as customer_segment,
    COUNT(*) as customer_count,
    AVG(lifetime_value) as avg_lifetime_value,
    AVG(avg_order_value) as avg_order_value
FROM (
    SELECT 
        c.customer_id,
        COUNT(o.order_id) as total_orders,
        SUM(o.total_amount) as lifetime_value,
        AVG(o.total_amount) as avg_order_value
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id
) customer_stats
GROUP BY customer_segment;

-- =====================================================
-- SECTION 4: PERFORMANCE ANALYSIS - ORDER OPERATIONS
-- =====================================================

-- Query 5: Order Processing - Join Orders with Order Items and Products
EXPLAIN ANALYZE
SELECT 
    o.order_id,
    o.order_date,
    o.order_status,
    c.first_name,
    c.last_name,
    c.email,
    COUNT(oi.order_item_id) as items_count,
    SUM(oi.quantity) as total_quantity,
    o.total_amount,
    GROUP_CONCAT(p.product_name SEPARATOR ', ') as products
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
WHERE o.order_date >= '2024-01-01'
GROUP BY o.order_id, o.order_date, o.order_status, c.first_name, c.last_name, c.email, o.total_amount
ORDER BY o.order_date DESC
LIMIT 100;

-- Optimization: Ensure proper indexing
CREATE INDEX idx_order_date_status ON orders(order_date, order_status);
CREATE INDEX idx_orderitem_product ON order_items(product_id);

-- Query 6: Sales Analytics - Revenue by Product Category
EXPLAIN ANALYZE
SELECT 
    c.category_name,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(oi.quantity) as units_sold,
    SUM(oi.quantity * oi.unit_price) as total_revenue,
    AVG(oi.unit_price) as avg_unit_price,
    COUNT(DISTINCT p.product_id) as products_in_category
FROM categories c
JOIN products p ON c.category_id = p.category_id
JOIN order_items oi ON p.product_id = oi.product_id
JOIN orders o ON oi.order_id = o.order_id
WHERE o.order_status != 'cancelled'
  AND o.order_date >= DATE_SUB(NOW(), INTERVAL 6 MONTH)
GROUP BY c.category_id, c.category_name
ORDER BY total_revenue DESC;

-- =====================================================
-- SECTION 5: PERFORMANCE ANALYSIS - REVIEW OPERATIONS
-- =====================================================

-- Query 7: Review Analysis with Vector Similarity
EXPLAIN ANALYZE
SELECT 
    r.review_id,
    p.product_name,
    c.first_name,
    c.last_name,
    r.rating,
    r.review_text,
    r.review_date
FROM reviews r
JOIN products p ON r.product_id = p.product_id
JOIN customers c ON r.customer_id = c.customer_id
WHERE r.review_text IS NOT NULL
  AND r.rating <= 2
ORDER BY r.review_date DESC
LIMIT 50;

-- Query 8: Vector Similarity Search on Reviews (Sentiment Analysis)
EXPLAIN ANALYZE
SELECT 
    r.review_id,
    p.product_name,
    r.rating,
    r.review_text,
    VEC_DISTANCE(r.review_embedding, (
        SELECT review_embedding 
        FROM reviews 
        WHERE review_id = 1 AND review_embedding IS NOT NULL
    )) as similarity_distance
FROM reviews r
JOIN products p ON r.product_id = p.product_id
WHERE r.review_embedding IS NOT NULL
ORDER BY similarity_distance ASC
LIMIT 10;

-- Optimization for review queries
CREATE INDEX idx_review_rating_date ON reviews(rating, review_date);
CREATE INDEX idx_review_product_rating ON reviews(product_id, rating);

-- =====================================================
-- SECTION 6: SYSTEM-LEVEL PERFORMANCE METRICS
-- =====================================================

-- Global Status Variables
SHOW GLOBAL STATUS LIKE 'Threads_connected';
SHOW GLOBAL STATUS LIKE 'Threads_running';
SHOW GLOBAL STATUS LIKE 'Queries';
SHOW GLOBAL STATUS LIKE 'Slow_queries';
SHOW GLOBAL STATUS LIKE 'Questions';

-- InnoDB Buffer Pool Statistics
SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool_read_requests';
SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool_reads';
SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool_pages_free';
SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool_pages_total';

-- Table Handler Statistics
SHOW GLOBAL STATUS LIKE 'Handler_read_rnd_next';
SHOW GLOBAL STATUS LIKE 'Handler_read_key';
SHOW GLOBAL STATUS LIKE 'Select_full_join';
SHOW GLOBAL STATUS LIKE 'Select_scan';

-- =====================================================
-- SECTION 7: IDENTIFY BOTTLENECKS - ALL TABLES
-- =====================================================

-- Check for missing indexes
SELECT 
    TABLE_NAME,
    TABLE_ROWS,
    AVG_ROW_LENGTH,
    DATA_LENGTH / 1024 / 1024 as data_size_mb,
    INDEX_LENGTH / 1024 / 1024 as index_size_mb
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'AetherMart'
  AND TABLE_NAME IN ('products', 'customers', 'orders', 'order_items', 'reviews', 'categories', 'suppliers')
ORDER BY DATA_LENGTH DESC;

-- Check table statistics
ANALYZE TABLE products, customers, orders, order_items, reviews, categories, suppliers;

-- =====================================================
-- SECTION 8: ADVANCED OPTIMIZATIONS
-- =====================================================

-- Enable query cache (if not already enabled)
SET GLOBAL query_cache_size = 67108864;  -- 64MB
SET GLOBAL query_cache_type = 1;

-- Optimize InnoDB buffer pool
SET GLOBAL innodb_buffer_pool_size = 1073741824;  -- 1GB (adjust based on RAM)

-- Enable slow query log for monitoring
SET GLOBAL slow_query_log = 'ON';
SET GLOBAL long_query_time = 2;  -- Log queries > 2 seconds
SET GLOBAL log_queries_not_using_indexes = 'ON';

-- =====================================================
-- SECTION 9: VERIFICATION QUERIES
-- =====================================================

-- Verify all indexes are created
SELECT 
    TABLE_NAME,
    INDEX_NAME,
    COLUMN_NAME,
    SEQ_IN_INDEX,
    INDEX_TYPE
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = 'AetherMart'
  AND TABLE_NAME IN ('products', 'customers', 'orders', 'order_items', 'reviews')
ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX;

-- Check for slow queries in the log
SELECT * FROM mysql.slow_log 
ORDER BY start_time DESC 
LIMIT 20;

-- =====================================================
-- SECTION 10: PERFORMANCE COMPARISON METRICS
-- =====================================================

-- Run BEFORE optimization
SELECT 
    'OPTIMIZATION' as phase,
    NOW() as timestamp,
    (SELECT COUNT(*) FROM products) as product_count,
    (SELECT COUNT(*) FROM customers) as customer_count,
    (SELECT COUNT(*) FROM orders) as order_count,
    (SELECT COUNT(*) FROM order_items) as order_item_count,
    (SELECT COUNT(*) FROM reviews WHERE review_embedding IS NOT NULL) as review_embedding_count,
    (SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME = 'Queries') as total_queries,
    (SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME = 'Slow_queries') as slow_queries;

-- Run AFTER optimization
-- (Same query, compare results)

-- =====================================================
-- NOTES FOR PRESENTATION:
-- =====================================================
-- 1. Document BEFORE vs AFTER metrics for each optimization
-- 2. Show EXPLAIN output highlighting improvements
-- 3. Include Sysbench results (TPS, latency)
-- 4. Demonstrate vector similarity search working
-- 5. Show cluster health across all 3 nodes
-- 6. Document any issues found and how they were resolved
-- =====================================================