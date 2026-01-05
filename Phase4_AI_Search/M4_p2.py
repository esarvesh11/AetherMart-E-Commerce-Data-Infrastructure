#!/usr/bin/env python3
"""
Milestone 4: Advanced ELT Pipeline for AetherMart
Multi-stage pipeline: Staging → Transformation → Production
Transformations ONLY on: customers, reviews, orders
"""

import mariadb
import csv
import os
import logging
from datetime import datetime
from typing import Dict, List, Tuple
from dotenv import load_dotenv
import sys

load_dotenv('vector_db.env')

# =====================================================
# CONFIGURATION
# =====================================================

DB_CONFIG = {
    'host': os.getenv("MARIADB_HOST"),
    'port': 3306,
    'user': os.getenv("MARIADB_USER"),
    'password': os.getenv("MARIADB_PASSWORD"),
    'database': 'aethermart_db2',
    'local_infile': True
}

# Data file paths - CHANGE THIS TO YOUR DATA DIRECTORY
DATA_DIR = "./data"  # Current directory where CSVs are located

CSV_FILES = {
    'categories': 'categories.csv',
    'suppliers': 'suppliers.csv',
    'customers': 'customers.csv',
    'products': 'products.csv',
    'orders': 'orders.csv',
    'order_items': 'order_items.csv',
    'reviews': 'reviews.csv'
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'elt_pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# =====================================================
# ELT PIPELINE CLASS
# =====================================================

class AetherMartELTPipeline:
    """Advanced ELT Pipeline with Staging Area"""
    
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.stats = {
            'loaded_staging': {},
            'transformed': {},
            'loaded_production': {},
            'invalid_records': {}
        }
    
    def connect(self):
        """Establish database connection"""
        try:
            self.conn = mariadb.connect(**DB_CONFIG)
            self.cursor = self.conn.cursor()
            logger.info("✅ Database connection established")
            return True
        except mariadb.Error as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")
    
    # =====================================================
    # STAGE 0: SCHEMA SETUP
    # =====================================================
    
    def create_all_schemas(self):
        """Create production, staging, and audit tables"""
        logger.info("\n" + "="*70)
        logger.info("📋 STAGE 0: Creating Complete Schema")
        logger.info("="*70 + "\n")
        
        try:
            # PRODUCTION TABLES
            logger.info("Creating production tables...")
            
            production_schema = """
            -- Categories
            DROP TABLE IF EXISTS categories;
            CREATE TABLE categories (
                category_id SERIAL PRIMARY KEY,
                category_name VARCHAR(100) NOT NULL UNIQUE
            );
            
            -- Suppliers
            DROP TABLE IF EXISTS suppliers;
            CREATE TABLE suppliers (
                supplier_id SERIAL PRIMARY KEY,
                supplier_name VARCHAR(150) NOT NULL,
                contact_email VARCHAR(100)
            );
            
            -- Customers
            DROP TABLE IF EXISTS customers;
            CREATE TABLE customers (
                customer_id SERIAL PRIMARY KEY,
                first_name VARCHAR(50) NOT NULL,
                last_name VARCHAR(50) NOT NULL,
                email VARCHAR(100),
                registration_date DATE,
                city VARCHAR(100),
                state VARCHAR(50),
                zipcode VARCHAR(20)
            );
            
            -- Products
            DROP TABLE IF EXISTS products;
            CREATE TABLE products (
                product_id SERIAL PRIMARY KEY,
                product_name VARCHAR(255) NOT NULL,
                price DECIMAL(10, 2) NOT NULL,
                category_id BIGINT UNSIGNED,
                supplier_id BIGINT UNSIGNED,
                CONSTRAINT fk_products_categories FOREIGN KEY (category_id) REFERENCES categories(category_id),
                CONSTRAINT fk_products_suppliers FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
            );
            
            -- Orders
            DROP TABLE IF EXISTS orders;
            CREATE TABLE orders (
                order_id SERIAL PRIMARY KEY,
                customer_id BIGINT UNSIGNED,
                order_date DATE,
                total_amount DECIMAL(12, 2),
                CONSTRAINT fk_orders_customers FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
            );
            
            -- Order Items
            DROP TABLE IF EXISTS order_items;
            CREATE TABLE order_items (
                order_item_id SERIAL PRIMARY KEY,
                order_id BIGINT UNSIGNED,
                product_id BIGINT UNSIGNED,
                quantity INT NOT NULL,
                price_per_unit DECIMAL(10, 2) NOT NULL,
                CONSTRAINT fk_order_items_orders FOREIGN KEY (order_id) REFERENCES orders(order_id),
                CONSTRAINT fk_order_items_products FOREIGN KEY (product_id) REFERENCES products(product_id)
            );
            
            -- Reviews
            DROP TABLE IF EXISTS reviews;
            CREATE TABLE reviews (
                review_id SERIAL PRIMARY KEY,
                product_id BIGINT UNSIGNED,
                customer_id BIGINT UNSIGNED,
                rating INT,
                review_text TEXT,
                review_date DATE,
                CONSTRAINT fk_reviews_products FOREIGN KEY (product_id) REFERENCES products(product_id),
                CONSTRAINT fk_reviews_customers FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
            );
            """
            
            for statement in production_schema.split(';'):
                if statement.strip():
                    self.cursor.execute(statement)
            
            logger.info("✅ Production tables created")
            
            # STAGING TABLES
            logger.info("Creating staging tables...")
            
            staging_schema = """
            -- Staging: Categories (no transformation needed)
            DROP TABLE IF EXISTS stg_categories;
            CREATE TABLE stg_categories (
                category_id INT,
                category_name VARCHAR(100),
                load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Staging: Suppliers (no transformation needed)
            DROP TABLE IF EXISTS stg_suppliers;
            CREATE TABLE stg_suppliers (
                supplier_id INT,
                supplier_name VARCHAR(150),
                contact_email VARCHAR(100),
                load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Staging: Customers (NEEDS TRANSFORMATION)
            DROP TABLE IF EXISTS stg_customers;
            CREATE TABLE stg_customers (
                customer_id INT,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                email VARCHAR(100),
                registration_date VARCHAR(50),
                city VARCHAR(100),
                state VARCHAR(50),
                zipcode VARCHAR(20),
                load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_valid BOOLEAN DEFAULT FALSE,
                error_message TEXT
            );
            
            -- Staging: Products (no transformation needed)
            DROP TABLE IF EXISTS stg_products;
            CREATE TABLE stg_products (
                product_id INT,
                product_name VARCHAR(255),
                price DECIMAL(10, 2),
                category_id INT,
                supplier_id INT,
                load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Staging: Orders (NEEDS TRANSFORMATION)
            DROP TABLE IF EXISTS stg_orders;
            CREATE TABLE stg_orders (
                order_id INT,
                customer_id INT,
                order_date VARCHAR(50),
                total_amount DECIMAL(12, 2),
                load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_valid BOOLEAN DEFAULT FALSE,
                error_message TEXT
            );
            
            -- Staging: Order Items (no transformation needed)
            DROP TABLE IF EXISTS stg_order_items;
            CREATE TABLE stg_order_items (
                order_item_id INT,
                order_id INT,
                product_id INT,
                quantity INT,
                price_per_unit DECIMAL(10, 2),
                load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Staging: Reviews (NEEDS TRANSFORMATION)
            DROP TABLE IF EXISTS stg_reviews;
            CREATE TABLE stg_reviews (
                review_id INT,
                product_id INT,
                customer_id INT,
                rating VARCHAR(10),
                review_text TEXT,
                review_date VARCHAR(50),
                load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_valid BOOLEAN DEFAULT FALSE,
                error_message TEXT
            );
            
            -- Error Log Table
            DROP TABLE IF EXISTS elt_error_log;
            CREATE TABLE elt_error_log (
                log_id SERIAL PRIMARY KEY,
                table_name VARCHAR(50),
                error_type VARCHAR(100),
                error_message TEXT,
                log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Audit Log Table
            DROP TABLE IF EXISTS elt_audit_log;
            CREATE TABLE elt_audit_log (
                audit_id SERIAL PRIMARY KEY,
                pipeline_run_id VARCHAR(50),
                stage VARCHAR(20),
                table_name VARCHAR(50),
                records_processed INT,
                records_valid INT,
                records_invalid INT,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                duration_seconds DECIMAL(10,2),
                status VARCHAR(20)
            );
            """
            
            for statement in staging_schema.split(';'):
                if statement.strip():
                    self.cursor.execute(statement)
            
            logger.info("✅ Staging tables created")
            
            self.conn.commit()
            logger.info("✅ Complete schema created successfully\n")
            return True
            
        except mariadb.Error as e:
            logger.error(f"❌ Schema creation failed: {e}")
            return False
    
    # =====================================================
    # STAGE 1: LOAD TO STAGING
    # =====================================================
    
    def load_to_staging(self, table_name: str, csv_file: str, pipeline_run_id: str):
        """Load data from CSV into staging table"""
        logger.info(f"\n{'='*70}")
        logger.info(f"📥 STAGE 1: Load to Staging - {table_name}")
        logger.info(f"{'='*70}\n")
        
        start_time = datetime.now()
        csv_path = os.path.join(DATA_DIR, csv_file)
        stg_table = f"stg_{table_name}"
        
        if not os.path.exists(csv_path):
            logger.error(f"❌ CSV file not found: {csv_path}")
            return False
        
        try:
            # Count records in CSV
            with open(csv_path, 'r') as f:
                record_count = sum(1 for line in f) - 1
            
            logger.info(f"Found {record_count} records in {csv_file}")
            
            # Truncate staging table
            self.cursor.execute(f"TRUNCATE TABLE {stg_table}")
            
            # Get column names from CSV header
            with open(csv_path, 'r') as f:
                reader = csv.reader(f)
                columns = next(reader)
            
            # Load data
            load_query = f"""
                LOAD DATA LOCAL INFILE '{csv_path}'
                INTO TABLE {stg_table}
                FIELDS TERMINATED BY ','
                OPTIONALLY ENCLOSED BY '"'
                LINES TERMINATED BY '\\n'
                IGNORE 1 ROWS
                ({', '.join(columns)})
            """
            
            self.cursor.execute(load_query)
            self.conn.commit()
            
            # Verify load
            self.cursor.execute(f"SELECT COUNT(*) FROM {stg_table}")
            loaded_count = self.cursor.fetchone()[0]
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info(f"✅ Loaded {loaded_count}/{record_count} records to {stg_table}")
            logger.info(f"⏱️  Duration: {duration:.2f} seconds\n")
            
            self.stats['loaded_staging'][table_name] = loaded_count
            self.log_audit(pipeline_run_id, 'LOAD_STAGING', table_name, record_count, loaded_count, 0, start_time, end_time, 'SUCCESS')
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Load to staging failed: {e}")
            end_time = datetime.now()
            self.log_audit(pipeline_run_id, 'LOAD_STAGING', table_name, 0, 0, 0, start_time, end_time, 'FAILED')
            return False
    
    # =====================================================
    # STAGE 2: TRANSFORM (ONLY customers, reviews, orders)
    # =====================================================
    
    def transform_staging_data(self, table_name: str, pipeline_run_id: str):
        """Transform data in staging - ONLY for customers, reviews, orders"""
        
        # Skip transformation for tables that don't need it
        if table_name not in ['customers', 'reviews', 'orders']:
            logger.info(f"⏭️  Skipping transformation for {table_name} (not required)\n")
            return True
        
        logger.info(f"\n{'='*70}")
        logger.info(f"🔄 STAGE 2: Transform - {table_name}")
        logger.info(f"{'='*70}\n")
        
        start_time = datetime.now()
        stg_table = f"stg_{table_name}"
        
        try:
            # Get initial count
            self.cursor.execute(f"SELECT COUNT(*) FROM {stg_table}")
            total_records = self.cursor.fetchone()[0]
            
            # Apply table-specific transformations
            if table_name == 'customers':
                self._transform_customers(stg_table)
            elif table_name == 'orders':
                self._transform_orders(stg_table)
            elif table_name == 'reviews':
                self._transform_reviews(stg_table)
            
            self.conn.commit()
            
            # Count valid/invalid
            self.cursor.execute(f"SELECT COUNT(*) FROM {stg_table} WHERE is_valid = TRUE")
            valid_count = self.cursor.fetchone()[0]
            invalid_count = total_records - valid_count
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info(f"✅ Transformation complete:")
            logger.info(f"   Total: {total_records} | Valid: {valid_count} | Invalid: {invalid_count}")
            logger.info(f"⏱️  Duration: {duration:.2f} seconds\n")
            
            self.stats['transformed'][table_name] = valid_count
            self.stats['invalid_records'][table_name] = invalid_count
            
            self.log_audit(pipeline_run_id, 'TRANSFORM', table_name, total_records, valid_count, invalid_count, start_time, end_time, 'SUCCESS')
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Transformation failed: {e}")
            import traceback
            traceback.print_exc()
            end_time = datetime.now()
            self.log_audit(pipeline_run_id, 'TRANSFORM', table_name, 0, 0, 0, start_time, end_time, 'FAILED')
            return False
    
    def _transform_customers(self, stg_table: str):
        """Transform and validate customers"""
        logger.info("Transforming customers...")
        
        # Clean empty emails
        self.cursor.execute(f"""
            UPDATE {stg_table}
            SET email = NULL
            WHERE email = '' OR TRIM(email) = ''
        """)
        
        # Mark valid records
        self.cursor.execute(f"""
            UPDATE {stg_table}
            SET is_valid = TRUE,
                error_message = NULL
            WHERE first_name IS NOT NULL 
              AND last_name IS NOT NULL
              AND TRIM(first_name) != ''
              AND TRIM(last_name) != ''
              AND customer_id > 0
              AND (email IS NULL OR email REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{{2,}}$')
        """)
        
        # Mark invalid with error messages
        self.cursor.execute(f"""
            UPDATE {stg_table}
            SET is_valid = FALSE,
                error_message = 'Missing required fields or invalid email'
            WHERE is_valid = FALSE OR is_valid IS NULL
        """)
    
    def _transform_orders(self, stg_table: str):
        """Transform and validate orders"""
        logger.info("Transforming orders...")
        
        # Standardize date formats
        self.cursor.execute(f"""
            UPDATE {stg_table}
            SET order_date = CASE
                WHEN order_date LIKE '____-__-__' THEN order_date
                WHEN order_date LIKE '__-__-____' THEN DATE_FORMAT(STR_TO_DATE(order_date, '%m-%d-%Y'), '%Y-%m-%d')
                WHEN order_date LIKE '__/__/____' THEN DATE_FORMAT(STR_TO_DATE(order_date, '%m/%d/%Y'), '%Y-%m-%d')
                ELSE NULL
            END
        """)
        
        # Mark valid records
        self.cursor.execute(f"""
            UPDATE {stg_table}
            SET is_valid = TRUE,
                error_message = NULL
            WHERE order_id > 0
              AND customer_id > 0
              AND order_date IS NOT NULL
              AND order_date != ''
              AND total_amount >= 0
        """)
        
        # Mark invalid
        self.cursor.execute(f"""
            UPDATE {stg_table}
            SET is_valid = FALSE,
                error_message = 'Invalid date format or missing required fields'
            WHERE is_valid = FALSE OR is_valid IS NULL
        """)
    
    def _transform_reviews(self, stg_table: str):
        """Transform and validate reviews"""
        logger.info("Transforming reviews...")
        
        # Clean invalid ratings
        self.cursor.execute(f"""
            UPDATE {stg_table}
            SET rating = NULL
            WHERE rating IN ('NULL', '', 'invalid')
               OR rating NOT REGEXP '^[0-9]+$'
        """)
        
        # Mark valid records
        self.cursor.execute(f"""
            UPDATE {stg_table}
            SET is_valid = TRUE,
                error_message = NULL
            WHERE review_id > 0
              AND product_id > 0
              AND customer_id > 0
              AND rating IS NOT NULL
              AND rating REGEXP '^[0-9]+$'
              AND CAST(rating AS UNSIGNED) BETWEEN 1 AND 5
        """)
        
        # Mark invalid
        self.cursor.execute(f"""
            UPDATE {stg_table}
            SET is_valid = FALSE,
                error_message = 'Missing or invalid rating (must be 1-5)'
            WHERE is_valid = FALSE OR is_valid IS NULL
        """)
    
    # =====================================================
    # STAGE 3: LOAD TO PRODUCTION
    # =====================================================
    
    def load_to_production(self, table_name: str, pipeline_run_id: str):
        """Load validated data to production"""
        logger.info(f"\n{'='*70}")
        logger.info(f"📤 STAGE 3: Load to Production - {table_name}")
        logger.info(f"{'='*70}\n")
        
        start_time = datetime.now()
        stg_table = f"stg_{table_name}"
        
        try:
            # For tables with transformations, load only valid records
            if table_name in ['customers', 'reviews', 'orders']:
                self.cursor.execute(f"SELECT COUNT(*) FROM {stg_table} WHERE is_valid = TRUE")
                valid_count = self.cursor.fetchone()[0]
                
                if valid_count == 0:
                    logger.warning(f"⚠️  No valid records to load for {table_name}")
                    return True
                
                # Table-specific loads
                if table_name == 'customers':
                    self._load_customers_to_prod(stg_table)
                elif table_name == 'orders':
                    self._load_orders_to_prod(stg_table)
                elif table_name == 'reviews':
                    self._load_reviews_to_prod(stg_table)
                
                loaded_count = valid_count
            else:
                # For tables without transformation, load all records
                self.cursor.execute(f"SELECT COUNT(*) FROM {stg_table}")
                total_count = self.cursor.fetchone()[0]
                
                if total_count == 0:
                    logger.warning(f"⚠️  No records to load for {table_name}")
                    return True
                
                # Direct load
                if table_name == 'categories':
                    self._load_categories_to_prod(stg_table)
                elif table_name == 'suppliers':
                    self._load_suppliers_to_prod(stg_table)
                elif table_name == 'products':
                    self._load_products_to_prod(stg_table)
                elif table_name == 'order_items':
                    self._load_order_items_to_prod(stg_table)
                
                loaded_count = total_count
            
            self.conn.commit()
            
            # Verify
            self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            prod_count = self.cursor.fetchone()[0]
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info(f"✅ Loaded {loaded_count} records to {table_name}")
            logger.info(f"   Total in production: {prod_count}")
            logger.info(f"⏱️  Duration: {duration:.2f} seconds\n")
            
            self.stats['loaded_production'][table_name] = loaded_count
            self.log_audit(pipeline_run_id, 'LOAD_PROD', table_name, loaded_count, loaded_count, 0, start_time, end_time, 'SUCCESS')
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Load to production failed: {e}")
            import traceback
            traceback.print_exc()
            self.conn.rollback()
            end_time = datetime.now()
            self.log_audit(pipeline_run_id, 'LOAD_PROD', table_name, 0, 0, 0, start_time, end_time, 'FAILED')
            return False
    
    def _load_categories_to_prod(self, stg_table: str):
        self.cursor.execute(f"""
            INSERT INTO categories (category_id, category_name)
            SELECT category_id, category_name FROM {stg_table}
        """)
    
    def _load_suppliers_to_prod(self, stg_table: str):
        self.cursor.execute(f"""
            INSERT INTO suppliers (supplier_id, supplier_name, contact_email)
            SELECT supplier_id, supplier_name, contact_email FROM {stg_table}
        """)
    
    def _load_customers_to_prod(self, stg_table: str):
        self.cursor.execute(f"""
            INSERT INTO customers (customer_id, first_name, last_name, email, registration_date, city, state, zipcode)
            SELECT customer_id, first_name, last_name, email, 
                   STR_TO_DATE(registration_date, '%Y-%m-%d'), city, state, zipcode
            FROM {stg_table}
            WHERE is_valid = TRUE
        """)
    
    def _load_products_to_prod(self, stg_table: str):
        self.cursor.execute(f"""
            INSERT INTO products (product_id, product_name, price, category_id, supplier_id)
            SELECT product_id, product_name, price, category_id, supplier_id FROM {stg_table}
        """)
    
    def _load_orders_to_prod(self, stg_table: str):
        self.cursor.execute(f"""
            INSERT INTO orders (order_id, customer_id, order_date, total_amount)
            SELECT order_id, customer_id, 
                   STR_TO_DATE(order_date, '%Y-%m-%d'), 
                   total_amount
            FROM {stg_table}
            WHERE is_valid = TRUE
        """)
    
    def _load_order_items_to_prod(self, stg_table: str):
        self.cursor.execute(f"""
            INSERT INTO order_items (order_item_id, order_id, product_id, quantity, price_per_unit)
            SELECT order_item_id, order_id, product_id, quantity, price_per_unit FROM {stg_table}
        """)
    
    def _load_reviews_to_prod(self, stg_table: str):
        self.cursor.execute(f"""
            INSERT INTO reviews (review_id, product_id, customer_id, rating, review_text, review_date)
            SELECT review_id, product_id, customer_id, 
                   CAST(rating AS UNSIGNED), 
                   review_text, 
                   STR_TO_DATE(review_date, '%Y-%m-%d')
            FROM {stg_table}
            WHERE is_valid = TRUE
        """)
    
    # =====================================================
    # AUDIT LOGGING
    # =====================================================
    
    def log_audit(self, pipeline_run_id: str, stage: str, table_name: str, 
                  records_processed: int, records_valid: int, records_invalid: int,
                  start_time: datetime, end_time: datetime, status: str):
        """Log to audit table"""
        try:
            duration = (end_time - start_time).total_seconds()
            
            self.cursor.execute("""
                INSERT INTO elt_audit_log 
                (pipeline_run_id, stage, table_name, records_processed, records_valid, 
                 records_invalid, start_time, end_time, duration_seconds, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (pipeline_run_id, stage, table_name, records_processed, records_valid,
                  records_invalid, start_time, end_time, duration, status))
            
            self.conn.commit()
            
        except Exception as e:
            logger.warning(f"⚠️  Failed to log audit: {e}")
    
    # =====================================================
    # MAIN PIPELINE
    # =====================================================
    
    def run_full_pipeline(self):
        """Execute complete ELT pipeline"""
        
        pipeline_run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        logger.info("\n" + "="*70)
        logger.info("🚀 STARTING ELT PIPELINE")
        logger.info("="*70)
        logger.info(f"Pipeline Run ID: {pipeline_run_id}\n")
        
        overall_start = datetime.now()
        
        # Stage 0: Create schema
        if not self.create_all_schemas():
            logger.error("❌ Schema creation failed. Aborting.")
            return False
        
        # Processing order (respects FK dependencies)
        processing_order = [
            'categories',
            'suppliers',
            'customers',
            'products',
            'orders',
            'order_items',
            'reviews'
        ]
        
        # Process each table
        for table_name in processing_order:
            csv_file = CSV_FILES.get(table_name)
            
            if not csv_file:
                logger.warning(f"⚠️  No CSV for {table_name}. Skipping.")
                continue
            
            # Stage 1: Load to Staging
            if not self.load_to_staging(table_name, csv_file, pipeline_run_id):
                logger.error(f"❌ Failed staging for {table_name}.")
                continue
            
            # Stage 2: Transform (only customers, reviews, orders)
            if not self.transform_staging_data(table_name, pipeline_run_id):
                logger.error(f"❌ Failed transform for {table_name}.")
                continue
            
            # Stage 3: Load to Production
            if not self.load_to_production(table_name, pipeline_run_id):
                logger.error(f"❌ Failed production load for {table_name}.")
                continue
        
        overall_end = datetime.now()
        overall_duration = (overall_end - overall_start).total_seconds()
        
        # Summary
        self.print_summary(pipeline_run_id, overall_duration)
        
        return True
    
    def print_summary(self, pipeline_run_id: str, duration: float):
        """Print pipeline summary"""
        
        logger.info("\n" + "="*70)
        logger.info("📊 PIPELINE SUMMARY")
        logger.info("="*70)
        logger.info(f"Run ID: {pipeline_run_id}")
        logger.info(f"Duration: {duration:.2f} seconds\n")
        
        logger.info(f"{'Table':<20} {'Staged':<10} {'Valid':<10} {'Invalid':<10} {'Loaded':<10}")
        logger.info("-" * 70)
        
        for table in ['categories', 'suppliers', 'customers', 'products', 'orders', 'order_items', 'reviews']:
            staged = self.stats['loaded_staging'].get(table, 0)
            valid = self.stats['transformed'].get(table, staged)  # If not transformed, all are valid
            invalid = self.stats['invalid_records'].get(table, 0)
            loaded = self.stats['loaded_production'].get(table, 0)
            
            logger.info(f"{table:<20} {staged:<10} {valid:<10} {invalid:<10} {loaded:<10}")
        
        logger.info("-" * 70)
        
        total_staged = sum(self.stats['loaded_staging'].values())
        total_loaded = sum(self.stats['loaded_production'].values())
        
        logger.info(f"{'TOTAL':<20} {total_staged:<10} {'':<10} {'':<10} {total_loaded:<10}")
        logger.info("\n" + "="*70)
        logger.info("✅ PIPELINE COMPLETE")
        logger.info("="*70 + "\n")


# =====================================================
# MAIN EXECUTION
# =====================================================

def main():
    """Main function"""
    
    print("\n" + "="*70)
    print("AetherMart ELT Pipeline")
    print("Transformations: customers, reviews, orders ONLY")
    print("="*70 + "\n")
    
    # Check for CSV files
    missing_files = []
    for table, csv_file in CSV_FILES.items():
        csv_path = os.path.join(DATA_DIR, csv_file)
        if not os.path.exists(csv_path):
            missing_files.append(csv_file)
    
    if missing_files:
        print(f"❌ Missing CSV files in {DATA_DIR}:")
        for f in missing_files:
            print(f"   - {f}")
        print("\nPlease ensure all CSV files are in the correct directory.")
        print("Update DATA_DIR variable if needed.\n")
        sys.exit(1)
    
    # Check for .env file
    if not os.path.exists('vector_db.env'):
        print("❌ .env file not found!")
        print("\nCreate 'vector_db.env' with:")
        print("  MARIADB_HOST=your_host")
        print("  MARIADB_USER=your_user")
        print("  MARIADB_PASSWORD=your_password")
        print("  MARIADB_DATABASE=aethermart_db2\n")
        sys.exit(1)
    
    # Initialize pipeline
    pipeline = AetherMartELTPipeline()
    
    try:
        # Connect
        if not pipeline.connect():
            print("❌ Failed to connect. Exiting.")
            sys.exit(1)
        
        # Run pipeline
        success = pipeline.run_full_pipeline()
        
        if success:
            print("\n✅ Pipeline completed successfully!")
            print("\nNext steps:")
            print("1. Check audit log: SELECT * FROM elt_audit_log;")
            print("2. Check production tables: SELECT COUNT(*) FROM customers;")
            print("3. Check invalid records: SELECT * FROM stg_customers WHERE is_valid = FALSE;")
        else:
            print("\n⚠️  Pipeline completed with errors. Check logs.")
    
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        pipeline.disconnect()
        print("\n👋 Pipeline finished.\n")


if __name__ == "__main__":
    main()