#!/usr/bin/env python3
"""
Milestone 6: Complete Standalone ETL Orchestrator for AetherMart
No dependencies on M4/M5 code - fully self-contained

Demonstrates:
- DAG-based task orchestration
- Retry logic with exponential backoff
- Error handling and alerting
- Centralized logging and monitoring
- Health checks
"""

import mariadb
import csv
import os
import sys
import json
import time
import logging
from enum import Enum
from typing import Dict, List, Callable, Optional, Any
from datetime import datetime
from dataclasses import dataclass
from functools import wraps
from pathlib import Path

# =====================================================
# CONFIGURATION
# =====================================================

CONFIG = {
    # Database connection
    'DB_HOST': 'localhost',
    'DB_USER': 'etl_user',
    'DB_PASSWORD': 'Test@123',
    'DB_NAME': 'aethermart_db',
    
    # Data files location
    'DATA_DIR': '',
    
    # Orchestration settings
    'MAX_RETRIES': 3,
    'RETRY_DELAY': 2,  # seconds
    'RETRY_BACKOFF': 2.0,
    
    # Monitoring
    'LOG_DIR': './orch_logs',
    'ALERT_ON_FAILURE': True,
    'MIN_RECORDS': {
        'customers': 100,
        'products': 200,
        'orders': 50
    }
}

# =====================================================
# ENUMS AND DATA CLASSES
# =====================================================

class Status(Enum):
    PENDING = "⏳"
    RUNNING = "▶️"
    SUCCESS = "✅"
    FAILED = "❌"
    SKIPPED = "⏭️"

@dataclass
class TaskResult:
    name: str
    status: Status
    start: datetime
    end: datetime = None
    records: int = 0
    error: str = None
    retries: int = 0
    
    @property
    def duration(self):
        return (self.end - self.start).total_seconds() if self.end else 0

@dataclass
class Task:
    name: str
    func: Callable
    deps: List[str] = None
    critical: bool = True
    
    def __post_init__(self):
        self.deps = self.deps or []

# =====================================================
# UTILITIES
# =====================================================

def retry_with_backoff(max_retries=CONFIG['MAX_RETRIES']):
    """Decorator for automatic retry with exponential backoff"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt >= max_retries:
                        raise
                    delay = CONFIG['RETRY_DELAY'] * (CONFIG['RETRY_BACKOFF'] ** attempt)
                    logging.warning(f"Retry {attempt+1}/{max_retries} in {delay}s: {e}")
                    time.sleep(delay)
        return wrapper
    return decorator

# =====================================================
# DATABASE CONNECTION
# =====================================================

class DatabaseConnection:
    """Manages database connection lifecycle"""
    
    def __init__(self):
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Establish connection"""
        try:
            self.conn = mariadb.connect(
                host=CONFIG['DB_HOST'],
                user=CONFIG['DB_USER'],
                password=CONFIG['DB_PASSWORD'],
                database=CONFIG['DB_NAME']
            )
            self.cursor = self.conn.cursor()
            logging.info(f"Connected to {CONFIG['DB_HOST']}/{CONFIG['DB_NAME']}")
            return True
        except mariadb.Error as e:
            logging.error(f"Connection failed: {e}")
            return False
    
    def disconnect(self):
        """Close connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logging.info("Database disconnected")
    
    def execute(self, query, params=None):
        """Execute query"""
        self.cursor.execute(query, params or ())
        return self.cursor
    
    def commit(self):
        """Commit transaction"""
        self.conn.commit()

# Global DB connection
db = DatabaseConnection()

# =====================================================
# ETL TASKS
# =====================================================

@retry_with_backoff()
def create_schema():
    """Create database schema with staging tables (like M4)"""
    logging.info("Creating schema with staging tables...")
    
    schema = """
    -- Drop production tables
    DROP TABLE IF EXISTS order_items;
    DROP TABLE IF EXISTS reviews;
    DROP TABLE IF EXISTS orders;
    DROP TABLE IF EXISTS products;
    DROP TABLE IF EXISTS customers;
    DROP TABLE IF EXISTS suppliers;
    DROP TABLE IF EXISTS categories;
    
    -- Drop staging tables
    DROP TABLE IF EXISTS stg_order_items;
    DROP TABLE IF EXISTS stg_reviews;
    DROP TABLE IF EXISTS stg_orders;
    DROP TABLE IF EXISTS stg_products;
    DROP TABLE IF EXISTS stg_customers;
    DROP TABLE IF EXISTS stg_suppliers;
    DROP TABLE IF EXISTS stg_categories;
    
    -- Production tables
    CREATE TABLE categories (
        category_id INT PRIMARY KEY,
        category_name VARCHAR(100) NOT NULL
    );
    
    CREATE TABLE suppliers (
        supplier_id INT PRIMARY KEY,
        supplier_name VARCHAR(150) NOT NULL,
        contact_email VARCHAR(100)
    );
    
    CREATE TABLE customers (
        customer_id INT PRIMARY KEY,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        email VARCHAR(100),
        registration_date DATE,
        city VARCHAR(100),
        state VARCHAR(50),
        zipcode VARCHAR(20)
    );
    
    CREATE TABLE products (
        product_id INT PRIMARY KEY,
        product_name VARCHAR(255),
        price DECIMAL(10,2),
        category_id INT,
        supplier_id INT
    );
    
    CREATE TABLE orders (
        order_id INT PRIMARY KEY,
        customer_id INT,
        order_date DATE,
        total_amount DECIMAL(12,2)
    );
    
    CREATE TABLE order_items (
        order_item_id INT PRIMARY KEY,
        order_id INT,
        product_id INT,
        quantity INT,
        price_per_unit DECIMAL(10,2)
    );
    
    CREATE TABLE reviews (
        review_id INT PRIMARY KEY,
        product_id INT,
        customer_id INT,
        rating INT,
        review_text TEXT,
        review_date DATE
    );
    
    -- Staging tables (with validation columns like M4)
    CREATE TABLE stg_categories (
        category_id INT,
        category_name VARCHAR(100)
    );
    
    CREATE TABLE stg_suppliers (
        supplier_id INT,
        supplier_name VARCHAR(150),
        contact_email VARCHAR(100)
    );
    
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
    
    CREATE TABLE stg_products (
        product_id INT,
        product_name VARCHAR(255),
        price DECIMAL(10,2),
        category_id INT,
        supplier_id INT,
        load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE stg_orders (
        order_id INT,
        customer_id INT,
        order_date VARCHAR(50),
        total_amount DECIMAL(12,2),
        load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        is_valid BOOLEAN DEFAULT FALSE,
        error_message TEXT
    );
    
    CREATE TABLE stg_order_items (
        order_item_id INT,
        order_id INT,
        product_id INT,
        quantity INT,
        price_per_unit DECIMAL(10,2),
        load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
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
    """
    
    for stmt in schema.split(';'):
        if stmt.strip():
            db.execute(stmt)
    db.commit()
    return 14  # 7 production + 7 staging tables

@retry_with_backoff()
def load_to_staging(table_name, csv_file):
    """Load CSV into staging table (like M4)"""
    csv_path = Path(CONFIG['DATA_DIR']) / csv_file
    
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    
    stg_table = f"stg_{table_name}"
    logging.info(f"Loading {csv_file} into {stg_table}...")
    
    # Read CSV
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    if not rows:
        logging.warning(f"No data in {csv_file}")
        return 0
    
    # Truncate staging
    db.execute(f"TRUNCATE TABLE {stg_table}")
    
    # Prepare insert
    columns = rows[0].keys()
    placeholders = ', '.join(['?' for _ in columns])
    query = f"INSERT INTO {stg_table} ({', '.join(columns)}) VALUES ({placeholders})"
    
    # Insert rows
    count = 0
    for row in rows:
        try:
            values = [row[col] if row[col] != '' else None for col in columns]
            db.execute(query, values)
            count += 1
        except Exception as e:
            logging.warning(f"Skipped row in {stg_table}: {e}")
    
    db.commit()
    logging.info(f"Loaded {count}/{len(rows)} records into {stg_table}")
    return count

@retry_with_backoff()
def transform_and_validate(table_name):
    """Transform and validate data in staging (like M4)"""
    stg_table = f"stg_{table_name}"
    logging.info(f"Transforming {stg_table}...")
    
    # Example transformations (simplified)
    if table_name == 'customers':
        # Clean empty emails
        db.execute(f"""
            UPDATE {stg_table}
            SET email = NULL
            WHERE email = '' OR TRIM(email) = ''
        """)
        db.commit()
    
    elif table_name == 'orders':
        # Standardize dates
        db.execute(f"""
            UPDATE {stg_table}
            SET order_date = CASE
                WHEN order_date LIKE '____-__-__' THEN order_date
                WHEN order_date LIKE '__/__/____' THEN 
                    DATE_FORMAT(STR_TO_DATE(order_date, '%m/%d/%Y'), '%Y-%m-%d')
                ELSE NULL
            END
        """)
        db.commit()
    
    # Count valid records
    result = db.execute(f"SELECT COUNT(*) FROM {stg_table}").fetchone()
    count = result[0] if result else 0
    logging.info(f"Transformed {count} records in {stg_table}")
    return count

@retry_with_backoff()
def load_to_production(table_name):
    """Load from staging to production (like M4)"""
    stg_table = f"stg_{table_name}"
    logging.info(f"Loading {stg_table} → {table_name}...")
    
    # Get columns from staging
    result = db.execute(f"SHOW COLUMNS FROM {stg_table}")
    columns = [row[0] for row in result.fetchall() if row[0] not in ['load_timestamp', 'is_valid', 'error_message']]
    
    # Insert from staging to production
    cols_str = ', '.join(columns)
    db.execute(f"""
        INSERT INTO {table_name} ({cols_str})
        SELECT {cols_str} FROM {stg_table}
    """)
    db.commit()
    
    # Count loaded records
    result = db.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    count = result[0] if result else 0
    logging.info(f"Loaded {count} records into {table_name}")
    return count

# Individual task wrappers (3-stage ELT like M4)
def stage_categories():
    return load_to_staging('categories', 'categories.csv')

def stage_suppliers():
    return load_to_staging('suppliers', 'suppliers.csv')

def stage_customers():
    return load_to_staging('customers', 'customers.csv')

def stage_products():
    return load_to_staging('products', 'products.csv')

def stage_orders():
    return load_to_staging('orders', 'orders.csv')

def stage_order_items():
    return load_to_staging('order_items', 'order_items.csv')

def stage_reviews():
    return load_to_staging('reviews', 'reviews.csv')

# Transformation tasks (only for tables that need it, like M4)
def transform_customers():
    return transform_and_validate('customers')

def transform_orders():
    return transform_and_validate('orders')

def transform_reviews():
    return transform_and_validate('reviews')

# Production loading tasks
def load_categories():
    return load_to_production('categories')

def load_suppliers():
    return load_to_production('suppliers')

def load_customers():
    return load_to_production('customers')

def load_products():
    return load_to_production('products')

def load_orders():
    return load_to_production('orders')

def load_order_items():
    return load_to_production('order_items')

def load_reviews():
    return load_to_production('reviews')

@retry_with_backoff()
def validate_data():
    """Run data quality checks"""
    logging.info("Validating data quality...")
    
    checks = {
        'customers': "SELECT COUNT(*) FROM customers WHERE email IS NOT NULL",
        'products': "SELECT COUNT(*) FROM products WHERE price > 0",
        'orders': "SELECT COUNT(*) FROM orders WHERE total_amount > 0"
    }
    
    issues = []
    for table, query in checks.items():
        result = db.execute(query).fetchone()[0]
        threshold = CONFIG['MIN_RECORDS'].get(table, 0)
        if result < threshold:
            issues.append(f"{table}: {result} < {threshold}")
    
    if issues:
        logging.warning(f"Quality issues: {', '.join(issues)}")
    else:
        logging.info("All quality checks passed")
    
    return len(checks)

# =====================================================
# ORCHESTRATOR
# =====================================================

class Orchestrator:
    """DAG-based pipeline orchestrator"""
    
    def __init__(self, name: str):
        self.name = name
        self.tasks: Dict[str, Task] = {}
        self.results: Dict[str, TaskResult] = {}
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._setup_logging()
    
    def _setup_logging(self):
        """Setup logging"""
        os.makedirs(CONFIG['LOG_DIR'], exist_ok=True)
        log_file = f"{CONFIG['LOG_DIR']}/{self.name}_{self.run_id}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s | %(levelname)-7s | %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ],
            force=True  # Override any existing config
        )
    
    def add(self, name: str, func: Callable, deps: List[str] = None, critical: bool = True):
        """Add task to pipeline"""
        self.tasks[name] = Task(name, func, deps, critical)
        return self
    
    def _topological_sort(self) -> List[str]:
        """Get execution order based on dependencies"""
        visited = set()
        order = []
        
        def visit(name):
            if name in visited:
                return
            visited.add(name)
            task = self.tasks.get(name)
            if task:
                for dep in task.deps:
                    visit(dep)
                order.append(name)
        
        for name in self.tasks:
            visit(name)
        return order
    
    def _can_run(self, task: Task) -> bool:
        """Check if dependencies succeeded"""
        for dep in task.deps:
            result = self.results.get(dep)
            if not result or result.status != Status.SUCCESS:
                return False
        return True
    
    def _execute_task(self, task: Task) -> TaskResult:
        """Execute a single task"""
        result = TaskResult(task.name, Status.RUNNING, datetime.now())
        logging.info(f"{Status.RUNNING.value} {task.name}")
        
        try:
            output = task.func()
            result.records = output if isinstance(output, int) else 0
            result.status = Status.SUCCESS
            result.end = datetime.now()
            logging.info(
                f"{Status.SUCCESS.value} {task.name} | "
                f"{result.duration:.1f}s | {result.records} records"
            )
        except Exception as e:
            result.status = Status.FAILED
            result.error = str(e)
            result.end = datetime.now()
            logging.error(f"{Status.FAILED.value} {task.name} | {e}")
            if CONFIG['ALERT_ON_FAILURE']:
                logging.error(f"🚨 ALERT: Task '{task.name}' failed!")
        
        return result
    
    def run(self):
        """Execute pipeline"""
        logging.info("=" * 70)
        logging.info(f"🚀 PIPELINE: {self.name} | Run ID: {self.run_id}")
        logging.info("=" * 70)
        
        order = self._topological_sort()
        logging.info(f"Execution order: {' → '.join(order)}\n")
        
        start = datetime.now()
        
        for name in order:
            task = self.tasks[name]
            
            if not self._can_run(task):
                logging.warning(f"{Status.SKIPPED.value} {name} (deps failed)")
                self.results[name] = TaskResult(
                    name, Status.SKIPPED, datetime.now(), datetime.now()
                )
                continue
            
            result = self._execute_task(task)
            self.results[name] = result
            
            if result.status == Status.FAILED and task.critical:
                logging.error(f"🛑 Critical task failed. Stopping.")
                break
        
        duration = (datetime.now() - start).total_seconds()
        self._print_summary(duration)
        self._export_results()
    
    def _print_summary(self, duration: float):
        """Print execution summary"""
        success = sum(1 for r in self.results.values() if r.status == Status.SUCCESS)
        failed = sum(1 for r in self.results.values() if r.status == Status.FAILED)
        
        logging.info("\n" + "=" * 70)
        logging.info(f"📊 SUMMARY | Duration: {duration:.1f}s")
        logging.info(f"   Success: {success} | Failed: {failed}")
        logging.info("-" * 70)
        
        for name, result in self.results.items():
            logging.info(
                f"{result.status.value} {name:<30} | "
                f"{result.duration:>6.1f}s | {result.records:>5} records"
            )
        logging.info("=" * 70)
    
    def _export_results(self):
        """Export results to JSON"""
        output = {
            'pipeline': self.name,
            'run_id': self.run_id,
            'results': {
                name: {
                    'status': r.status.name,
                    'duration': r.duration,
                    'records': r.records,
                    'error': r.error
                }
                for name, r in self.results.items()
            }
        }
        filepath = f"{CONFIG['LOG_DIR']}/results_{self.run_id}.json"
        with open(filepath, 'w') as f:
            json.dump(output, f, indent=2)
        logging.info(f"Results exported: {filepath}")
    
    def visualize(self):
        """Show DAG"""
        print("\n📈 PIPELINE DAG")
        print("=" * 60)
        for name, task in self.tasks.items():
            deps = ', '.join(task.deps) if task.deps else "no dependencies"
            icon = "🔴" if task.critical else "🟡"
            print(f"{icon} {name:<30} ← {deps}")
        print("=" * 60 + "\n")

# =====================================================
# MAIN PIPELINE DEFINITION
# =====================================================

def main():
    """Main entry point"""
    
    print("\n" + "=" * 70)
    print("AetherMart Advanced ETL Orchestrator - Milestone 6")
    print("=" * 70 + "\n")
    
    # Check prerequisites
    if not Path(CONFIG['DATA_DIR']).exists():
        print(f"❌ Data directory not found: {CONFIG['DATA_DIR']}")
        print("Create it and add CSV files: categories.csv, suppliers.csv, etc.")
        sys.exit(1)
    
    # Connect to database
    if not db.connect():
        print("❌ Database connection failed. Check CONFIG settings.")
        sys.exit(1)
    
    try:
        # Build pipeline with 3-stage ELT (like M4)
        orch = Orchestrator("AetherMart_Complete_ELT")
        
        # Stage 0: Schema creation
        orch.add("create_schema", create_schema)
        
        # Stage 1: Load to Staging (Extract)
        orch.add("stage_categories", stage_categories, deps=["create_schema"])
        orch.add("stage_suppliers", stage_suppliers, deps=["create_schema"])
        orch.add("stage_customers", stage_customers, deps=["create_schema"])
        orch.add("stage_products", stage_products, deps=["create_schema"])
        orch.add("stage_orders", stage_orders, deps=["create_schema"])
        orch.add("stage_order_items", stage_order_items, deps=["create_schema"])
        orch.add("stage_reviews", stage_reviews, deps=["create_schema"])
        
        # Stage 2: Transform (only for tables that need it)
        orch.add("transform_customers", transform_customers, deps=["stage_customers"])
        orch.add("transform_orders", transform_orders, deps=["stage_orders"])
        orch.add("transform_reviews", transform_reviews, deps=["stage_reviews"])
        
        # Stage 3: Load to Production
        orch.add("load_categories", load_categories, deps=["stage_categories"])
        orch.add("load_suppliers", load_suppliers, deps=["stage_suppliers"])
        orch.add("load_customers", load_customers, deps=["transform_customers"])
        orch.add("load_products", load_products, deps=["load_categories", "load_suppliers", "stage_products"])
        orch.add("load_orders", load_orders, deps=["load_customers", "transform_orders"])
        orch.add("load_order_items", load_order_items, deps=["load_orders", "load_products", "stage_order_items"])
        orch.add("load_reviews", load_reviews, deps=["load_customers", "load_products", "transform_reviews"])
        
        # Stage 4: Validation (non-critical)
        orch.add("validate_data", validate_data, 
                 deps=["load_orders", "load_products", "load_reviews"], 
                 critical=False)
        
        # Show DAG
        orch.visualize()
        
        # Run pipeline
        orch.run()
        
        print(f"\n✅ Pipeline completed! Logs: {CONFIG['LOG_DIR']}/\n")
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}\n")
        import traceback
        traceback.print_exc()
    finally:
        db.disconnect()

if __name__ == "__main__":
    main()