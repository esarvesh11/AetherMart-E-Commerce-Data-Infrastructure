import mysql.connector
import pymongo
import sys
# Import date and datetime objects for type checking
from datetime import date, datetime
# Import Decimal for type checking
from decimal import Decimal

# --- 1. MariaDB (Source) Configuration ---
MARIADB_CONFIG = {
    'user': 'etl_user',
    'password': 'Test@123',  # Your MariaDB root password
    'host': '172.31.22.159',        # <-- !! IMPORTANT !!
    'database': 'aethermart_db',
    'port': 3306
}

# --- 2. MongoDB (Destination) Configuration ---
MONGO_CONFIG = {
    'host': 'localhost',
    'port': 27017,
    'database': 'aethermart_import',  # New database to create
}

def migrate_data():
    print("Starting data migration...")
    total_docs_inserted = 0
    tables_migrated = []
    
    try:
        # --- 3. Connect to MariaDB ---
        print(f"Connecting to MariaDB at {MARIADB_CONFIG['host']}...")
        mdb_conn = mysql.connector.connect(**MARIADB_CONFIG)
        cursor = mdb_conn.cursor(dictionary=True) 
        
        print(f"Fetching table list from '{MARIADB_CONFIG['database']}'...")
        cursor.execute("SHOW TABLES")
        tables = [row[f'Tables_in_{MARIADB_CONFIG["database"]}'] for row in cursor.fetchall()]
        
        if not tables:
            print("No tables found in the database. Exiting.")
            return

        print(f"Found {len(tables)} tables: {', '.join(tables)}")

        # --- 4. Connect to MongoDB ---
        print(f"Connecting to MongoDB at {MONGO_CONFIG['host']}...")
        mongo_client = pymongo.MongoClient(
            host=MONGO_CONFIG['host'],
            port=MONGO_CONFIG['port']
        )
        db = mongo_client[MONGO_CONFIG['database']]

        # --- 5. Loop Through Each Table and Migrate Data ---
        for table_name in tables:
            print(f"\nProcessing table: '{table_name}'...")
            
            print(f"  Fetching data from MariaDB...")
            cursor.execute(f"SELECT * FROM {table_name}")
            data_to_load = cursor.fetchall()

            if not data_to_load:
                print(f"  No data found in table '{table_name}'. Skipping.")
                continue

            print(f"  Successfully fetched {len(data_to_load)} rows.")

            # -----------------------------------------------------------------
            # --- DATA PROCESSING STEP ---
            # -----------------------------------------------------------------
            print(f"  Cleaning data types (converting DATE to DATETIME and DECIMAL to float)...")
            for row in data_to_load:
                for key, value in row.items():
                    # Fix 1: Convert 'date' to 'datetime'
                    if type(value) is date:
                        # Convert it to a datetime object at midnight
                        row[key] = datetime(value.year, value.month, value.day)
                    
                    # Fix 2: Convert 'Decimal' to 'float'
                    if isinstance(value, Decimal):
                        # Convert it to a float, which MongoDB understands
                        row[key] = float(value)
            # -----------------------------------------------------------------
            # --- End of new code ---
            # -----------------------------------------------------------------

            # Get the corresponding MongoDB collection
            collection = db[table_name]
            
            print(f"  Clearing old data from MongoDB collection '{table_name}'...")
            collection.delete_many({})
            
            print(f"  Loading {len(data_to_load)} documents into MongoDB...")
            # We now load the cleaned data_to_load list
            result = collection.insert_many(data_to_load)
            
            inserted_count = len(result.inserted_ids)
            total_docs_inserted += inserted_count
            tables_migrated.append(table_name)
            print(f"  Successfully inserted {inserted_count} documents.")

        # --- 6. Close Connections and Print Summary ---
        cursor.close()
        mdb_conn.close()
        mongo_client.close()
        
        print("\n--- Migration Complete! ---")
        print(f"Successfully migrated {len(tables_migrated)} tables.")
        print(f"Total documents inserted: {total_docs_inserted}")
        print(f"Database: '{MONGO_CONFIG['database']}'")
        print(f"Collections: {', '.join(tables_migrated)}")

    except mysql.connector.Error as err:
        print(f"MariaDB Error: {err}")
        sys.exit(1)
    except pymongo.errors.ConnectionFailure as err:
        print(f"MongoDB Connection Error: {err}")
        print("Is the MongoDB service running? (sudo systemctl start mongod)")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    migrate_data()