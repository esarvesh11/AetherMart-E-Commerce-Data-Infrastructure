#!/usr/bin/env python3
"""
Milestone 4: Enhanced Vector Generation for AetherMart Products
Features:
- Batch processing with configurable batch size
- Dynamic checking for products without descriptions
- Automatic retry logic with exponential backoff
- Progress tracking and detailed logging
- Handles Google API rate limits (10 req/min free tier)
"""

import os
import mariadb
import google.generativeai as genai
from dotenv import load_dotenv
from langchain_google_genai import GoogleGenerativeAIEmbeddings
import time
import json
from datetime import datetime

load_dotenv('.env')

# =====================================================
# CONFIGURATION
# =====================================================

# --- API and Model Configuration ---
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
generation_model = genai.GenerativeModel('gemini-flash-latest')
embedding_model = GoogleGenerativeAIEmbeddings(model="models/text-embedding-004")

# --- Database Connection Details from .env ---
db_user = os.getenv("MARIADB_USER")
db_password = os.getenv("MARIADB_PASSWORD")
db_host = os.getenv("MARIADB_HOST")
db_database = os.getenv("MARIADB_DATABASE")

# --- Processing Configuration ---
BATCH_SIZE = 5              # Products per batch
DELAY_BETWEEN_CALLS = 2     # Seconds between API calls (within batch)
DELAY_BETWEEN_BATCHES = 60  # Seconds between batches (1 minute)
MAX_RETRIES = 3             # Retry attempts for failed API calls
RETRY_DELAY = 5             # Initial retry delay (seconds)

# --- Processing Control ---
# Set to None to process all products, or specify a number to limit
MAX_PRODUCTS_TO_PROCESS = 50  # e.g., 20 for testing, None for all

# =====================================================
# HELPER FUNCTIONS
# =====================================================

def safe_get_text(response):
    """Safely extract text from response, handling blocked content"""
    try:
        return response.text
    except ValueError as e:
        if hasattr(response, 'prompt_feedback'):
            print(f"    ⚠️  Content blocked. Reason: {response.prompt_feedback}")
        if hasattr(response, 'candidates') and response.candidates:
            candidate = response.candidates[0]
            print(f"    ⚠️  Finish reason: {candidate.finish_reason}")
            if hasattr(candidate, 'safety_ratings'):
                print(f"    ⚠️  Safety ratings: {candidate.safety_ratings}")
        raise ValueError(f"No valid response generated: {str(e)}")


def generate_description_with_fallback(product_name):
    """Generate description with multiple fallback strategies"""
    prompts = [
        f"Write a brief, professional product description for: {product_name}",
        f"Describe the features and benefits of: {product_name}",
        f"Product: {product_name}. Description:"
    ]
    
    for idx, prompt in enumerate(prompts):
        try:
            print(f"    Attempt {idx + 1} with prompt variation...")
            response = generation_model.generate_content(
                prompt,
                generation_config={
                    "max_output_tokens": 150,
                    "temperature": 0.7,
                    "top_p": 0.8,
                    "top_k": 40
                },
                safety_settings=[
                    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
                    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
                    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
                    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"}
                ]
            )
            return safe_get_text(response)
        except Exception as e:
            print(f"    Prompt variation {idx + 1} failed: {e}")
            if idx < len(prompts) - 1:
                time.sleep(1)
                continue
            else:
                # Fallback to generic description
                return f"A high-quality {product_name} designed for everyday use."


def process_with_retry(func, *args, **kwargs):
    """Helper function to retry API calls with exponential backoff"""
    for attempt in range(MAX_RETRIES):
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            error_str = str(e)
            if "429" in error_str or "ResourceExhausted" in error_str or "quota" in error_str.lower():
                if attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_DELAY * (2 ** attempt)
                    print(f"  Rate limit hit. Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                else:
                    raise
            else:
                raise


def check_vector_support(cursor):
    """Check if database supports vector functions"""
    try:
        cursor.execute("SELECT vec_fromtext('[1,2,3]')")
        return True
    except mariadb.Error:
        return False


def get_products_needing_processing(cursor, limit=None):
    """
    Dynamically fetch products that need descriptions/embeddings.
    Returns count of products needing processing and fetches batch.
    """
    # First, get total count of products needing processing
    cursor.execute("""
        SELECT COUNT(*) 
        FROM products 
        WHERE product_description IS NULL 
           OR product_description = ''
           OR product_embedding IS NULL
    """)
    total_remaining = cursor.fetchone()[0]
    
    # Fetch batch for processing
    if limit:
        cursor.execute(f"""
            SELECT product_id, product_name 
            FROM products 
            WHERE product_description IS NULL 
               OR product_description = ''
               OR product_embedding IS NULL
            LIMIT {limit}
        """)
    else:
        cursor.execute("""
            SELECT product_id, product_name 
            FROM products 
            WHERE product_description IS NULL 
               OR product_description = ''
               OR product_embedding IS NULL
        """)
    
    products = cursor.fetchall()
    return total_remaining, products


def process_batch(cursor, batch_products, batch_num, total_batches, has_vector_support, update_query):
    """Process a single batch of products"""
    successful = 0
    failed = 0
    
    print(f"\n{'='*70}")
    print(f"BATCH {batch_num}/{total_batches} - Processing {len(batch_products)} products")
    print(f"{'='*70}\n")
    
    for idx, (product_id, product_name) in enumerate(batch_products, 1):
        print(f"[Batch {batch_num} - Product {idx}/{len(batch_products)}] Processing '{product_name}' (ID: {product_id})...")
        
        try:
            # Generate description
            print("  Generating description...")
            product_description = generate_description_with_fallback(product_name)
            
            time.sleep(DELAY_BETWEEN_CALLS)
            
            # Create vector embedding
            print("  Creating vector embedding...")
            product_vector = process_with_retry(
                embedding_model.embed_query,
                product_description
            )
            
            # Format vector for storage
            if has_vector_support:
                vector_string = str(product_vector)
            else:
                vector_string = json.dumps(product_vector)
            
            time.sleep(DELAY_BETWEEN_CALLS)
            
            # Update database
            print("  Updating database...")
            cursor.execute(update_query, (product_description, vector_string, product_id))
            
            print(f"  ✓ Successfully updated '{product_name}'.\n")
            successful += 1
            
        except Exception as e:
            print(f"  ✗ Error processing '{product_name}': {e}\n")
            failed += 1
            continue
    
    return successful, failed


def display_progress_summary(batch_num, total_batches, batch_successful, batch_failed, 
                            overall_successful, overall_failed, remaining):
    """Display progress summary after each batch"""
    print(f"\n{'='*70}")
    print(f"BATCH {batch_num}/{total_batches} COMPLETE")
    print(f"{'='*70}")
    print(f"Batch Results:")
    print(f"  ✓ Successful: {batch_successful}")
    print(f"  ✗ Failed: {batch_failed}")
    print(f"\nOverall Progress:")
    print(f"  ✓ Total Successful: {overall_successful}")
    print(f"  ✗ Total Failed: {overall_failed}")
    print(f"  📊 Success Rate: {(overall_successful/(overall_successful+overall_failed)*100) if (overall_successful+overall_failed) > 0 else 0:.1f}%")
    print(f"  ⏳ Products Remaining: {remaining}")
    print(f"{'='*70}\n")


# =====================================================
# MAIN PROCESSING FUNCTION
# =====================================================

def main():
    """Main processing loop with batch handling"""
    
    print("\n" + "="*70)
    print("AetherMart - Milestone 4 Vector Generation")
    print("Processing Products with Batch Loop")
    print("="*70)
    print(f"\nConfiguration:")
    print(f"  Batch Size: {BATCH_SIZE} products")
    print(f"  Delay Between API Calls: {DELAY_BETWEEN_CALLS} seconds")
    print(f"  Delay Between Batches: {DELAY_BETWEEN_BATCHES} seconds")
    print(f"  Max Products to Process: {'All' if MAX_PRODUCTS_TO_PROCESS is None else MAX_PRODUCTS_TO_PROCESS}")
    print("="*70 + "\n")
    
    conn = None
    cursor = None
    
    try:
        # Connect to database
        print("Connecting to database...")
        conn = mariadb.connect(
            user=db_user,
            password=db_password,
            host=db_host,
            database=db_database,
            autocommit=False
        )
        cursor = conn.cursor()
        print("✓ Connected to AetherMart database\n")
        
        # Check vector support
        has_vector_support = check_vector_support(cursor)
        
        if has_vector_support:
            print("✓ Vector functions are available")
            update_query = "UPDATE products SET product_description = ?, product_embedding = vec_fromtext(?) WHERE product_id = ?"
        else:
            print("⚠️  Vector functions not available. Storing embeddings as JSON text.")
            update_query = "UPDATE products SET product_description = ?, product_embedding = ? WHERE product_id = ?"
        
        # Get initial count of products needing processing
        total_remaining, _ = get_products_needing_processing(cursor, limit=0)
        
        if total_remaining == 0:
            print("\n✓ All products already have descriptions and embeddings!")
            print("Nothing to process.\n")
            return
        
        print(f"\n📊 Found {total_remaining} products needing processing")
        
        # Calculate number of batches
        products_to_process = min(total_remaining, MAX_PRODUCTS_TO_PROCESS) if MAX_PRODUCTS_TO_PROCESS else total_remaining
        total_batches = (products_to_process + BATCH_SIZE - 1) // BATCH_SIZE
        
        print(f"📦 Will process in {total_batches} batches\n")
        
        # Tracking variables
        overall_successful = 0
        overall_failed = 0
        batch_num = 0
        start_time = datetime.now()
        
        # Main processing loop
        while True:
            # Fetch next batch dynamically
            remaining, batch_products = get_products_needing_processing(cursor, limit=BATCH_SIZE)
            
            # Check if we're done
            if not batch_products:
                print("\n✓ All products processed!")
                break
            
            # Check if we've hit our processing limit
            if MAX_PRODUCTS_TO_PROCESS and overall_successful + overall_failed >= MAX_PRODUCTS_TO_PROCESS:
                print(f"\n✓ Reached processing limit of {MAX_PRODUCTS_TO_PROCESS} products")
                break
            
            batch_num += 1
            
            # Process batch
            batch_successful, batch_failed = process_batch(
                cursor, 
                batch_products, 
                batch_num, 
                total_batches,
                has_vector_support,
                update_query
            )
            
            # Commit after each batch
            conn.commit()
            print("  ✓ Batch committed to database")
            
            # Update overall counters
            overall_successful += batch_successful
            overall_failed += batch_failed
            
            # Get updated remaining count
            remaining, _ = get_products_needing_processing(cursor, limit=0)
            
            # Display progress
            display_progress_summary(
                batch_num, 
                total_batches, 
                batch_successful, 
                batch_failed,
                overall_successful, 
                overall_failed, 
                remaining
            )
            
            # Wait between batches (except after last batch)
            if batch_products and remaining > 0:
                print(f"⏸️  Waiting {DELAY_BETWEEN_BATCHES} seconds before next batch...")
                print(f"   (Google API free tier: 10 requests/minute limit)")
                
                # Countdown timer
                for remaining_time in range(DELAY_BETWEEN_BATCHES, 0, -10):
                    print(f"   ⏱️  {remaining_time} seconds remaining...", end='\r')
                    time.sleep(10)
                print()  # New line after countdown
        
        # Final summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "="*70)
        print("PROCESSING COMPLETE")
        print("="*70)
        print(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Duration: {duration/60:.1f} minutes ({duration:.0f} seconds)")
        print(f"\nFinal Results:")
        print(f"  ✓ Total Successful: {overall_successful}")
        print(f"  ✗ Total Failed: {overall_failed}")
        print(f"  📊 Overall Success Rate: {(overall_successful/(overall_successful+overall_failed)*100) if (overall_successful+overall_failed) > 0 else 0:.1f}%")
        print(f"  ⚡ Average Time per Product: {(duration/overall_successful):.1f} seconds" if overall_successful > 0 else "")
        print("="*70 + "\n")
        
        # Check for any remaining products
        final_remaining, _ = get_products_needing_processing(cursor, limit=0)
        if final_remaining > 0:
            print(f"⚠️  Note: {final_remaining} products still need processing")
            print(f"   Run the script again to continue, or check for errors above.\n")
        else:
            print("✓ All products successfully processed!\n")
    
    except mariadb.Error as e:
        print(f"\n❌ Database Error: {e}")
        if conn:
            conn.rollback()
            print("   Transaction rolled back")
    
    except KeyboardInterrupt:
        print(f"\n\n⚠️  Process interrupted by user")
        if conn:
            conn.commit()
            print("   Partial progress saved")
    
    except Exception as e:
        print(f"\n❌ Unexpected Error: {e}")
        if conn:
            conn.rollback()
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            print("Database connection closed.\n")


# =====================================================
# ENTRY POINT
# =====================================================

if __name__ == "__main__":
    # Verify required packages
    required_packages = {
        'mariadb': 'pip install mariadb',
        'google.generativeai': 'pip install google-generativeai',
        'langchain_google_genai': 'pip install langchain-google-genai',
        'dotenv': 'pip install python-dotenv'
    }
    
    missing_packages = []
    for package, install_cmd in required_packages.items():
        try:
            __import__(package.replace('-', '_'))
        except ImportError:
            missing_packages.append((package, install_cmd))
    
    if missing_packages:
        print("❌ Missing required packages:")
        for package, cmd in missing_packages:
            print(f"   {package}: {cmd}")
        print("\nInstall missing packages and try again.\n")
        exit(1)
    
    # Check for .env file
    if not os.path.exists('.env'):
        print("❌ .env file not found!")
        print("\nCreate a .env file with:")
        print("  GOOGLE_API_KEY=your_key")
        print("  MARIADB_HOST=your_host")
        print("  MARIADB_USER=your_user")
        print("  MARIADB_PASSWORD=your_password")
        print("  MARIADB_DATABASE=AetherMart\n")
        exit(1)
    
    # Run main process
    main()