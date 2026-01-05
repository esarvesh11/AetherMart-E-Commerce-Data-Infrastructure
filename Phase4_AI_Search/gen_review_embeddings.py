#!/usr/bin/env python3
"""
Milestone 4: Generate Vector Embeddings for AetherMart Reviews
Generates embeddings for reviews with review_text
"""

import mariadb
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from dotenv import load_dotenv
import os
import sys
import time

load_dotenv('vector_db.env')

# =====================================================
# CONFIGURATION
# =====================================================

# Initialize embedding model
embeddings = GoogleGenerativeAIEmbeddings(model="models/text-embedding-004")

# Database connection
DB_CONFIG = {
    'host': os.getenv("MARIADB_HOST"),
    'port': 3306,
    'user': os.getenv("MARIADB_USER"),
    'password': os.getenv("MARIADB_PASSWORD"),
    'database': os.getenv("MARIADB_DATABASE")
}

# Batch processing configuration
BATCH_SIZE = 10  # Process reviews in batches
RATE_LIMIT_DELAY = 1  # Seconds between batches (to respect API limits)

# =====================================================
# EMBEDDING GENERATION FUNCTIONS
# =====================================================

def check_review_embedding_column(cursor):
    """Check if review_embedding column exists, create if not"""
    print("\n" + "="*70)
    print("Checking review_embedding column...")
    print("="*70)
    
    try:
        # Check if column exists
        cursor.execute("""
            SELECT COUNT(*) 
            FROM information_schema.COLUMNS 
            WHERE TABLE_SCHEMA = ? 
              AND TABLE_NAME = 'reviews' 
              AND COLUMN_NAME = 'review_embedding'
        """, (DB_CONFIG['database'],))
        
        exists = cursor.fetchone()[0]
        
        if exists:
            print("✅ review_embedding column already exists\n")
            return True
        
        # Create column if it doesn't exist
        print("Creating review_embedding column...")
        cursor.execute("""
            ALTER TABLE reviews 
            ADD COLUMN review_embedding VECTOR(768) DEFAULT NULL
        """)
        
        print("✅ review_embedding column created successfully\n")
        return True
        
    except mariadb.Error as e:
        print(f"❌ Error checking/creating column: {e}")
        return False


def get_reviews_to_embed(cursor):
    """Get reviews that have text but no embeddings"""
    try:
        cursor.execute("""
            SELECT review_id, product_id, customer_id, rating, review_text
            FROM reviews
            WHERE review_text IS NOT NULL
              AND (review_embedding IS NULL OR review_embedding = '')
            ORDER BY review_id
        """)
        
        reviews = cursor.fetchall()
        
        print(f"Found {len(reviews)} reviews to process\n")
        return reviews
        
    except mariadb.Error as e:
        print(f"❌ Error fetching reviews: {e}")
        return []


def generate_review_embeddings(cursor, reviews):
    """Generate embeddings for reviews in batches"""
    print("="*70)
    print(f"Generating Embeddings for {len(reviews)} Reviews")
    print("="*70 + "\n")
    
    successful = 0
    failed = 0
    total_batches = (len(reviews) + BATCH_SIZE - 1) // BATCH_SIZE
    
    for batch_num in range(0, len(reviews), BATCH_SIZE):
        batch = reviews[batch_num:batch_num + BATCH_SIZE]
        current_batch = (batch_num // BATCH_SIZE) + 1
        
        print(f"Processing Batch {current_batch}/{total_batches} ({len(batch)} reviews)...")
        
        for review_id, product_id, customer_id, rating, review_text in batch:
            try:
                # Generate embedding
                embedding_vector = embeddings.embed_query(review_text)
                embedding_str = str(embedding_vector)
                
                # Update database
                cursor.execute("""
                    UPDATE reviews
                    SET review_embedding = vec_fromtext(?)
                    WHERE review_id = ?
                """, (embedding_str, review_id))
                
                # Display progress
                int_rating = int(rating) if rating else 0
                stars = "⭐" * int_rating
                preview = review_text[:60].replace('\n', ' ')
                
                print(f"  ✅ Review ID {review_id} | {stars} ({int_rating}/5)")
                print(f"     Product: {product_id} | Preview: {preview}...")
                
                successful += 1
                
            except Exception as e:
                print(f"  ❌ Failed Review ID {review_id}: {e}")
                failed += 1
        
        print(f"  Batch {current_batch} complete: {successful} successful, {failed} failed")
        print(f"  {'-'*66}\n")
        
        # Rate limiting delay between batches
        if current_batch < total_batches:
            time.sleep(RATE_LIMIT_DELAY)
    
    return successful, failed


def verify_embeddings(cursor):
    """Verify that embeddings were created successfully"""
    print("="*70)
    print("Verification")
    print("="*70 + "\n")
    
    try:
        # Count reviews with text
        cursor.execute("""
            SELECT COUNT(*) 
            FROM reviews 
            WHERE review_text IS NOT NULL
        """)
        total_with_text = cursor.fetchone()[0]
        
        # Count reviews with embeddings
        cursor.execute("""
            SELECT COUNT(*) 
            FROM reviews 
            WHERE review_embedding IS NOT NULL
        """)
        total_with_embeddings = cursor.fetchone()[0]
        
        # Display verification results
        print(f"Reviews with text: {total_with_text}")
        print(f"Reviews with embeddings: {total_with_embeddings}")
        
        if total_with_text == total_with_embeddings:
            print(f"✅ All reviews have embeddings!\n")
        else:
            missing = total_with_text - total_with_embeddings
            print(f"⚠️  {missing} reviews still need embeddings\n")
        
        # Display sample
        print("Sample of reviews with embeddings:")
        print("-" * 70)
        
        cursor.execute("""
            SELECT 
                r.review_id,
                p.product_name,
                r.rating,
                LEFT(r.review_text, 80) as preview
            FROM reviews r
            JOIN products p ON r.product_id = p.product_id
            WHERE r.review_embedding IS NOT NULL
            ORDER BY r.review_id
            LIMIT 5
        """)
        
        samples = cursor.fetchall()
        
        for idx, (review_id, product_name, rating, preview) in enumerate(samples, 1):
            int_rating = int(rating) if rating else 0
            stars = "⭐" * int_rating
            print(f"{idx}. Review ID {review_id} | {stars} ({int_rating}/5)")
            print(f"   Product: {product_name}")
            print(f"   Preview: {preview}...")
            print()
        
    except mariadb.Error as e:
        print(f"❌ Error during verification: {e}")


# =====================================================
# MAIN EXECUTION
# =====================================================

def main():
    """Main execution flow"""
    
    print("\n" + "="*70)
    print("AetherMart - Review Embedding Generation")
    print("="*70)
    print(f"\nConfiguration:")
    print(f"  Database: {DB_CONFIG['database']}")
    print(f"  Batch Size: {BATCH_SIZE}")
    print(f"  Embedding Model: text-embedding-004")
    print("="*70)
    
    # Check for .env file
    if not os.path.exists('vector_db.env'):
        print("\n❌ .env file not found!")
        print("\nCreate 'vector_db.env' with:")
        print("  GOOGLE_API_KEY=your_key")
        print("  MARIADB_HOST=your_host")
        print("  MARIADB_USER=your_user")
        print("  MARIADB_PASSWORD=your_password")
        print("  MARIADB_DATABASE=AetherMart\n")
        sys.exit(1)
    
    conn = None
    cursor = None
    
    try:
        # Connect to database
        print("\nConnecting to database...")
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("✅ Connected successfully")
        
        # Check/create review_embedding column
        if not check_review_embedding_column(cursor):
            print("❌ Failed to verify embedding column. Exiting.")
            return
        
        conn.commit()
        
        # Get reviews to process
        reviews_to_embed = get_reviews_to_embed(cursor)
        
        if not reviews_to_embed:
            print("✅ All reviews already have embeddings!\n")
            verify_embeddings(cursor)
            return
        
        # Generate embeddings
        print(f"Starting embedding generation for {len(reviews_to_embed)} reviews...")
        print("="*70 + "\n")
        
        successful, failed = generate_review_embeddings(cursor, reviews_to_embed)
        
        # Commit changes
        conn.commit()
        print("="*70)
        print("✅ All changes committed to database")
        print("="*70 + "\n")
        
        # Display summary
        print("Summary:")
        print(f"  ✅ Successful: {successful}")
        print(f"  ❌ Failed: {failed}")
        total = successful + failed
        success_rate = (successful / total * 100) if total > 0 else 0
        print(f"  📊 Success Rate: {success_rate:.1f}%")
        print("="*70 + "\n")
        
        # Verify results
        verify_embeddings(cursor)
        
        print("="*70)
        print("✅ Process Complete!")
        print("="*70 + "\n")
        
        print("Next Steps:")
        print("  1. Run review similarity search script")
        print("  2. Test semantic search on reviews")
        print("  3. Document findings in presentation\n")
        
    except mariadb.Error as e:
        print(f"\n❌ Database Error: {e}")
        if conn:
            conn.rollback()
            print("   Transaction rolled back")
    
    except Exception as e:
        print(f"\n❌ Unexpected Error: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            print("Database connection closed.\n")


if __name__ == "__main__":
    main()