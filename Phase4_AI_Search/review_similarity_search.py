#!/usr/bin/env python3
"""
Milestone 4: Review Similarity Search for AetherMart
Semantic search on customer reviews using vector embeddings
"""

import mariadb
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from dotenv import load_dotenv
import os
import sys

load_dotenv('vector_db.env')

# =====================================================
# CONFIGURATION
# =====================================================

# Initialize embedding model
embeddings = GoogleGenerativeAIEmbeddings(model="models/text-embedding-004")

# Database connection
DB_CONFIG = {
    'user': os.getenv("MARIADB_USER"),
    'password': os.getenv("MARIADB_PASSWORD"),
    'host': os.getenv("MARIADB_HOST"),
    'database': os.getenv("MARIADB_DATABASE")
}

# =====================================================
# SIMILARITY SEARCH FUNCTIONS
# =====================================================

def search_reviews(query, top_k=5, min_rating=None, max_rating=None):
    """
    Perform semantic similarity search on reviews
    
    Args:
        query: Search query string
        top_k: Number of results to return
        min_rating: Minimum rating filter (1-5)
        max_rating: Maximum rating filter (1-5)
    """
    print(f"\n{'='*70}")
    print(f"Searching reviews for: '{query}'")
    if min_rating or max_rating:
        rating_filter = f" (Rating: {min_rating or 1}-{max_rating or 5})"
        print(f"Filter:{rating_filter}")
    print(f"{'='*70}\n")
    
    try:
        # Generate query embedding
        print("Generating query embedding...")
        query_vector = embeddings.embed_query(query)
        query_vector_str = str(query_vector)
        
        # Connect to database
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Build rating filter
        rating_filter_sql = ""
        params = [query_vector_str]
        
        if min_rating is not None:
            rating_filter_sql += " AND r.rating >= ?"
            params.append(min_rating)
        
        if max_rating is not None:
            rating_filter_sql += " AND r.rating <= ?"
            params.append(max_rating)
        
        params.append(top_k)
        
        # Perform similarity search
        search_query = f"""
        SELECT 
            r.review_id,
            r.product_id,
            p.product_name,
            r.customer_id,
            r.rating,
            r.review_text,
            r.review_date,
            VEC_DISTANCE(r.review_embedding, vec_fromtext(?)) as distance
        FROM reviews r
        JOIN products p ON r.product_id = p.product_id
        WHERE r.review_text IS NOT NULL 
          AND r.review_embedding IS NOT NULL
          {rating_filter_sql}
        ORDER BY distance ASC
        LIMIT ?
        """
        
        cursor.execute(search_query, params)
        results = cursor.fetchall()
        
        if not results:
            print("❌ No results found.")
            print("   Make sure reviews have text and embeddings.")
        else:
            print(f"✅ Found {len(results)} relevant review(s):\n")
            
            for idx, (review_id, product_id, product_name, customer_id, 
                     rating, review_text, review_date, distance) in enumerate(results, 1):
                
                similarity_score = 1 - distance
                int_rating = int(rating) if rating else 0
                stars = "⭐" * int_rating
                
                print(f"{idx}. Review ID: {review_id}")
                print(f"   Product: {product_name} (ID: {product_id})")
                print(f"   Customer ID: {customer_id}")
                print(f"   Rating: {stars} ({int_rating}/5)")
                print(f"   Date: {review_date}")
                print(f"   Review: {review_text}")
                print(f"   Similarity Score: {similarity_score:.4f} ({similarity_score*100:.1f}%)")
                print(f"   {'-'*66}\n")
        
        cursor.close()
        conn.close()
        
    except mariadb.Error as e:
        print(f"❌ Database Error: {e}")
    except Exception as e:
        print(f"❌ Search Error: {e}")


def search_by_review_id(review_id, top_k=5):
    """
    Find reviews similar to a specific review
    
    Args:
        review_id: ID of the reference review
        top_k: Number of similar reviews to return
    """
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Get the reference review
        cursor.execute("""
            SELECT r.review_id, p.product_name, r.rating, 
                   r.review_text, r.review_embedding
            FROM reviews r
            JOIN products p ON r.product_id = p.product_id
            WHERE r.review_id = ? AND r.review_embedding IS NOT NULL
        """, (review_id,))
        
        result = cursor.fetchone()
        
        if not result:
            print(f"❌ Review ID {review_id} not found or has no embedding.")
            return
        
        ref_id, product_name, rating, review_text, review_embedding = result
        int_rating = int(rating) if rating else 0
        stars = "⭐" * int_rating
        
        print(f"\n{'='*70}")
        print(f"Finding reviews similar to Review ID: {ref_id}")
        print(f"{'='*70}\n")
        print(f"Reference Review:")
        print(f"  Product: {product_name}")
        print(f"  Rating: {stars} ({int_rating}/5)")
        print(f"  Review: {review_text}\n")
        
        # Find similar reviews
        search_query = """
        SELECT 
            r.review_id,
            r.product_id,
            p.product_name,
            r.customer_id,
            r.rating,
            r.review_text,
            VEC_DISTANCE(r.review_embedding, ?) as distance
        FROM reviews r
        JOIN products p ON r.product_id = p.product_id
        WHERE r.review_id != ?
          AND r.review_embedding IS NOT NULL
        ORDER BY distance ASC
        LIMIT ?
        """
        
        cursor.execute(search_query, (review_embedding, review_id, top_k))
        results = cursor.fetchall()
        
        if results:
            print(f"✅ Found {len(results)} similar review(s):\n")
            
            for idx, (rid, pid, pname, cid, rat, text, distance) in enumerate(results, 1):
                similarity_score = 1 - distance
                int_rat = int(rat) if rat else 0
                stars = "⭐" * int_rat
                
                print(f"{idx}. Review ID: {rid}")
                print(f"   Product: {pname} (ID: {pid})")
                print(f"   Customer ID: {cid}")
                print(f"   Rating: {stars} ({int_rat}/5)")
                print(f"   Review: {text}")
                print(f"   Similarity: {similarity_score:.4f} ({similarity_score*100:.1f}%)")
                print(f"   {'-'*66}\n")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")


def search_product_reviews(product_id, sentiment_query=None, top_k=5):
    """
    Search reviews for a specific product, optionally filtered by sentiment
    
    Args:
        product_id: Product ID to search reviews for
        sentiment_query: Optional sentiment search (e.g., "positive", "negative")
        top_k: Number of results to return
    """
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Get product name
        cursor.execute("SELECT product_name FROM products WHERE product_id = ?", 
                      (product_id,))
        result = cursor.fetchone()
        
        if not result:
            print(f"❌ Product ID {product_id} not found.")
            return
        
        product_name = result[0]
        
        print(f"\n{'='*70}")
        print(f"Searching reviews for: {product_name} (ID: {product_id})")
        if sentiment_query:
            print(f"Sentiment filter: '{sentiment_query}'")
        print(f"{'='*70}\n")
        
        if sentiment_query:
            # Generate sentiment query embedding
            query_vector = embeddings.embed_query(sentiment_query)
            query_vector_str = str(query_vector)
            
            # Search with sentiment
            search_query = """
            SELECT 
                r.review_id,
                r.rating,
                r.review_text,
                r.review_date,
                VEC_DISTANCE(r.review_embedding, vec_fromtext(?)) as distance
            FROM reviews r
            WHERE r.product_id = ?
              AND r.review_text IS NOT NULL 
              AND r.review_embedding IS NOT NULL
            ORDER BY distance ASC
            LIMIT ?
            """
            cursor.execute(search_query, (query_vector_str, product_id, top_k))
        else:
            # Get all reviews for product
            search_query = """
            SELECT 
                r.review_id,
                r.rating,
                r.review_text,
                r.review_date,
                0 as distance
            FROM reviews r
            WHERE r.product_id = ?
              AND r.review_text IS NOT NULL
            ORDER BY r.review_date DESC
            LIMIT ?
            """
            cursor.execute(search_query, (product_id, top_k))
        
        results = cursor.fetchall()
        
        if not results:
            print(f"❌ No reviews found for this product.")
        else:
            print(f"✅ Found {len(results)} review(s):\n")
            
            for idx, (review_id, rating, review_text, review_date, distance) in enumerate(results, 1):
                int_rating = int(rating) if rating else 0
                stars = "⭐" * int_rating
                
                print(f"{idx}. Review ID: {review_id}")
                print(f"   Rating: {stars} ({int_rating}/5)")
                print(f"   Date: {review_date}")
                print(f"   Review: {review_text}")
                
                if sentiment_query:
                    similarity_score = 1 - distance
                    print(f"   Relevance: {similarity_score:.4f} ({similarity_score*100:.1f}%)")
                
                print(f"   {'-'*66}\n")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")


# =====================================================
# INTERACTIVE SEARCH MENU
# =====================================================

def interactive_search():
    """Interactive menu for review similarity search"""
    
    print("\n" + "="*70)
    print("AetherMart Review Similarity Search")
    print("="*70)
    
    while True:
        print("\nSearch Options:")
        print("1. Search reviews by text query")
        print("2. Search reviews with rating filter")
        print("3. Find similar reviews by Review ID")
        print("4. Search product reviews")
        print("5. Run demo searches")
        print("6. Exit")
        
        choice = input("\nEnter your choice (1-6): ").strip()
        
        if choice == '1':
            query = input("\nEnter your search query: ").strip()
            if query:
                num_results = input("Number of results (default 5): ").strip()
                top_k = int(num_results) if num_results.isdigit() else 5
                search_reviews(query, top_k)
        
        elif choice == '2':
            query = input("\nEnter your search query: ").strip()
            if query:
                min_r = input("Minimum rating (1-5, press Enter to skip): ").strip()
                max_r = input("Maximum rating (1-5, press Enter to skip): ").strip()
                num_results = input("Number of results (default 5): ").strip()
                
                min_rating = int(min_r) if min_r.isdigit() else None
                max_rating = int(max_r) if max_r.isdigit() else None
                top_k = int(num_results) if num_results.isdigit() else 5
                
                search_reviews(query, top_k, min_rating, max_rating)
        
        elif choice == '3':
            review_id = input("\nEnter Review ID: ").strip()
            if review_id.isdigit():
                num_results = input("Number of similar reviews (default 5): ").strip()
                top_k = int(num_results) if num_results.isdigit() else 5
                search_by_review_id(int(review_id), top_k)
            else:
                print("❌ Invalid Review ID")
        
        elif choice == '4':
            product_id = input("\nEnter Product ID: ").strip()
            if product_id.isdigit():
                sentiment = input("Sentiment filter (or press Enter to skip): ").strip()
                num_results = input("Number of results (default 5): ").strip()
                top_k = int(num_results) if num_results.isdigit() else 5
                
                search_product_reviews(int(product_id), 
                                     sentiment if sentiment else None, 
                                     top_k)
            else:
                print("❌ Invalid Product ID")
        
        elif choice == '5':
            run_demo_searches()
        
        elif choice == '6':
            print("\n👋 Goodbye!\n")
            break
        
        else:
            print("❌ Invalid choice. Please enter 1-6.")


def run_demo_searches():
    """Run demonstration searches with pre-defined queries"""
    
    demo_queries = [
        ("excellent quality", 5, None, None),
        ("disappointed with purchase", 5, None, None),
        ("great value for money", 3, 4, 5),
        ("poor quality", 3, 1, 2),
    ]
    
    print("\n" + "="*70)
    print("Running Demo Searches")
    print("="*70)
    
    for query, top_k, min_rating, max_rating in demo_queries:
        search_reviews(query, top_k, min_rating, max_rating)
        input("\nPress Enter to continue to next search...")


# =====================================================
# MAIN ENTRY POINT
# =====================================================

def main():
    """Main function"""
    
    # Check for .env file
    if not os.path.exists('vector_db.env'):
        print("❌ .env file not found!")
        print("\nCreate 'vector_db.env' with:")
        print("  GOOGLE_API_KEY=your_key")
        print("  MARIADB_HOST=your_host")
        print("  MARIADB_USER=your_user")
        print("  MARIADB_PASSWORD=your_password")
        print("  MARIADB_DATABASE=AetherMart\n")
        sys.exit(1)
    
    # Check if reviews have embeddings
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT COUNT(*) 
            FROM reviews 
            WHERE review_embedding IS NOT NULL
        """)
        count = cursor.fetchone()[0]
        
        if count == 0:
            print("⚠️  Warning: No reviews have embeddings yet.")
            print("   Run the review embedding generation script first.\n")
            sys.exit(1)
        else:
            print(f"✅ Found {count} reviews with embeddings\n")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error checking database: {e}\n")
        sys.exit(1)
    
    # Start interactive search
    interactive_search()


if __name__ == "__main__":
    main()