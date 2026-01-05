#!/usr/bin/env python3
"""
Milestone 4: Product Similarity Search for AetherMart
Semantic search using vector embeddings
"""

import mariadb
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from dotenv import load_dotenv
import os
import sys

load_dotenv('.env')

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
# SIMILARITY SEARCH FUNCTION
# =====================================================

def search_products(query, top_k=5):
    """
    Perform semantic similarity search on products
    
    Args:
        query: Search query string
        top_k: Number of results to return
    """
    print(f"\n{'='*70}")
    print(f"Searching for: '{query}'")
    print(f"{'='*70}\n")
    
    try:
        # Generate query embedding
        print("Generating query embedding...")
        query_vector = embeddings.embed_query(query)
        query_vector_str = str(query_vector)
        
        # Connect to database
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Perform similarity search
        search_query = """
        SELECT 
            p.product_id, 
            p.product_name, 
            p.product_description,
            p.price,
            c.category_name,
            VEC_DISTANCE(p.product_embedding, vec_fromtext(?)) as distance
        FROM products p
        JOIN categories c ON p.category_id = c.category_id
        WHERE p.product_description IS NOT NULL 
          AND p.product_embedding IS NOT NULL
        ORDER BY distance ASC
        LIMIT ?
        """
        
        cursor.execute(search_query, (query_vector_str, top_k))
        results = cursor.fetchall()
        
        if not results:
            print("❌ No results found.")
            print("   Make sure products have descriptions and embeddings.")
        else:
            print(f"✅ Found {len(results)} relevant product(s):\n")
            
            for idx, (product_id, name, description, price, category, distance) in enumerate(results, 1):
                similarity_score = 1 - distance
                
                print(f"{idx}. {name}")
                print(f"   Product ID: {product_id}")
                print(f"   Category: {category}")
                print(f"   Price: ${price:.2f}")
                print(f"   Description: {description[:120]}...")
                print(f"   Similarity Score: {similarity_score:.4f} ({similarity_score*100:.1f}%)")
                print(f"   {'-'*66}\n")
        
        cursor.close()
        conn.close()
        
    except mariadb.Error as e:
        print(f"❌ Database Error: {e}")
    except Exception as e:
        print(f"❌ Search Error: {e}")


def search_by_product_id(product_id, top_k=5):
    """
    Find products similar to a specific product
    
    Args:
        product_id: ID of the reference product
        top_k: Number of similar products to return
    """
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Get the reference product
        cursor.execute("""
            SELECT product_name, product_description, product_embedding
            FROM products
            WHERE product_id = ? AND product_embedding IS NOT NULL
        """, (product_id,))
        
        result = cursor.fetchone()
        
        if not result:
            print(f"❌ Product ID {product_id} not found or has no embedding.")
            return
        
        product_name, product_desc, product_embedding = result
        
        print(f"\n{'='*70}")
        print(f"Finding products similar to: '{product_name}' (ID: {product_id})")
        print(f"{'='*70}\n")
        print(f"Reference Product Description:")
        print(f"  {product_desc[:150]}...\n")
        
        # Find similar products
        search_query = """
        SELECT 
            p.product_id, 
            p.product_name, 
            p.product_description,
            p.price,
            c.category_name,
            VEC_DISTANCE(p.product_embedding, ?) as distance
        FROM products p
        JOIN categories c ON p.category_id = c.category_id
        WHERE p.product_id != ?
          AND p.product_embedding IS NOT NULL
        ORDER BY distance ASC
        LIMIT ?
        """
        
        cursor.execute(search_query, (product_embedding, product_id, top_k))
        results = cursor.fetchall()
        
        if results:
            print(f"✅ Found {len(results)} similar product(s):\n")
            
            for idx, (pid, name, description, price, category, distance) in enumerate(results, 1):
                similarity_score = 1 - distance
                
                print(f"{idx}. {name}")
                print(f"   Product ID: {pid}")
                print(f"   Category: {category}")
                print(f"   Price: ${price:.2f}")
                print(f"   Description: {description[:120]}...")
                print(f"   Similarity: {similarity_score:.4f} ({similarity_score*100:.1f}%)")
                print(f"   {'-'*66}\n")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")


# =====================================================
# INTERACTIVE SEARCH MENU
# =====================================================

def interactive_search():
    """Interactive menu for similarity search"""
    
    print("\n" + "="*70)
    print("AetherMart Product Similarity Search")
    print("="*70)
    
    while True:
        print("\nSearch Options:")
        print("1. Search by text query")
        print("2. Find similar products by Product ID")
        print("3. Run demo searches")
        print("4. Exit")
        
        choice = input("\nEnter your choice (1-4): ").strip()
        
        if choice == '1':
            query = input("\nEnter your search query: ").strip()
            if query:
                num_results = input("Number of results (default 5): ").strip()
                top_k = int(num_results) if num_results.isdigit() else 5
                search_products(query, top_k)
        
        elif choice == '2':
            product_id = input("\nEnter Product ID: ").strip()
            if product_id.isdigit():
                num_results = input("Number of similar products (default 5): ").strip()
                top_k = int(num_results) if num_results.isdigit() else 5
                search_by_product_id(int(product_id), top_k)
            else:
                print("❌ Invalid Product ID")
        
        elif choice == '3':
            run_demo_searches()
        
        elif choice == '4':
            print("\n👋 Goodbye!\n")
            break
        
        else:
            print("❌ Invalid choice. Please enter 1-4.")


def run_demo_searches():
    """Run demonstration searches with pre-defined queries"""
    
    demo_queries = [
        ("a gadget for my kitchen", 5),
        ("gaming computer", 3),
        ("wireless device", 3),
        ("home office furniture", 3),
    ]
    
    print("\n" + "="*70)
    print("Running Demo Searches")
    print("="*70)
    
    for query, top_k in demo_queries:
        search_products(query, top_k)
        input("\nPress Enter to continue to next search...")


# =====================================================
# MAIN ENTRY POINT
# =====================================================

def main():
    """Main function"""
    
    # Check for .env file
    if not os.path.exists('.env'):
        print("❌ .env file not found!")
        print("\nCreate a .env file with:")
        print("  GOOGLE_API_KEY=your_key")
        print("  MARIADB_HOST=your_host")
        print("  MARIADB_USER=your_user")
        print("  MARIADB_PASSWORD=your_password")
        print("  MARIADB_DATABASE=AetherMart\n")
        sys.exit(1)
    
    # Check if products have embeddings
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT COUNT(*) 
            FROM products 
            WHERE product_embedding IS NOT NULL
        """)
        count = cursor.fetchone()[0]
        
        if count == 0:
            print("⚠️  Warning: No products have embeddings yet.")
            print("   Run the vector generation script first.\n")
            sys.exit(1)
        else:
            print(f"✅ Found {count} products with embeddings\n")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error checking database: {e}\n")
        sys.exit(1)
    
    # Start interactive search
    interactive_search()


if __name__ == "__main__":
    main()