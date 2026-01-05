#!/usr/bin/env python3
"""
Milestone 4: Generate Meaningful Review Text for AetherMart
Step 1: Reset review_text to NULL for all reviews
Step 2: Generate meaningful reviews for 50 random reviews
"""

import mariadb
import os
from dotenv import load_dotenv
import random

load_dotenv('vector_db.env')

# =====================================================
# CONFIGURATION
# =====================================================

DB_CONFIG = {
    'host': os.getenv("MARIADB_HOST"),
    'port': 3306,
    'user': os.getenv("MARIADB_USER"),
    'password': os.getenv("MARIADB_PASSWORD"),
    'database': os.getenv("MARIADB_DATABASE")
}

# Number of reviews to update
NUM_REVIEWS_TO_UPDATE = 50

# =====================================================
# REVIEW TEMPLATES BY RATING
# =====================================================

# 5-star reviews (Excellent)
FIVE_STAR_REVIEWS = [
    "Absolutely love this product! Exceeded all my expectations. The quality is outstanding and it arrived quickly. Highly recommend!",
    "Best purchase I've made in a long time. Works perfectly and the build quality is amazing. Worth every penny!",
    "This product is fantastic! Easy to use, great quality, and excellent value for money. Will definitely buy again.",
    "Couldn't be happier with this purchase. The product does exactly what it promises and more. Five stars all the way!",
    "Outstanding product! The attention to detail is impressive. Fast shipping and excellent customer service too.",
    "Amazing quality and functionality. This has become an essential part of my daily routine. Highly satisfied!",
    "Exactly what I was looking for! Superior quality, great design, and performs flawlessly. Totally recommend it.",
    "This product exceeded my expectations in every way. Durable, efficient, and looks great. Very impressed!",
    "Perfect! Works like a charm and the quality is top-notch. Best investment I've made this year.",
    "Incredible product! Easy to use, well-made, and does exactly what it's supposed to do. Love it!",
]

# 4-star reviews (Good)
FOUR_STAR_REVIEWS = [
    "Really good product overall. Works well and good quality. Only minor issue is the instructions could be clearer.",
    "Very satisfied with this purchase. Does what it's supposed to do. Would give 5 stars but delivery took a bit long.",
    "Great product for the price. Quality is good and it works well. Just wish it came in more color options.",
    "Solid product. Does the job well and seems durable. Taking off one star because assembly was a bit tricky.",
    "Good purchase. Works as advertised and quality seems good. Only downside is it's a bit bulkier than expected.",
    "Happy with this product. Performs well and good value for money. Could be slightly better but no major complaints.",
    "Nice product. Quality is good and it works reliably. Would be perfect if it included a carrying case.",
    "Pleased with this purchase. Functions well and appears well-made. Minor gripe: wish it had more features.",
    "Pretty good overall. Does what I need it to do. Quality is decent, just a couple of minor design quirks.",
    "Solid choice. Works well and good build quality. Only issue is the user manual isn't very detailed.",
]

# 3-star reviews (Average)
THREE_STAR_REVIEWS = [
    "It's okay. Does the basic job but nothing special. Quality is average. For the price, expected a bit more.",
    "Decent product but has some issues. Works most of the time but occasionally glitchy. Could be better.",
    "Average product. It works but doesn't impress. Quality is so-so. Might look for alternatives next time.",
    "Mixed feelings about this. Some features are good, others not so much. Quality could be improved.",
    "It's alright for the price. Does what it claims but don't expect too much. Average quality and performance.",
    "Functional but not impressive. Gets the job done but feels cheaply made. Probably won't last long.",
    "Mediocre product. Works but nothing to write home about. Expected better quality for this price point.",
    "Just okay. Has some good points but also some disappointing aspects. Quality is hit or miss.",
    "Neither great nor terrible. Does its basic function but lacks polish. Build quality leaves something to be desired.",
    "Fair product. Works as described but quality isn't great. For the money, you get what you pay for.",
]

# 2-star reviews (Below Average)
TWO_STAR_REVIEWS = [
    "Pretty disappointed with this purchase. Quality is poor and it stopped working properly after a week. Not recommended.",
    "Not what I expected. Feels cheaply made and doesn't work as well as advertised. Returning it.",
    "Poor quality product. Had issues right out of the box. Customer service wasn't helpful either. Waste of money.",
    "Regret buying this. Doesn't live up to the description. Quality is subpar and it broke after minimal use.",
    "Very disappointing. Product feels flimsy and doesn't function as expected. Would not purchase again.",
    "Not worth the money. Quality is terrible and it didn't even last a month. Looking for a refund.",
    "Unhappy with this purchase. Arrived damaged and replacement wasn't much better. Poor quality control.",
    "Below expectations. Doesn't work properly and feels like it could break any moment. Skip this one.",
    "Disappointed. Product description was misleading. Quality is poor and functionality is limited. Not satisfied.",
    "Not good. Had high hopes but product is poorly made and doesn't perform well. Returning ASAP.",
]

# 1-star reviews (Poor)
ONE_STAR_REVIEWS = [
    "Terrible product! Broke after one use. Complete waste of money. Do NOT buy this!",
    "Worst purchase ever. Doesn't work at all and customer service is terrible. Avoid at all costs!",
    "Absolutely awful. Product arrived broken and getting a refund has been a nightmare. Zero stars if I could.",
    "Complete garbage. Nothing works as described. Feels like a scam. Stay away!",
    "Don't waste your money! This product is junk. Stopped working immediately and no response from seller.",
    "Horrible quality. Broke within hours of use. Clearly cheaply made. Total rip-off!",
    "Worst product I've ever bought. Doesn't function at all. Requesting immediate refund. Terrible!",
    "Completely useless! Doesn't work as advertised and fell apart after first use. Absolutely terrible.",
    "Total disappointment. Product is defective and seller won't respond. Scam alert! Don't buy!",
    "Awful in every way. Poor quality, doesn't work, and impossible to return. Save your money!",
]

# =====================================================
# HELPER FUNCTIONS
# =====================================================

def get_review_template(rating):
    """Get a random review template based on rating"""
    if rating == 5:
        return random.choice(FIVE_STAR_REVIEWS)
    elif rating == 4:
        return random.choice(FOUR_STAR_REVIEWS)
    elif rating == 3:
        return random.choice(THREE_STAR_REVIEWS)
    elif rating == 2:
        return random.choice(TWO_STAR_REVIEWS)
    elif rating == 1:
        return random.choice(ONE_STAR_REVIEWS)
    else:
        return "This product is okay."  # Fallback


def reset_all_reviews(cursor):
    """Set all review_text to NULL"""
    print("\n" + "="*70)
    print("STEP 1: Resetting all review_text to NULL")
    print("="*70)
    
    try:
        # Get current count
        cursor.execute("SELECT COUNT(*) FROM reviews WHERE review_text IS NOT NULL")
        before_count = cursor.fetchone()[0]
        print(f"\nReviews with text before reset: {before_count}")
        
        # Reset to NULL
        cursor.execute("UPDATE reviews SET review_text = NULL")
        
        # Verify
        cursor.execute("SELECT COUNT(*) FROM reviews WHERE review_text IS NOT NULL")
        after_count = cursor.fetchone()[0]
        
        print(f"Reviews with text after reset: {after_count}")
        print(f"✅ Successfully reset {before_count} reviews to NULL\n")
        
        return True
        
    except mariadb.Error as e:
        print(f"❌ Error resetting reviews: {e}")
        return False


def select_random_reviews(cursor, num_reviews):
    """Select random reviews with valid ratings"""
    print("="*70)
    print(f"STEP 2: Selecting {num_reviews} random reviews")
    print("="*70 + "\n")
    
    try:
        # Get reviews with valid ratings (1-5)
        cursor.execute("""
            SELECT review_id, product_id, customer_id, rating
            FROM reviews
            WHERE rating IS NOT NULL
              AND rating BETWEEN 1 AND 5
            ORDER BY RAND()
            LIMIT ?
        """, (num_reviews,))
        
        reviews = cursor.fetchall()
        
        if not reviews:
            print("❌ No reviews found with valid ratings")
            return []
        
        print(f"✅ Selected {len(reviews)} random reviews\n")
        return reviews
        
    except mariadb.Error as e:
        print(f"❌ Error selecting reviews: {e}")
        return []


def generate_meaningful_reviews(cursor, reviews):
    """Generate and update meaningful review text"""
    print("="*70)
    print(f"STEP 3: Generating meaningful review text")
    print("="*70 + "\n")
    
    successful = 0
    failed = 0
    
    for review_id, product_id, customer_id, rating in reviews:
        try:

            # --- START OF FIX ---
            # Convert the rating string (e.g., "5") to an integer (e.g., 5)
            int_rating = int(rating)
            # --- END OF FIX ---

            # Get appropriate review template (use the new integer)
            review_text = get_review_template(int_rating)
            
            # Update review
            cursor.execute("""
                UPDATE reviews
                SET review_text = ?
                WHERE review_id = ?
            """, (review_text, review_id))
            
            # Display progress (use the new integer for multiplication)
            stars = "⭐" * int_rating
            print(f"✅ Review ID {review_id} | {stars} ({int_rating}/5) | Product ID: {product_id}")
            print(f"  Preview: {review_text[:80]}...\n")
            
            successful += 1
            
        except mariadb.Error as e:
            print(f"❌ Error updating review ID {review_id}: {e}\n")
            failed += 1
    
    return successful, failed


def display_sample_reviews(cursor, num_samples=10):
    """Display sample of updated reviews"""
    print("\n" + "="*70)
    print(f"Sample of Updated Reviews")
    print("="*70 + "\n")
    
    try:
        # ... (cursor.execute) ...
        samples = cursor.fetchall()
        
        for idx, (review_id, product_name, rating, preview) in enumerate(samples, 1):
            
            # --- START OF FIX ---
            # Convert the rating string to an integer
            int_rating = int(rating)
            stars = "⭐" * int_rating
            # --- END OF FIX ---
            
            print(f"{idx}. Review ID: {review_id}")
            print(f"   Product: {product_name}")
            # Use the int_rating here
            print(f"   Rating: {stars} ({int_rating}/5)")
            print(f"   Review: {preview}...")
            print(f"   {'-'*66}\n")
            
    except Exception as e:
        print(f"❌ Error displaying samples: {e}")


# =====================================================
# MAIN EXECUTION
# =====================================================

def main():
    """Main execution flow"""
    
    print("\n" + "="*70)
    print("AetherMart - Meaningful Review Text Generation")
    print("="*70)
    print(f"\nConfiguration:")
    print(f"  Reviews to update: {NUM_REVIEWS_TO_UPDATE}")
    print(f"  Database: {DB_CONFIG['database']}")
    print("="*70)
    
    conn = None
    cursor = None
    
    try:
        # Connect to database
        print("\nConnecting to database...")
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("✅ Connected successfully\n")
        
        # Step 1: Reset all review_text to NULL
        if not reset_all_reviews(cursor):
            print("❌ Failed to reset reviews. Exiting.")
            return
        
        conn.commit()
        print("✅ Changes committed\n")
        
        # Step 2: Select random reviews
        selected_reviews = select_random_reviews(cursor, NUM_REVIEWS_TO_UPDATE)
        
        if not selected_reviews:
            print("❌ No reviews selected. Exiting.")
            return
        
        # Step 3: Generate meaningful reviews
        successful, failed = generate_meaningful_reviews(cursor, selected_reviews)
        
        # Commit changes
        conn.commit()
        print("="*70)
        print("✅ All changes committed to database")
        print("="*70)
        
        # Display summary
        print(f"\nFinal Summary:")
        print(f"  ✅ Successful: {successful}")
        print(f"  ❌ Failed: {failed}")
        print(f"  📊 Success Rate: {(successful/(successful+failed)*100) if (successful+failed) > 0 else 0:.1f}%")
        print("="*70)
        
        # Display sample reviews
        display_sample_reviews(cursor, num_samples=10)
        
        # Final verification
        cursor.execute("SELECT COUNT(*) FROM reviews WHERE review_text IS NOT NULL")
        final_count = cursor.fetchone()[0]
        
        print("\n" + "="*70)
        print(f"✅ Process Complete!")
        print(f"   Total reviews with meaningful text: {final_count}")
        print("="*70 + "\n")
        
        print("Next Steps:")
        print("  1. Generate embeddings for these 50 reviews")
        print("  2. Perform similarity search on reviews")
        print("  3. Document findings in presentation\n")
        
    except mariadb.Error as e:
        print(f"\n❌ Database Error: {e}")
        if conn:
            conn.rollback()
            print("   Transaction rolled back")
    
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


if __name__ == "__main__":
    # Check for .env file
    if not os.path.exists('vector_db.env'):
        print("❌ .env file not found!")
        print("\nCreate a .env file with:")
        print("  MARIADB_HOST=your_host")
        print("  MARIADB_USER=your_user")
        print("  MARIADB_PASSWORD=your_password")
        print("  MARIADB_DATABASE=AetherMart\n")
        exit(1)
    
    main()