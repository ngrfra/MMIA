"""
DEBUG SCRIPT - Capire perché Content non appare
"""

from database import get_connection
import pandas as pd

def check_database_content():
    """Verifica cosa c'è veramente nel database"""
    
    conn = get_connection()
    
    print("=" * 70)
    print("DATABASE CONTENT CHECK")
    print("=" * 70)
    
    # 1. Check posts_inventory
    print("\n1. POSTS_INVENTORY TABLE:")
    print("-" * 70)
    
    try:
        count_inventory = conn.execute("SELECT COUNT(*) FROM posts_inventory").fetchone()[0]
        print(f"Total rows: {count_inventory}")
        
        if count_inventory > 0:
            df_inv = pd.read_sql_query("SELECT * FROM posts_inventory LIMIT 5", conn)
            print("\nSample data:")
            print(df_inv)
            print("\nColumns:", df_inv.columns.tolist())
        else:
            print("❌ TABLE IS EMPTY!")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # 2. Check posts_performance
    print("\n\n2. POSTS_PERFORMANCE TABLE:")
    print("-" * 70)
    
    try:
        count_perf = conn.execute("SELECT COUNT(*) FROM posts_performance").fetchone()[0]
        print(f"Total rows: {count_perf}")
        
        if count_perf > 0:
            df_perf = pd.read_sql_query("SELECT * FROM posts_performance LIMIT 5", conn)
            print("\nSample data:")
            print(df_perf)
            print("\nColumns:", df_perf.columns.tolist())
        else:
            print("❌ TABLE IS EMPTY!")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # 3. Test JOIN query (quello usato da get_content_health)
    print("\n\n3. JOIN QUERY TEST (get_content_health):")
    print("-" * 70)
    
    try:
        query = """
        SELECT i.post_id, 
               i.platform, 
               i.date_published, 
               i.caption, 
               i.link,
               p.views, 
               p.likes, 
               p.comments, 
               p.shares,
               p.date_recorded
        FROM posts_inventory i
        JOIN posts_performance p ON i.post_id = p.post_id
        WHERE p.date_recorded = (
            SELECT MAX(date_recorded) 
            FROM posts_performance 
            WHERE post_id = i.post_id
        )
        ORDER BY p.views DESC
        LIMIT 10
        """
        
        df_join = pd.read_sql_query(query, conn)
        
        print(f"Rows returned: {len(df_join)}")
        
        if not df_join.empty:
            print("\n✅ JOIN WORKS! Data:")
            print(df_join)
        else:
            print("❌ JOIN RETURNS EMPTY!")
            print("\nDEBUGGING:")
            
            # Check if post_ids match
            post_ids_inv = conn.execute("SELECT DISTINCT post_id FROM posts_inventory").fetchall()
            post_ids_perf = conn.execute("SELECT DISTINCT post_id FROM posts_performance").fetchall()
            
            print(f"\nPost IDs in inventory: {[p[0] for p in post_ids_inv[:5]]}")
            print(f"Post IDs in performance: {[p[0] for p in post_ids_perf[:5]]}")
            
            # Check if they match
            inv_set = set(p[0] for p in post_ids_inv)
            perf_set = set(p[0] for p in post_ids_perf)
            
            matching = inv_set & perf_set
            print(f"\nMatching post_ids: {len(matching)}")
            
            if len(matching) == 0:
                print("❌ NO MATCHING POST IDS - This is the problem!")
    
    except Exception as e:
        print(f"❌ Query error: {e}")
        import traceback
        traceback.print_exc()
    
    # 4. Check social_stats for comparison
    print("\n\n4. SOCIAL_STATS (for comparison):")
    print("-" * 70)
    
    try:
        count_stats = conn.execute("SELECT COUNT(*) FROM social_stats").fetchone()[0]
        print(f"Total rows: {count_stats}")
        
        if count_stats > 0:
            df_stats = pd.read_sql_query("SELECT * FROM social_stats LIMIT 5", conn)
            print("\nSample data:")
            print(df_stats)
    except Exception as e:
        print(f"❌ Error: {e}")
    
    conn.close()
    
    print("\n" + "=" * 70)
    print("END OF DEBUG")
    print("=" * 70)

def test_content_insert():
    """Test manuale insert di content"""
    
    print("\n" + "=" * 70)
    print("MANUAL CONTENT INSERT TEST")
    print("=" * 70)
    
    conn = get_connection()
    
    try:
        # Insert test post
        test_post_id = "TEST_POST_123"
        test_date = "2024-11-25"
        
        print(f"\nInserting test post: {test_post_id}")
        
        # Insert inventory
        conn.execute(
            "INSERT OR REPLACE INTO posts_inventory (post_id, platform, date_published, caption, link) VALUES (?,?,?,?,?)",
            (test_post_id, "Instagram", test_date, "Test Caption", "https://instagram.com/p/TEST")
        )
        print("✅ Inventory inserted")
        
        # Insert performance
        conn.execute(
            "INSERT OR REPLACE INTO posts_performance (post_id, date_recorded, views, likes, comments, shares) VALUES (?,?,?,?,?,?)",
            (test_post_id, test_date, 5000, 234, 12, 45)
        )
        print("✅ Performance inserted")
        
        conn.commit()
        
        # Try to retrieve
        query = """
        SELECT i.post_id, i.platform, i.caption, p.views, p.likes
        FROM posts_inventory i
        JOIN posts_performance p ON i.post_id = p.post_id
        WHERE i.post_id = ?
        """
        
        result = pd.read_sql_query(query, conn, params=(test_post_id,))
        
        if not result.empty:
            print("\n✅ TEST POST RETRIEVED:")
            print(result)
        else:
            print("\n❌ TEST POST NOT RETRIEVED")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    print("\n🔍 YANGKIDD CONTENT DEBUG TOOL\n")
    
    check_database_content()
    test_content_insert()
    
    print("\n\n📋 NEXT STEPS:")
    print("1. If posts_inventory is EMPTY -> Content CSV not being parsed")
    print("2. If posts_performance is EMPTY -> Content not being saved to performance table")
    print("3. If JOIN returns EMPTY but tables have data -> post_id mismatch")
    print("4. Run this script and send me the output!")