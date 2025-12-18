"""
Test logic functions with PostgreSQL
"""
from logic import get_db_connection, available_months_from_db, get_shabbat_times_cache
import traceback

def test_basic_queries():
    try:
        print("1. מתחבר למסד נתונים...")
        conn = get_db_connection()
        print("[OK] התחברנו!")
        
        print("\n2. בודק available_months_from_db...")
        months = available_months_from_db(conn)
        print(f"[OK] מצא {len(months)} חודשים עם דיווחים")
        if months:
            print(f"    חודשים אחרונים: {months[-3:]}")
        
        print("\n3. בודק get_shabbat_times_cache...")
        shabbat_cache = get_shabbat_times_cache(conn)
        print(f"[OK] מצא {len(shabbat_cache)} שבתות")
        if shabbat_cache:
            first_key = list(shabbat_cache.keys())[0]
            print(f"    דוגמה: {first_key} -> {shabbat_cache[first_key]}")
        
        print("\n4. בודק שאילתה ישירה...")
        import psycopg2.extras
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("SELECT COUNT(*) as count FROM people WHERE is_active = %s", (1,))
        result = cursor.fetchone()
        print(f"[OK] מצא {result['count']} מדריכים פעילים")
        cursor.close()
        
        conn.close()
        print("\n[SUCCESS] כל הבדיקות עברו בהצלחה!")
        return True
        
    except Exception as e:
        print(f"\n[ERROR] שגיאה: {e}")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_basic_queries()

