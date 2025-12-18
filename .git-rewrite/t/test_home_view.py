"""
Test the home view directly
"""
from app import get_conn, available_months_from_db
import traceback

def test_home_logic():
    try:
        print("1. יוצר חיבור...")
        conn = get_conn()
        print("[OK] חיבור נוצר!")
        
        print("\n2. בודק available_months_from_db...")
        months_all = available_months_from_db(conn.conn)  # Access underlying connection
        print(f"[OK] מצא {len(months_all)} חודשים")
        
        print("\n3. בודק שאילתת המדריכים...")
        cursor = conn.execute("""
            SELECT id, name, type, is_active, start_date
            FROM people 
            WHERE is_active::integer = 1
            ORDER BY name
            LIMIT 5
        """)
        guides = cursor.fetchall()
        print(f"[OK] מצא {len(guides)} מדריכים (הצגת 5 ראשונים)")
        for g in guides:
            print(f"    - {g['name']} (ID: {g['id']})")
        
        conn.close()
        print("\n[SUCCESS] הבדיקה הצליחה!")
        return True
        
    except Exception as e:
        print(f"\n[ERROR] שגיאה: {e}")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_home_logic()




