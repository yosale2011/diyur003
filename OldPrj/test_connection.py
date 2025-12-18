"""
Test script to verify PostgreSQL connection
"""
from logic import get_db_connection
import psycopg2.extras

def test_connection():
    try:
        print("מנסה להתחבר למסד הנתונים...")
        conn = get_db_connection()
        print("[OK] החיבור הצליח!")
        
        # Test a simple query
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("SELECT version()")
        result = cursor.fetchone()
        print(f"[OK] PostgreSQL version: {result['version']}")
        
        # Test tables exist
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        tables = cursor.fetchall()
        print(f"[OK] מצא {len(tables)} טבלאות במסד הנתונים:")
        for table in tables[:10]:  # Show first 10 tables
            print(f"  - {table['table_name']}")
        if len(tables) > 10:
            print(f"  ... ועוד {len(tables) - 10} טבלאות")
        
        cursor.close()
        conn.close()
        print("\n[OK] הבדיקה הושלמה בהצלחה!")
        return True
        
    except Exception as e:
        print(f"[ERROR] שגיאה בהתחברות: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_connection()

