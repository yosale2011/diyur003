"""
Full calculation test with PostgreSQL
"""
from logic import (
    get_db_connection, 
    calculate_person_monthly_totals, 
    get_shabbat_times_cache,
    DEFAULT_MINIMUM_WAGE
)
import psycopg2.extras
import traceback

def test_calculation():
    try:
        print("1. מתחבר למסד הנתונים...")
        conn = get_db_connection()
        print("[OK] התחברנו!")
        
        # Get a person with reports
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("""
            SELECT DISTINCT p.id, p.name
            FROM people p
            JOIN time_reports tr ON tr.person_id = p.id
            WHERE p.is_active::integer = 1
            LIMIT 1
        """)
        person = cursor.fetchone()
        
        if not person:
            print("[WARNING] לא נמצא מדריך עם דיווחים")
            return
            
        print(f"[OK] נמצא מדריך: {person['name']} (ID: {person['id']})")
        
        # Get shabbat cache
        print("\n2. טוען זמני שבת...")
        from logic import get_shabbat_times_cache
        shabbat_cache = get_shabbat_times_cache(conn)
        print(f"[OK] נטענו {len(shabbat_cache)} זמני שבת")
        
        # Calculate monthly totals
        print("\n3. מחשב סיכום חודשי...")
        year, month = 2025, 11  # November 2025
        
        totals = calculate_person_monthly_totals(
            conn=conn,
            person_id=person['id'],
            year=year,
            month=month,
            shabbat_cache=shabbat_cache,
            minimum_wage=DEFAULT_MINIMUM_WAGE
        )
        
        print(f"[OK] חישוב הושלם!")
        print(f"\n=== סיכום עבור {person['name']} - {month}/{year} ===")
        print(f"  סה\"כ שעות: {totals.get('total_hours', 0) / 60:.2f}")
        print(f"  ימי עבודה: {totals.get('actual_work_days', 0)}")
        print(f"  כוננויות: {totals.get('standby', 0)}")
        print(f"  תשלום בסיסי: ₪{totals.get('payment', 0):.2f}")
        print(f"  תשלום כוננויות: ₪{totals.get('standby_payment', 0):.2f}")
        print(f"  סה\"כ תשלום: ₪{totals.get('total_payment', 0):.2f}")
        
        print("\n  פירוט שעות:")
        print(f"    100%: {totals.get('calc100', 0) / 60:.2f} שעות")
        print(f"    125%: {totals.get('calc125', 0) / 60:.2f} שעות")
        print(f"    150%: {totals.get('calc150', 0) / 60:.2f} שעות")
        print(f"    175%: {totals.get('calc175', 0) / 60:.2f} שעות")
        print(f"    200%: {totals.get('calc200', 0) / 60:.2f} שעות")
        
        cursor.close()
        conn.close()
        print("\n[SUCCESS] כל הבדיקות עברו בהצלחה!")
        return True
        
    except Exception as e:
        print(f"\n[ERROR] שגיאה: {e}")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_calculation()

