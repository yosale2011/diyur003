from logic import get_db_connection
import traceback

conn = get_db_connection()

# Try with regular cursor (no RealDictCursor)
cursor = conn.cursor()

person_id = 78

print(f"Testing with plain cursor for person_id={person_id}...")

try:
    cursor.execute("SELECT id, name, start_date FROM people WHERE id = %s", (person_id,))
    print("[1] Query executed")
    
    print("[2] Fetching result...")
    person = cursor.fetchone()
    
    print(f"[3] Result fetched!")
    print(f"    Result: {person}")
            
except Exception as e:
    print(f"[ERROR] {e}")
    traceback.print_exc()
finally:
    cursor.close()
    conn.close()




