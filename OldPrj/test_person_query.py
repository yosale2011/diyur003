from logic import get_db_connection
import psycopg2.extras
import traceback

conn = get_db_connection()
cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

person_id = 78

print(f"Executing query for person_id={person_id}...")

try:
    cursor.execute("SELECT * FROM people WHERE id = %s", (person_id,))
    print("[1] Query executed")
    
    print("[2] Fetching result...")
    person = cursor.fetchone()
    
    print(f"[3] Result fetched: {person is not None}")
    if person:
        print(f"[4] Person fields:")
        for key in person.keys():
            val = person[key]
            print(f"    {key}: {val} (type: {type(val).__name__})")
            
except Exception as e:
    print(f"[ERROR] {e}")
    traceback.print_exc()
finally:
    cursor.close()
    conn.close()




