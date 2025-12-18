from logic import get_db_connection
import psycopg2.extras

conn = get_db_connection()
cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# Check people table schema
cursor.execute("""
    SELECT column_name, data_type, udt_name
    FROM information_schema.columns 
    WHERE table_name = 'people'
    ORDER BY ordinal_position
""")

print("People table schema:")
for col in cursor.fetchall():
    print(f"  {col['column_name']}: {col['data_type']} ({col['udt_name']})")

# Check a sample row
cursor.execute("SELECT id, name, start_date FROM people LIMIT 1")
row = cursor.fetchone()
print(f"\nSample row:")
print(f"  start_date value: {row['start_date']}")
print(f"  start_date type: {type(row['start_date'])}")

cursor.close()
conn.close()




