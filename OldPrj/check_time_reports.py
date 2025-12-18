from logic import get_db_connection
import psycopg2.extras

conn = get_db_connection()
cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# Check time_reports schema - focusing on date field
cursor.execute("""
    SELECT column_name, data_type, udt_name
    FROM information_schema.columns 
    WHERE table_name = 'time_reports' AND column_name = 'date'
""")

print("time_reports.date column:")
for col in cursor.fetchall():
    print(f"  {col['column_name']}: {col['data_type']} ({col['udt_name']})")

# Check a sample row
cursor.execute("SELECT id, person_id, date, start_time, end_time FROM time_reports LIMIT 3")
print(f"\nSample rows:")
for row in cursor.fetchall():
    print(f"  date value: {row['date']}, type: {type(row['date'])}")
    print(f"  start_time: {row['start_time']}, type: {type(row['start_time'])}")

cursor.close()
conn.close()




