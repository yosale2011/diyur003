import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

def check_person():
    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    # 1. Find the person
    cursor.execute("SELECT id, name FROM people WHERE name LIKE %s", ('%אברמוף יוכבד חנה%',))
    person = cursor.fetchone()
    if not person:
        print("Person not found")
        return
    
    person_id = person['id']
    print(f"Found: {person['name']} (ID: {person_id})")
    
    # 2. Get reports for November 2025
    # The date is stored as a date or timestamp? logic.py uses unix timestamp for month_range_ts but queries against date.
    # In logic.py: WHERE tr.person_id = %s AND tr.date >= %s AND tr.date < %s
    # date column in time_reports is DATE in PostgreSQL.
    
    from datetime import date
    start_date = date(2025, 11, 20)
    end_date = date(2025, 11, 26)
    
    cursor.execute("""
        SELECT tr.*, st.name as shift_name 
        FROM time_reports tr
        LEFT JOIN shift_types st ON st.id = tr.shift_type_id
        WHERE tr.person_id = %s AND tr.date >= %s AND tr.date < %s
        ORDER BY tr.date, tr.start_time
    """, (person_id, start_date, end_date))
    
    reports = cursor.fetchall()
    print("\nReports:")
    shift_ids = set()
    for r in reports:
        print(f"ID: {r['id']}, Date: {r['date']}, Start: {r['start_time']}, End: {r['end_time']}, Shift: {r['shift_name']} (Type ID: {r['shift_type_id']})")
        if r['shift_type_id']:
            shift_ids.add(r['shift_type_id'])

    if shift_ids:
        print("\nShift Segments:")
        placeholders = ",".join(["%s"] * len(shift_ids))
        cursor.execute(f"SELECT * FROM shift_time_segments WHERE shift_type_id IN ({placeholders})", tuple(shift_ids))
        segments = cursor.fetchall()
        for s in segments:
            print(f"Shift ID: {s['shift_type_id']}, Start: {s['start_time']}, End: {s['end_time']}, Type: {s['segment_type']}, Wage: {s['wage_percent']}%")

    conn.close()

if __name__ == "__main__":
    check_person()

