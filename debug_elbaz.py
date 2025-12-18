import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from datetime import date, timedelta
from logic import span_minutes, MINUTES_PER_DAY, overlap_minutes

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

def debug_elbaz():
    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    # 1. Find person
    cursor.execute("SELECT id, name FROM people WHERE name LIKE %s", ('%אלבז אריאל%',))
    person = cursor.fetchone()
    if not person:
        print("Person not found")
        return
    person_id = person['id']
    print(f"Found: {person['name']} (ID: {person_id})")
    
    year, month = 2025, 11
    start_date = date(year, month, 1)
    end_date = date(year, month, 3) # Look at first few days
    
    cursor.execute("""
        SELECT tr.*, st.name as shift_name 
        FROM time_reports tr
        LEFT JOIN shift_types st ON st.id = tr.shift_type_id
        WHERE tr.person_id = %s AND tr.date >= %s AND tr.date < %s
        ORDER BY tr.date, tr.start_time
    """, (person_id, start_date, end_date))
    
    reports = cursor.fetchall()
    
    print("\n=== REPORTS ===")
    shift_ids = set()
    for r in reports:
        print(f"Report ID {r['id']}: {r['date']} | {r['start_time']} - {r['end_time']} | Shift {r['shift_type_id']}")
        if r['shift_type_id']:
            shift_ids.add(r['shift_type_id'])
            
    # Get shift segments
    print(f"\n=== SHIFT SEGMENTS (IDs: {shift_ids}) ===")
    segments_by_shift = {}
    for shift_id in shift_ids:
        cursor.execute("""
            SELECT * FROM shift_time_segments 
            WHERE shift_type_id = %s 
            ORDER BY order_index
        """, (shift_id,))
        segs = cursor.fetchall()
        segments_by_shift[shift_id] = segs
        print(f"\nShift {shift_id}:")
        for s in segs:
            seg_start, seg_end = span_minutes(s["start_time"], s["end_time"])
            print(f"  {s['start_time']}-{s['end_time']} ({seg_start}-{seg_end} min) | Type: {s['segment_type']} | Wage: {s['wage_percent']}%")

    conn.close()

if __name__ == "__main__":
    debug_elbaz()
