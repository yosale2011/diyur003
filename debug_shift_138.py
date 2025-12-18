import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

def check_shift_138():
    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    cursor.execute("SELECT * FROM shift_types WHERE id = 138")
    shift_type = cursor.fetchone()
    print(f"Shift Type 138: {shift_type['name'] if shift_type else 'Not found'}")
    
    cursor.execute("SELECT * FROM shift_time_segments WHERE shift_type_id = 138")
    segments = cursor.fetchall()
    print("\nSegments for 138:")
    for s in segments:
        print(f"Start: {s['start_time']}, End: {s['end_time']}, Type: {s['segment_type']}, Wage: {s['wage_percent']}%")
    
    conn.close()

if __name__ == "__main__":
    check_shift_138()

