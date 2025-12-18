import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from datetime import date, timedelta
from logic import span_minutes, MINUTES_PER_DAY, overlap_minutes

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

def debug_avrahamov():
    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    person_id = 78
    year, month = 2025, 11
    
    # Get reports
    start_date = date(year, month, 20)
    end_date = date(year, month, 26)
    
    cursor.execute("""
        SELECT tr.*, st.name as shift_name 
        FROM time_reports tr
        LEFT JOIN shift_types st ON st.id = tr.shift_type_id
        WHERE tr.person_id = %s AND tr.date >= %s AND tr.date < %s
        ORDER BY tr.date, tr.start_time
    """, (person_id, start_date, end_date))
    
    reports = cursor.fetchall()
    
    print("=== REPORTS ===")
    for r in reports:
        print(f"Report ID {r['id']}: {r['date']} | {r['start_time']} - {r['end_time']} | Shift {r['shift_type_id']}")
        rep_start, rep_end = span_minutes(r["start_time"], r["end_time"])
        print(f"  Minutes: {rep_start} - {rep_end} (duration: {rep_end - rep_start})")
    
    # Get shift segments
    shift_ids = {r["shift_type_id"] for r in reports if r["shift_type_id"]}
    print(f"\n=== SHIFT SEGMENTS (IDs: {shift_ids}) ===")
    
    for shift_id in shift_ids:
        cursor.execute("""
            SELECT * FROM shift_time_segments 
            WHERE shift_type_id = %s 
            ORDER BY order_index
        """, (shift_id,))
        segs = cursor.fetchall()
        print(f"\nShift {shift_id}:")
        for s in segs:
            seg_start, seg_end = span_minutes(s["start_time"], s["end_time"])
            print(f"  {s['start_time']}-{s['end_time']} ({seg_start}-{seg_end} min) | Type: {s['segment_type']} | Wage: {s['wage_percent']}%")
    
    # Simulate the logic for report 992 (17:30-08:00)
    print("\n=== SIMULATING LOGIC FOR REPORT 992 ===")
    r = reports[0]  # First report
    rep_start, rep_end = span_minutes(r["start_time"], r["end_time"])
    
    # Split across midnight
    parts = []
    if rep_end <= MINUTES_PER_DAY:
        parts.append((r["date"], rep_start, rep_end))
    else:
        parts.append((r["date"], rep_start, MINUTES_PER_DAY))
        next_day = r["date"] + timedelta(days=1)
        parts.append((next_day, 0, rep_end - MINUTES_PER_DAY))
    
    print(f"Parts after midnight split:")
    for p_date, p_start, p_end in parts:
        print(f"  {p_date}: {p_start}-{p_end} minutes")
    
    # Get segments for this shift
    cursor.execute("""
        SELECT * FROM shift_time_segments 
        WHERE shift_type_id = %s 
        ORDER BY order_index
    """, (r["shift_type_id"],))
    seg_list = cursor.fetchall()
    
    print(f"\nProcessing segments for part 1 ({parts[0][1]}-{parts[0][2]}):")
    p_date, p_start, p_end = parts[0]
    
    # Split at 08:00 cutoff
    CUTOFF = 480
    sub_parts = []
    if p_start < CUTOFF < p_end:
        sub_parts.append((p_start, CUTOFF))
        sub_parts.append((CUTOFF, p_end))
    else:
        sub_parts.append((p_start, p_end))
    
    print(f"Sub-parts after 08:00 split:")
    for s_start, s_end in sub_parts:
        print(f"  {s_start}-{s_end} minutes")
    
    # Process first sub-part
    s_start, s_end = sub_parts[0]
    print(f"\nProcessing sub-part: {s_start}-{s_end}")
    
    # Sort segments chronologically FIRST
    seg_list_sorted = sorted(seg_list, key=lambda s: span_minutes(s["start_time"], s["end_time"])[0])
    print(f"\nSegments sorted chronologically:")
    for seg in seg_list_sorted:
        orig_s_start, orig_s_end = span_minutes(seg["start_time"], seg["end_time"])
        print(f"  {seg['start_time']}-{seg['end_time']} ({orig_s_start}-{orig_s_end} min)")
    
    last_s_end_norm = -1
    for seg in seg_list_sorted:
        orig_s_start, orig_s_end = span_minutes(seg["start_time"], seg["end_time"])
        print(f"\n  Segment: {seg['start_time']}-{seg['end_time']} ({orig_s_start}-{orig_s_end} min)")
        print(f"    Before normalization: {orig_s_start}-{orig_s_end}")
        
        while orig_s_start < last_s_end_norm:
            orig_s_start += MINUTES_PER_DAY
            orig_s_end += MINUTES_PER_DAY
            print(f"    After +1440: {orig_s_start}-{orig_s_end}")
        last_s_end_norm = orig_s_end
        
        is_second_day = False
        if is_second_day:
            current_seg_start = orig_s_start - MINUTES_PER_DAY
            current_seg_end = orig_s_end - MINUTES_PER_DAY
        else:
            current_seg_start = orig_s_start
            current_seg_end = orig_s_end
        
        print(f"    Final segment: {current_seg_start}-{current_seg_end}")
        print(f"    Report part: {s_start}-{s_end}")
        
        overlap = overlap_minutes(s_start, s_end, current_seg_start, current_seg_end)
        print(f"    OVERLAP: {overlap} minutes ({overlap/60:.2f} hours)")
        
        if overlap > 0:
            eff_start = max(current_seg_start, s_start)
            eff_end = min(current_seg_end, s_end)
            print(f"    Effective: {eff_start}-{eff_end} ({eff_start//60}:{eff_start%60:02d} - {eff_end//60}:{eff_end%60:02d})")
    
    conn.close()

if __name__ == "__main__":
    debug_avrahamov()

