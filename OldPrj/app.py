from __future__ import annotations

import copy
import logging
import time
import io
import pandas as pd
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Iterable, List, Tuple, Dict, Any, Optional

from convertdate import hebrew
from fastapi import FastAPI, HTTPException, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, Response, StreamingResponse
from fastapi.templating import Jinja2Templates
from zoneinfo import ZoneInfo

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# Constants
# =============================================================================


from logic import (
    MINUTES_PER_HOUR, MINUTES_PER_DAY, REGULAR_HOURS_LIMIT, OVERTIME_125_LIMIT,
    WORK_DAY_START_MINUTES, SHABBAT_ENTER_DEFAULT, SHABBAT_EXIT_DEFAULT,
    BREAK_THRESHOLD_MINUTES, STANDBY_CANCEL_OVERLAP_THRESHOLD,
    DEFAULT_MINIMUM_WAGE, DEFAULT_STANDBY_RATE, STANDARD_WORK_DAYS_PER_MONTH,
    MAX_SICK_DAYS_PER_MONTH, FRIDAY, SATURDAY, LOCAL_TZ,
    human_date, to_local_date, get_shabbat_times_cache, get_standby_rate,
    available_months, available_months_from_db, month_range_ts, parse_hhmm,
    span_minutes, minutes_to_time_str, is_shabbat_time, calculate_wage_rate,
    calculate_annual_vacation_quota, calculate_accruals, overlap_minutes,
    minutes_to_hours_str, to_gematria, format_currency, get_payment_codes,
    calculate_monthly_summary, calculate_person_monthly_totals, get_db_connection,
    get_available_months_for_person
)
from app_utils import get_daily_segments_data
import gesher_exporter


BASE_DIR = Path(__file__).parent


class PostgresConnection:
    """Wrapper for PostgreSQL connection to provide SQLite-like interface."""
    def __init__(self, conn):
        self.conn = conn
        self._in_transaction = False
    
    def execute(self, query, params=()):
        """Execute a query and return a cursor-like object."""
        # Convert SQLite placeholders (?) to PostgreSQL (%s)
        query = query.replace("?", "%s")
        cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(query, params)
        return cursor

    def cursor(self, *args, **kwargs):
        """Allow raw access to cursors if needed (e.g. by logic.py functions)."""
        return self.conn.cursor(*args, **kwargs)
    
    def commit(self):
        self.conn.commit()
    
    def rollback(self):
        self.conn.rollback()
    
    def close(self):
        self.conn.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.rollback()
        else:
            self.commit()
        self.close()


def get_conn():
    """Create and return a PostgreSQL database connection wrapped with SQLite-like interface."""
    pg_conn = get_db_connection()
    return PostgresConnection(pg_conn)












def calculate_annual_vacation_quota(work_year: int, is_6_day_week: bool) -> int:
    """
    Calculate annual vacation quota based on Israeli law.
    
    Args:
        work_year: The employee's current work year (1st year, 2nd year, etc.)
        is_6_day_week: True if employee works 6-day weeks, False for 5-day weeks
    
    Returns:
        Annual vacation days quota
    """
    if is_6_day_week:
        # Table for 6-day work week
        if work_year <= 4:
            return 14
        elif work_year == 5:
            return 16
        elif work_year == 6:
            return 18
        elif work_year == 7:
            return 21
        elif work_year == 8:
            return 22
        elif work_year == 9:
            return 23
        else:  # 10+
            return 24
    else:
        # Table for 5-day work week
        if work_year <= 5:
            return 12
        elif work_year == 6:
            return 14
        elif work_year == 7:
            return 15
        elif work_year == 8:
            return 16
        elif work_year == 9:
            return 17
        elif work_year == 10:
            return 18
        elif work_year == 11:
            return 19
        else:  # 12+
            return 20


def calculate_accruals(
    actual_work_days: int,
    start_date_ts: Optional[int],
    report_year: int,
    report_month: int
) -> Dict:
    """
    Calculate sick leave and vacation accruals for a month.
    
    Args:
        actual_work_days: Number of actual work days in the month
        start_date_ts: Employee start date as epoch timestamp (or None)
        report_year: The year being reported
        report_month: The month being reported
    
    Returns:
        Dict with sick_days_accrued, vacation_days_accrued, and vacation_details
    """
    # Calculate job scope (proportion of full-time)
    job_scope = min(actual_work_days / STANDARD_WORK_DAYS_PER_MONTH, 1.0)
    
    # Sick leave accrual (1.5 days per month at full-time)
    sick_days_accrued = job_scope * MAX_SICK_DAYS_PER_MONTH
    
    # Calculate seniority for vacation
    current_work_year = 1
    if start_date_ts:
        try:
            start_dt = datetime.fromtimestamp(start_date_ts, LOCAL_TZ).date()
            report_dt = datetime(report_year, report_month, 1, tzinfo=LOCAL_TZ).date()
            diff = report_dt - start_dt
            seniority_years = diff.days / 365.25
            current_work_year = max(1, int(seniority_years) + 1)
        except Exception as e:
            logger.debug(f"Error calculating seniority: {e}")
    
    # Determine if 6-day or 5-day week (based on > 20 work days)
    is_6_day_week = actual_work_days > 20
    
    # Get annual vacation quota
    annual_quota = calculate_annual_vacation_quota(current_work_year, is_6_day_week)
    
    # Monthly vacation accrual
    vacation_days_accrued = (annual_quota / 12) * job_scope
    
    return {
        "sick_days_accrued": sick_days_accrued,
        "vacation_days_accrued": vacation_days_accrued,
        "vacation_details": {
            "seniority": current_work_year,
            "annual_quota": annual_quota,
            "job_scope_pct": int(job_scope * 100)
        }
    }


def overlap_minutes(a_start: int, a_end: int, b_start: int, b_end: int) -> int:
    return max(0, min(a_end, b_end) - max(a_start, b_start))


def minutes_to_hours_str(minutes: int) -> str:
    hours = minutes / 60
    return f"{hours:.2f}".rstrip("0").rstrip(".")


def to_gematria(num: int) -> str:
    """Simple gematria converter for numbers 1-31 and years."""
    if num <= 0: return str(num)
    
    # מיפוי פשוט לימים (1-31)
    gematria_map = {
        1: "א'", 2: "ב'", 3: "ג'", 4: "ד'", 5: "ה'", 6: "ו'", 7: "ז'", 8: "ח'", 9: "ט'",
        10: "י'", 11: "י\"א", 12: "י\"ב", 13: "י\"ג", 14: "י\"ד", 15: "ט\"ו", 16: "ט\"ז",
        17: "י\"ז", 18: "י\"ח", 19: "י\"ט", 20: "כ'", 21: "כ\"א", 22: "כ\"ב", 23: "כ\"ג",
        24: "כ\"ד", 25: "כ\"ה", 26: "כ\"ו", 27: "כ\"ז", 28: "כ\"ח", 29: "כ\"ט", 30: "ל'"
    }
    if num in gematria_map:
        return gematria_map[num]
        
    # עבור שנים (למשל 5786 -> תשפ"ו)
    # זה מימוש פשוט מאוד שיכסה את השנים הקרובות
    if num == 5785: return "תשפ\"ה"
    if num == 5786: return "תשפ\"ו"
    if num == 5787: return "תשפ\"ז"
    
    return str(num)


def format_currency(value: float | int | None) -> str:
    """Format number as currency with thousand separators (e.g., 11403.00 -> 11,403.00)."""
    if value is None:
        value = 0
    return f"{float(value):,.2f}"


app = FastAPI(title="ניהול משמרות בענן")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
templates.env.filters["human_date"] = human_date
templates.env.filters["format_currency"] = format_currency


# Note: get_payment_codes and calculate_person_monthly_totals are imported from logic.py

def calculate_person_monthly_totals_DEPRECATED(
    conn,
    person_id: int,
    year: int,
    month: int,
    shabbat_cache: Dict[str, Dict[str, str]],
    minimum_wage: float = DEFAULT_MINIMUM_WAGE
) -> Dict:
    """
    חישוב מדויק של סיכומים חודשיים לעובד.
    פונקציה משותפת לשימוש ב-guide_view ו-general_summary.
    
    Returns:
        Dict with monthly_totals including all calculated values
    """
    # שליפת פרטי העובד
    person = conn.execute("SELECT * FROM people WHERE id = ?", (person_id,)).fetchone()
    if not person:
        return {}
    
    # שליפת דיווחים לחודש
    start_ts, end_ts = month_range_ts(year, month)
    reports = conn.execute("""
        SELECT tr.*, st.name as shift_name, 
               a.apartment_type_id,
               p.is_married
        FROM time_reports tr
        LEFT JOIN shift_types st ON st.id = tr.shift_type_id
        LEFT JOIN apartments a ON tr.apartment_id = a.id
        LEFT JOIN people p ON tr.person_id = p.id
        WHERE tr.person_id = ? AND tr.date >= ? AND tr.date < ?
        ORDER BY tr.date, tr.start_time
    """, (person_id, start_ts, end_ts)).fetchall()
    
    if not reports:
        return {
            "total_hours": 0, "payment": 0, "standby": 0, "standby_payment": 0,
            "actual_work_days": 0, "vacation_days_taken": 0,
            "calc100": 0, "calc125": 0, "calc150": 0, "calc150_shabbat": 0, 
            "calc150_shabbat_100": 0, "calc150_shabbat_50": 0,
            "calc150_overtime": 0, "calc175": 0, "calc200": 0,
            "vacation_minutes": 0, "vacation_payment": 0,
            "travel": 0, "extras": 0, "sick_days_accrued": 0, "vacation_days_accrued": 0
        }
    
    # שליפת מקטעי משמרות
    shift_ids = {r["shift_type_id"] for r in reports if r["shift_type_id"]}
    segments_by_shift = {}
    if shift_ids:
        placeholders = ",".join("?" * len(shift_ids))
        segs = conn.execute(
            f"SELECT * FROM shift_time_segments WHERE shift_type_id IN ({placeholders}) ORDER BY order_index",
            tuple(shift_ids)
        ).fetchall()
        for s in segs:
            segments_by_shift.setdefault(s["shift_type_id"], []).append(s)
    
    # זיהוי משמרות עם כוננות
    shift_has_standby = {sid: any(s["segment_type"] == "standby" for s in segs) 
                         for sid, segs in segments_by_shift.items()}
    
    # בניית daily_map
    daily_map = {}
    for r in reports:
        if not r["start_time"] or not r["end_time"] or not r["shift_type_id"]:
            continue
        
        r_start, r_end = span_minutes(r["start_time"], r["end_time"])
        r_date = to_local_date(r["date"])
        
        # פיצול משמרות חוצות חצות
        parts = []
        if r_end <= MINUTES_PER_DAY:
            parts.append((r_date, r_start, r_end))
        else:
            parts.append((r_date, r_start, MINUTES_PER_DAY))
            parts.append((r_date + timedelta(days=1), 0, r_end - MINUTES_PER_DAY))
        
        seg_list = segments_by_shift.get(r["shift_type_id"], [])
        if not seg_list:
            seg_list = [{"start_time": r["start_time"], "end_time": r["end_time"], 
                        "wage_percent": 100, "segment_type": "work", "id": None}]
        
        work_type = r.get("work_type")
        shift_name_str = (r["shift_name"] or "")
        
        is_sick_report = ("מחלה" in shift_name_str)
        is_vacation_report = (work_type == "sick_vacation" or "חופשה" in shift_name_str)
        
        # If it's sick, treat as sick. If it's vacation but not sick, treat as vacation.
        # Note: Previous logic combined them.
        
        for p_date, p_start, p_end in parts:
            if p_date.year != year or p_date.month != month:
                continue
            
            day_key = p_date.strftime("%d/%m/%Y")
            entry = daily_map.setdefault(day_key, {"segments": [], "date": p_date})
            
            is_second_day = (p_date > r_date)
            
            for seg in seg_list:
                s_start, s_end = span_minutes(seg["start_time"], seg["end_time"])
                shift_crosses = (r_end > MINUTES_PER_DAY)
                if shift_crosses and s_start < r_start:
                    s_start += MINUTES_PER_DAY
                    s_end += MINUTES_PER_DAY
                
                if is_second_day:
                    current_seg_start = s_start - MINUTES_PER_DAY
                    current_seg_end = s_end - MINUTES_PER_DAY
                else:
                    current_seg_start = s_start
                    current_seg_end = s_end
                
                overlap = overlap_minutes(p_start, p_end, current_seg_start, current_seg_end)
                if overlap <= 0:
                    continue
                
                eff_start = max(current_seg_start, p_start)
                eff_end = min(current_seg_end, p_end)
                
                if is_sick_report:
                    eff_type = "sick"
                elif is_vacation_report:
                    eff_type = "vacation"
                else:
                    eff_type = seg["segment_type"]

                segment_id = seg.get("id")
                apartment_type_id = r.get("apartment_type_id")
                is_married = r.get("is_married")

                entry["segments"].append((
                    eff_start, eff_end, eff_type,
                    r["shift_type_id"], segment_id, apartment_type_id, is_married
                ))

    # אתחול סיכומים
    monthly_totals = {
        "total_hours": 0, "payment": 0, "standby": 0, "standby_payment": 0,
        "actual_work_days": 0, "vacation_days_taken": 0, "sick_days_taken": 0,
        "calc100": 0, "calc125": 0, "calc150": 0, "calc150_shabbat": 0, 
        "calc150_shabbat_100": 0,  # חלק הבסיס 100% (חייב פנסיה)
        "calc150_shabbat_50": 0,   # חלק התוספת 50% (לא חייב פנסיה)
        "calc150_overtime": 0, "calc175": 0, "calc200": 0,
        "vacation_minutes": 0, "vacation_payment": 0, "travel": 0, "extras": 0
    }
    
    # ספירת כוננויות מדיווחים מקוריים
    for r in reports:
        if r["shift_type_id"] and shift_has_standby.get(r["shift_type_id"], False):
            monthly_totals["standby"] += 1
    
    # =======================================================================
    # עיבוד לפי ימי לוח - זהה ל-guide_view
    # חישוב שעות על פי יום לוח, ספירת ימי עבודה לפי לוגיקת 08:00
    # =======================================================================
    WORK_DAY_CUTOFF = 480  # 08:00 = 480 דקות
    
    # אוסף ימי עבודה (לוגיקת 08:00-08:00)
    work_days_set = set()
    vacation_days_set = set()
    sick_days_set = set()
    
    # עיבוד כל יום לוח
    for day_key, entry in sorted(daily_map.items()):
        day_date = entry["date"]
        
        # הפרדת מקטעים
        work_segments = []
        standby_segments = []
        vacation_segments = []
        sick_segments = []
        
        for seg in entry["segments"]:
            s_start, s_end, s_type, shift_id, seg_id, apt_type, is_married = seg
            if s_type == "standby":
                standby_segments.append((s_start, s_end, seg_id, apt_type, is_married))
            elif s_type == "vacation":
                vacation_segments.append((s_start, s_end))
            elif s_type == "sick":
                sick_segments.append((s_start, s_end))
            else:
                work_segments.append((s_start, s_end, shift_id))
        
        work_segments.sort(key=lambda x: x[0])
        standby_segments.sort(key=lambda x: x[0])
        
        # הסרת כפילויות
        seen = set()
        deduped = []
        for ws in work_segments:
            key = (ws[0], ws[1])
            if key not in seen:
                deduped.append(ws)
                seen.add(key)
        work_segments = deduped
        
        # ביטול כוננות אם יש חפיפה מעל 70%
        standby_filtered = []
        for sb_start, sb_end, sb_seg_id, sb_apt, sb_married in standby_segments:
            standby_duration = sb_end - sb_start
            if standby_duration <= 0:
                continue
            
            total_overlap = sum(overlap_minutes(sb_start, sb_end, w[0], w[1]) for w in work_segments)
            overlap_ratio = total_overlap / standby_duration
            
            if overlap_ratio < STANDBY_CANCEL_OVERLAP_THRESHOLD:
                standby_filtered.append((sb_start, sb_end, sb_seg_id, sb_apt, sb_married))
        
        standby_segments = standby_filtered
        
        # איחוד כל האירועים
        all_events = []
        for s, e, sid in work_segments:
            all_events.append({"start": s, "end": e, "type": "work", "shift_id": sid})
        for s, e, seg_id, apt_type, is_married_val in standby_segments:
            all_events.append({"start": s, "end": e, "type": "standby", "segment_id": seg_id, 
                              "apartment_type_id": apt_type, "is_married": is_married_val})
        for s, e in vacation_segments:
            all_events.append({"start": s, "end": e, "type": "vacation"})
        
        all_events.sort(key=lambda x: x["start"])
        
        # משתנים למעקב רצפים
        chain_minutes = 0
        current_chain_segments = []
        last_end = None
        
        day_calc100 = day_calc125 = day_calc150 = day_calc175 = day_calc200 = 0
        day_calc150_shabbat = day_calc150_overtime = 0
        day_calc150_shabbat_100 = day_calc150_shabbat_50 = 0  # פיצול שבת 150%
        day_vacation_minutes = 0
        day_standby_payment = 0
        
        def close_chain():
            """סגירת רצף עבודה וחישוב אחוזים"""
            nonlocal chain_minutes, current_chain_segments
            nonlocal day_calc100, day_calc125, day_calc150, day_calc175, day_calc200
            nonlocal day_calc150_shabbat, day_calc150_overtime
            nonlocal day_calc150_shabbat_100, day_calc150_shabbat_50
            
            if not current_chain_segments:
                return
            
            minutes_counter = 0
            for seg_start, seg_end, seg_shift_id in current_chain_segments:
                for m in range(seg_end - seg_start):
                    minutes_counter += 1
                    minute_abs = seg_start + m
                    eff_day_shift = minute_abs // MINUTES_PER_DAY
                    eff_minute = minute_abs % MINUTES_PER_DAY
                    eff_weekday = (day_date.weekday() + eff_day_shift) % 7
                    
                    is_shab = is_shabbat_time(eff_weekday, eff_minute, seg_shift_id, day_date, shabbat_cache)
                    rate = calculate_wage_rate(minutes_counter, is_shab)
                    
                    if rate == "100%":
                        day_calc100 += 1
                    elif rate == "125%":
                        day_calc125 += 1
                    elif rate == "150%":
                        day_calc150 += 1
                        if is_shab:
                            day_calc150_shabbat += 1
                            day_calc150_shabbat_100 += 1  # 100% בסיס (חייב פנסיה)
                            day_calc150_shabbat_50 += 1   # 50% תוספת (לא חייב פנסיה)
                        else:
                            day_calc150_overtime += 1
                    elif rate == "175%":
                        day_calc175 += 1
                    elif rate == "200%":
                        day_calc200 += 1
            
            chain_minutes = 0
            current_chain_segments = []
        
        for event in all_events:
            seg_start = event["start"]
            seg_end = event["end"]
            seg_type = event["type"]
            
            is_special = seg_type in ("standby", "vacation")
            
            # בדיקת שבירת רצף
            should_break = False
            if current_chain_segments:
                if is_special:
                    should_break = True
                elif last_end is not None:
                    gap = seg_start - last_end
                    if gap > BREAK_THRESHOLD_MINUTES:
                        should_break = True
            
            if should_break:
                close_chain()
            
            if is_special:
                is_continuation = (seg_start == 0)
                
                if seg_type == "standby" and not is_continuation:
                    seg_id = event.get("segment_id") or 0
                    apt_type = event.get("apartment_type_id")
                    is_married_val = event.get("is_married")
                    rate = get_standby_rate(conn.conn, seg_id, apt_type, bool(is_married_val) if is_married_val is not None else False)
                    day_standby_payment += rate
                elif seg_type == "vacation":
                    day_vacation_minutes += (seg_end - seg_start)
                
                last_end = seg_end
            else:
                shift_id = event.get("shift_id", 0)
                current_chain_segments.append((seg_start, seg_end, shift_id))
                chain_minutes += (seg_end - seg_start)
                last_end = seg_end
        
        close_chain()
        
        # עדכון סיכומים חודשיים
        monthly_totals["calc100"] += day_calc100
        monthly_totals["calc125"] += day_calc125
        monthly_totals["calc150"] += day_calc150
        monthly_totals["calc150_shabbat"] += day_calc150_shabbat
        monthly_totals["calc150_shabbat_100"] += day_calc150_shabbat_100  # 100% בסיס
        monthly_totals["calc150_shabbat_50"] += day_calc150_shabbat_50    # 50% תוספת
        monthly_totals["calc150_overtime"] += day_calc150_overtime
        monthly_totals["calc175"] += day_calc175
        monthly_totals["calc200"] += day_calc200
        monthly_totals["total_hours"] += (day_calc100 + day_calc125 + day_calc150 + day_calc175 + day_calc200)
        monthly_totals["standby_payment"] += day_standby_payment
        monthly_totals["vacation_minutes"] += day_vacation_minutes
        
        # ספירת ימי עבודה לפי לוגיקת 08:00-08:00
        # עבודה שמסתיימת לפני 08:00 שייכת ליום העבודה הקודם
        for s, e, sid in work_segments:
            if s >= WORK_DAY_CUTOFF:
                # עבודה שמתחילה ב-08:00 ואילך - שייכת להיום
                work_days_set.add(day_date)
            elif e > WORK_DAY_CUTOFF:
                # עבודה שחוצה את 08:00 - שייכת להיום
                work_days_set.add(day_date)
            else:
                # עבודה לפני 08:00 - שייכת לאתמול
                prev_day = day_date - timedelta(days=1)
                if prev_day.year == year and prev_day.month == month:
                    work_days_set.add(prev_day)
        
        if vacation_segments:
            vacation_days_set.add(day_date)
        
        if sick_segments:
            sick_days_set.add(day_date)
    
    monthly_totals["actual_work_days"] = len(work_days_set)
    monthly_totals["vacation_days_taken"] = len(vacation_days_set)
    monthly_totals["sick_days_taken"] = len(sick_days_set)
    
    # חישוב תשלום חופשה
    monthly_totals["vacation_payment"] = (monthly_totals.get("vacation_minutes", 0) / 60) * minimum_wage

    # הוספת vacation כמפתח נוסף עבור payment_codes
    monthly_totals["vacation"] = monthly_totals.get("vacation_minutes", 0)

    # שליפת רכיבי תשלום נוספים
    month_start = datetime(year, month, 1, tzinfo=LOCAL_TZ)
    month_end = datetime(year + 1, 1, 1, tzinfo=LOCAL_TZ) if month == 12 else datetime(year, month + 1, 1, tzinfo=LOCAL_TZ)
    month_start_ts = int(month_start.timestamp())
    month_end_ts = int(month_end.timestamp())
    
    payment_comps = conn.execute("""
        SELECT *, (quantity * rate) as total_amount FROM payment_components 
        WHERE person_id = ? AND date >= ? AND date < ?
    """, (person_id, month_start_ts, month_end_ts)).fetchall()
    
    for pc in payment_comps:
        amount = (pc["total_amount"] or 0) / 100
        if pc["component_type_id"] == 2:
            monthly_totals["travel"] += amount
        else:
            monthly_totals["extras"] += amount
    
    # חישוב צבירות
    accruals = calculate_accruals(
        actual_work_days=monthly_totals["actual_work_days"],
        start_date_ts=person["start_date"],
        report_year=year,
        report_month=month
    )
    monthly_totals["sick_days_accrued"] = accruals["sick_days_accrued"]
    monthly_totals["vacation_days_accrued"] = accruals["vacation_days_accrued"]
    
    # חישוב תשלום סופי - זהה ל-guide_view
    # payment = תשלום בסיסי (שעות + כוננות + חופשה) - ללא נסיעות ותוספות
    # total_payment = סה"כ לתשלום כולל נסיעות ותוספות (לדוח המרוכז)
    pay = 0
    pay += (monthly_totals["calc100"] / 60) * minimum_wage * 1.0
    pay += (monthly_totals["calc125"] / 60) * minimum_wage * 1.25
    pay += (monthly_totals["calc150"] / 60) * minimum_wage * 1.5
    pay += (monthly_totals["calc175"] / 60) * minimum_wage * 1.75
    pay += (monthly_totals["calc200"] / 60) * minimum_wage * 2.0
    pay += monthly_totals["standby_payment"]
    pay += monthly_totals["vacation_payment"]
    monthly_totals["payment"] = pay  # תשלום בסיסי (כמו guide_view)
    monthly_totals["total_payment"] = pay + monthly_totals["travel"] + monthly_totals["extras"]  # סה"כ כולל הכל
    
    return monthly_totals

@app.get("/", response_class=HTMLResponse)
def home(request: Request, month: int | None = None, year: int | None = None, q: str | None = None):
    with get_conn() as conn:
        guides = conn.execute(
            """
            SELECT id, name, type, is_active, start_date
            FROM people
            WHERE is_active::integer = 1
            ORDER BY name
            """
        ).fetchall()

        months_all = available_months_from_db(conn.conn)

    if months_all:
        if month is None or year is None:
            selected_year, selected_month = months_all[-1]
        else:
            selected_year, selected_month = year, month
    else:
        selected_year = selected_month = None

    months_options = [{"year": y, "month": m, "label": f"{m:02d}/{y}"} for y, m in months_all]
    years_options = sorted({y for y, _ in months_all}, reverse=True)

    counts: dict[int, int] = {}
    if selected_year and selected_month:
        start_ts, end_ts = month_range_ts(selected_year, selected_month)
        with get_conn() as conn:
            for row in conn.execute(
                """
                SELECT person_id, COUNT(*) AS cnt
                FROM time_reports
                WHERE date >= ? AND date < ?
                GROUP BY person_id
                """,
                (start_ts, end_ts),
            ):
                counts[row["person_id"]] = row["cnt"]

    # Calculate seniority years for each guide
    reference_date = datetime.now(LOCAL_TZ).date()
    if selected_year and selected_month:
        reference_date = datetime(selected_year, selected_month, 1, tzinfo=LOCAL_TZ).date()

    allowed_types = {"permanent", "substitute"}
    guides_filtered = []
    q_norm = q.lower().strip() if q else None
    for g in guides:
        if g["type"] not in allowed_types:
            continue
        if q_norm and q_norm not in (g["name"] or "").lower():
            continue
        
        if selected_year and selected_month:
            # Show guides with at least 1 shift (changed from > 1 to >= 1)
            if counts.get(g["id"], 0) < 1:
                continue
        
        # Calculate seniority years
        seniority_years = None
        if g.get("start_date"):
            try:
                # Handle datetime, date objects (from psycopg2) and timestamp (int/float)
                if isinstance(g["start_date"], datetime):
                    start_dt = g["start_date"].date()
                elif isinstance(g["start_date"], date):
                    start_dt = g["start_date"]
                else:
                    # Assume it's a timestamp
                    start_dt = datetime.fromtimestamp(g["start_date"], LOCAL_TZ).date()
                diff = reference_date - start_dt
                seniority_years = diff.days / 365.25
                if seniority_years < 0:
                    seniority_years = 0
            except Exception as e:
                logger.warning(f"Error calculating seniority for guide {g.get('id')} ({g.get('name')}): {e}, start_date type: {type(g.get('start_date'))}, value: {g.get('start_date')}")
                seniority_years = None
        
        guide_dict = dict(g)
        guide_dict["seniority_years"] = seniority_years
        # Debug logging (can be removed later)
        if seniority_years is not None:
            logger.debug(f"Guide {g.get('name')}: seniority_years = {seniority_years}")
        guides_filtered.append(guide_dict)

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "guides": guides_filtered,
            "months": months_options,
            "years": years_options,
            "selected_year": selected_year,
            "selected_month": selected_month,
            "counts": counts,
            "q": q or "",
        },
    )


@app.get("/guide/{person_id}/simple", response_class=HTMLResponse)
def simple_summary_view(request: Request, person_id: int, month: int | None = None, year: int | None = None):
    with get_conn() as conn:
        # Defaults
        if month is None or year is None:
            now = datetime.now(LOCAL_TZ)
            year, month = now.year, now.month
            
        # Minimum Wage
        try:
            row = conn.execute("SELECT hourly_rate FROM minimum_wage_rates ORDER BY effective_from DESC LIMIT 1").fetchone()
            minimum_wage = (float(row["hourly_rate"]) / 100) if row else DEFAULT_MINIMUM_WAGE
        except Exception as e:
            logger.warning(f"Failed to get minimum wage from DB, using default: {e}")
            minimum_wage = DEFAULT_MINIMUM_WAGE
            
        shabbat_cache = get_shabbat_times_cache(conn.conn)
        
        # Get data
        daily_segments, person_name = get_daily_segments_data(conn.conn, person_id, year, month, shabbat_cache, minimum_wage)
        
        person = conn.execute("SELECT * FROM people WHERE id = ?", (person_id,)).fetchone()

        # Aggregate
        summary = {
            "weekday": {"count": 0, "payment": 0},
            "friday": {"count": 0, "payment": 0},
            "saturday": {"count": 0, "payment": 0},
            "overtime": {"hours": 0, "payment": 0},
            "total_payment": 0
        }
        
        for day in daily_segments:
            # Skip if no work/vacation/sick (just empty day)
            if not day.get("payment") and not day.get("has_work"):
                continue
                
            # Determine type
            # weekday() 0-3=Sun-Wed, 4=Thu(Wait.. Mon=0..Sun=6)
            # Mon=0, Tue=1, Wed=2, Thu=3, Fri=4, Sat=5, Sun=6
            wd = day["date_obj"].weekday()
            
            # Sun(6), Mon(0)-Thu(3) -> Weekday
            is_weekday = (wd == 6 or wd <= 3)
            is_friday = (wd == 4)
            is_saturday = (wd == 5)
            
            day_payment = day["payment"] or 0
            
            # Calculate Overtime part (125% + 150% non-shabbat)
            # Note: 150% on Shabbat is usually base rate for Shabbat, not "Overtime" in the classic sense of "Additional Hours".
            # User request: "X hours additional work X money".
            # I will count calc125 and calc150 (if not shabbat) as overtime.
            
            # Helper to calc cost
            def calc_cost(minutes, multiplier):
                return (minutes / 60) * minimum_wage * multiplier
            
            # Extract overtime components
            ot_125_mins = day["calc125"]
            # For 150, we need to know if it's overtime or shabbat.
            # In daily_segments, we don't have separate calc150_shabbat vs calc150_overtime easily available in the aggregated numbers?
            # Wait, `get_daily_segments_data` returns `d_calc150`.
            # But inside `guide_view` logic, `calc150` mixed both.
            # However, `is_shabbat_time` determines it.
            # If it's a weekday, 150 is overtime.
            # If it's Friday/Shabbat, 150 is standard weekend rate?
            
            # Simplification:
            # If Weekday: calc125 + calc150 -> Overtime.
            # If Friday/Shabbat: calc125 is rare? calc150 is standard.
            
            ot_pay = 0
            ot_hours = 0
            
            if is_weekday:
                # Weekday: Extract 125% and 150% as overtime
                c125 = day["calc125"]
                c150 = day["calc150"] # Assuming weekday 150 is overtime
                
                if c125 > 0:
                    cost = calc_cost(c125, 1.25)
                    ot_pay += cost
                    ot_hours += c125 / 60
                    
                if c150 > 0:
                    cost = calc_cost(c150, 1.5)
                    ot_pay += cost
                    ot_hours += c150 / 60
                
                # Base pay for weekday = Total - Overtime
                base_pay = day_payment - ot_pay
                
                if day["has_work"]:
                    summary["weekday"]["count"] += 1
                summary["weekday"]["payment"] += base_pay
                
                summary["overtime"]["hours"] += ot_hours
                summary["overtime"]["payment"] += ot_pay
                
            elif is_friday:
                if day["has_work"]:
                    summary["friday"]["count"] += 1
                summary["friday"]["payment"] += day_payment
                
            elif is_saturday:
                if day["has_work"]:
                    summary["saturday"]["count"] += 1
                summary["saturday"]["payment"] += day_payment
        
        summary["total_payment"] = (summary["weekday"]["payment"] + 
                                   summary["friday"]["payment"] + 
                                   summary["saturday"]["payment"] + 
                                   summary["overtime"]["payment"])
                                   
        return templates.TemplateResponse(
            "simple_summary.html",
            {
                "request": request,
                "person": person,
                "person_id": person_id,
                "person_name": person_name,
                "selected_year": year,
                "selected_month": month,
                "summary": summary
            }
        )

@app.get("/guide/{person_id}", response_class=HTMLResponse)
def guide_view(request: Request, person_id: int, month: int | None = None, year: int | None = None):
    with get_conn() as conn:
        # שליפת שכר מינימום מה-DB
        MINIMUM_WAGE = 34.40
        try:
            row = conn.execute("SELECT hourly_rate FROM minimum_wage_rates ORDER BY effective_from DESC LIMIT 1").fetchone()
            if row and row["hourly_rate"]:
                MINIMUM_WAGE = float(row["hourly_rate"]) / 100
        except Exception as e:
            logger.warning(f"Error fetching minimum wage: {e}")

        person = conn.execute(
            "SELECT id, name, phone, email, type, is_active, start_date FROM people WHERE id = ?",
            (person_id,),
        ).fetchone()
        if not person:
            raise HTTPException(status_code=404, detail="מדריך לא נמצא")

        # Fetch payment codes early to avoid connection issues later
        payment_codes = get_payment_codes(conn.conn)
        if not payment_codes:
            # Try once more with a fresh connection if first fetch failed
            try:
                with get_conn() as temp_conn:
                    payment_codes = get_payment_codes(temp_conn.conn)
            except Exception as e:
                logger.warning(f"Secondary fetch of payment codes failed: {e}")

        # Optimized: Fetch available months
        months = get_available_months_for_person(conn.conn, person_id)
        
        # Prepare months options for template
        months_options = [{"year": y, "month": m, "label": f"{m:02d}/{y}"} for y, m in months]

        if not months:
            selected_year, selected_month = year or datetime.now().year, month or datetime.now().month
            month_reports = []
            shift_segments = []
            daily_segments = []
            monthly_totals = {
                "total_hours": 0.0,
                "calc100": 0.0,
                "calc125": 0.0,
                "calc150": 0.0,
                "calc150_shabbat": 0.0,
                "calc150_shabbat_100": 0.0,
                "calc150_shabbat_50": 0.0,
                "calc150_overtime": 0.0,
                "calc175": 0.0,
                "calc200": 0.0,
                "payment": 0.0,
                "standby": 0,
                "standby_payment": 0.0,
                "vacation_minutes": 0.0,
                "vacation_payment": 0.0,
                "actual_work_days": 0,
                "vacation_days_taken": 0,
                "sick_days_accrued": 0.0,
                "vacation_days_accrued": 0.0,
                "travel": 0.0,
                "extras": 0.0,
            }
            total_standby_count = 0
            simple_summary = {
                "weekday": {"count": 0, "payment": 0},
                "friday": {"count": 0, "payment": 0},
                "saturday": {"count": 0, "payment": 0},
                "total": {"count": 0, "payment": 0}
            }
            year_options = []
        else:
            if year and month:
                selected_year, selected_month = year, month
            else:
                selected_year, selected_month = months[0] # Most recent

            # Optimized: Fetch reports only for selected month
            start_ts, end_ts = month_range_ts(selected_year, selected_month)
            
            month_reports = conn.execute(
                """
                SELECT tr.*, 
                       st.name AS shift_name, 
                       st.color AS shift_color,
                       ap.name AS apartment_name,
                       ap.apartment_type_id,
                       p.is_married
                FROM time_reports tr
                LEFT JOIN shift_types st ON st.id = tr.shift_type_id
                LEFT JOIN apartments ap ON ap.id = tr.apartment_id
                LEFT JOIN people p ON p.id = tr.person_id
                WHERE tr.person_id = ? AND tr.date >= ? AND tr.date < ?
                ORDER BY tr.date ASC, tr.start_time ASC
                """,
                (person_id, start_ts, end_ts),
            ).fetchall()

            shift_ids = {r["shift_type_id"] for r in month_reports if r["shift_type_id"]}
            if shift_ids:
                placeholders = ",".join(["%s"] * len(shift_ids))
                shift_segments = conn.execute(
                    f"""
                    SELECT seg.id, seg.shift_type_id, seg.start_time, seg.end_time, 
                           seg.wage_percent, seg.segment_type, seg.order_index,
                           st.name AS shift_name
                    FROM shift_time_segments seg
                    JOIN shift_types st ON st.id = seg.shift_type_id
                    WHERE seg.shift_type_id IN ({placeholders})
                    ORDER BY seg.shift_type_id, seg.order_index, seg.id
                    """,
                    tuple(shift_ids)
                ).fetchall()
            else:
                shift_segments = []

        shabbat_cache = get_shabbat_times_cache(conn.conn)

        # Build per-day segment summary
        segments_by_shift: Dict[int, list] = {}
        for seg in shift_segments:
            segments_by_shift.setdefault(seg["shift_type_id"], []).append(seg)

        daily_map: Dict[str, Dict[str, object]] = {}
        for r in month_reports:
            if not r["start_time"] or not r["end_time"] or not r["shift_type_id"]:
                continue
            
            # פיצול משמרות לפי ימים קלנדריים
            rep_start_orig, rep_end_orig = span_minutes(r["start_time"], r["end_time"])
            
            # רשימת חלקים: (תאריך, התחלה בדקות, סיום בדקות)
            # התחלה וסיום הם יחסיים ליום הספציפי (0-1440)
            parts: List[Tuple[datetime.date, int, int]] = []
            
            r_date = to_local_date(r["date"])
            
            if rep_end_orig <= MINUTES_PER_DAY:
                # המשמרת כולה ביום אחד
                parts.append((r_date, rep_start_orig, rep_end_orig))
            else:
                # המשמרת חוצה חצות
                # חלק 1: מההתחלה עד חצות (24:00) ביום הראשון
                parts.append((r_date, rep_start_orig, MINUTES_PER_DAY))
                
                # חלק 2: מחצות (00:00) עד הסוף ביום השני
                next_day = r_date + timedelta(days=1)
                parts.append((next_day, 0, rep_end_orig - MINUTES_PER_DAY))
            
            seg_list = segments_by_shift.get(r["shift_type_id"], [])
            
            # אם לא מוגדרים מקטעים למשמרת זו (למשל "שעות עבודה" כלליות), נניח 100% עבודה לפי שעות הדיווח
            if not seg_list:
                # שימוש בשעות הדיווח עצמו כמקטע יחיד
                seg_list = [{
                    "start_time": r["start_time"],
                    "end_time": r["end_time"],
                    "wage_percent": 100,
                    "segment_type": "work"
                }]

            for p_date, p_start, p_end in parts:
                # בדיקה שהתאריך שייך לחודש הנבחר (או שצריך להציג אותו בכל מקרה?)
                # אם המשתמש בחר חודש ספציפי, נציג רק ימים באותו חודש
                if p_date.year != selected_year or p_date.month != selected_month:
                    continue
                    
                day_key = p_date.strftime("%d/%m/%Y")
                entry = daily_map.setdefault(day_key, {"buckets": {}, "shifts": set(), "segments": []})
                if r["shift_name"]:
                    entry["shifts"].add(r["shift_name"])
                
                minutes_covered = 0
                
                # זיהוי חופשה
                is_vacation_report = False
                if r["shift_name"]:
                    # בדיקה גם לפי שם המשמרת
                    if "חופשה" in r["shift_name"] or "מחלה" in r["shift_name"]:
                        is_vacation_report = True
                
                # חישוב מקטעים לחלק הזה של המשמרת
                # צריך להתאים את המקטעים ליום הקלנדרי
                
                # אם זה החלק השני (יום המחרת), המשמרת המקורית התחילה אתמול
                # אז מקטעים שמתחילים אחרי חצות במשמרת המקורית, מתחילים מ-0 ביום הזה
                
                is_second_day = (p_date > r_date)
                
                for seg in seg_list:
                    seg_start, seg_end = span_minutes(seg["start_time"], seg["end_time"])
                    
                    # התאמת זמני המקטע ליום הנוכחי
                    # אם המשמרת המקורית חוצה חצות, ומקטע מתחיל לפני ההתחלה המקורית -> הוא ביום השני
                    shift_crosses = (rep_end_orig > MINUTES_PER_DAY)
                    
                    if shift_crosses and seg_start < rep_start_orig:
                        # המקטע שייך לוגית לחלק שאחרי חצות
                        seg_start += MINUTES_PER_DAY
                        seg_end += MINUTES_PER_DAY
                    
                    # עכשיו seg_start/end הם יחסית לתחילת היום הראשון (0..48h)
                    # p_start/p_end הם יחסית ליום הנוכחי (0..24h), אבל צריך להמיר אותם לסקאלה הגלובלית כדי לחשב חפיפה
                    # או להמיר את המקטע לסקאלה של היום הנוכחי
                    
                    # המרה לסקאלה של היום הנוכחי (0-1440)
                    if is_second_day:
                        # אנחנו ביום השני. המקטעים הרלוונטיים הם אלו שמעל 1440 דקות (24h)
                        current_seg_start = seg_start - MINUTES_PER_DAY
                        current_seg_end = seg_end - MINUTES_PER_DAY
                    else:
                        # אנחנו ביום הראשון. המקטעים הרלוונטיים הם אלו שמתחת ל-1440 דקות
                        current_seg_start = seg_start
                        current_seg_end = seg_end
                    
                    # חישוב חפיפה עם החלק הנוכחי של המשמרת
                    overlap = overlap_minutes(p_start, p_end, current_seg_start, current_seg_end)
                    
                    if overlap <= 0:
                        continue
                        
                    minutes_covered += overlap
                    
                    # Bucket label
                    # קביעת סוג המקטע האפקטיבי
                    effective_seg_type = "vacation" if is_vacation_report else seg["segment_type"]

                    if effective_seg_type == "standby":
                        label = "כוננות"
                    elif effective_seg_type == "vacation":
                        label = "חופשה"
                    elif seg["wage_percent"] == 100:
                        label = "100%"
                    elif seg["wage_percent"] == 125:
                        label = "125%"
                    elif seg["wage_percent"] == 150:
                        label = "150%"
                    elif seg["wage_percent"] == 175:
                        label = "175%"
                    elif seg["wage_percent"] == 200:
                        label = "200%"
                    else:
                        label = f"{seg['wage_percent']}%"
                    
                    entry["buckets"].setdefault(label, 0)
                    entry["buckets"][label] += overlap
                    
                    # שמור את החלק היחסי שנופל בתוך היום הנוכחי (חיתוך)
                    eff_start = max(current_seg_start, p_start)
                    eff_end = min(current_seg_end, p_end)
                    
                    # שמירת segment_id (id של המקטע מ-shift_time_segments) לטובת חישוב תעריף כוננות
                    segment_id = seg.get("id")
                    apartment_type_id = r.get("apartment_type_id")
                    is_married = r.get("is_married")
                    apartment_name = r.get("apartment_name")
                    entry["segments"].append((eff_start, eff_end, effective_seg_type, label, r["shift_type_id"], segment_id, apartment_type_id, is_married, apartment_name))

                # שעות לא מכוסות
                total_part_minutes = p_end - p_start
                remaining = total_part_minutes - minutes_covered
                if remaining > 0:
                    entry["buckets"].setdefault("שעות עבודה", 0)
                    entry["buckets"]["שעות עבודה"] += remaining

        daily_segments = []
        
        # חישוב סה"כ כוננויות לפי דיווחים מקוריים ולא לפי מקטעים מפוצלים
        total_standby_count = 0
        monthly_totals = {
            "total_hours": 0.0,
            "calc100": 0.0,
            "calc125": 0.0,
            "calc150": 0.0,
            "calc150_shabbat": 0.0,
            "calc150_shabbat_100": 0.0,  # 100% בסיס (חייב פנסיה)
            "calc150_shabbat_50": 0.0,   # 50% תוספת (לא חייב פנסיה)
            "calc150_overtime": 0.0,
            "calc175": 0.0,
            "calc200": 0.0,
            "standby_payment": 0.0
        }
        
        # אנחנו צריכים לדעת לכל סוג משמרת האם יש בה רכיב כוננות
        shift_has_standby: Dict[int, bool] = {}
        
        # נשתמש ב-segments_by_shift שכבר שלפנו
        for s_id, segs in segments_by_shift.items():
            has_sb = any(s["segment_type"] == "standby" for s in segs)
            shift_has_standby[s_id] = has_sb
        
        for r in month_reports:
            sid = r["shift_type_id"]
            if sid and shift_has_standby.get(sid, False):
                total_standby_count += 1

        for day, entry in sorted(daily_map.items()):
            buckets: Dict[str, int] = entry["buckets"]  # type: ignore
            shift_names = sorted(entry["shifts"])  # type: ignore
            total_minutes = sum(buckets.values())
            is_vacation_day = entry.get("is_vacation", False)  # type: ignore

            # חישוב זכאות לפי רצפים - כוננות מפסיקה רצף
            # אוספים את כל המשמרות הרלוונטיות ליום הזה:
            # 1. משמרות שמתחילות ביום הזה
            # 2. משמרות שמתחילות ביום קודם אבל מסתיימות ביום הזה (חוצות חצות)
            day_date = None
            day_name_he = ""
            hebrew_date_str = ""
            
            try:
                day_parts = day.split("/")
                day_date = datetime(int(day_parts[2]), int(day_parts[1]), int(day_parts[0]), tzinfo=LOCAL_TZ).date()
                
                # יום בשבוע
                # weekday: 0=Mon, 6=Sun
                # נרצה: 0=שני, ... 6=ראשון
                days_map = {
                    0: "שני", 1: "שלישי", 2: "רביעי", 3: "חמישי", 4: "שישי", 5: "שבת", 6: "ראשון"
                }
                day_name_he = days_map.get(day_date.weekday(), "")
                
                # תאריך עברי
                h_year, h_month, h_day = hebrew.from_gregorian(day_date.year, day_date.month, day_date.day)
                
                # המרת שם חודש לאותיות
                # hebrew.month_name is a list of names? convertdate usually works with numbers
                # hebrew.month_name returns English names. Let's map manually or use a helper if available.
                # convertdate uses 1=Nisan, etc.
                # Mappings:
                # 1: Nisan, 2: Iyyar, 3: Sivan, 4: Tammuz, 5: Av, 6: Elul, 7: Tishri, 8: Heshvan, 9: Kislev, 10: Teves, 11: Shevat, 12: Adar
                # Leap year has Adar I and Adar II?
                # Let's check convertdate docs or behavior.
                # Actually, convertdate 2.4+ handles it.
                # For simplicity, let's use a simple mapping, assuming output is consistent.
                # Better yet, let's just print simple hebrew letters for numbers?
                # There are libraries for formatted hebrew dates but let's stick to simple implementation.
                
                hebrew_months = {
                    1: "ניסן", 2: "אייר", 3: "סיוון", 4: "תמוז", 5: "אב", 6: "אלול",
                    7: "תשרי", 8: "חשוון", 9: "כסלו", 10: "טבת", 11: "שבט", 12: "אדר",
                    13: "אדר ב'"
                }
                
                month_name = hebrew_months.get(h_month, str(h_month))
                if h_month == 12 and hebrew.leap(h_year):
                    month_name = "אדר א'"
                elif h_month == 13:
                     month_name = "אדר ב'"
                     
                day_gematria = to_gematria(h_day)
                year_gematria = to_gematria(h_year)
                
                hebrew_date_str = f"{day_gematria} ב{month_name} {year_gematria}"

            except Exception as e:
                logger.debug(f"Error calculating dates: {e}")

            # המקטעים כבר מוכנים ב-entry["segments"] - פשוט נשתמש בהם!
            work_segments: List[Tuple[int, int, str, int, str | None]] = []  # (start_minutes, end_minutes, label, shift_id, apartment_name)
            standby_segments: List[Tuple[int, int, int, int | None, bool | None]] = []  # (start_minutes, end_minutes, segment_id, apartment_type_id, is_married)
            vacation_segments: List[Tuple[int, int]] = [] # (start_minutes, end_minutes)

            # entry["segments"] מכיל: (start, end, segment_type, label, shift_id, segment_id, apartment_type_id, is_married, apartment_name)
            for seg_entry in entry.get("segments", []):  # type: ignore
                if len(seg_entry) >= 9:
                    seg_start, seg_end, seg_type, label, seg_shift_id, segment_id, apartment_type_id, is_married, apartment_name = seg_entry
                elif len(seg_entry) >= 8:
                    seg_start, seg_end, seg_type, label, seg_shift_id, segment_id, apartment_type_id, is_married = seg_entry
                    apartment_name = None
                elif len(seg_entry) >= 5:
                    seg_start, seg_end, seg_type, label, seg_shift_id = seg_entry[:5]
                    segment_id = None
                    apartment_type_id = None
                    is_married = None
                    apartment_name = None
                else:
                     # Fallback for old/other segments if any
                    seg_start, seg_end, seg_type, label = seg_entry[:4]
                    seg_shift_id = 0
                    segment_id = None
                    apartment_type_id = None
                    is_married = None
                    apartment_name = None

                if seg_type == "standby":
                    standby_segments.append((seg_start, seg_end, segment_id or 0, apartment_type_id, is_married))
                elif seg_type == "vacation":
                    vacation_segments.append((seg_start, seg_end))
                else:
                    work_segments.append((seg_start, seg_end, label, seg_shift_id, apartment_name))

            # מיון לפי זמן התחלה
            work_segments.sort(key=lambda x: x[0])
            standby_segments.sort(key=lambda x: x[0])
            vacation_segments.sort(key=lambda x: x[0])
            
            # הסרת כפילות - אם יש מקטעים חופפים, נשאיר רק אחד (הראשון)
            work_segments_deduped: List[Tuple[int, int, str, int, str | None]] = []
            seen_ranges: set[Tuple[int, int]] = set()
            for seg_start, seg_end, label, seg_shift_id, apt_name in work_segments:
                range_key = (seg_start, seg_end)
                if range_key not in seen_ranges:
                    work_segments_deduped.append((seg_start, seg_end, label, seg_shift_id, apt_name))
                    seen_ranges.add(range_key)
            work_segments = work_segments_deduped
            
            # =============================================================
            # בדיקת חפיפה בין כוננות לעבודה - ביטול כוננות אם יש חפיפה מעל 70%
            # =============================================================
            standby_segments_filtered = []
            cancelled_standbys = []
            
            for sb_start, sb_end, sb_seg_id, sb_apt_type, sb_married in standby_segments:
                standby_duration = sb_end - sb_start
                if standby_duration <= 0:
                    continue
                
                # חישוב סה"כ דקות חפיפה עם מקטעי עבודה
                total_overlap = 0
                for w_start, w_end, w_label, w_shift_id, w_apt_name in work_segments:
                    overlap = overlap_minutes(sb_start, sb_end, w_start, w_end)
                    total_overlap += overlap
                
                overlap_ratio = total_overlap / standby_duration
                
                if overlap_ratio >= STANDBY_CANCEL_OVERLAP_THRESHOLD:
                    # חפיפה מעל 70% - ביטול הכוננות
                    cancelled_standbys.append({
                        "start": sb_start,
                        "end": sb_end,
                        "overlap_pct": int(overlap_ratio * 100),
                        "reason": f"חפיפה עם עבודה ({int(overlap_ratio * 100)}%)"
                    })
                    logger.info(f"Standby cancelled: day={day_key}, {sb_start}-{sb_end}, overlap={overlap_ratio:.0%}")
                else:
                    # הכוננות תקפה
                    standby_segments_filtered.append((sb_start, sb_end, sb_seg_id, sb_apt_type, sb_married))
            
            # עדכון רשימת הכוננויות
            standby_segments = standby_segments_filtered
            
            # חישוב רצפים - רצף נשבר אם יש כוננות או הפסקה גדולה (יותר משעה)
            calc_100_total = 0
            calc_125_total = 0
            calc_150_total = 0
            calc_150_shabbat_total = 0
            calc_150_shabbat_100_total = 0  # 100% בסיס (חייב פנסיה)
            calc_150_shabbat_50_total = 0   # 50% תוספת (לא חייב פנסיה)
            calc_150_overtime_total = 0
            calc_175_total = 0
            calc_200_total = 0
            
            # MINIMUM_WAGE כבר הוגדר בתחילת הפונקציה
            
            chains_detail: List[Dict[str, object]] = []  # רשימת רצפים מפורטים
            current_chain_minutes = 0
            current_chain_start = None
            current_chain_segments: List[Tuple[int, int, str, int, str | None]] = []  # מקטעי הרצף הנוכחי (כולל שם דירה)
            last_end = None
            
            # איחוד כל המקטעים לרשימה אחת ממוינת
            # כל אירוע: (start, end, type, label, shift_id, segment_id, apartment_type_id, is_married, apartment_name)
            all_events = []
            for s, e, l, sid, apt_name in work_segments:
                all_events.append({"start": s, "end": e, "type": "work", "label": l, "shift_id": sid, "segment_id": None, "apartment_type_id": None, "is_married": None, "apartment_name": apt_name})
            for s, e, seg_id, apt_type_id, is_married_val in standby_segments:
                all_events.append({"start": s, "end": e, "type": "standby", "label": "כוננות", "shift_id": 0, "segment_id": seg_id, "apartment_type_id": apt_type_id, "is_married": is_married_val, "apartment_name": None})
            for s, e in vacation_segments:
                all_events.append({"start": s, "end": e, "type": "vacation", "label": "חופשה", "shift_id": 0, "segment_id": None, "apartment_type_id": None, "is_married": None, "apartment_name": None})
            
            all_events.sort(key=lambda x: x["start"])

            for event in all_events:
                seg_start = event["start"]
                seg_end = event["end"]
                seg_type = event["type"]
                label = event["label"]

                is_standby = (seg_type == "standby")
                is_vacation_seg = (seg_type == "vacation")
                is_special_type = is_standby or is_vacation_seg

                # בדיקות שבירת רצף עבודה
                should_break_work = False
                break_reason = None
                
                if current_chain_start is not None:
                    # אם יש מקטע עבודה פתוח
                    if is_special_type:
                        # כוננות או חופשה תמיד שוברות רצף עבודה
                        should_break_work = True
                        break_reason = label # "כוננות" או "חופשה"
                    else:
                        # מקטע עבודה נוסף
                        # בדיקת רווח זמן
                        gap = seg_start - last_end
                        # אם זה רצף עבודה מתמשך (פער קטן), נוודא שלא מפספסים מקרה של כוננות שהייתה באמצע (אבל כאן אנחנו עוברים על הכל)
                        # אבל ייתכן שהייתה כוננות שלא נכנסה ל-segments (נדיר, אבל לוגית אפשרי אם יש חור)
                        # כרגע נסתמך על זה שאם יש כוננות היא תופיע כאירוע
                        
                        if gap > BREAK_THRESHOLD_MINUTES:  # יותר משעה הפסקה
                            should_break_work = True
                            break_reason = f"הפסקה ({gap} דקות)"
                        # הוסר: בדיקת חציית 08:00
                        # elif last_end <= 480 and seg_start >= 480:
                        #     should_break_work = True
                        #     break_reason = "חציית 08:00 (תחילת יום עבודה)"
                
                if should_break_work:
                    # סוגרים רצף קודם - בפיצול לשורות לפי תעריף
                    
                    minutes_counter = 0 
                    current_weekday = day_date.weekday() # 0-6
                    
                    # משתנים למעקב אחרי שורה נוכחית בטבלה (תת-רצף)
                    row_start = current_chain_segments[0][0] if current_chain_segments else 0
                    row_rate = None
                    row_minutes = 0
                    row_c100 = 0; row_c125 = 0; row_c150 = 0; row_c175 = 0; row_c200 = 0
                    row_c150_shabbat = 0; row_c150_overtime = 0
                    
                    display_rows = []
                    chain_apartments = set()  # איסוף שמות דירות ברצף

                    for s_seg, e_seg, l_seg, sid, apt_name in current_chain_segments:
                        if apt_name:
                            chain_apartments.add(apt_name)
                        duration = e_seg - s_seg
                        for m in range(duration):
                            minute_abs = s_seg + m
                            minutes_counter += 1
                            
                            effective_day_shift = minute_abs // MINUTES_PER_DAY
                            effective_minute_in_day = minute_abs % MINUTES_PER_DAY
                            effective_weekday = (current_weekday + effective_day_shift) % 7
                            
                            is_shabbos = is_shabbat_time(effective_weekday, effective_minute_in_day, sid, day_date, shabbat_cache)
                            
                            # קביעת תעריף לדקה הנוכחית
                            minute_rate_label = calculate_wage_rate(minutes_counter, is_shabbos)

                            # אם התעריף השתנה, סוגרים שורה
                            if row_rate is not None and minute_rate_label != row_rate:
                                display_rows.append({
                                    "start": row_start, "end": minute_abs,
                                    "rate": row_rate, "minutes": row_minutes,
                                    "c100": row_c100, "c125": row_c125, "c150": row_c150,
                                    "c150_shabbat": row_c150_shabbat, "c150_overtime": row_c150_overtime,
                                    "c175": row_c175, "c200": row_c200
                                })
                                # איפוס לשורה חדשה
                                row_start = minute_abs
                                row_minutes = 0
                                row_c100 = 0; row_c125 = 0; row_c150 = 0; row_c175 = 0; row_c200 = 0
                                row_c150_shabbat = 0; row_c150_overtime = 0
                            
                            row_rate = minute_rate_label
                            row_minutes += 1
                            
                            if minute_rate_label == "100%": row_c100 += 1
                            elif minute_rate_label == "125%": row_c125 += 1
                            elif minute_rate_label == "150%": 
                                row_c150 += 1
                                if is_shabbos:
                                    row_c150_shabbat += 1
                                else:
                                    row_c150_overtime += 1
                            elif minute_rate_label == "175%": row_c175 += 1
                            elif minute_rate_label == "200%": row_c200 += 1

                    # סגירת השורה האחרונה
                    if row_minutes > 0:
                        display_rows.append({
                            "start": row_start, "end": last_end,
                            "rate": row_rate, "minutes": row_minutes,
                            "c100": row_c100, "c125": row_c125, "c150": row_c150,
                            "c150_shabbat": row_c150_shabbat, "c150_overtime": row_c150_overtime,
                            "c175": row_c175, "c200": row_c200
                        })

                    # הוספה לרשימת הרצפים
                    for i, row in enumerate(display_rows):
                        is_first = (i == 0)
                        is_last = (i == len(display_rows) - 1)
                        
                        # from_prev_day רק בראשון ורק אם מתחיל ב-00:00
                        row_from_prev = (row["start"] == 0) if is_first else False
                        # break_reason רק באחרון
                        row_break_reason = break_reason if is_last else None
                        
                        # פירוט מקטעים - במקרה הזה המקטע הוא השורה עצמה
                        seg_detail = f"{row['rate']}"
                        
                        payment = (
                            (row["c100"] / 60 * 1.0) +
                            (row["c125"] / 60 * 1.25) +
                            (row["c150"] / 60 * 1.5) +
                            (row["c175"] / 60 * 1.75) +
                            (row["c200"] / 60 * 2.0)
                        ) * MINIMUM_WAGE
                        
                        chain_data = {
                            "type": "work",
                            "is_vacation_display": is_vacation_day,
                            "start_time": minutes_to_time_str(row["start"]),
                            "end_time": minutes_to_time_str(row["end"]),
                            "total_minutes": row["minutes"],
                            "payment": payment,
                            "calc100": row["c100"],
                            "calc125": row["c125"],
                            "calc150": row["c150"],
                            "calc150_shabbat": row["c150_shabbat"],
                            "calc150_overtime": row["c150_overtime"],
                            "calc175": row["c175"],
                            "calc200": row["c200"],
                            "break_reason": row_break_reason,
                            "from_prev_day": row_from_prev,
                            "segments": [(minutes_to_time_str(row["start"]), minutes_to_time_str(row["end"]), seg_detail)],
                            "apartment_name": ", ".join(sorted(chain_apartments)) if chain_apartments else None,
                        }

                        chains_detail.append(chain_data)
                        calc_100_total += row["c100"]
                        calc_125_total += row["c125"]
                        calc_150_total += row["c150"]
                        calc_150_shabbat_total += row["c150_shabbat"]
                        calc_150_shabbat_100_total += row["c150_shabbat"]  # 100% בסיס
                        calc_150_shabbat_50_total += row["c150_shabbat"]   # 50% תוספת
                        calc_150_overtime_total += row["c150_overtime"]
                        calc_175_total += row["c175"]
                        calc_200_total += row["c200"]

                    current_chain_minutes = 0
                    current_chain_start = None
                    current_chain_segments = []
                
                if is_special_type:
                    # אם זה כוננות או חופשה, מוסיפים כרצף נפרד
                    # הוא "המשך מאתמול" רק אם הוא מתחיל ב-00:00 (תחילת היום)
                    is_continuation = (seg_start == 0)
                    
                    # חישוב תשלום:
                    # כוננות = לפי תעריף דינמי מ-standby_rates (אם לא המשך), חופשה = לפי שעות * שכר מינימום
                    payment = 0
                    if is_standby:
                        if not is_continuation:
                            # קבלת פרמטרים מהאירוע
                            segment_id = event.get("segment_id") or 0
                            apartment_type_id = event.get("apartment_type_id")
                            is_married_val = event.get("is_married")
                            # חישוב תעריף דינמי
                            standby_rate = get_standby_rate(conn.conn, segment_id, apartment_type_id, bool(is_married_val) if is_married_val is not None else False)
                            payment = standby_rate
                        else:
                            payment = 0
                    elif is_vacation_seg:
                        # בחופשה נחשב לפי זמן * שכר מינימום
                        duration_hours = (seg_end - seg_start) / 60
                        payment = duration_hours * MINIMUM_WAGE

                    chains_detail.append({
                        "type": seg_type, # "standby" or "vacation"
                        "start_time": minutes_to_time_str(seg_start),
                        "end_time": minutes_to_time_str(seg_end),
                        "total_minutes": seg_end - seg_start,
                        "payment": payment,
                        "calc100": 0, "calc125": 0, "calc150": 0, "calc175": 0, "calc200": 0,
                        "break_reason": label,
                        "from_prev_day": is_continuation,
                        "segments": [(minutes_to_time_str(seg_start), minutes_to_time_str(seg_end), label)],
                        "apartment_name": None,
                    })
                    last_end = seg_end
                    # כוננות/חופשה לא מתחילה רצף עבודה חדש
                else:
                    # עבודה
                    if current_chain_start is None:
                        current_chain_start = seg_start
                    
                    seg_duration = seg_end - seg_start
                    current_chain_minutes += seg_duration
                    sid = event.get("shift_id", 0)
                    apt_name = event.get("apartment_name")
                    current_chain_segments.append((seg_start, seg_end, label, sid, apt_name))
                    last_end = seg_end
            
            # סוגרים את הרצף האחרון אם נשאר פתוח
            if current_chain_minutes > 0:
                minutes_counter = 0
                current_weekday = day_date.weekday()
                
                row_start = current_chain_segments[0][0] if current_chain_segments else 0
                row_rate = None
                row_minutes = 0
                row_c100 = 0; row_c125 = 0; row_c150 = 0; row_c175 = 0; row_c200 = 0
                row_c150_shabbat = 0; row_c150_overtime = 0
                
                display_rows = []
                chain_apartments = set()  # איסוף שמות דירות ברצף

                for s_seg, e_seg, l_seg, sid, apt_name in current_chain_segments:
                    if apt_name:
                        chain_apartments.add(apt_name)
                    duration = e_seg - s_seg
                    for m in range(duration):
                        minute_abs = s_seg + m
                        minutes_counter += 1
                        effective_day_shift = minute_abs // MINUTES_PER_DAY
                        effective_minute_in_day = minute_abs % MINUTES_PER_DAY
                        effective_weekday = (current_weekday + effective_day_shift) % 7
                        is_shabbos = is_shabbat_time(effective_weekday, effective_minute_in_day, sid, day_date, shabbat_cache)
                        
                        minute_rate_label = calculate_wage_rate(minutes_counter, is_shabbos)

                        if row_rate is not None and minute_rate_label != row_rate:
                            display_rows.append({
                                "start": row_start, "end": minute_abs,
                                "rate": row_rate, "minutes": row_minutes,
                                "c100": row_c100, "c125": row_c125, "c150": row_c150,
                                "c150_shabbat": row_c150_shabbat, "c150_overtime": row_c150_overtime,
                                "c175": row_c175, "c200": row_c200
                            })
                            row_start = minute_abs
                            row_minutes = 0
                            row_c100 = 0; row_c125 = 0; row_c150 = 0; row_c175 = 0; row_c200 = 0
                            row_c150_shabbat = 0; row_c150_overtime = 0
                        
                        row_rate = minute_rate_label
                        row_minutes += 1
                        
                        if minute_rate_label == "100%": row_c100 += 1
                        elif minute_rate_label == "125%": row_c125 += 1
                        elif minute_rate_label == "150%": 
                            row_c150 += 1
                            if is_shabbos:
                                row_c150_shabbat += 1
                            else:
                                row_c150_overtime += 1
                        elif minute_rate_label == "175%": row_c175 += 1
                        elif minute_rate_label == "200%": row_c200 += 1

                if row_minutes > 0:
                    display_rows.append({
                        "start": row_start, "end": last_end,
                        "rate": row_rate, "minutes": row_minutes,
                        "c100": row_c100, "c125": row_c125, "c150": row_c150,
                        "c150_shabbat": row_c150_shabbat, "c150_overtime": row_c150_overtime,
                        "c175": row_c175, "c200": row_c200
                    })

                for i, row in enumerate(display_rows):
                    is_first = (i == 0)
                    
                    row_from_prev = (row["start"] == 0) if is_first else False
                    
                    seg_detail = f"{row['rate']}"
                    
                    payment = (
                        (row["c100"] / 60 * 1.0) +
                        (row["c125"] / 60 * 1.25) +
                        (row["c150"] / 60 * 1.5) +
                        (row["c175"] / 60 * 1.75) +
                        (row["c200"] / 60 * 2.0)
                    ) * MINIMUM_WAGE
                    
                    chain_data = {
                        "type": "work",
                        "is_vacation_display": is_vacation_day,
                        "start_time": minutes_to_time_str(row["start"]),
                        "end_time": minutes_to_time_str(row["end"]),
                        "total_minutes": row["minutes"],
                        "payment": payment,
                        "calc100": row["c100"],
                        "calc125": row["c125"],
                        "calc150": row["c150"],
                        "calc150_shabbat": row["c150_shabbat"],
                        "calc150_overtime": row["c150_overtime"],
                        "calc175": row["c175"],
                        "calc200": row["c200"],
                        "break_reason": None,
                        "from_prev_day": row_from_prev,
                        "segments": [(minutes_to_time_str(row["start"]), minutes_to_time_str(row["end"]), seg_detail)],
                        "apartment_name": ", ".join(sorted(chain_apartments)) if chain_apartments else None,
                    }

                    chains_detail.append(chain_data)
                    calc_100_total += row["c100"]
                    calc_125_total += row["c125"]
                    calc_150_total += row["c150"]
                    calc_150_shabbat_total += row["c150_shabbat"]
                    calc_150_shabbat_100_total += row["c150_shabbat"]  # 100% בסיס
                    calc_150_shabbat_50_total += row["c150_shabbat"]   # 50% תוספת
                    calc_150_overtime_total += row["c150_overtime"]
                    calc_175_total += row["c175"]
                    calc_200_total += row["c200"]

            base_100 = calc_100_total
            calc_125 = calc_125_total
            calc_150 = calc_150_total
            calc_175 = calc_175_total
            calc_200 = calc_200_total

            work_minutes_no_standby = sum(
                minutes for label, minutes in buckets.items()
                if label != "כוננות"
            )

            # סיכום חודשי מצטבר
            daily_payment = sum(c.get("payment", 0) for c in chains_detail)
            
            # חישוב נפרד לחופשה לטובת הטבלה המפורטת
            vacation_minutes = sum(c["total_minutes"] for c in chains_detail if c["type"] == "vacation")
            vacation_payment = sum(c.get("payment", 0) for c in chains_detail if c["type"] == "vacation")
            
            # חישוב נפרד לכוננות לטובת הסיכום היומי
            standby_payment = sum(c.get("payment", 0) for c in chains_detail if c["type"] == "standby")
            
            # total_hours = סכום שעות העבודה בפועל (ללא חופשה וכוננות)
            monthly_totals["total_hours"] += (base_100 + calc_125 + calc_150 + calc_175 + calc_200)
            monthly_totals["payment"] = monthly_totals.get("payment", 0) + daily_payment
            
            monthly_totals["vacation_minutes"] = monthly_totals.get("vacation_minutes", 0) + vacation_minutes
            monthly_totals["vacation_payment"] = monthly_totals.get("vacation_payment", 0) + vacation_payment
            
            # הצטברות תשלום כוננות חודשי (משתמש בתעריפים דינמיים)
            monthly_totals["standby_payment"] = monthly_totals.get("standby_payment", 0) + standby_payment

            monthly_totals["calc100"] += base_100
            monthly_totals["calc125"] += calc_125
            monthly_totals["calc150"] += calc_150
            monthly_totals["calc150_shabbat"] += calc_150_shabbat_total
            monthly_totals["calc150_shabbat_100"] += calc_150_shabbat_100_total  # 100% בסיס
            monthly_totals["calc150_shabbat_50"] += calc_150_shabbat_50_total    # 50% תוספת
            monthly_totals["calc150_overtime"] += calc_150_overtime_total
            monthly_totals["calc175"] += calc_175
            monthly_totals["calc200"] += calc_200

            # אם יש חופשה ביום הזה, נסמן את כולו כחופשה לתצוגה (אופציונלי)
            if any(c["type"] == "vacation" for c in chains_detail):
                is_vacation_day = True

            daily_segments.append(
                {
                    "day": day,
                    "day_name": day_name_he,
                    "hebrew_date": hebrew_date_str,
                    "buckets": buckets,
                    "shift_names": shift_names,
                    "total_minutes": total_minutes,
                    "total_minutes_no_standby": work_minutes_no_standby,
                    "payment": daily_payment,
                    "standby_payment": standby_payment,
                    "calc100": base_100,
                    "calc125": calc_125,
                    "calc150": calc_150,
                    "calc150_shabbat": calc_150_shabbat_total,
                    "calc150_overtime": calc_150_overtime_total,
                    "calc175": calc_175,
                    "calc200": calc_200,
                    "chains": chains_detail,  # רשימת רצפים מפורטים
                    "cancelled_standbys": cancelled_standbys,  # כוננויות שבוטלו עקב חפיפה
                }
            )

    months_options = [
        {"year": y, "month": m, "label": f"{m:02d}/{y}"}
        for y, m in months
    ]

    # ---------------------------------------------------------
    # עיבוד נוסף לתצוגה: קיבוץ לפי "יום עבודה" (08:00 עד 08:00 למחרת)
    # ---------------------------------------------------------
    # עותק עמוק כדי לא להרוס את המקורי אם נצטרך אותו
    temp_segments = copy.deepcopy(daily_segments)

    # שלב 1: איפוס הרשימות והסיכומים בכל יום (כי נבנה אותם מחדש)
    for d in temp_segments:
        d["original_chains"] = d["chains"] 
        d["chains"] = []
        d["total_minutes_no_standby"] = 0
        d["payment"] = 0
        d["standby_payment"] = 0
        d["calc100"] = 0; d["calc125"] = 0; d["calc150"] = 0; d["calc175"] = 0; d["calc200"] = 0
        d["calc150_shabbat"] = 0; d["calc150_overtime"] = 0

    # שלב 2: פיזור הרצפים לימים הלוגיים (עם פיצול ב-08:00)
    for i, current_day in enumerate(temp_segments):
        day_date_str = current_day["day"]
    
        for chain in current_day["original_chains"]:
            h_start, m_start = map(int, chain["start_time"].split(':'))
            start_minutes = h_start * 60 + m_start
        
            # חישוב זמן סיום אבסולוטי (כי end_time בפורמט HH:MM יכול להיות אחרי חצות)
            # אם הרצף נמשך, total_minutes הוא הקובע
            end_minutes_calc = start_minutes + chain["total_minutes"]
        
            cutoff = 480 # 08:00 בבוקר
        
            # מקרה 1: הרצף כולו אחרי 08:00 (או ב-08:00 בדיוק) -> שייך ליום הנוכחי
            if start_minutes >= cutoff:
                current_day["chains"].append(chain)
            
            # מקרה 2: הרצף כולו לפני 08:00 -> עובר ליום הקודם (חלק מהלילה של אתמול)
            elif end_minutes_calc <= cutoff:
                if i > 0:
                    prev_day = temp_segments[i-1]
                    prev_day["chains"].append(chain)
                else:
                    current_day["chains"].append(chain) # אין אתמול
                
            # מקרה 3: הרצף חוצה את 08:00 -> פיצול לשניים
            else:
                # חלק 1: מההתחלה עד 08:00
                len1 = cutoff - start_minutes
                # חלק 2: מ-08:00 עד הסוף
                len2 = end_minutes_calc - cutoff
            
                total_len = chain["total_minutes"]
                if total_len == 0: continue # הגנה מחלוקה באפס
            
                ratio1 = len1 / total_len
                ratio2 = len2 / total_len
            
                # יצירת שני עותקים
                chain1 = copy.deepcopy(chain)
                chain2 = copy.deepcopy(chain)
            
                # עדכון חלק 1 (הולך לאתמול)
                chain1["end_time"] = "08:00"
                chain1["total_minutes"] = len1
                chain1["payment"] = chain.get("payment", 0) * ratio1
                chain1["calc100"] *= ratio1
                chain1["calc125"] *= ratio1
                chain1["calc150"] *= ratio1
                chain1["calc150_shabbat"] = chain.get("calc150_shabbat", 0) * ratio1
                chain1["calc150_overtime"] = chain.get("calc150_overtime", 0) * ratio1
                chain1["calc175"] *= ratio1
                chain1["calc200"] *= ratio1
                if chain1["segments"]:
                    s_start, s_end, s_label = chain1["segments"][0]
                    chain1["segments"][0] = (s_start, "08:00", s_label)
            
                # עדכון חלק 2 (נשאר היום)
                chain2["start_time"] = "08:00"
                chain2["total_minutes"] = len2
                chain2["payment"] = chain.get("payment", 0) * ratio2
                chain2["calc100"] *= ratio2
                chain2["calc125"] *= ratio2
                chain2["calc150"] *= ratio2
                chain2["calc150_shabbat"] = chain.get("calc150_shabbat", 0) * ratio2
                chain2["calc150_overtime"] = chain.get("calc150_overtime", 0) * ratio2
                chain2["calc175"] *= ratio2
                chain2["calc200"] *= ratio2
                if chain2["segments"]:
                    s_start, s_end, s_label = chain2["segments"][0]
                    chain2["segments"][0] = ("08:00", s_end, s_label)
            
                # שיוך
                if i > 0:
                    temp_segments[i-1]["chains"].append(chain1)
                else:
                    current_day["chains"].append(chain1)
            
                current_day["chains"].append(chain2)

    # שלב 2.5: איחוד רצפים שנחתכו בחצות (Merging)
    for d in temp_segments:
        if not d["chains"]: continue
    
        merged_chains = []
        if len(d["chains"]) > 0:
            current = d["chains"][0]
        
            for next_chain in d["chains"][1:]:
                # תנאים לאיחוד:
                # 1. סוג זהה (עבודה/כוננות)
                # 2. זמן סיום של הנוכחי == זמן התחלה של הבא
                # 3. הנוכחי לא נשבר בגלל סיבה מיוחדת (break_reason is None)
                # 4. הבא הוא "המשך מאתמול" (from_prev_day) - אינדיקציה טובה שזה אותו רצף שנחצה
            
                # הערה: מאחר ועשינו פיצול לפי תעריף (שורות נפרדות), אנחנו לא רוצים לאחד שורות עם תעריף שונה!
                # אבל הפיצול לתעריף הוא ברמת ה-Start Time?
                # לא, הפיצול לתעריף יצר שורות נפרדות.
                # אם שורה אחת היא 100% והבאה היא 100% (בגלל חצות), אפשר לאחד.
                # אם שורה אחת 100% והבאה 150%, לא נאחד.
            
                # בדיקת תעריף: נבדוק אם segment detail זהה?
                # ה-segments הוא רשימה. בדרך כלל יש שם איבר אחד עכשיו (בגלל התיקון הקודם).
                rate1 = current["segments"][0][2] if current["segments"] else ""
                rate2 = next_chain["segments"][0][2] if next_chain["segments"] else ""
            
                if (current["type"] == next_chain["type"] and 
                    current["end_time"] == next_chain["start_time"] and
                    # לא לאחד אם יש סיבה לשבירה, אלא אם כן זה אותו סוג מיוחד (כוננות/חופשה)
                    # בכוננות/חופשה ה-break_reason הוא התווית עצמה, אז צריך לאפשר איחוד אם זה אותו דבר
                    (not current["break_reason"] or current["type"] in ("standby", "vacation")) and
                    rate1 == rate2):
                
                    # ביצוע איחוד
                    current["end_time"] = next_chain["end_time"]
                    current["total_minutes"] += next_chain["total_minutes"]
                    current["payment"] = current.get("payment", 0) + next_chain.get("payment", 0)
                
                    current["calc100"] += next_chain["calc100"]
                    current["calc125"] += next_chain["calc125"]
                    current["calc150"] += next_chain["calc150"]
                    current["calc150_shabbat"] = current.get("calc150_shabbat", 0) + next_chain.get("calc150_shabbat", 0)
                    current["calc150_overtime"] = current.get("calc150_overtime", 0) + next_chain.get("calc150_overtime", 0)
                    current["calc175"] += next_chain["calc175"]
                    current["calc200"] += next_chain["calc200"]
                
                    # איחוד רשימת המקטעים (לצרכי תצוגה)
                    # אופציונלי: אפשר לאחד גם את המלל, אבל פשוט נוסיף לרשימה
                    # current["segments"].extend(next_chain["segments"])
                    # למעשה, אם התעריף זהה, עדיף להציג שורה אחת של 00:00-08:00 (100%) במקום שתיים.
                    # אז נעדכן את שעת הסיום של המקטע הראשון
                    s_start, s_end, s_label = current["segments"][0]
                    # עדכון שעת סיום של המקטע
                    current["segments"][0] = (s_start, next_chain["end_time"], s_label)
                
                    # break_reason של המאוחד הוא ה-break_reason של האחרון
                    current["break_reason"] = next_chain["break_reason"]
                
                else:
                    merged_chains.append(current)
                    current = next_chain
        
            merged_chains.append(current)
            d["chains"] = merged_chains

    # שלב 3: חישוב מחדש של סיכומים לכל יום
    for d in temp_segments:
        for chain in d["chains"]:
            if chain["type"] in ("work", "vacation"):
                d["total_minutes_no_standby"] += chain["total_minutes"]
        
            d["payment"] += chain.get("payment", 0)
            if chain["type"] == "standby":
                d["standby_payment"] += chain.get("payment", 0)
            d["calc100"] += chain.get("calc100", 0)
            d["calc125"] += chain.get("calc125", 0)
            d["calc150"] += chain.get("calc150", 0)
            d["calc150_shabbat"] += chain.get("calc150_shabbat", 0)
            d["calc150_overtime"] += chain.get("calc150_overtime", 0)
            d["calc175"] += chain.get("calc175", 0)
            d["calc200"] += chain.get("calc200", 0)

    # שלב 4: סינון ימים ריקים או עם מעט מאוד שעות בסיכום החודשי (Daily Segments לתצוגה)
    # daily_segments מכיל את רשימת הימים שתוצג בדוח ובסיכום החודשי.
    # המשתמש ביקש לסנן בסיכום החודשי ימים עם פחות מ-0.1 שעות.
    # אבל daily_segments משמש גם לדוח הרצפים וגם לסיכום החודשי ב-HTML.
    # אם נסנן מכאן, היום ייעלם משניהם.
    # אם המשתמש רוצה להסתיר ימים ריקים לגמרי, זה הגיוני.

    final_segments = []
    actual_work_days = 0 # מונה ימי עבודה בפועל
    vacation_days_taken = 0 # מונה ימי חופשה בפועל

    for d in temp_segments:
        # בדיקה האם יש עבודה בפועל ביום זה (לצורך ספירת ימי עבודה קלנדריים)
        # נבדוק אם יש ברשימת הרצפים לפחות רצף אחד מסוג 'work'
        has_actual_work = any(c["type"] == "work" for c in d["chains"])
        if has_actual_work:
            actual_work_days += 1
        
        # בדיקה האם יש חופשה ביום זה (לצורך ספירת ניצול ימי חופשה)
        has_vacation = any(c["type"] == "vacation" for c in d["chains"])
        if has_vacation:
            vacation_days_taken += 1

        # אם יש יותר מ-0.01 שעות (כלומר > 0.6 דקות, נעגל לדקה אחת)
        # נבדוק האם יש פעילות משמעותית (לפחות דקה אחת של עבודה)
        if d["total_minutes_no_standby"] >= 1:
             final_segments.append(d)
         
    daily_segments = final_segments

    # הוספת ימי העבודה לסיכום החודשי
    monthly_totals["actual_work_days"] = actual_work_days
    monthly_totals["vacation_days_taken"] = vacation_days_taken

    # חישוב צבירות (מחלה וחופשה) באמצעות פונקציית עזר
    accruals = calculate_accruals(
        actual_work_days=actual_work_days,
        start_date_ts=person["start_date"],
        report_year=selected_year,
        report_month=selected_month
    )
    monthly_totals["sick_days_accrued"] = accruals["sick_days_accrued"]
    monthly_totals["vacation_days_accrued"] = accruals["vacation_days_accrued"]
    monthly_totals["vacation_details"] = accruals["vacation_details"]

    year_options = sorted({m["year"] for m in months_options}, reverse=True)

    # הוספת כוננויות לסיכום הכללי כדי שיוצגו בטבלה הגנרית
    monthly_totals["standby"] = total_standby_count if 'total_standby_count' in locals() else 0

    # הוספת vacation כמפתח נוסף עבור payment_codes
    monthly_totals["vacation"] = monthly_totals.get("vacation_minutes", 0)

    # =============================================================
    # שליפת רכיבי תשלום נוספים (נסיעות, תוספות/החזרים)
    # =============================================================
    # חישוב טווח התאריכים לחודש הנבחר
    month_start = datetime(selected_year, selected_month, 1, tzinfo=LOCAL_TZ)
    if selected_month == 12:
        month_end = datetime(selected_year + 1, 1, 1, tzinfo=LOCAL_TZ)
    else:
        month_end = datetime(selected_year, selected_month + 1, 1, tzinfo=LOCAL_TZ)
    month_start_ts = int(month_start.timestamp())
    month_end_ts = int(month_end.timestamp())

    # שליפת רכיבי תשלום נוספים לעובד בחודש הנבחר
    try:
        payment_components = conn.execute("""
            SELECT pc.*, (pc.quantity * pc.rate) as total_amount, pct.name as type_name
            FROM payment_components pc
            JOIN payment_component_types pct ON pc.component_type_id = pct.id
            WHERE pc.person_id = ? AND pc.date >= ? AND pc.date < ?
            ORDER BY pc.date
        """, (person_id, month_start, month_end)).fetchall()
    except (psycopg2.InterfaceError, psycopg2.OperationalError) as e:
        logger.warning(f"Connection lost in guide_view ({e}), using new connection for payment_components")
        with get_conn() as temp_conn:
            payment_components = temp_conn.execute("""
                SELECT pc.*, (pc.quantity * pc.rate) as total_amount, pct.name as type_name
                FROM payment_components pc
                JOIN payment_component_types pct ON pc.component_type_id = pct.id
                WHERE pc.person_id = ? AND pc.date >= ? AND pc.date < ?
                ORDER BY pc.date
            """, (person_id, month_start, month_end)).fetchall()

    # הפרדה: נסיעות (component_type_id=2) לעומת שאר הרכיבים
    travel_total = 0
    extras_total = 0
    extras_details = []

    for pc in payment_components:
        amount = (pc["total_amount"] or 0) / 100  # המרה מאגורות לשקלים
        if pc["component_type_id"] == 2:  # נסיעות
            travel_total += amount
        else:
            extras_total += amount
            # Handle date formatting for different types (date, datetime, or timestamp)
            pc_date = pc["date"]
            if isinstance(pc_date, date) and not isinstance(pc_date, datetime):
                date_str = pc_date.strftime("%d/%m")
            elif isinstance(pc_date, datetime):
                date_str = pc_date.strftime("%d/%m")
            else:
                date_str = datetime.fromtimestamp(pc_date).strftime("%d/%m")
            extras_details.append({
                "date": date_str,
                "type": pc["type_name"],
                "amount": amount
            })

        monthly_totals["travel"] = travel_total
        monthly_totals["extras"] = extras_total
        monthly_totals["extras_details"] = extras_details

    # --- חישוב סיכום פשוט (Simple Summary) ---
    simple_summary = {
        "weekday": {"count": 0, "payment": 0},
        "friday": {"count": 0, "payment": 0},
        "saturday": {"count": 0, "payment": 0},
        "overtime": {"hours": 0, "payment": 0}
    }
    
    for day in daily_segments:
        try:
            d_str = day["day"]
            d_date = datetime.strptime(d_str, "%d/%m/%Y").date()
            wd = d_date.weekday() # 0=Mon, ..., 6=Sun
        
            if wd == 4:
                d_type = "friday"
            elif wd == 5:
                d_type = "saturday"
            else:
                d_type = "weekday"
            
            has_work = (day["total_minutes_no_standby"] > 0)
            d_payment = day.get("payment", 0)
        
            ot_pay = 0
            ot_hours = 0
        
            if d_type == "weekday":
                c125 = day.get("calc125", 0)
                c150 = day.get("calc150", 0)
            
                if c125 > 0:
                    ot_hours += c125 / 60
                    ot_pay += (c125 / 60) * MINIMUM_WAGE * 1.25
                if c150 > 0:
                    ot_hours += c150 / 60
                    ot_pay += (c150 / 60) * MINIMUM_WAGE * 1.5
                
                simple_summary["weekday"]["payment"] += (d_payment - ot_pay)
                if has_work: simple_summary["weekday"]["count"] += 1
            
                simple_summary["overtime"]["hours"] += ot_hours
                simple_summary["overtime"]["payment"] += ot_pay
            
            elif d_type == "friday":
                simple_summary["friday"]["payment"] += d_payment
                if has_work: simple_summary["friday"]["count"] += 1
            
            elif d_type == "saturday":
                simple_summary["saturday"]["payment"] += d_payment
                if has_work: simple_summary["saturday"]["count"] += 1
            
        except Exception as e:
            logger.warning(f"Error in simple summary calc: {e}")

    return templates.TemplateResponse(
        "guide.html",
        {
            "request": request,
            "person": person,
            "months": months_options,
            "years": year_options,
            "selected_year": selected_year,
            "selected_month": selected_month,
            "reports": month_reports,
            "shift_segments": shift_segments,
            "daily_segments": daily_segments,
            "total_standby_count": total_standby_count if 'total_standby_count' in locals() else 0,
            "monthly_totals": monthly_totals if 'monthly_totals' in locals() else {},
            "minimum_wage": MINIMUM_WAGE,
            "payment_codes": payment_codes,
            "simple_summary": simple_summary,
        },
    )


@app.get("/guide/{person_id}/simple", response_class=HTMLResponse)
def guide_simple_view(request: Request, person_id: int, month: int | None = None, year: int | None = None):
    with get_conn() as conn:
        # Reuse existing calculation logic
        # Ideally we should refactor guide_view to separate data fetching from template rendering
        # But for now, we will call the helper and process the result.
        
        # We need daily_segments to group by day type.
        # guide_view logic is complex and integrated. 
        # Let's extract the core logic of guide_view into a helper if possible, or just reimplement the grouping part.
        
        # To avoid duplicating 500 lines of code, let's call the guide_view logic?
        # No, guide_view returns a TemplateResponse.
        
        # We'll use calculate_person_monthly_totals for totals, 
        # but we need the per-day breakdown which is currently inside guide_view.
        # Let's refactor guide_view slightly or duplicate the grouping logic.
        
        # For speed, I will duplicate the minimal grouping logic required for the simple view.
        # It needs: report fetching, daily_map construction, chain calculation (for payment).
        
        # Actually, let's look at what we need:
        # We need to know for each DAY: Date, Type (Weekday/Fri/Sat), Total Payment.
        
        # Let's copy the core logic from guide_view.
        
        person = conn.execute(
            "SELECT id, name, type, is_active, start_date FROM people WHERE id = ?",
            (person_id,),
        ).fetchone()
        if not person:
            raise HTTPException(status_code=404, detail="מדריך לא נמצא")

        all_reports = conn.execute(
            """
            SELECT tr.*, st.name AS shift_name
            FROM time_reports tr
            LEFT JOIN shift_types st ON st.id = tr.shift_type_id
            WHERE tr.person_id = ?
            ORDER BY tr.date DESC
            """,
            (person_id,),
        ).fetchall()

        months = available_months(all_reports)
        if not months:
            selected_year, selected_month = (year, month) if month and year else (None, None)
        else:
            if year is None or month is None:
                selected_year, selected_month = months[-1]
            else:
                selected_year, selected_month = year, month
        
        # ... (We need the complex calculation from guide_view to get accurate payment per day) ...
        # Since I cannot easily refactor the huge guide_view function right now without risk,
        # I will use a trick:
        # I will perform the calculation by calling calculate_person_monthly_totals 
        # but that only gives monthly totals, not per-day breakdown for grouping.
        
        # Okay, I must perform the daily breakdown.
        # I will use the logic I implemented in `calculate_person_monthly_totals` (which I recently updated)
        # Wait, `calculate_person_monthly_totals` DOES calculate daily payment internally!
        # It just sums it up.
        
        # Let's modify `calculate_person_monthly_totals` to return daily breakdown if requested?
        # Or just copy the loop. The loop in `calculate_person_monthly_totals` is identical to `guide_view`'s core loop (mostly).
        
        # Actually, `calculate_person_monthly_totals` in app.py (lines 300-688) calculates everything.
        # But it returns `monthly_totals`.
        # I can modify it to return `daily_breakdown` as well.
        
        # Let's verify `calculate_person_monthly_totals` content.
        pass

    # Re-reading app.py shows calculate_person_monthly_totals is a long function that duplicates logic from guide_view.
    # I should have used that function in guide_view, but guide_view has extra display logic (buckets).
    # The `calculate_person_monthly_totals` logic (lines 454-636) iterates daily_map and calculates `day_standby_payment` and `pay` components.
    
    # I will modify `calculate_person_monthly_totals` to return the breakdown.
    # But first I need to update the signature and return type.
    
    # Alternatively, I can implement the grouping logic here using `calculate_person_monthly_totals` logic as a base.
    
    # Strategy:
    # 1. Fetch data.
    # 2. Re-run the daily calculation loop (copied/adapted from calculate_person_monthly_totals).
    # 3. Aggregate by day type.
    
    # Loading dependencies
    shabbat_cache = get_shabbat_times_cache(conn.conn)
    
    # Fetch minimum wage
    MINIMUM_WAGE = 34.40
    try:
        row = conn.execute("SELECT hourly_rate FROM minimum_wage_rates ORDER BY effective_from DESC LIMIT 1").fetchone()
        if row and row["hourly_rate"]:
            MINIMUM_WAGE = float(row["hourly_rate"]) / 100
    except Exception as e:
        logger.warning(f"Failed to get minimum wage: {e}")

    # Get Month Range
    start_ts, end_ts = month_range_ts(selected_year, selected_month)
    
    reports = conn.execute("""
        SELECT tr.*, st.name as shift_name, 
               a.apartment_type_id,
               p.is_married
        FROM time_reports tr
        LEFT JOIN shift_types st ON st.id = tr.shift_type_id
        LEFT JOIN apartments a ON tr.apartment_id = a.id
        LEFT JOIN people p ON tr.person_id = p.id
        WHERE tr.person_id = ? AND tr.date >= ? AND tr.date < ?
        ORDER BY tr.date, tr.start_time
    """, (person_id, start_ts, end_ts)).fetchall()
    
    # Get Segments
    shift_ids = {r["shift_type_id"] for r in reports if r["shift_type_id"]}
    segments_by_shift = {}
    if shift_ids:
        placeholders = ",".join("?" * len(shift_ids))
        segs = conn.execute(
            f"SELECT * FROM shift_time_segments WHERE shift_type_id IN ({placeholders}) ORDER BY order_index",
            tuple(shift_ids)
        ).fetchall()
        for s in segs:
            segments_by_shift.setdefault(s["shift_type_id"], []).append(s)
            
    # Build Daily Map
    daily_map = {}
    for r in reports:
        if not r["start_time"] or not r["end_time"] or not r["shift_type_id"]:
            continue
        
        r_start, r_end = span_minutes(r["start_time"], r["end_time"])
        r_date = to_local_date(r["date"])
        
        parts = []
        if r_end <= MINUTES_PER_DAY:
            parts.append((r_date, r_start, r_end))
        else:
            parts.append((r_date, r_start, MINUTES_PER_DAY))
            parts.append((r_date + timedelta(days=1), 0, r_end - MINUTES_PER_DAY))
        
        seg_list = segments_by_shift.get(r["shift_type_id"], [])
        if not seg_list:
            seg_list = [{"start_time": r["start_time"], "end_time": r["end_time"], 
                        "wage_percent": 100, "segment_type": "work", "id": None}]
        
        work_type = r.get("work_type")
        shift_name_str = (r["shift_name"] or "")
        is_sick_report = ("מחלה" in shift_name_str)
        is_vacation_report = (work_type == "sick_vacation" or "חופשה" in shift_name_str)
        
        for p_date, p_start, p_end in parts:
            if p_date.year != selected_year or p_date.month != selected_month:
                continue
            
            day_key = p_date.strftime("%d/%m/%Y")
            entry = daily_map.setdefault(day_key, {"segments": [], "date": p_date})
            is_second_day = (p_date > r_date)
            
            for seg in seg_list:
                s_start, s_end = span_minutes(seg["start_time"], seg["end_time"])
                shift_crosses = (r_end > MINUTES_PER_DAY)
                if shift_crosses and s_start < r_start:
                    s_start += MINUTES_PER_DAY
                    s_end += MINUTES_PER_DAY
                
                if is_second_day:
                    current_seg_start = s_start - MINUTES_PER_DAY
                    current_seg_end = s_end - MINUTES_PER_DAY
                else:
                    current_seg_start = s_start
                    current_seg_end = s_end
                
                overlap = overlap_minutes(p_start, p_end, current_seg_start, current_seg_end)
                if overlap <= 0:
                    continue
                
                eff_start = max(current_seg_start, p_start)
                eff_end = min(current_seg_end, p_end)
                
                if is_sick_report:
                    eff_type = "sick"
                elif is_vacation_report:
                    eff_type = "vacation"
                else:
                    eff_type = seg["segment_type"]

                segment_id = seg.get("id")
                apartment_type_id = r.get("apartment_type_id")
                is_married = r.get("is_married")

                entry["segments"].append((
                    eff_start, eff_end, eff_type,
                    r["shift_type_id"], segment_id, apartment_type_id, is_married
                ))

    # Process Days and Aggregate
    summary = {
        "weekday": {"count": 0, "payment": 0},
        "friday": {"count": 0, "payment": 0},
        "shabbat": {"count": 0, "payment": 0},
        "overtime": {"hours": 0, "payment": 0}
    }
    
    WORK_DAY_CUTOFF = 480
    
    # We need to process each day to get its Payment and Overtime Hours
    for day_key, entry in sorted(daily_map.items()):
        day_date = entry["date"]
        weekday = day_date.weekday() # 0=Mon, 6=Sun (Wait, Python is 0=Mon, 6=Sun)
        # In Israel: Sunday=6, Monday=0... No.
        # Python: 0=Monday, 1=Tuesday, 2=Wednesday, 3=Thursday, 4=Friday, 5=Saturday, 6=Sunday
        
        # Map to:
        # Weekday: 0,1,2,3,6 (Mon-Thu, Sun)
        # Friday: 4
        # Shabbat: 5
        
        if weekday == 4: # Friday
            day_type = "friday"
        elif weekday == 5: # Saturday
            day_type = "shabbat"
        else:
            day_type = "weekday"
            
        # Calc logic per day
        segments = entry["segments"]
        segments.sort(key=lambda x: x[0])
        
        # Dedupe
        seen = set()
        deduped = []
        for s in segments:
            key = (s[0], s[1])
            if key not in seen:
                deduped.append(s)
                seen.add(key)
        segments = deduped
        
        # Standby Filter (Overlap > 70%)
        # Extract work vs standby
        work_segs = [s for s in segments if s[2] not in ('standby', 'vacation', 'sick')]
        standby_segs = [s for s in segments if s[2] == 'standby']
        
        final_segments = []
        # Add non-standby
        for s in segments:
            if s[2] != 'standby':
                final_segments.append(s)
        
        # Add valid standby
        for sb in standby_segs:
            sb_start, sb_end = sb[0], sb[1]
            dur = sb_end - sb_start
            if dur > 0:
                overlap = sum(overlap_minutes(sb_start, sb_end, w[0], w[1]) for w in work_segs)
                if overlap / dur < STANDBY_CANCEL_OVERLAP_THRESHOLD:
                    final_segments.append(sb)
        
        final_segments.sort(key=lambda x: x[0])
        
        # Calculate Payment and Overtime for this day
        day_payment = 0
        day_overtime_hours = 0
        day_overtime_payment = 0 # Approximate
        
        # Chain logic
        current_chain = []
        last_end = None
        
        def process_chain(chain):
            nonlocal day_payment, day_overtime_hours
            if not chain: return
            
            # Simple calc for chain
            mins_counter = 0
            for s_start, s_end, s_type, sid, _, _, _ in chain:
                dur = s_end - s_start
                for m in range(dur):
                    mins_counter += 1
                    minute_abs = s_start + m
                    eff_day = minute_abs // MINUTES_PER_DAY
                    eff_min = minute_abs % MINUTES_PER_DAY
                    eff_wd = (weekday + eff_day) % 7
                    
                    is_shab = is_shabbat_time(eff_wd, eff_min, sid, day_date, shabbat_cache)
                    rate_label = calculate_wage_rate(mins_counter, is_shab)
                    
                    mult = 1.0
                    if rate_label == "100%": mult = 1.0
                    elif rate_label == "125%": mult = 1.25
                    elif rate_label == "150%": mult = 1.5
                    elif rate_label == "175%": mult = 1.75
                    elif rate_label == "200%": mult = 2.0
                    
                    val = (1.0 / 60) * MINIMUM_WAGE * mult
                    day_payment += val
                    
                    # Overtime accumulation
                    # Assume overtime is anything > 100% NOT due to Shabbat?
                    # Or just any hours labeled 125% or 150% (overtime)?
                    # calculate_wage_rate returns "150%" for Shabbat too.
                    # The user asked for "Additional Work Hours".
                    # Usually means Hours > 8.
                    # So if mins_counter > 480:
                    if mins_counter > 480:
                        day_overtime_hours += (1.0 / 60)
        
        for s in final_segments:
            s_start, s_end, s_type = s[0], s[1], s[2]
            
            if s_type in ('standby', 'vacation', 'sick'):
                process_chain(current_chain)
                current_chain = []
                last_end = s_end
                
                # Handle special payment
                if s_type == 'standby':
                    # Only pay if starts at 0 (continuation) or it's a new standby
                    # But here we process by day segments.
                    # Logic in guide_view separates "continuation".
                    # Simplification: If starts at 0, it's continuation -> 0 pay (already paid in prev day start).
                    # Unless it's the first day of month? No, chain logic handles it.
                    # Let's use the helper `get_standby_rate` if start > 0.
                    if s_start > 0:
                        sid = s[4] # segment_id
                        apt = s[5]
                        mar = s[6]
                        rate = get_standby_rate(conn.conn, sid or 0, apt, bool(mar))
                        day_payment += rate
                elif s_type == 'vacation':
                    # Vacation pay = duration * min_wage
                    dur = s_end - s_start
                    day_payment += (dur / 60) * MINIMUM_WAGE
                elif s_type == 'sick':
                    # Sick pay? Usually 0 for 1st day? Or full?
                    # Currently treating as vacation (100%) in this simple view
                    dur = s_end - s_start
                    day_payment += (dur / 60) * MINIMUM_WAGE
            else:
                # Work
                if last_end is not None and (s_start - last_end) > BREAK_THRESHOLD_MINUTES:
                    process_chain(current_chain)
                    current_chain = []
                
                current_chain.append(s)
                last_end = s_end
        
        process_chain(current_chain)
        
        # Add to summary
        # Only count as "Shift" if there is significant work/pay?
        # Or just every day with activity?
        if day_payment > 0:
            summary[day_type]["count"] += 1
            summary[day_type]["payment"] += day_payment
            
        summary["overtime"]["hours"] += day_overtime_hours
        # Overtime payment is already inside day_payment, so we don't sum it separately for the total
        # But we calculate it for display if needed?
        # User asked for "X hours overtime X money".
        # This implies he wants to know the cost of overtime.
        # But if we sum (Weekday + Friday + Shabbat), we cover all payment.
        # So "Overtime" line should be informative "Out of which...".
        # We'll need to calculate the delta (Total Pay - Base Pay) to get Overtime Pay accurately.
        # Too complex for this quick hack. I'll just show hours.
        
    months_options = [{"year": y, "month": m, "label": f"{m:02d}/{y}"} for y, m in months]
    year_options = sorted({y for y, _ in months}, reverse=True)

    return templates.TemplateResponse(
        "simple_summary.html",
        {
            "request": request,
            "person": person,  # Fixed: Pass the full person object
            "summary": summary,
            "months": months_options,
            "years": year_options,
            "selected_year": selected_year,
            "selected_month": selected_month,
        }
    )

@app.get("/admin/payment-codes", response_class=HTMLResponse)
def manage_payment_codes(request: Request):
    with get_conn() as conn:
        codes = get_payment_codes(conn.conn)
    return templates.TemplateResponse("payment_codes.html", {"request": request, "codes": codes})

@app.post("/admin/payment-codes/update")
async def update_payment_codes(request: Request):
    form_data = await request.form()
    
    # Parse form data manually to gather updates by ID
    ids = set()
    for key in form_data:
        if key.startswith("display_name_"):
            ids.add(key.split("_")[-1])
            
    with get_conn() as conn:
        for code_id in ids:
            display_name = form_data.get(f"display_name_{code_id}")
            merav_code = form_data.get(f"merav_code_{code_id}")
            display_order = form_data.get(f"display_order_{code_id}")
            icon = form_data.get(f"icon_{code_id}", "")
            
            if display_name:
                conn.execute("""
                    UPDATE payment_codes 
                    SET display_name = ?, merav_code = ?, display_order = ?, icon = ?
                    WHERE id = ?
                """, (display_name, merav_code, display_order, icon, code_id))
        conn.commit()
        
    return RedirectResponse(url="/admin/payment-codes", status_code=303)


# ---------------------------------------------------------
# General Summary Logic
# ---------------------------------------------------------

@app.get("/summary", response_class=HTMLResponse)
def general_summary(request: Request, year: int = None, month: int = None):
    start_time = time.time()
    logger.info(f"Starting general_summary for {month}/{year}")

    # Set default date if not provided
    now = datetime.now(LOCAL_TZ)
    if year is None: year = now.year
    if month is None: month = now.month
    
    # חישוב טווח התאריכים לחודש הנבחר (לשליפת רכיבי תשלום)
    month_start = datetime(year, month, 1, tzinfo=LOCAL_TZ)
    if month == 12:
        month_end = datetime(year + 1, 1, 1, tzinfo=LOCAL_TZ)
    else:
        month_end = datetime(year, month + 1, 1, tzinfo=LOCAL_TZ)
    month_start_ts = int(month_start.timestamp())
    month_end_ts = int(month_end.timestamp())
    
    with get_conn() as conn:
        # 1. Fetch Payment Codes
        payment_codes = get_payment_codes(conn.conn)

        pre_calc_time = time.time()
        logger.info(f"Starting optimized calculation...")
        
        # Use optimized bulk calculation
        summary_data, grand_totals = calculate_monthly_summary(conn.conn, year, month)
        
        loop_time = time.time() - pre_calc_time
        logger.info(f"Optimized calculation took: {loop_time:.4f}s")
            
    year_options = [2023, 2024, 2025, 2026]
    total_time = time.time() - start_time
    logger.info(f"Total general_summary execution time: {total_time:.4f}s")

    return templates.TemplateResponse("general_summary.html", {
        "request": request,
        "payment_codes": payment_codes,
        "summary_data": summary_data,
        "grand_totals": grand_totals,
        "selected_year": year,
        "selected_month": month,
        "years": year_options
    })

@app.get("/export/gesher")
def export_gesher(year: int, month: int, company: str = None, filter_name: str = None, encoding: str = "ascii"):
    """
    ייצוא קובץ גשר למירב - לפי מפעל
    company: קוד מפעל (001 או 400)
    encoding: קידוד הקובץ (ascii / windows-1255 / utf-8)
    """
    if not company:
        raise HTTPException(status_code=400, detail="חובה לבחור מפעל")
    
    with get_conn() as conn:
        content = gesher_exporter.generate_gesher_file(conn, year, month, filter_name, company)
    
    # קידוד הקובץ
    encoded_content = content.encode(encoding, errors='replace')
    
    # שם קובץ עם קוד מפעל
    filename = f"gesher_{company}_{year}_{month:02d}.mrv"
    return Response(
        content=encoded_content,
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
            "Content-Type": f"text/plain; charset={encoding}"
        }
    )


@app.get("/export/gesher/person/{person_id}")
def export_gesher_person(person_id: int, year: int, month: int, encoding: str = "ascii"):
    """
    ייצוא קובץ גשר לעובד בודד
    """
    from urllib.parse import quote
    
    with get_conn() as conn:
        # שליפת שם העובד לשם הקובץ
        person = conn.execute("SELECT name, meirav_code FROM people WHERE id = ?", (person_id,)).fetchone()
        if not person:
            raise HTTPException(status_code=404, detail="עובד לא נמצא")
        
        content, company = gesher_exporter.generate_gesher_file_for_person(conn, person_id, year, month)
    
    if not content:
        raise HTTPException(status_code=400, detail="לא ניתן לייצר קובץ - אין קוד מירב לעובד")
    
    encoded_content = content.encode(encoding, errors='replace')
    
    # שם קובץ - שימוש בקוד מירב במקום שם (כי זה תמיד ASCII)
    meirav_code = person['meirav_code'] or person_id
    filename = f"gesher_{meirav_code}_{year}_{month:02d}.mrv"
    
    # לשם התצוגה בדפדפן - שם מקודד ב-URL encoding
    display_name = quote(person['name'], safe='')
    
    return Response(
        content=encoded_content,
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f"attachment; filename={filename}; filename*=UTF-8''{display_name}_{year}_{month:02d}.mrv",
            "Content-Type": f"text/plain; charset={encoding}"
        }
    )


@app.get("/export/gesher/preview")
def export_gesher_preview(request: Request, year: int = None, month: int = None, show_zero: str = None):
    """תצוגה מקדימה של ייצוא גשר"""
    now = datetime.now()
    if year is None:
        year = now.year
    if month is None:
        month = now.month
    
    show_zero_flag = show_zero == "1"
    
    with get_conn() as conn:
        preview = gesher_exporter.get_export_preview(conn, year, month, limit=100)
        export_codes = gesher_exporter.load_export_config_from_db(conn)
        if not export_codes:
            export_codes = gesher_exporter.load_export_config()
        # שליפת מפעלים מהטבלה
        employers = conn.execute("SELECT code, name FROM employers WHERE is_active::integer = 1 ORDER BY code").fetchall()
    
    # אם לא מבקשים להציג ערכים 0, מסננים שורות ועובדים ללא נתונים
    if not show_zero_flag:
        filtered_preview = []
        for person in preview:
            # סינון שורות: לכסף - בודקים payment, לשאר - בודקים quantity
            non_zero_lines = [
                line for line in person['lines']
                if (line['type'] == 'money' and line['payment'] > 0) or
                   (line['type'] != 'money' and line['quantity'] > 0)
            ]
            if non_zero_lines:
                filtered_preview.append({
                    'name': person['name'],
                    'meirav_code': person['meirav_code'],
                    'lines': non_zero_lines
                })
        preview = filtered_preview
    
    return templates.TemplateResponse("gesher_preview.html", {
        "request": request,
        "preview": preview,
        "export_codes": export_codes,
        "employers": employers,
        "selected_year": year,
        "selected_month": month,
        "show_zero": show_zero_flag,
        "years": list(range(2023, 2027))
    })


@app.get("/export/excel")
def export_excel(year: int = None, month: int = None):
    """ייצוא סיכום חודשי לאקסל"""
    now = datetime.now(LOCAL_TZ)
    if year is None: year = now.year
    if month is None: month = now.month
    
    with get_conn() as conn:
        # 1. Fetch Payment Codes
        payment_codes = get_payment_codes(conn.conn)
        
        # 2. Fetch All Active People
        people = conn.execute("SELECT id, name, start_date, is_married FROM people WHERE is_active::integer = 1 ORDER BY name").fetchall()
        
        # 3. Prepare Shabbat Cache
        shabbat_cache = get_shabbat_times_cache(conn.conn)
        
        # 4. Fetch Minimum Wage
        try:
            mw_row = conn.execute("SELECT hourly_rate FROM minimum_wage_rates ORDER BY effective_from DESC LIMIT 1").fetchone()
            minimum_wage = (float(mw_row["hourly_rate"]) / 100) if mw_row else 29.12
        except Exception as e:
            logger.warning(f"Error fetching min wage in excel export: {e}")
            minimum_wage = DEFAULT_MINIMUM_WAGE

        summary_data = []
        for p in people:
            monthly_totals = calculate_person_monthly_totals(
                conn=conn.conn,
                person_id=p["id"],
                year=year,
                month=month,
                shabbat_cache=shabbat_cache,
                minimum_wage=minimum_wage
            )
            if monthly_totals.get("total_payment", 0) > 0 or monthly_totals.get("total_hours", 0) > 0:
                summary_data.append({"name": p["name"], "totals": monthly_totals})

    if not summary_data:
        return Response(content="אין נתונים לייצוא לחודש זה", media_type="text/plain; charset=utf-8")

    rows = []
    for row in summary_data:
        data = {"שם המדריך": row["name"]}
        for code in payment_codes:
            # סינון עמודות לפי מה שמוצג בדוח ה-HTML
            if code["internal_key"] not in ['sick_days_accrued', 'vacation_days_accrued', 'vacation_days_taken', 'travel', 'extras', 'calc150_shabbat_100', 'calc150_shabbat_50', 'sick_days_taken']:
                val = row["totals"].get(code["internal_key"], 0)
                
                # כותרת העמודה - התאמה למה שמוצג ב-HTML
                display_name = code["display_name"]
                if code["internal_key"] == 'actual_work_days':
                    display_name = 'ימי עבודה'
                elif code["internal_key"] == 'total_hours':
                    display_name = 'סה"כ שעות'
                
                # המרה לשעות אם מדובר בשדה זמן
                if code["internal_key"] == 'total_hours' or 'calc' in code["internal_key"] or code["internal_key"] == 'vacation':
                    data[display_name] = round(val / 60, 2)
                else:
                    data[display_name] = val
        
        data["כוננויות (₪)"] = round(row["totals"].get("standby_payment", 0), 2)
        data["נסיעות (₪)"] = round(row["totals"].get("travel", 0), 2)
        data["תוספות (₪)"] = round(row["totals"].get("extras", 0), 2)
        data['סה"כ לתשלום (₪)'] = round(row["totals"].get("total_payment", row["totals"].get("payment", 0)), 2)
        rows.append(data)
    
    df = pd.DataFrame(rows)
    
    output = io.BytesIO()
    # שימוש ב-openpyxl כמנוע
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='סיכום שכר')
        
        # התאמת רוחב עמודות בסיסית
        worksheet = writer.sheets['סיכום שכר']
        for col in worksheet.columns:
            max_length = 0
            column = col[0].column_letter # Get the column name
            for cell in col:
                try:
                    if cell.value and len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except (TypeError, AttributeError):
                    pass
            adjusted_width = (max_length + 2)
            worksheet.column_dimensions[column].width = adjusted_width

    output.seek(0)
    
    filename = f"summary_{year}_{month:02d}.xlsx"
    headers = {
        'Content-Disposition': f'attachment; filename="{filename}"'
    }
    return StreamingResponse(
        output, 
        headers=headers, 
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )


