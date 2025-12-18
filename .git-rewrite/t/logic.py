from __future__ import annotations

import logging
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta, date
from typing import Iterable, List, Tuple, Dict, Optional, Any, Callable
from zoneinfo import ZoneInfo

from convertdate import hebrew

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection string
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_CONNECTION_STRING = os.getenv("DATABASE_URL")
if not DB_CONNECTION_STRING:
    raise RuntimeError("DATABASE_URL environment variable is required. Please set it in .env file.")

def get_db_connection():
    """Create and return a PostgreSQL database connection."""
    conn = psycopg2.connect(DB_CONNECTION_STRING)
    # Don't set cursor_factory at connection level - let each cursor decide
    return conn


def dict_cursor(conn):
    """Create a cursor that returns rows as dicts, avoiding psycopg2.extras.RealDictCursor bugs."""
    return conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

# =============================================================================
# Constants
# =============================================================================

# Time constants (in minutes)
MINUTES_PER_HOUR = 60
MINUTES_PER_DAY = 24 * MINUTES_PER_HOUR  # 1440

# Work hour thresholds (in minutes)
REGULAR_HOURS_LIMIT = 8 * MINUTES_PER_HOUR   # 480 - First 8 hours at 100%
OVERTIME_125_LIMIT = 10 * MINUTES_PER_HOUR   # 600 - Hours 9-10 at 125%
# Beyond 600 minutes = 150%

# Work day boundaries
WORK_DAY_START_MINUTES = 8 * MINUTES_PER_HOUR  # 480 = 08:00

# Shabbat defaults (when not found in DB)
SHABBAT_ENTER_DEFAULT = 16 * MINUTES_PER_HOUR  # 960 = 16:00 on Friday
SHABBAT_EXIT_DEFAULT = 22 * MINUTES_PER_HOUR   # 1320 = 22:00 on Saturday

# Break threshold (in minutes) - breaks longer than this split work chains
BREAK_THRESHOLD_MINUTES = 60

# Standby cancellation threshold
# If work overlaps with standby by more than this percentage, standby is cancelled
STANDBY_CANCEL_OVERLAP_THRESHOLD = 0.70  # 70%

# Wage/Accrual constants
DEFAULT_MINIMUM_WAGE = 34.40
DEFAULT_STANDBY_RATE = 70.0
STANDARD_WORK_DAYS_PER_MONTH = 21.66
MAX_SICK_DAYS_PER_MONTH = 1.5

# Weekday indices (Python's weekday())
FRIDAY = 4
SATURDAY = 5

LOCAL_TZ = ZoneInfo("Asia/Jerusalem")


def human_date(ts: int | datetime | date | None) -> str:
    """Format epoch seconds, datetime, or date to dd/mm/yyyy in local timezone."""
    if ts is None:
        return "-"
    try:
        if isinstance(ts, date) and not isinstance(ts, datetime):
            # PostgreSQL can return date objects directly
            return ts.strftime("%d/%m/%Y")
        if isinstance(ts, datetime):
            # PostgreSQL returns datetime objects
            dt = ts if ts.tzinfo else ts.replace(tzinfo=LOCAL_TZ)
        else:
            # SQLite returns epoch timestamps
            dt = datetime.fromtimestamp(ts, LOCAL_TZ)
        return dt.strftime("%d/%m/%Y")
    except Exception as e:
        logger.warning(f"Failed to format date: {e}")
        return "-"


def to_local_date(ts: int | datetime | date) -> date:
    """Convert epoch timestamp, datetime, or date object to local date."""
    if isinstance(ts, date) and not isinstance(ts, datetime):
        # Already a date object (PostgreSQL can return date directly)
        return ts
    if isinstance(ts, datetime):
        # PostgreSQL returns datetime objects directly
        if ts.tzinfo is None:
            # Assume UTC if no timezone
            return ts.replace(tzinfo=ZoneInfo("UTC")).astimezone(LOCAL_TZ).date()
        return ts.astimezone(LOCAL_TZ).date()
    # SQLite returns epoch timestamps
    return datetime.fromtimestamp(ts, LOCAL_TZ).date()


def get_shabbat_times_cache(conn) -> Dict[str, Dict[str, str]]:
    """
    Load Shabbat times from DB into a dictionary.
    Key: Date string (YYYY-MM-DD) representing Friday.
    Value: {'enter': HH:MM, 'exit': HH:MM}
    """
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute("SELECT shabbat_date, candle_lighting, havdalah FROM shabbat_times")
        rows = cursor.fetchall()
        cache = {}
        for r in rows:
            if r["shabbat_date"] and r["candle_lighting"] and r["havdalah"]:
                cache[r["shabbat_date"]] = {"enter": r["candle_lighting"], "exit": r["havdalah"]}
        cursor.close()
        return cache
    except Exception as e:
        logger.warning(f"Failed to load shabbat times cache: {e}")
        return {}


def get_standby_rate(conn, segment_id: int, apartment_type_id: int | None, is_married: bool) -> float:
    """
    Get standby rate from standby_rates table.
    Priority: specific apartment_type (priority=10) > general (priority=0)
    
    Args:
        conn: Database connection
        segment_id: ID of the standby segment from shift_time_segments
        apartment_type_id: Type of apartment (None for general)
        is_married: True if person is married, False if single
    
    Returns:
        Standby rate in shekels (amount / 100)
    """
    marital_status = "married" if is_married else "single"
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    # First try to find specific rate for apartment type (priority=10)
    if apartment_type_id is not None:
        cursor.execute("""
            SELECT amount FROM standby_rates
            WHERE segment_id = %s AND apartment_type_id = %s AND marital_status = %s AND priority = 10
            LIMIT 1
        """, (segment_id, apartment_type_id, marital_status))
        row = cursor.fetchone()
        if row:
            cursor.close()
            return float(row["amount"]) / 100
    
    # Fallback to general rate (priority=0, apartment_type_id IS NULL)
    cursor.execute("""
        SELECT amount FROM standby_rates
        WHERE segment_id = %s AND apartment_type_id IS NULL AND marital_status = %s AND priority = 0
        LIMIT 1
    """, (segment_id, marital_status))
    row = cursor.fetchone()
    cursor.close()
    
    if row:
        return float(row["amount"]) / 100
    
    # Default fallback if nothing found
    return DEFAULT_STANDBY_RATE


def available_months(rows: Iterable[Dict]) -> List[Tuple[int, int]]:
    months: set[Tuple[int, int]] = set()
    for r in rows:
        ts = r["date"]
        if not ts:
            continue
        d = to_local_date(ts)
        months.add((d.year, d.month))
    return sorted(months)


def available_months_from_db(conn) -> List[Tuple[int, int]]:
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute("SELECT DISTINCT date FROM time_reports")
    rows = cursor.fetchall()
    cursor.close()
    months: set[Tuple[int, int]] = set()
    for r in rows:
        ts = r["date"]
        if ts is None:
            continue
        d = to_local_date(ts)
        months.add((d.year, d.month))
    return sorted(months)


def get_available_months_for_person(conn, person_id: int) -> List[Tuple[int, int]]:
    """
    Fetch distinct months for a specific person efficiently using SQL.
    """
    cursor = conn.cursor()
    try:
        # Postgres specific optimization
        # date is timestamp without time zone, extract year/month directly
        cursor.execute("""
            SELECT DISTINCT 
                CAST(EXTRACT(YEAR FROM date) AS INTEGER) as year,
                CAST(EXTRACT(MONTH FROM date) AS INTEGER) as month
            FROM time_reports 
            WHERE person_id = %s
            ORDER BY year DESC, month DESC
        """, (person_id,))
        rows = cursor.fetchall()
        return [(r[0], r[1]) for r in rows]
    except Exception as e:
        logger.warning(f"Error fetching months for person {person_id}: {e}")
        return []
    finally:
        cursor.close()


def month_range_ts(year: int, month: int) -> Tuple[int, int]:
    """Return datetime range [start, end) for the given month in local TZ.
    Returns datetime objects for PostgreSQL, epoch seconds for SQLite."""
    start_dt = datetime(year, month, 1, tzinfo=LOCAL_TZ)
    if month == 12:
        end_dt = datetime(year + 1, 1, 1, tzinfo=LOCAL_TZ)
    else:
        end_dt = datetime(year, month + 1, 1, tzinfo=LOCAL_TZ)
    # Return datetime objects directly (PostgreSQL prefers this)
    return start_dt, end_dt


def parse_hhmm(value: str) -> Tuple[int, int]:
    """Return (hours, minutes) integers from 'HH:MM'."""
    h, m = value.split(":")
    return int(h), int(m)


def span_minutes(start_str: str, end_str: str) -> Tuple[int, int]:
    """Return start/end minutes-from-midnight, handling overnight end < start."""
    sh, sm = parse_hhmm(start_str)
    eh, em = parse_hhmm(end_str)
    start = sh * MINUTES_PER_HOUR + sm
    end = eh * MINUTES_PER_HOUR + em
    if end <= start:
        end += MINUTES_PER_DAY
    return start, end


def minutes_to_time_str(minutes: int) -> str:
    """Convert minutes from midnight to HH:MM format (handles >24h wrapping)."""
    day_minutes = minutes % MINUTES_PER_DAY
    h = day_minutes // MINUTES_PER_HOUR
    m = day_minutes % MINUTES_PER_HOUR
    return f"{h:02d}:{m:02d}"


def is_shabbat_time(
    day_of_week: int,
    minute_in_day: int,
    shift_id: int,
    current_date: date,
    shabbat_cache: Dict[str, Dict[str, str]]
) -> bool:
    """
    Check if a specific time is within Shabbat hours.
    
    Args:
        day_of_week: Python weekday (0=Monday, 4=Friday, 5=Saturday)
        minute_in_day: Minutes from midnight (0-1439)
        shift_id: The shift type ID (not used anymore - all shifts use DB times)
        current_date: The current date being checked
        shabbat_cache: Cache of Shabbat times from DB
    
    Returns:
        True if the time is within Shabbat hours
    """
    # Check if it's Friday or Saturday
    if day_of_week not in (FRIDAY, SATURDAY):
        return False
    
    # Find the Saturday for this shabbat (cache is keyed by Saturday date)
    if day_of_week == FRIDAY:
        target_saturday = current_date + timedelta(days=1)
    else:  # SATURDAY
        target_saturday = current_date
    
    saturday_str = target_saturday.strftime("%Y-%m-%d")
    shabbat_data = shabbat_cache.get(saturday_str)
    
    # Use DB times if available
    if shabbat_data:
        try:
            eh, em = map(int, shabbat_data["enter"].split(":"))
            enter_minutes = eh * MINUTES_PER_HOUR + em
            
            xh, xm = map(int, shabbat_data["exit"].split(":"))
            exit_minutes = xh * MINUTES_PER_HOUR + xm
            
            if day_of_week == FRIDAY and minute_in_day >= enter_minutes:
                return True
            if day_of_week == SATURDAY and minute_in_day < exit_minutes:
                return True
            return False
        except (ValueError, KeyError, AttributeError):
            pass

    # Fallback: use default Shabbat times
    if day_of_week == FRIDAY and minute_in_day >= SHABBAT_ENTER_DEFAULT:
        return True
    if day_of_week == SATURDAY and minute_in_day < SHABBAT_EXIT_DEFAULT:
        return True
    
    return False


def calculate_wage_rate(
    minutes_in_chain: int,
    is_shabbat: bool
) -> str:
    """
    Determine the wage rate label based on hours worked in chain and Shabbat status.
    
    Args:
        minutes_in_chain: Total minutes worked so far in the current chain
        is_shabbat: Whether this minute falls within Shabbat hours
    
    Returns:
        Rate label: "100%", "125%", "150%", "175%", or "200%"
    """
    if minutes_in_chain <= REGULAR_HOURS_LIMIT:
        return "150%" if is_shabbat else "100%"
    elif minutes_in_chain <= OVERTIME_125_LIMIT:
        return "175%" if is_shabbat else "125%"
    else:
        return "200%" if is_shabbat else "150%"


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
            # Handle both datetime object (from psycopg2) and timestamp (int/float)
            if isinstance(start_date_ts, datetime):
                start_dt = start_date_ts.date() if hasattr(start_date_ts, 'date') else start_date_ts
            else:
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

def get_payment_codes(conn):
    """Fetch payment codes sorted by display order."""
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute("SELECT * FROM payment_codes ORDER BY display_order")
        result = cursor.fetchall()
        cursor.close()
        return result
    except Exception as e:
        logger.error(f"Error fetching payment codes: {e}")
        return []


# =============================================================================
# פונקציות עזר לחישוב שכר - מאוחדות
# =============================================================================

def _build_daily_map(
    reports: List[Any],
    segments_by_shift: Dict[int, List[Any]],
    year: int,
    month: int
) -> Dict[str, Dict]:
    """
    בניית מפת ימים מדיווחים.
    מחלצת את הלוגיקה המשותפת של בניית daily_map משתי הפונקציות.
    """
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
        shift_name = r.get("shift_name") or ""
        is_vacation_report = (work_type == "sick_vacation" or
                             "חופשה" in shift_name or
                             "מחלה" in shift_name)

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
                eff_type = "vacation" if is_vacation_report else seg["segment_type"]

                segment_id = seg.get("id")
                apartment_type_id = r.get("apartment_type_id")
                is_married = r.get("is_married")

                entry["segments"].append((
                    eff_start, eff_end, eff_type,
                    r["shift_type_id"], segment_id, apartment_type_id, is_married
                ))

    return daily_map


def _calculate_chain_wages(
    chain_segments: List[Tuple[int, int, int]],
    day_date: date,
    shabbat_cache: Dict[str, Dict[str, str]]
) -> Dict[str, int]:
    """
    חישוב שכר לרצף עבודה (chain) בשיטת בלוקים.

    במקום לעבור דקה-דקה (480+ איטרציות), מחשב בלוקים לפי גבולות:
    - 480 דקות (מעבר 100% -> 125%)
    - 600 דקות (מעבר 125% -> 150%)
    - גבולות שבת (כניסה/יציאה)

    Returns:
        מילון עם דקות לכל קטגוריה: calc100, calc125, calc150, etc.
    """
    result = {
        "calc100": 0, "calc125": 0, "calc150": 0, "calc175": 0, "calc200": 0,
        "calc150_shabbat": 0, "calc150_overtime": 0,
        "calc150_shabbat_100": 0, "calc150_shabbat_50": 0
    }

    if not chain_segments:
        return result

    # חישוב דקה-דקה (כמו קודם) - זמנית, עד שנממש חישוב בלוקים מלא
    # הסיבה: חישוב בלוקים דורש טיפול מורכב בגבולות שבת שחוצים סגמנטים
    minutes_counter = 0
    for seg_start, seg_end, seg_shift_id in chain_segments:
        for m in range(seg_end - seg_start):
            minutes_counter += 1
            minute_abs = seg_start + m
            eff_day_shift = minute_abs // MINUTES_PER_DAY
            eff_minute = minute_abs % MINUTES_PER_DAY
            eff_weekday = (day_date.weekday() + eff_day_shift) % 7

            is_shab = is_shabbat_time(eff_weekday, eff_minute, seg_shift_id, day_date, shabbat_cache)
            rate = calculate_wage_rate(minutes_counter, is_shab)

            if rate == "100%":
                result["calc100"] += 1
            elif rate == "125%":
                result["calc125"] += 1
            elif rate == "150%":
                result["calc150"] += 1
                if is_shab:
                    result["calc150_shabbat"] += 1
                    result["calc150_shabbat_100"] += 1
                    result["calc150_shabbat_50"] += 1
                else:
                    result["calc150_overtime"] += 1
            elif rate == "175%":
                result["calc175"] += 1
            elif rate == "200%":
                result["calc200"] += 1

    return result


def _process_daily_map(
    daily_map: Dict[str, Dict],
    shabbat_cache: Dict[str, Dict[str, str]],
    get_standby_rate_fn: Callable[[int, Optional[int], bool], float],
    year: int,
    month: int
) -> Tuple[Dict[str, int], set, set]:
    """
    עיבוד מפת ימים וחישוב סיכומים.

    Args:
        daily_map: מפת הימים שנבנתה ע"י _build_daily_map
        shabbat_cache: זמני שבת
        get_standby_rate_fn: פונקציה לקבלת תעריף כוננות (מאפשרת DB או cache)
        year, month: שנה וחודש לסינון

    Returns:
        (day_totals, work_days_set, vacation_days_set)
    """
    WORK_DAY_CUTOFF = 480  # 08:00

    totals = {
        "calc100": 0, "calc125": 0, "calc150": 0, "calc175": 0, "calc200": 0,
        "calc150_shabbat": 0, "calc150_overtime": 0,
        "calc150_shabbat_100": 0, "calc150_shabbat_50": 0,
        "total_hours": 0, "standby_payment": 0, "vacation_minutes": 0
    }
    work_days_set = set()
    vacation_days_set = set()

    for day_key, entry in sorted(daily_map.items()):
        day_date = entry["date"]

        # הפרדת מקטעים לסוגים
        work_segments = []
        standby_segments = []
        vacation_segments = []

        for seg in entry["segments"]:
            s_start, s_end, s_type, shift_id, seg_id, apt_type, is_married = seg
            if s_type == "standby":
                standby_segments.append((s_start, s_end, seg_id, apt_type, is_married))
            elif s_type == "vacation":
                vacation_segments.append((s_start, s_end))
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

        # איחוד אירועים
        all_events = []
        for s, e, sid in work_segments:
            all_events.append({"start": s, "end": e, "type": "work", "shift_id": sid})
        for s, e, seg_id, apt_type, is_married_val in standby_segments:
            all_events.append({"start": s, "end": e, "type": "standby", "segment_id": seg_id,
                              "apartment_type_id": apt_type, "is_married": is_married_val})
        for s, e in vacation_segments:
            all_events.append({"start": s, "end": e, "type": "vacation"})

        all_events.sort(key=lambda x: x["start"])

        # משתני רצף
        current_chain_segments = []
        last_end = None
        day_standby_payment = 0
        day_vacation_minutes = 0
        day_wages = {
            "calc100": 0, "calc125": 0, "calc150": 0, "calc175": 0, "calc200": 0,
            "calc150_shabbat": 0, "calc150_overtime": 0,
            "calc150_shabbat_100": 0, "calc150_shabbat_50": 0
        }

        def close_chain():
            nonlocal current_chain_segments, day_wages
            if not current_chain_segments:
                return

            chain_wages = _calculate_chain_wages(current_chain_segments, day_date, shabbat_cache)
            for key in day_wages:
                day_wages[key] += chain_wages[key]

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
                    is_married_bool = bool(is_married_val) if is_married_val is not None else False
                    rate = get_standby_rate_fn(seg_id, apt_type, is_married_bool)
                    day_standby_payment += rate
                elif seg_type == "vacation":
                    day_vacation_minutes += (seg_end - seg_start)

                last_end = seg_end
            else:
                shift_id = event.get("shift_id", 0)
                current_chain_segments.append((seg_start, seg_end, shift_id))
                last_end = seg_end

        close_chain()

        # עדכון סיכומים
        for key in day_wages:
            totals[key] += day_wages[key]
        totals["total_hours"] += sum(day_wages[k] for k in ["calc100", "calc125", "calc150", "calc175", "calc200"])
        totals["standby_payment"] += day_standby_payment
        totals["vacation_minutes"] += day_vacation_minutes

        # ספירת ימי עבודה לפי לוגיקת 08:00-08:00
        for s, e, sid in work_segments:
            if s >= WORK_DAY_CUTOFF:
                work_days_set.add(day_date)
            elif e > WORK_DAY_CUTOFF:
                work_days_set.add(day_date)
            else:
                prev_day = day_date - timedelta(days=1)
                if prev_day.year == year and prev_day.month == month:
                    work_days_set.add(prev_day)

        if vacation_segments:
            vacation_days_set.add(day_date)

    return totals, work_days_set, vacation_days_set


def calculate_person_monthly_totals(
    conn,
    person_id: int,
    year: int,
    month: int,
    shabbat_cache: Dict[str, Dict[str, str]],
    minimum_wage: float = DEFAULT_MINIMUM_WAGE
) -> Dict:
    """
    חישוב מדויק של סיכומים חודשיים לעובד.
    """
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    # שליפת פרטי העובד
    cursor.execute("""
        SELECT id, name, phone, email, is_active, start_date, is_married, type
        FROM people WHERE id = %s
    """, (person_id,))
    person = cursor.fetchone()
    if not person:
        cursor.close()
        return {}
    
    # שליפת דיווחים לחודש
    start_ts, end_ts = month_range_ts(year, month)
    cursor.execute("""
        SELECT tr.*, st.name as shift_name, 
               a.apartment_type_id,
               p.is_married
        FROM time_reports tr
        LEFT JOIN shift_types st ON st.id = tr.shift_type_id
        LEFT JOIN apartments a ON tr.apartment_id = a.id
        LEFT JOIN people p ON tr.person_id = p.id
        WHERE tr.person_id = %s AND tr.date >= %s AND tr.date < %s
        ORDER BY tr.date, tr.start_time
    """, (person_id, start_ts, end_ts))
    reports = cursor.fetchall()
    
    if not reports:
        cursor.close()
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
        placeholders = ",".join(["%s"] * len(shift_ids))
        cursor.execute(
            f"""SELECT id, shift_type_id, start_time, end_time, wage_percent, segment_type, order_index
                FROM shift_time_segments 
                WHERE shift_type_id IN ({placeholders}) 
                ORDER BY order_index""",
            tuple(shift_ids)
        )
        segs = cursor.fetchall()
        for s in segs:
            segments_by_shift.setdefault(s["shift_type_id"], []).append(s)
    
    # זיהוי משמרות עם כוננות
    shift_has_standby = {sid: any(s["segment_type"] == "standby" for s in segs)
                         for sid, segs in segments_by_shift.items()}

    # בניית מפת ימים באמצעות הפונקציה המשותפת
    daily_map = _build_daily_map(reports, segments_by_shift, year, month)

    # אתחול סיכומים
    monthly_totals = {
        "total_hours": 0, "payment": 0, "standby": 0, "standby_payment": 0,
        "actual_work_days": 0, "vacation_days_taken": 0,
        "calc100": 0, "calc125": 0, "calc150": 0, "calc150_shabbat": 0,
        "calc150_shabbat_100": 0, "calc150_shabbat_50": 0,
        "calc150_overtime": 0, "calc175": 0, "calc200": 0,
        "vacation_minutes": 0, "vacation_payment": 0, "travel": 0, "extras": 0
    }

    # ספירת כוננויות מדיווחים מקוריים
    for r in reports:
        if r["shift_type_id"] and shift_has_standby.get(r["shift_type_id"], False):
            monthly_totals["standby"] += 1

    # פונקציה לקבלת תעריף כוננות מDB
    def get_standby_rate_from_db(seg_id: int, apt_type: Optional[int], is_married: bool) -> float:
        return get_standby_rate(conn, seg_id, apt_type, is_married)

    # עיבוד מפת הימים באמצעות הפונקציה המשותפת
    totals, work_days_set, vacation_days_set = _process_daily_map(
        daily_map, shabbat_cache, get_standby_rate_from_db, year, month
    )

    # העברת הסיכומים
    for key in ["calc100", "calc125", "calc150", "calc175", "calc200",
                "calc150_shabbat", "calc150_overtime", "calc150_shabbat_100",
                "calc150_shabbat_50", "total_hours", "standby_payment", "vacation_minutes"]:
        monthly_totals[key] = totals[key]

    monthly_totals["actual_work_days"] = len(work_days_set)
    monthly_totals["vacation_days_taken"] = len(vacation_days_set)
    
    # חישוב תשלום חופשה
    monthly_totals["vacation_payment"] = (monthly_totals.get("vacation_minutes", 0) / 60) * minimum_wage
    
    # שליפת רכיבי תשלום נוספים
    month_start = datetime(year, month, 1, tzinfo=LOCAL_TZ)
    month_end = datetime(year + 1, 1, 1, tzinfo=LOCAL_TZ) if month == 12 else datetime(year, month + 1, 1, tzinfo=LOCAL_TZ)
    
    cursor.execute("""
        SELECT (quantity * rate) as total_amount, component_type_id FROM payment_components 
        WHERE person_id = %s AND date >= %s AND date < %s
    """, (person_id, month_start, month_end))
    payment_comps = cursor.fetchall()
    
    for pc in payment_comps:
        amount = (pc["total_amount"] or 0) / 100
        if pc["component_type_id"] == 2:
            monthly_totals["travel"] += amount
        else:
            monthly_totals["extras"] += amount
    
    cursor.close()
    
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
    pay = 0
    pay += (monthly_totals["calc100"] / 60) * minimum_wage * 1.0
    pay += (monthly_totals["calc125"] / 60) * minimum_wage * 1.25
    pay += (monthly_totals["calc150"] / 60) * minimum_wage * 1.5
    pay += (monthly_totals["calc175"] / 60) * minimum_wage * 1.75
    pay += (monthly_totals["calc200"] / 60) * minimum_wage * 2.0
    pay += monthly_totals["standby_payment"]
    pay += monthly_totals["vacation_payment"]
    monthly_totals["payment"] = pay  # תשלום בסיסי
    monthly_totals["total_payment"] = pay + monthly_totals["travel"] + monthly_totals["extras"]  # סה"כ כולל הכל
    
    # populate vacation display
    monthly_totals["vacation"] = monthly_totals["vacation_minutes"]
    
    return monthly_totals

def _calculate_totals_from_data(
    person,
    reports,
    segments_by_shift,
    shift_has_standby,
    payment_comps,
    standby_rates_cache,
    shabbat_cache,
    minimum_wage,
    year,
    month
) -> Dict:
    """
    Helper for calculating totals from pre-fetched data.
    Uses shared helper functions to avoid code duplication.
    """
    # Initialize totals
    monthly_totals = {
        "total_hours": 0, "payment": 0, "standby": 0, "standby_payment": 0,
        "actual_work_days": 0, "vacation_days_taken": 0,
        "calc100": 0, "calc125": 0, "calc150": 0, "calc150_shabbat": 0,
        "calc150_shabbat_100": 0, "calc150_shabbat_50": 0,
        "calc150_overtime": 0, "calc175": 0, "calc200": 0,
        "vacation_minutes": 0, "vacation_payment": 0, "travel": 0, "extras": 0,
        "sick_days_accrued": 0, "vacation_days_accrued": 0
    }

    if not reports:
        return monthly_totals

    # Count standby from reports
    for r in reports:
        if r["shift_type_id"] and shift_has_standby.get(r["shift_type_id"], False):
            monthly_totals["standby"] += 1

    # בניית מפת ימים באמצעות הפונקציה המשותפת
    daily_map = _build_daily_map(reports, segments_by_shift, year, month)

    # פונקציה לקבלת תעריף כוננות מcache
    def get_standby_rate_from_cache(seg_id: int, apt_type: Optional[int], is_married: bool) -> float:
        marital_status = "married" if is_married else "single"
        rate = DEFAULT_STANDBY_RATE

        # Priority 10 - specific apartment type
        if apt_type is not None:
            val = standby_rates_cache.get((seg_id, apt_type, marital_status, 10))
            if val is not None:
                return val

        # Priority 0 - default
        val = standby_rates_cache.get((seg_id, None, marital_status, 0))
        if val is not None:
            return val

        return rate

    # עיבוד מפת הימים באמצעות הפונקציה המשותפת
    totals, work_days_set, vacation_days_set = _process_daily_map(
        daily_map, shabbat_cache, get_standby_rate_from_cache, year, month
    )

    # העברת הסיכומים
    for key in ["calc100", "calc125", "calc150", "calc175", "calc200",
                "calc150_shabbat", "calc150_overtime", "calc150_shabbat_100",
                "calc150_shabbat_50", "total_hours", "standby_payment", "vacation_minutes"]:
        monthly_totals[key] = totals[key]

    monthly_totals["actual_work_days"] = len(work_days_set)
    monthly_totals["vacation_days_taken"] = len(vacation_days_set)
    monthly_totals["vacation_payment"] = (monthly_totals.get("vacation_minutes", 0) / 60) * minimum_wage

    # Extras
    for pc in payment_comps:
        amount = (pc["total_amount"] or 0) / 100
        if pc["component_type_id"] == 2:
            monthly_totals["travel"] += amount
        else:
            monthly_totals["extras"] += amount

    # Accruals
    accruals = calculate_accruals(
        actual_work_days=monthly_totals["actual_work_days"],
        start_date_ts=person["start_date"],
        report_year=year,
        report_month=month
    )
    monthly_totals["sick_days_accrued"] = accruals["sick_days_accrued"]
    monthly_totals["vacation_days_accrued"] = accruals["vacation_days_accrued"]

    # Final Pay
    pay = 0
    pay += (monthly_totals["calc100"] / 60) * minimum_wage * 1.0
    pay += (monthly_totals["calc125"] / 60) * minimum_wage * 1.25
    pay += (monthly_totals["calc150"] / 60) * minimum_wage * 1.5
    pay += (monthly_totals["calc175"] / 60) * minimum_wage * 1.75
    pay += (monthly_totals["calc200"] / 60) * minimum_wage * 2.0
    pay += monthly_totals["standby_payment"]
    pay += monthly_totals["vacation_payment"]
    monthly_totals["payment"] = pay
    monthly_totals["total_payment"] = pay + monthly_totals["travel"] + monthly_totals["extras"]

    monthly_totals["vacation"] = monthly_totals["vacation_minutes"]

    return monthly_totals

def calculate_monthly_summary(conn, year: int, month: int) -> Tuple[List[Dict], Dict]:
    # 1. Fetch Payment Codes
    payment_codes = get_payment_codes(conn)
    
    # 2. Fetch All Active People
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute("SELECT id, name, start_date, is_married, meirav_code FROM people WHERE is_active::integer = 1 ORDER BY name")
    people = cursor.fetchall()
    
    # 3. Time Reports (bulk)
    start_ts, end_ts = month_range_ts(year, month)
    cursor.execute("""
        SELECT tr.*, st.name as shift_name, 
               a.apartment_type_id,
               p.is_married
        FROM time_reports tr
        LEFT JOIN shift_types st ON st.id = tr.shift_type_id
        LEFT JOIN apartments a ON tr.apartment_id = a.id
        LEFT JOIN people p ON tr.person_id = p.id
        WHERE tr.date >= %s AND tr.date < %s
        ORDER BY tr.person_id, tr.date, tr.start_time
    """, (start_ts, end_ts))
    all_reports = cursor.fetchall()
    
    reports_by_person = {}
    shift_type_ids = set()
    for r in all_reports:
        reports_by_person.setdefault(r["person_id"], []).append(r)
        if r["shift_type_id"]:
            shift_type_ids.add(r["shift_type_id"])
            
    # 4. Shift Segments
    segments_by_shift = {}
    shift_has_standby = {}
    if shift_type_ids:
        placeholders = ",".join(["%s"] * len(shift_type_ids))
        cursor.execute(f"""
            SELECT id, shift_type_id, start_time, end_time, wage_percent, segment_type, order_index
            FROM shift_time_segments 
            WHERE shift_type_id IN ({placeholders}) 
            ORDER BY order_index
        """, tuple(shift_type_ids))
        all_segs = cursor.fetchall()
        for s in all_segs:
            segments_by_shift.setdefault(s["shift_type_id"], []).append(s)
            
        for sid, segs in segments_by_shift.items():
            shift_has_standby[sid] = any(s["segment_type"] == "standby" for s in segs)

    # 5. Payment Components
    month_start = datetime(year, month, 1, tzinfo=LOCAL_TZ)
    if month == 12:
        month_end = datetime(year + 1, 1, 1, tzinfo=LOCAL_TZ)
    else:
        month_end = datetime(year, month + 1, 1, tzinfo=LOCAL_TZ)
        
    cursor.execute("""
        SELECT person_id, (quantity * rate) as total_amount, component_type_id 
        FROM payment_components 
        WHERE date >= %s AND date < %s
    """, (month_start, month_end))
    all_payment_comps = cursor.fetchall()
    payment_comps_by_person = {}
    for pc in all_payment_comps:
        payment_comps_by_person.setdefault(pc["person_id"], []).append(pc)

    # 6. Standby Rates
    cursor.execute("SELECT * FROM standby_rates")
    all_standby_rates = cursor.fetchall()
    standby_rates_cache = {}
    for row in all_standby_rates:
        key = (row["segment_id"], row["apartment_type_id"], row["marital_status"], row["priority"])
        standby_rates_cache[key] = float(row["amount"]) / 100

    # 7. Min Wage & Shabbat
    shabbat_cache = get_shabbat_times_cache(conn)
    try:
        cursor.execute("SELECT hourly_rate FROM minimum_wage_rates ORDER BY effective_from DESC LIMIT 1")
        mw_row = cursor.fetchone()
        minimum_wage = (float(mw_row["hourly_rate"]) / 100) if mw_row else 29.12
    except Exception as e:
        logger.warning(f"Error fetching min wage: {e}")
        minimum_wage = DEFAULT_MINIMUM_WAGE
        
    cursor.close()

    summary_data = []
    grand_totals = {code["internal_key"]: 0 for code in payment_codes}
    grand_totals.update({
        "payment": 0, "standby_payment": 0, "travel": 0, "extras": 0, "total_payment": 0,
        "calc150_shabbat_100": 0, "calc150_shabbat_50": 0
    })

    # 8. Iterate and Calculate
    for p in people:
        pid = p["id"]
        monthly_totals = _calculate_totals_from_data(
            person=p,
            reports=reports_by_person.get(pid, []),
            segments_by_shift=segments_by_shift,
            shift_has_standby=shift_has_standby,
            payment_comps=payment_comps_by_person.get(pid, []),
            standby_rates_cache=standby_rates_cache,
            shabbat_cache=shabbat_cache,
            minimum_wage=minimum_wage,
            year=year,
            month=month
        )
        
        if monthly_totals.get("total_payment", 0) > 0 or monthly_totals.get("total_hours", 0) > 0:
            summary_data.append({"name": p["name"], "person_id": p["id"], "merav_code": p["meirav_code"], "totals": monthly_totals})
            
            # Add to Grand Totals
            # Note: The template uses grand_totals["payment"] for the final "Total Payment" column,
            # so we must accumulate the FULL total (including travel/extras) into "payment".
            grand_totals["payment"] += monthly_totals.get("total_payment", 0)
            grand_totals["total_payment"] += monthly_totals.get("total_payment", 0)
            
            for k, v in monthly_totals.items():
                if k in grand_totals and isinstance(v, (int, float)) and k not in ("payment", "total_payment"):
                    grand_totals[k] += v

    return summary_data, grand_totals


