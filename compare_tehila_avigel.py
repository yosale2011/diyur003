"""
Compare Tehila (correct) vs Avigel (problem) for the same shift type
"""
from database import get_conn
from app_utils import get_daily_segments_data
from logic import get_shabbat_times_cache
from datetime import date

def analyze_shift(person_id, person_name, year, month, target_date):
    """Analyze a specific shift"""
    minimum_wage = 34.40

    with get_conn() as conn:
        print(f"\n{'='*80}")
        print(f"{person_name} - {target_date}")
        print(f"{'='*80}\n")

        # Get the report
        cursor = conn.execute("""
            SELECT tr.*, st.name as shift_name, a.name as apartment_name
            FROM time_reports tr
            LEFT JOIN shift_types st ON tr.shift_type_id = st.id
            LEFT JOIN apartments a ON tr.apartment_id = a.id
            WHERE tr.person_id = %s
            AND tr.date = %s
        """, (person_id, target_date))

        report = cursor.fetchone()
        if not report:
            print(f"No report found for {target_date}")
            return None

        print(f"RAW REPORT:")
        print(f"  Date: {report['date']}")
        print(f"  Time: {report['start_time']} - {report['end_time']}")
        print(f"  Shift Type: {report['shift_name']} (ID: {report['shift_type_id']})")
        print(f"  Apartment: {report['apartment_name']}")

        # Get shift segments definition
        cursor = conn.execute("""
            SELECT *
            FROM shift_time_segments
            WHERE shift_type_id = %s
            ORDER BY order_index
        """, (report['shift_type_id'],))

        segments = cursor.fetchall()
        print(f"\nSHIFT TYPE SEGMENTS:")
        for seg in segments:
            print(f"  {seg['start_time']}-{seg['end_time']}: {seg['segment_type']} ({seg['wage_percent']}%)")

        # Get calculated data
        shabbat_cache = get_shabbat_times_cache(conn)
        daily_segments, _ = get_daily_segments_data(conn, person_id, year, month, shabbat_cache, minimum_wage)

        target_date_str = target_date.strftime("%d/%m/%Y")
        matching = None
        for seg in daily_segments:
            if seg['day'] == target_date_str:
                matching = seg
                break

        if not matching:
            print(f"\nNo calculated data found")
            return None

        print(f"\nCALCULATED RESULT:")
        print(f"  Total Minutes: {matching['total_minutes']} ({matching['total_minutes']/60:.2f} hours)")
        print(f"  Work Minutes (no standby): {matching['total_minutes_no_standby']} ({matching['total_minutes_no_standby']/60:.2f} hours)")

        print(f"\n  Buckets:")
        for bucket, minutes in matching['buckets'].items():
            print(f"    {bucket}: {minutes} min ({minutes/60:.2f} hours)")

        print(f"\n  Payment:")
        print(f"    Work: {matching['payment']:.2f} NIS")
        print(f"    Standby: {matching['standby_payment']:.2f} NIS")
        print(f"    TOTAL: {matching['payment'] + matching['standby_payment']:.2f} NIS")

        print(f"\n  Chains:")
        for i, chain in enumerate(matching['chains'], 1):
            print(f"    Chain {i}: {chain['start_time']}-{chain['end_time']} ({chain['type']})")
            print(f"      Duration: {chain['total_minutes']/60:.2f} hours")
            print(f"      Payment: {chain['payment']:.2f} NIS")
            if chain['segments']:
                print(f"      Segments: {chain['segments']}")

        return {
            'report': report,
            'calculated': matching
        }

if __name__ == "__main__":
    print("\n" + "="*80)
    print("COMPARISON: Tehila (CORRECT) vs Avigel (PROBLEM)")
    print("="*80)

    tehila = analyze_shift(
        person_id=231,
        person_name="Tehila Ahronovitz",
        year=2025,
        month=11,
        target_date=date(2025, 11, 19)
    )

    avigel = analyze_shift(
        person_id=76,
        person_name="Avigel Avigail",
        year=2025,
        month=12,
        target_date=date(2025, 12, 10)
    )

    if tehila and avigel:
        print(f"\n\n{'='*80}")
        print("COMPARISON SUMMARY")
        print(f"{'='*80}\n")

        print(f"Report Times:")
        print(f"  Tehila: {tehila['report']['start_time']} - {tehila['report']['end_time']}")
        print(f"  Avigel: {avigel['report']['start_time']} - {avigel['report']['end_time']}")

        print(f"\nShift Type:")
        print(f"  Tehila: {tehila['report']['shift_type_id']}")
        print(f"  Avigel: {avigel['report']['shift_type_id']}")

        print(f"\nBuckets Comparison:")
        print(f"  Tehila: {tehila['calculated']['buckets']}")
        print(f"  Avigel: {avigel['calculated']['buckets']}")

        print(f"\nTotal Payment:")
        print(f"  Tehila: {tehila['calculated']['payment'] + tehila['calculated']['standby_payment']:.2f} NIS")
        print(f"  Avigel: {avigel['calculated']['payment'] + avigel['calculated']['standby_payment']:.2f} NIS")

        print(f"\nNumber of Chains:")
        print(f"  Tehila: {len(tehila['calculated']['chains'])}")
        print(f"  Avigel: {len(avigel['calculated']['chains'])}")
