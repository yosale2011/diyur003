"""
Compare the segment calculations between two people to understand the difference
"""
from database import get_conn
from app_utils import get_daily_segments_data
from logic import get_shabbat_times_cache

def check_person_calculation(person_id, year, month, target_date):
    """Check calculation for a specific person and date"""
    minimum_wage = 34.40

    with get_conn() as conn:
        # Get person info
        cursor = conn.execute("""
            SELECT id, name, email FROM people WHERE id = %s
        """, (person_id,))
        person = cursor.fetchone()

        print(f"{'='*80}")
        print(f"Person: {person['name']} (ID: {person_id})")
        print(f"Month: {month}/{year}")
        print(f"{'='*80}\n")

        # Get time reports for the month
        cursor = conn.execute("""
            SELECT tr.*, st.name as shift_type_name, a.name as apartment_name
            FROM time_reports tr
            LEFT JOIN shift_types st ON tr.shift_type_id = st.id
            LEFT JOIN apartments a ON tr.apartment_id = a.id
            WHERE tr.person_id = %s
            AND EXTRACT(YEAR FROM tr.date) = %s
            AND EXTRACT(MONTH FROM tr.date) = %s
            ORDER BY tr.date, tr.start_time
        """, (person_id, year, month))

        reports = cursor.fetchall()
        print(f"Total reports in month: {len(reports)}\n")

        for report in reports:
            print(f"Report Date: {report['date']}")
            print(f"  Time: {report['start_time']} - {report['end_time']}")
            print(f"  Shift: {report['shift_type_name']} (ID: {report['shift_type_id']})")
            print(f"  Apartment: {report['apartment_name']}")

            # Get segments for this shift type
            cursor = conn.execute("""
                SELECT *
                FROM shift_time_segments
                WHERE shift_type_id = %s
                ORDER BY order_index
            """, (report['shift_type_id'],))

            segments = cursor.fetchall()
            if segments:
                print(f"  Shift Segments Definition:")
                for seg in segments:
                    print(f"    {seg['start_time']}-{seg['end_time']}: {seg['segment_type']} ({seg['wage_percent']}%)")
            print()

        # Get calculated segments
        print(f"\n{'='*80}")
        print(f"CALCULATED SEGMENTS FOR {target_date}")
        print(f"{'='*80}\n")

        shabbat_cache = get_shabbat_times_cache(conn)
        daily_segments, person_name = get_daily_segments_data(conn, person_id, year, month, shabbat_cache, minimum_wage)

        # Find matching date
        target_date_formatted = target_date.strftime("%d/%m/%Y")
        matching_segment = None
        for seg in daily_segments:
            if seg['day'] == target_date_formatted:
                matching_segment = seg
                break

        if matching_segment:
            print(f"Date: {matching_segment['day']}")
            print(f"Total Time: {matching_segment['total_minutes']/60:.2f} hours")
            print(f"Work Time (no standby): {matching_segment['total_minutes_no_standby']/60:.2f} hours")

            print(f"\nBuckets:")
            for bucket, minutes in matching_segment['buckets'].items():
                print(f"  {bucket}: {minutes} min ({minutes/60:.2f} hours)")

            print(f"\nPayment:")
            print(f"  Base: {matching_segment['payment']:.2f} NIS")
            print(f"  Standby: {matching_segment['standby_payment']:.2f} NIS")
            print(f"  Total: {matching_segment['payment'] + matching_segment['standby_payment']:.2f} NIS")

            print(f"\nChains:")
            for i, chain in enumerate(matching_segment['chains'], 1):
                print(f"  Chain {i}: {chain['start_time']}-{chain['end_time']}")
                print(f"    Type: {chain['type']}, Duration: {chain['total_minutes']/60:.2f} hours")
                print(f"    Payment: {chain['payment']:.2f} NIS")
                if chain['segments']:
                    print(f"    Segments: {chain['segments']}")
        else:
            print(f"No data found for {target_date_formatted}")

        return matching_segment

if __name__ == "__main__":
    from datetime import date

    print("\n" + "="*80)
    print("CASE 1: Avrigel Avigail - December 2025 (PROBLEM CASE)")
    print("="*80 + "\n")

    avigel_result = check_person_calculation(
        person_id=76,
        year=2025,
        month=12,
        target_date=date(2025, 12, 10)
    )

    print("\n\n" + "="*80)
    print("CASE 2: Ahronovitz Tehila - November 2025 (CORRECT CASE)")
    print("="*80 + "\n")

    # Need to find Tehila's person_id
    with get_conn() as conn:
        cursor = conn.execute("""
            SELECT id, name FROM people
            WHERE name LIKE %s
        """, ('%תהילה%',))
        tehila = cursor.fetchone()
        if tehila:
            print(f"Found: {tehila['name']} (ID: {tehila['id']})\n")
            tehila_result = check_person_calculation(
                person_id=tehila['id'],
                year=2025,
                month=11,
                target_date=date(2025, 11, 19)
            )
        else:
            print("Could not find Tehila in database")

    print("\n\n" + "="*80)
    print("COMPARISON SUMMARY")
    print("="*80 + "\n")

    if avigel_result and tehila_result:
        print("Key Differences:")
        print(f"  Avigel buckets: {avigel_result['buckets']}")
        print(f"  Tehila buckets: {tehila_result['buckets']}")
