"""
Analyze Abramoff Yeakev Hana - December 2025
To verify the hypothesis that starting before the first segment causes the problem
"""
from database import get_conn
from app_utils import get_daily_segments_data
from logic import get_shabbat_times_cache
from datetime import date

def analyze_abramoff():
    """Analyze Abramoff's shift on Dec 16"""
    person_id = None
    minimum_wage = 34.40

    with get_conn() as conn:
        # Find Abramoff
        cursor = conn.execute("""
            SELECT id, name, email FROM people
            WHERE name LIKE %s
        """, ('%אברמוף%',))

        people = cursor.fetchall()
        print("People matching 'אברמוף':")
        for person in people:
            print(f"  ID: {person['id']}, Name: {person['name']}, Email: {person.get('email')}")

        if not people:
            print("Not found!")
            return

        person_id = people[0]['id']
        print(f"\nUsing person ID: {person_id}")

        # Get December 2025 reports
        cursor = conn.execute("""
            SELECT tr.*, st.name as shift_name, a.name as apartment_name
            FROM time_reports tr
            LEFT JOIN shift_types st ON tr.shift_type_id = st.id
            LEFT JOIN apartments a ON tr.apartment_id = a.id
            WHERE tr.person_id = %s
            AND EXTRACT(YEAR FROM tr.date) = 2025
            AND EXTRACT(MONTH FROM tr.date) = 12
            ORDER BY tr.date, tr.start_time
        """, (person_id,))

        reports = cursor.fetchall()
        print(f"\nDecember 2025 reports: {len(reports)}")

        for report in reports:
            print(f"\n  Date: {report['date']}")
            print(f"  Time: {report['start_time']} - {report['end_time']}")
            print(f"  Shift: {report['shift_name']} (ID: {report['shift_type_id']})")
            print(f"  Apartment: {report['apartment_name']}")

            # Get shift segments
            cursor = conn.execute("""
                SELECT *
                FROM shift_time_segments
                WHERE shift_type_id = %s
                ORDER BY order_index
            """, (report['shift_type_id'],))

            segments = cursor.fetchall()
            if segments:
                print(f"  Shift Segments:")
                for seg in segments:
                    print(f"    {seg['start_time']}-{seg['end_time']}: {seg['segment_type']} ({seg['wage_percent']}%)")

        # Now get calculated segments
        print(f"\n{'='*80}")
        print("CALCULATED SEGMENTS")
        print(f"{'='*80}\n")

        shabbat_cache = get_shabbat_times_cache(conn)
        daily_segments, person_name = get_daily_segments_data(conn, person_id, 2025, 12, shabbat_cache, minimum_wage)

        # Focus on December 16
        target_date = '16/12/2025'
        matching = None
        for seg in daily_segments:
            if seg['day'] == target_date:
                matching = seg
                break

        if matching:
            print(f"Date: {matching['day']}")
            print(f"Total Minutes: {matching['total_minutes']} ({matching['total_minutes']/60:.2f} hours)")
            print(f"Work Minutes (no standby): {matching['total_minutes_no_standby']} ({matching['total_minutes_no_standby']/60:.2f} hours)")

            print(f"\nBuckets:")
            for bucket, minutes in matching['buckets'].items():
                print(f"  {bucket}: {minutes} min ({minutes/60:.2f} hours)")

            print(f"\nPayment:")
            print(f"  Work: {matching['payment']:.2f} NIS")
            print(f"  Standby: {matching['standby_payment']:.2f} NIS")
            print(f"  TOTAL: {matching['payment'] + matching['standby_payment']:.2f} NIS")

            print(f"\nChains: {len(matching['chains'])}")
            for i, chain in enumerate(matching['chains'], 1):
                print(f"  Chain {i}: {chain['start_time']}-{chain['end_time']} ({chain['type']})")
                print(f"    Duration: {chain['total_minutes']/60:.2f} hours")
                print(f"    Payment: {chain['payment']:.2f} NIS")
                if chain['segments']:
                    print(f"    Segments: {chain['segments']}")

            # Check if 06:30-08:00 is missing
            print(f"\n{'='*80}")
            print("DIAGNOSIS:")
            print(f"{'='*80}")

            has_630_800 = False
            for chain in matching['chains']:
                if '06:30' in chain['start_time'] and '08:00' in chain['end_time']:
                    has_630_800 = True
                    break

            if has_630_800:
                print("✅ Segment 06:30-08:00 is PRESENT")
            else:
                print("❌ Segment 06:30-08:00 is MISSING")
                print("\nExpected based on shift definition:")
                print("  - Should have 06:30-08:00 work segment")
                print("  - Missing payment: ~51.60 NIS (1.5 hours × 34.40)")

        else:
            print(f"No data found for {target_date}")

        return matching

if __name__ == "__main__":
    print("="*80)
    print("ANALYZING ABRAMOFF YEAKEV HANA - DECEMBER 16, 2025")
    print("="*80 + "\n")

    result = analyze_abramoff()

    if result:
        print(f"\n\n{'='*80}")
        print("HYPOTHESIS VERIFICATION")
        print(f"{'='*80}\n")

        print("Cases with the same shift type (103 - משמרת לילה):")
        print("\n1. Avigel Avigail (12/10/2025):")
        print("   - Report: 15:00-08:00")
        print("   - First segment starts at: 16:00")
        print("   - Starts 1 hour BEFORE first segment")
        print("   - Result: 06:30-08:00 MISSING ❌")

        print("\n2. Abramoff Yeakev (12/16/2025):")
        print("   - Report: 16:00-08:00")
        print("   - First segment starts at: 16:00")
        print("   - Starts EXACTLY at first segment")

        if result:
            has_630_800 = any('06:30' in c['start_time'] and '08:00' in c['end_time']
                            for c in result['chains'])
            if has_630_800:
                print("   - Result: 06:30-08:00 PRESENT ✅")
            else:
                print("   - Result: 06:30-08:00 MISSING ❌")

        print("\n3. Tehila Ahronovitz (11/19/2025):")
        print("   - Report: 16:00-08:00 (+ other reports)")
        print("   - First segment starts at: 16:00")
        print("   - Starts EXACTLY at first segment")
        print("   - Result: 06:30-08:00 PRESENT ✅")

        print("\n" + "="*80)
        print("CONCLUSION:")
        print("="*80)
        print("The hypothesis needs to be tested:")
        print("Does starting BEFORE the first defined segment cause the system")
        print("to skip the LAST segment (06:30-08:00)?")
