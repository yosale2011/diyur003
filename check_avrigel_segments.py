"""
Check shift time segments calculation for Avrigel Avigail on 2025-12-10
"""
from database import get_conn
from app_utils import get_daily_segments_data
from logic import get_shabbat_times_cache
import json

def check_avrigel_segments():
    """Check the segments calculation for Avrigel"""
    person_id = 76
    year = 2025
    month = 12
    minimum_wage = 34.40  # Default minimum wage

    print("="*70)
    print(f"Checking segments for person_id {person_id} in {month}/{year}")
    print("="*70)

    with get_conn() as conn:
        # First, let's see the raw time report
        cursor = conn.execute("""
            SELECT tr.*, st.name as shift_type_name, a.name as apartment_name
            FROM time_reports tr
            LEFT JOIN shift_types st ON tr.shift_type_id = st.id
            LEFT JOIN apartments a ON tr.apartment_id = a.id
            WHERE tr.person_id = %s
            AND tr.date = %s
        """, (person_id, '2025-12-10'))

        report = cursor.fetchone()
        if report:
            print("\n=== Raw Time Report ===")
            print(f"  Date: {report['date']}")
            print(f"  Time: {report['start_time']} - {report['end_time']}")
            print(f"  Shift Type: {report['shift_type_name']} (ID: {report['shift_type_id']})")
            print(f"  Apartment: {report['apartment_name']} (ID: {report['apartment_id']})")
            print(f"  Approved: {report['is_approved']}")
        else:
            print("\n=== No report found! ===")
            return

        # Get shift type segments definition
        print("\n" + "="*70)
        print("=== Shift Type Segments Definition (from shift_time_segments table) ===")
        print("="*70)

        cursor = conn.execute("""
            SELECT *
            FROM shift_time_segments
            WHERE shift_type_id = %s
            ORDER BY order_index
        """, (report['shift_type_id'],))

        shift_segments = cursor.fetchall()
        if shift_segments:
            for seg in shift_segments:
                print(f"\n  Segment {seg['order_index']}:")
                print(f"    Time: {seg['start_time']} - {seg['end_time']}")
                print(f"    Type: {seg['segment_type']}")
                print(f"    Wage %: {seg['wage_percent']}%")
                print(f"    Description: {seg['description']}")
        else:
            print("  WARNING: No segments defined for this shift type!")

        # Now get the calculated segments using app_utils
        print("\n" + "="*70)
        print("=== Calculated Daily Segments (using app_utils.get_daily_segments_data) ===")
        print("="*70)

        try:
            shabbat_cache = get_shabbat_times_cache(conn)
            daily_segments, person_name_from_calc = get_daily_segments_data(conn, person_id, year, month, shabbat_cache, minimum_wage)

            # Find the entry for 2025-12-10
            target_date = '10/12/2025'  # Format is DD/MM/YYYY in the segments data

            print(f"\nPerson from calculation: {person_name_from_calc}")
            print(f"Total days with data: {len(daily_segments)}")
            print(f"Looking for date: {target_date}")

            # First check what type daily_segments is and what it contains
            print(f"\nType of daily_segments: {type(daily_segments)}")
            if daily_segments:
                print(f"First element type: {type(daily_segments[0])}")
                print(f"First element keys: {daily_segments[0].keys() if hasattr(daily_segments[0], 'keys') else 'No keys method'}")
                print(f"\nFirst element content:")
                for key, value in daily_segments[0].items():
                    if key not in ['details', 'segments_details']:
                        print(f"  {key}: {value}")

            # Find the matching segment
            matching_segment = daily_segments[0] if daily_segments else None

            if matching_segment:
                print(f"\n{'='*70}")
                print(f"ANALYSIS OF SHIFT CALCULATION FOR {matching_segment['day']}")
                print(f"{'='*70}\n")

                print(f"Date: {matching_segment['day']} ({matching_segment['day_name']})")
                print(f"Hebrew Date: {matching_segment['hebrew_date']}")
                print(f"Shift: {', '.join(matching_segment['shift_names'])}")
                print(f"\nTotal Minutes: {matching_segment['total_minutes']} ({matching_segment['total_minutes']/60:.2f} hours)")
                print(f"Work Minutes (no standby): {matching_segment['total_minutes_no_standby']} ({matching_segment['total_minutes_no_standby']/60:.2f} hours)")

                print(f"\n--- Buckets (Time Categories) ---")
                for bucket_name, minutes in matching_segment['buckets'].items():
                    print(f"  {bucket_name}: {minutes} minutes ({minutes/60:.2f} hours)")

                print(f"\n--- Payment Calculation ---")
                print(f"  Base Payment: {matching_segment['payment']:.2f} NIS")
                print(f"  Standby Payment: {matching_segment['standby_payment']:.2f} NIS")
                print(f"  Total: {matching_segment['payment'] + matching_segment['standby_payment']:.2f} NIS")

                print(f"\n--- Time Distribution by Wage Rate ---")
                print(f"  100%: {matching_segment['calc100']} minutes ({matching_segment['calc100']/60:.2f} hours)")
                print(f"  125%: {matching_segment['calc125']} minutes ({matching_segment['calc125']/60:.2f} hours)")
                print(f"  150%: {matching_segment['calc150']} minutes ({matching_segment['calc150']/60:.2f} hours)")
                print(f"  175%: {matching_segment['calc175']} minutes ({matching_segment['calc175']/60:.2f} hours)")
                print(f"  200%: {matching_segment['calc200']} minutes ({matching_segment['calc200']/60:.2f} hours)")

                print(f"\n--- Chains (Work Periods) ---")
                for i, chain in enumerate(matching_segment['chains'], 1):
                    print(f"\n  Chain #{i}:")
                    print(f"    Time: {chain['start_time']} - {chain['end_time']}")
                    print(f"    Duration: {chain['total_minutes']} minutes ({chain['total_minutes']/60:.2f} hours)")
                    print(f"    Type: {chain['type']}")
                    print(f"    Apartment: {chain['apartment_name']}")
                    print(f"    Payment: {chain['payment']:.2f} NIS")
                    print(f"    Rate: {chain['effective_rate']:.2f} NIS/hour")
                    print(f"    Break Reason: {chain['break_reason']}")
                    if chain['segments']:
                        print(f"    Segments:")
                        for seg in chain['segments']:
                            print(f"      - {seg[0]} to {seg[1]}: {seg[2]}")

                if 'details' in matching_segment and matching_segment['details']:
                    print(f"\n--- Calculation Details ---")
                    for detail in matching_segment['details']:
                        print(f"  {detail}")
            else:
                print(f"\n  ERROR: No calculated data found for {target_date}")
                print(f"  Searched in {len(daily_segments)} day segments")

        except Exception as e:
            print(f"\n  ERROR calculating segments: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    check_avrigel_segments()
