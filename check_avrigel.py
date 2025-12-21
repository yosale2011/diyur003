"""
Check Avrigel Avigail's shifts on 12/12/2025
"""
from database import get_conn
from datetime import datetime

def check_avrigel_shifts():
    """Query all shifts for Avrigel Avigail on 12/12/2025"""
    with get_conn() as conn:
        # First, let's find the person_id for Avrigel Avigail
        search_name = "אבריג'ל אביגיל"
        cursor = conn.execute("""
            SELECT id, name, email FROM people
            WHERE name LIKE %s OR name LIKE %s
        """, (f'%אביגיל%', f'%אבריג%'))
        people = cursor.fetchall()

        print("=== Found People ===")
        for person in people:
            print(f"ID: {person['id']}, Name: {person['name']}, Email: {person.get('email', 'N/A')}")

        if not people:
            print("No matching people found!")
            return

        # Use the first match (likely the right one)
        person_id = people[0]['id']
        person_name = people[0]['name']

        # First check which person is the right one (ID 76 - with email avigail26@gmail.com)
        person_id = 76
        person_name = "אבריג'ל אביגיל"

        print(f"\n=== Shifts for {person_name} (ID: {person_id}) on 12/12/2025 ===\n")

        # Get all shifts for this person on 12/12/2025
        # Let's first check what tables exist
        cursor = conn.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
        """, ('public',))
        tables = cursor.fetchall()
        print("All tables in database:")
        for table in tables:
            print(f"  - {table['table_name']}")

        print("\n" + "="*50 + "\n")

        # Now try to get shifts from time_reports
        cursor = conn.execute("""
            SELECT *
            FROM time_reports
            WHERE person_id = %s
            AND date = %s
            ORDER BY start_time
        """, (person_id, '2025-12-12'))

        shifts = cursor.fetchall()

        if not shifts:
            print("No time reports found for this date!")
        else:
            for i, shift in enumerate(shifts, 1):
                print(f"Time Report #{i}:")
                for key, value in shift.items():
                    print(f"  {key}: {value}")
                print()

        print(f"Total time reports: {len(shifts)}")

        # Let's see what dates DO exist for this person
        print("\n" + "="*50)
        print("Checking all dates for this person in December 2025...")
        print("="*50 + "\n")

        cursor = conn.execute("""
            SELECT date, COUNT(*) as report_count
            FROM time_reports
            WHERE person_id = %s
            AND date >= %s AND date <= %s
            GROUP BY date
            ORDER BY date
        """, (person_id, '2025-12-01', '2025-12-31'))

        dates = cursor.fetchall()

        if dates:
            print(f"Found {len(dates)} dates with reports:")
            for date_row in dates:
                print(f"  {date_row['date']}: {date_row['report_count']} report(s)")
        else:
            print("No reports found in December 2025!")

        # Also check for the specific date 2025-12-12 with all people
        print("\n" + "="*50)
        print("All reports on 2025-12-12 (any person)...")
        print("="*50 + "\n")

        cursor = conn.execute("""
            SELECT tr.*, p.name
            FROM time_reports tr
            JOIN people p ON tr.person_id = p.id
            WHERE tr.date = %s
            ORDER BY tr.person_id, tr.start_time
        """, ('2025-12-12',))

        all_reports = cursor.fetchall()
        print(f"Total reports on 2025-12-12: {len(all_reports)}")
        if all_reports:
            for report in all_reports[:5]:  # Show first 5
                print(f"  Person {report['person_id']} ({report['name']}): {report['start_time']}-{report['end_time']}")

        # Now let's look at the actual report for 2025-12-10
        print("\n" + "="*50)
        print(f"Detailed report for {person_name} on 2025-12-10")
        print("="*50 + "\n")

        cursor = conn.execute("""
            SELECT *
            FROM time_reports
            WHERE person_id = %s
            AND date = %s
        """, (person_id, '2025-12-10'))

        report_10 = cursor.fetchall()
        if report_10:
            for report in report_10:
                print("Full report data:")
                for key, value in report.items():
                    print(f"  {key}: {value}")

        # Check if there's data in shift_time_segments
        print("\n" + "="*50)
        print("Checking shift_time_segments for this person...")
        print("="*50 + "\n")

        cursor = conn.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """, ('shift_time_segments',))

        columns = cursor.fetchall()
        print("Columns in shift_time_segments:")
        for col in columns:
            print(f"  {col['column_name']}: {col['data_type']}")

if __name__ == "__main__":
    check_avrigel_shifts()
