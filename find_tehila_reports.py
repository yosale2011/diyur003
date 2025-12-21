"""
Find Tehila's reports to compare with Avigel
"""
from database import get_conn

with get_conn() as conn:
    # Find Tehila
    cursor = conn.execute("""
        SELECT id, name, email FROM people
        WHERE name LIKE %s
    """, ('%תהילה%',))

    people = cursor.fetchall()
    print("People matching 'תהילה':")
    for person in people:
        print(f"  ID: {person['id']}, Name: {person['name']}, Email: {person.get('email')}")

    if not people:
        print("No one found. Let's search for 'אהרנוביץ'")
        cursor = conn.execute("""
            SELECT id, name, email FROM people
            WHERE name LIKE %s
        """, ('%אהרנוביץ%',))
        people = cursor.fetchall()
        for person in people:
            print(f"  ID: {person['id']}, Name: {person['name']}, Email: {person.get('email')}")

    # Use the one with email tehila19090@gmail.com
    person_id = 231
    if people:
        print(f"\nUsing person ID: {person_id}")

        # Find all reports
        cursor = conn.execute("""
            SELECT
                DATE_TRUNC('month', date) as month,
                COUNT(*) as report_count
            FROM time_reports
            WHERE person_id = %s
            GROUP BY DATE_TRUNC('month', date)
            ORDER BY month DESC
        """, (person_id,))

        months = cursor.fetchall()
        print(f"\nMonths with reports:")
        for month in months:
            print(f"  {month['month']}: {month['report_count']} reports")

        # Get some sample reports
        cursor = conn.execute("""
            SELECT tr.*, st.name as shift_name
            FROM time_reports tr
            LEFT JOIN shift_types st ON tr.shift_type_id = st.id
            WHERE tr.person_id = %s
            ORDER BY tr.date DESC
            LIMIT 10
        """, (person_id,))

        reports = cursor.fetchall()
        print(f"\nRecent reports:")
        for r in reports:
            print(f"  {r['date']}: {r['start_time']}-{r['end_time']}, Shift: {r['shift_name']} (ID: {r['shift_type_id']})")
