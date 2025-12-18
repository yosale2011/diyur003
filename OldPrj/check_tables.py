"""
Check all tables in the database
"""
from logic import get_db_connection
import psycopg2.extras

def check_tables():
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Get all tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        tables = cursor.fetchall()
        
        print(f"Found {len(tables)} tables:\n")
        for table in tables:
            table_name = table['table_name']
            
            # Get row count
            try:
                cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
                count = cursor.fetchone()['count']
                print(f"  {table_name}: {count} rows")
            except Exception as e:
                print(f"  {table_name}: Error - {e}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_tables()




