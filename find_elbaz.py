import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

def find_person():
    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    cursor.execute("SELECT id, name FROM people WHERE name LIKE '%אלבז%'")
    people = cursor.fetchall()
    for p in people:
        print(f"ID: {p['id']}, Name: {p['name']}")
    
    conn.close()

if __name__ == "__main__":
    find_person()

