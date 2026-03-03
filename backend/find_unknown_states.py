import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

def find_unknown_states():
    # Database connection details from .env
    host = os.getenv('DB_HOST', '127.0.0.1')
    user = os.getenv('DB_USER', 'local_dashboard')
    password = os.getenv('DB_PASSWORD_PLAIN', 'darshit@1912')
    database = os.getenv('DB_NAME', 'local_dashboard')
    port = int(os.getenv('DB_PORT', 3306))

    connection = pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port,
        cursorclass=pymysql.cursors.DictCursor
    )

    try:
        with connection.cursor() as cursor:
            # Query to find where state is 'unknown' with their city
            sql = "SELECT DISTINCT city FROM g_map_master_table WHERE state = 'unknown' ORDER BY city"
            cursor.execute(sql)
            results = cursor.fetchall()

            count_sql = "SELECT COUNT(*) as total FROM g_map_master_table WHERE state = 'unknown'"
            cursor.execute(count_sql)
            total_count = cursor.fetchone()['total']

            if not results:
                print("No records found with state = 'unknown'.")
            else:
                output_file = "unknown_state_cities.txt"
                with open(output_file, "w", encoding="utf-8") as f:
                    f.write(f"Total entries with state = 'unknown': {total_count}\n")
                    f.write(f"Distinct cities with state = 'unknown': {len(results)}\n")
                    f.write("-" * 50 + "\n")
                    for row in results:
                        f.write(f"{row['city']}\n")
                
                print(f"Summary:")
                print(f"- Total records with state='unknown': {total_count}")
                print(f"- Total distinct cities: {len(results)}")
                print(f"- Full list saved to: {output_file}")
                
                print("\nSample Cities:")
                for row in results[:10]:
                    print(f"- {row['city']}")
                print("...")

    finally:
        connection.close()

if __name__ == "__main__":
    find_unknown_states()
