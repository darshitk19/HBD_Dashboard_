import pymysql
import os
import re
from dotenv import load_dotenv

load_dotenv()

def extract_states_from_city():
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

    # List of Indian states to look for
    indian_states = [
        "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh", 
        "Goa", "Gujarat", "Haryana", "Himachal Pradesh", "Jharkhand", 
        "Karnataka", "Kerala", "Madhya Pradesh", "Maharashtra", "Manipur", 
        "Meghalaya", "Mizoram", "Nagaland", "Odisha", "Punjab", 
        "Rajasthan", "Sikkim", "Tamil Nadu", "Telangana", "Tripura", 
        "Uttar Pradesh", "Uttarakhand", "West Bengal"
    ]

    try:
        with connection.cursor() as cursor:
            # Query the cities where state is unknown
            sql = "SELECT DISTINCT city FROM g_map_master_table WHERE state = 'unknown'"
            cursor.execute(sql)
            results = cursor.fetchall()

            if not results:
                print("No records found with state = 'unknown'.")
                return

            extracted_pairs = []
            
            for row in results:
                city_val = str(row['city'])
                found_state = None
                for state in indian_states:
                    if state.lower() in city_val.lower():
                        found_state = state
                        break
                
                if found_state:
                    extracted_pairs.append({
                        "original_city": city_val,
                        "detected_state": found_state
                    })

            if not extracted_pairs:
                print("No states detected within the city column values.")
            else:
                summary_file = "detected_states_summary.txt"
                with open(summary_file, "w", encoding="utf-8") as f:
                    f.write(f"Detected {len(extracted_pairs)} city entries containing state names\n")
                    f.write("-" * 60 + "\n")
                    f.write(f"{'City Value':<40} | {'Detected State':<20}\n")
                    f.write("-" * 60 + "\n")
                    for pair in extracted_pairs:
                        f.write(f"{pair['original_city']:<40} | {pair['detected_state']:<20}\n")
                
                print(f"Summary of States detected in City Column:")
                unique_states = sorted(list(set(p['detected_state'] for p in extracted_pairs)))
                for state in unique_states:
                    cities_for_state = [p['original_city'] for p in extracted_pairs if p['detected_state'] == state]
                    print(f"- {state}: {len(cities_for_state)} city variations")
                
                print(f"\nTotal Detected Pairs: {len(extracted_pairs)}")
                print(f"Full list saved to: {summary_file}")

    finally:
        connection.close()

if __name__ == "__main__":
    extract_states_from_city()
