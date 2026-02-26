import requests
import json

base_url = "http://localhost:8001/api"

def check_api(endpoint):
    try:
        r = requests.get(f"{base_url}/{endpoint}")
        print(f"\n--- Testing {endpoint} ---")
        print(f"Status Code: {r.status_code}")
        data = r.json()
        print(f"Data Body: {json.dumps(data, indent=2)[:500]}...")
    except Exception as e:
        print(f"Error testing {endpoint}: {e}")

check_api("raw-cleaned-cities")
check_api("raw-cleaned-stats")
