import os
import requests
import pandas as pd
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("AVIATIONSTACK_API_KEY")

if not API_KEY:
    raise ValueError("API key not found.")

BASE_URL = "http://api.aviationstack.com/v1/flights"


def fetch_snapshot(filter_type, iata_code):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    params = {
        "access_key": API_KEY,
        f"{filter_type}_iata": iata_code,
        "limit": 100
    }

    response = requests.get(BASE_URL, params=params)

    if response.status_code == 429:
        print("Quota exceeded. Stopping execution.")
        return None

    if response.status_code != 200:
        print("Request failed:", response.status_code)
        print(response.text)
        return None

    data = response.json()

    # Save RAW JSON
    raw_filename = f"data/raw/{timestamp}_{filter_type}_{iata_code}.json"
    with open(raw_filename, "w") as f:
        json.dump(data, f, indent=4)

    records = data.get("data", [])

    structured_data = []
    for flight in records:
        structured_data.append({
            "snapshot_time": timestamp,
            "flight_date": flight.get("flight_date"),
            "flight_status": flight.get("flight_status"),
            "airline": flight.get("airline", {}).get("name"),
            "flight_number": flight.get("flight", {}).get("iata"),
            "departure_airport": flight.get("departure", {}).get("iata"),
            "arrival_airport": flight.get("arrival", {}).get("iata"),
            "scheduled_departure": flight.get("departure", {}).get("scheduled"),
            "actual_departure": flight.get("departure", {}).get("actual"),
            "departure_delay_minutes": flight.get("departure", {}).get("delay"),
            "scheduled_arrival": flight.get("arrival", {}).get("scheduled"),
            "actual_arrival": flight.get("arrival", {}).get("actual"),
            "arrival_delay_minutes": flight.get("arrival", {}).get("delay")
        })

    df = pd.DataFrame(structured_data)

    csv_filename = f"data/processed/{timestamp}_{filter_type}_{iata_code}.csv"
    df.to_csv(csv_filename, index=False)

    print(f"Saved JSON: {raw_filename}")
    print(f"Saved CSV: {csv_filename}")
    print(f"Records: {len(df)}")
    print("----")

    return df


if __name__ == "__main__":
    print("Starting snapshot process...")

    df_dep = fetch_snapshot("dep", "YIA")
    if df_dep is None:
        exit()

    df_arr = fetch_snapshot("arr", "YIA")
    if df_arr is None:
        exit()

    # Combine both
    combined_df = pd.concat([df_dep, df_arr], ignore_index=True)

    # Corridor filter YIA <-> DPS
    corridor_df = combined_df[
        ((combined_df["departure_airport"] == "YIA") & (combined_df["arrival_airport"] == "DPS")) |
        ((combined_df["departure_airport"] == "DPS") & (combined_df["arrival_airport"] == "YIA"))
    ]

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    corridor_filename = f"data/processed/{timestamp}_YIA_DPS_corridor.csv"
    corridor_df.to_csv(corridor_filename, index=False)

    print(f"Saved Corridor CSV: {corridor_filename}")
    print("Snapshot completed successfully.")