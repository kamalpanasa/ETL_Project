import os
import time
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv

# Initialize the Supabase
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("SUPABASE_URL or SUPABASE_KEY is not set in .env")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def load_to_supabase():

    # Load cleaned CSV
    csv_path = "../data/staged/weather_cleaned.csv"
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Missing file : {csv_path}")
    
    df = pd.read_csv(csv_path)

    # Make sure the column name matches Postgres (temperature_c)
    # In Postgres: temperature_C (unquoted) becomes temperature_c
    if "temperature_C" in df.columns:
        df = df.rename(columns={"temperature_C": "temperature_c"})
    
    # Convert timestamps ---> strings (ISO format)
    df["time"] = pd.to_datetime(df["time"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
    df["extracted_at"] = pd.to_datetime(df["extracted_at"]).dt.strftime("%Y-%m-%dT%H:%M:%S")

    # Ensure city exists (default if missing)
    if "city" not in df.columns:
        df["city"] = "Hyderabad"

    batch_size = 20

    for i in range(0, len(df), batch_size):
        batch_df = df.iloc[i:i + batch_size]

        # Replace NaN with None so we can treat them as NULL
        batch_df = batch_df.where(pd.notnull(batch_df), None)
        batch = batch_df.to_dict("records")

        # Build VALUES list for INSERT
        values_list = []
        for r in batch:
            # Numbers or NULL
            temp = "NULL" if r.get("temperature_c") is None else str(r["temperature_c"])
            humi = "NULL" if r.get("humidity_percent") is None else str(r["humidity_percent"])
            wind = "NULL" if r.get("wind_speed_kmph") is None else str(r["wind_speed_kmph"])

            city = r.get("city", "Hyderabad") or "Hyderabad"
            city = str(city).replace("'", "''")

            time_str = r["time"]
            extracted_at_str = r["extracted_at"]

            values_list.append(
                f"('{time_str}'::timestamptz, {temp}, {humi}, {wind}, '{city}', '{extracted_at_str}'::timestamptz)"
            )

        if not values_list:
            continue

        insert_sql = f"""
            INSERT INTO weather_data
                (time, temperature_c, humidity_percent, wind_speed_kmph, city, extracted_at)
            VALUES {", ".join(values_list)};
        """

        response = supabase.rpc("execute_sql", {"query": insert_sql}).execute()

        if getattr(response, "error", None):
            print("Error from execute_sql RPC:", response.error)
            break

        print(f"Inserted rows {i + 1} --- {min(i + batch_size, len(df))}")
        time.sleep(0.5)

    print("Finished Loading Weather Data")


if __name__ == "__main__":
    load_to_supabase()
