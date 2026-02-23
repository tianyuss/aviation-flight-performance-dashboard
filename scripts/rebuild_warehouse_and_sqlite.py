import pandas as pd
import glob
import os
import sqlite3

PROCESSED_PATH = "data/processed/"
WAREHOUSE_PATH = "data/warehouse/"

MASTER_FILE = os.path.join(WAREHOUSE_PATH, "YIA_DPS_master.csv")
FINAL_FILE = os.path.join(WAREHOUSE_PATH, "YIA_DPS_final_state.csv")
DB_PATH = os.path.join(WAREHOUSE_PATH, "YIA_DPS_master.db")

os.makedirs(WAREHOUSE_PATH, exist_ok=True)

corridor_files = sorted(
    glob.glob(os.path.join(PROCESSED_PATH, "*_YIA_DPS_corridor.csv"))
)

if not corridor_files:
    raise FileNotFoundError("No corridor files found.")

df_list = [pd.read_csv(file) for file in corridor_files]
master_df = pd.concat(df_list, ignore_index=True)

master_df.to_csv(MASTER_FILE, index=False)

df = master_df.copy()

df.rename(columns={
    "departure_delay_minutes": "departure_delay_minutes_api",
    "arrival_delay_minutes": "arrival_delay_minutes_api"
}, inplace=True)

for col in [
    "scheduled_departure",
    "actual_departure",
    "scheduled_arrival",
    "actual_arrival",
]:
    df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

def compute_local_date(row):
    ts = row["scheduled_departure"]
    if pd.isna(ts):
        return pd.NaT

    if row["departure_airport"] == "YIA":
        local_ts = ts.tz_convert("Asia/Jakarta")
    elif row["departure_airport"] == "DPS":
        local_ts = ts.tz_convert("Asia/Makassar")
    else:
        local_ts = ts

    return local_ts.date()

df["flight_date_local_computed"] = df.apply(compute_local_date, axis=1)

df["departure_delay_minutes_computed"] = (
    (df["actual_departure"] - df["scheduled_departure"])
    .dt.total_seconds() / 60
)

df["arrival_delay_minutes_computed"] = (
    (df["actual_arrival"] - df["scheduled_arrival"])
    .dt.total_seconds() / 60
)

df.loc[df["actual_departure"].isna(), "departure_delay_minutes_computed"] = None
df.loc[df["actual_arrival"].isna(), "arrival_delay_minutes_computed"] = None

df["snapshot_time"] = df["snapshot_time"].astype(str)

group_cols = [
    "flight_number",
    "departure_airport",
    "arrival_airport",
    "flight_date_local_computed"
]

df = df.sort_values("snapshot_time")

df_latest = df.loc[
    df.groupby(group_cols, dropna=False)["snapshot_time"].idxmax()
]

df_final = df_latest[
    df_latest["flight_status"] == "landed"
]

df_final.to_csv(FINAL_FILE, index=False)

if os.path.exists(DB_PATH):
    os.remove(DB_PATH)

conn = sqlite3.connect(DB_PATH)
df_final.to_sql("final_flights", conn, if_exists="replace", index=False)
conn.close()