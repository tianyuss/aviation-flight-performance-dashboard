import streamlit as st
import pandas as pd
import sqlite3
from pathlib import Path

# ======================
# PAGE CONFIG
# ======================

st.set_page_config(
    page_title="Flight Performance Overview",
    layout="wide"
)

st.markdown(
    """
    <style>
        .block-container { padding-top: 2rem; }
        h1 { font-weight: 600; }

        .route-highlight {
            font-size: 16px;
            font-weight: 500;
            margin-bottom: 8px;
        }

        .disclaimer {
            font-size: 12px;
            color: rgba(255,255,255,0.6);
            margin-top: 6px;
        }

        /* Vertical separator for computed columns */
        div[data-testid="stDataFrame"] table th:nth-child(11),
        div[data-testid="stDataFrame"] table td:nth-child(11) {
            border-left: 2px solid rgba(255,255,255,0.2);
        }

    </style>
    """,
    unsafe_allow_html=True
)

# ======================
# DATABASE
# ======================

DB_PATH = Path("data/warehouse/YIA_DPS_master.db")

@st.cache_data
def load_data():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM final_flights", conn)
    conn.close()
    return df

df = load_data()

# ======================
# FILTER AIRLINE
# ======================

df = df[df["airline"] == "Super Air Jet"]

# ======================
# ROUTE FILTER
# ======================

st.sidebar.header("Filter")

route_options = ["YIA – DPS", "DPS – YIA"]

selected_route = st.sidebar.selectbox(
    "Route",
    route_options
)

origin_selected, dest_selected = selected_route.split(" – ")

df = df[
    (df["departure_airport"] == origin_selected) &
    (df["arrival_airport"] == dest_selected)
]

# ======================
# DATA PREP
# ======================

df["scheduled_departure"] = pd.to_datetime(df["scheduled_departure"], errors="coerce")
df = df.sort_values("scheduled_departure", ascending=False)
df = df.reset_index(drop=True)

# ======================
# KPI SECTION
# ======================

st.title("Flight Performance Overview")
st.markdown(
    f"<div class='route-highlight'>Airline: Super Air Jet | Route: {selected_route}</div>",
    unsafe_allow_html=True
)
st.divider()

col1, col2, col3 = st.columns(3)

col1.metric("Total Flights", len(df))

if "departure_delay_minutes_api" in df.columns:
    col2.metric(
        "Average Departure Delay (Minutes)",
        f"{df['departure_delay_minutes_api'].mean():.1f}"
    )
else:
    col2.metric("Average Departure Delay (Minutes)", "N/A")

if "arrival_delay_minutes_api" in df.columns:
    col3.metric(
        "Average Arrival Delay (Minutes)",
        f"{df['arrival_delay_minutes_api'].mean():.1f}"
    )
else:
    col3.metric("Average Arrival Delay (Minutes)", "N/A")

st.markdown(
    "<div class='disclaimer'>"
    "*Total flights are based on the latest available Aviationstack API snapshot stored in the data warehouse."
    "</div>",
    unsafe_allow_html=True
)

st.divider()

# ======================
# TABLE FORMAT
# ======================

table_df = df.copy()

def format_minutes(value):
    if pd.isna(value):
        return None
    return f"{int(value)} minutes"

# Format delay columns
table_df["Departure Delay (API)"] = table_df["departure_delay_minutes_api"].apply(format_minutes)
table_df["Arrival Delay (API)"] = table_df["arrival_delay_minutes_api"].apply(format_minutes)
table_df["Departure Delay (Computed)"] = table_df["departure_delay_minutes_computed"].apply(format_minutes)
table_df["Arrival Delay (Computed)"] = table_df["arrival_delay_minutes_computed"].apply(format_minutes)

# Rename clean headers
rename_map = {
    "flight_date": "Flight Date",
    "flight_status": "Status",
    "airline": "Airline",
    "flight_number": "Flight Number",
    "scheduled_departure": "Scheduled Departure",
    "actual_departure": "Actual Departure",
    "scheduled_arrival": "Scheduled Arrival",
    "actual_arrival": "Actual Arrival"
}

table_df = table_df.rename(columns=rename_map)

# Final column order
display_columns = [
    "Flight Date",
    "Status",
    "Airline",
    "Flight Number",
    "Scheduled Departure",
    "Actual Departure",
    "Departure Delay (API)",
    "Scheduled Arrival",
    "Actual Arrival",
    "Arrival Delay (API)",
    "Departure Delay (Computed)",
    "Arrival Delay (Computed)"
]

display_columns = [col for col in display_columns if col in table_df.columns]

# Make index start from 1
table_df = table_df[display_columns]
table_df.index = range(1, len(table_df) + 1)

st.subheader("Flight Records")

st.dataframe(
    table_df,
    use_container_width=True
)

st.markdown(
    "<div class='disclaimer'>"
    "*Computed delay columns are calculated as the difference between scheduled and actual timestamps. "
    "</div>",
    unsafe_allow_html=True
)

# ======================
# FOOTER
# ======================

st.markdown(
    "<div style='position: fixed; bottom: 12px; left: 320px; "
    "font-size: 11px; color: rgba(255,255,255,0.25);'>"
    "Flight Performance Overview | a portfolio by tianyus"
    "</div>",
    unsafe_allow_html=True
)
