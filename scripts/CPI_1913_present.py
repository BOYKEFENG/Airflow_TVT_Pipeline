import os
import requests
import json
import pandas as pd

API_KEY    = ""
SERIES_ID  = "CUUR0000SA0"
START_YEAR = "1913"
END_YEAR   = "2025"

# Use Airflow's data directory inside the container
BASE_DIR = os.getenv('AIRFLOW_HOME', '/opt/airflow')
DATA_DIR = os.path.join(BASE_DIR, 'data')
OUTPUT_DIR = os.getenv('CPI_DIR', os.path.join(DATA_DIR, 'bls', 'cpi'))
os.makedirs(OUTPUT_DIR, exist_ok=True)
CSV_PATH = os.path.join(OUTPUT_DIR, "cpiu_1913_2025.csv")

# 1) Fetch from BLS
payload = {
    "seriesid": [SERIES_ID],
    "startyear": START_YEAR,
    "endyear": END_YEAR,
    "registrationKey": API_KEY
}
resp = requests.post(
    "https://api.bls.gov/publicAPI/v2/timeseries/data/",
    json=payload,
    headers={"Content-Type": "application/json"}
)
resp.raise_for_status()
data = resp.json()

# (Optional) save raw JSON
with open(os.path.join(OUTPUT_DIR, "cpiu_raw.json"), "w") as f:
    json.dump(data, f, indent=2)

# 2) Parse & sort the data
month_map = {
    "January": 1,  "February": 2,  "March":     3,
    "April":   4,  "May":      5,  "June":      6,
    "July":    7,  "August":   8,  "September": 9,
    "October":10,  "November":11,  "December": 12
}

series = data["Results"]["series"][0]["data"]
# sort ascending year → month
series.sort(key=lambda r: (int(r["year"]), month_map[r["periodName"]]))

# Build DataFrame
records = []
for rec in series:
    y = int(rec["year"])
    m = month_map[rec["periodName"]]
    cpi = float(rec["value"])
    records.append({"year": y, "month": m, "cpi": cpi})

df = pd.DataFrame(records)
df["Date"] = pd.to_datetime(df[["year", "month"]].assign(day=1))
df = df.sort_values("Date")

# Compute inflation vs. same month last year
df["Inflation"] = df["cpi"].pct_change(periods=12) * 100

# Format columns
df["Monthn"] = df["month"]
df["Year"] = df["year"]
df["CPI"] = df["cpi"]

final = df[["Date", "Monthn", "Year", "CPI", "Inflation"]]
final.to_csv(CSV_PATH, index=False)
print(f"Done! CSV written to: {CSV_PATH}")
