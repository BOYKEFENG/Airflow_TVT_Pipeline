import os
import requests
import json
import csv

API_KEY    = "650ef0e97a2c48fa93c4deb91e7eb5a9"
SERIES_ID  = "LNS14000000"
START_YEAR = "1948"
END_YEAR   = "2025"

BASE_DIR = os.getenv('AIRFLOW_HOME', '/opt/airflow')
DATA_DIR = os.path.join(BASE_DIR, 'data')
OUTPUT_DIR = os.getenv('UNEMP_DIR', os.path.join(DATA_DIR, 'bls', 'unemployment'))
os.makedirs(OUTPUT_DIR, exist_ok=True)

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

# save raw JSON
json_path = os.path.join(OUTPUT_DIR, "unemployment_rate.json")
with open(json_path, "w") as f:
    json.dump(data, f, indent=2)

# month name → number
month_map = {
    "January": 1,  "February": 2,  "March":     3,
    "April":   4,  "May":      5,  "June":      6,
    "July":    7,  "August":   8,  "September": 9,
    "October":10,  "November":11,  "December": 12
}

# extract & sort
series_data = data["Results"]["series"][0]["data"]
series_data.sort(
    key=lambda rec: (
        int(rec["year"]),
        month_map[rec["periodName"]]
    )
)

# write CSV
csv_path = os.path.join(OUTPUT_DIR, "unemployment_rate.csv")
with open(csv_path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "Date",
        "Monthn",
        "Year",
        "Monthly Unemployment rate",
        "Monthly Unemployment in tenth"
    ])
    for rec in series_data:
        year = int(rec["year"])
        mnum = month_map[rec["periodName"]]
        rate = float(rec["value"])
        rate_tenth = int(round(rate * 10))
        date = f"{year}-{mnum:02d}-01"
        writer.writerow([date, mnum, year, rate, rate_tenth])

print(f"Done. Files written to:\n  {json_path}\n  {csv_path}")
