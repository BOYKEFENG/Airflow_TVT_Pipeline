import os
import requests, json, csv

API_KEY    = ""
SERIES_ID  = "LNS11300000"
START_YEAR = "1948"
END_YEAR   = "2025"

BASE_DIR = os.getenv('AIRFLOW_HOME', '/opt/airflow')
DATA_DIR = os.path.join(BASE_DIR, 'data')
OUTPUT_DIR = os.getenv('LFS_DIR', os.path.join(DATA_DIR, 'bls', 'labor_participation'))
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 1) Fetch from BLS via POST
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

# (optional) save the raw JSON
with open(os.path.join(OUTPUT_DIR, "lfs_participation_raw.json"), "w") as jf:
    json.dump(data, jf, indent=2)

# 2) Parse & sort
month_map = {
    "January":   1, "February":  2, "March":     3,
    "April":     4, "May":       5, "June":      6,
    "July":      7, "August":    8, "September": 9,
    "October":  10, "November": 11, "December": 12
}

series = data["Results"]["series"][0]["data"]
# sort by year, then month
series.sort(key=lambda r: (int(r["year"]), month_map[r["periodName"]]))

# 3) Write CSV
csv_path = os.path.join(OUTPUT_DIR, "lfs_participation_1948_2025.csv")
with open(csv_path, "w", newline="") as cf:
    writer = csv.writer(cf)
    writer.writerow([
        "Date",
        "Monthn",
        "Year",
        "Monthly Labor Participation Rate"
    ])
    for rec in series:
        year = int(rec["year"])
        m    = month_map[rec["periodName"]]
        val  = float(rec["value"])
        date = f"{year}-{m:02d}-01"
        writer.writerow([date, m, year, val])

print(f"Done. CSV written to:\n  {csv_path}")
