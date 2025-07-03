import os
import requests
import pandas as pd

# 1) SETUP
BEA_API_KEY = ""
BASE_URL    = "https://apps.bea.gov/api/data/"

BASE_DIR = os.getenv('AIRFLOW_HOME', '/opt/airflow')
DATA_DIR = os.path.join(BASE_DIR, 'data')
OUTPUT_DIR = os.getenv('GDP_DIR', os.path.join(DATA_DIR, 'bea', 'gdp'))
os.makedirs(OUTPUT_DIR, exist_ok=True)
CSV_PATH = os.path.join(OUTPUT_DIR, "gdp_current_bil.csv")

# 2) FETCH the raw data
params = {
    "UserID":       BEA_API_KEY,
    "method":       "GetData",
    "DataSetName":  "NIPA",
    "TableName":    "T10105",    # Gross Domestic Product, current dollars (millions)
    "Frequency":    "Q",         # quarterly
    "Year":         "ALL",       # all years
    "ResultFormat": "JSON"
}
resp = requests.get(BASE_URL, params=params)
resp.raise_for_status()
raw = resp.json()["BEAAPI"]["Results"]["Data"]

# 3) LOAD into a DataFrame
df = pd.DataFrame(raw)

# 4) DROP duplicate quarters
df = df.drop_duplicates(subset="TimePeriod", keep="first")

# 5) CLEAN & TRANSFORM
# strip commas and cast to float
df["Millions"] = (
    df["DataValue"]
      .str.replace(",", "")
      .astype(float)
)

# convert to billions & round to one decimal
df["Billions"] = (df["Millions"] / 1000).round(1)

# parse Year and Quarter
df["Year"]    = df["TimePeriod"].str[:4].astype(int)
df["QtrNum"]  = df["TimePeriod"].str[-1].astype(int)
df["Quarter"] = df["Year"].astype(str) + " Q" + df["QtrNum"].astype(str)

# map quarter to month abbrev and two-digit year
q2mon = {1: "Jan", 2: "Apr", 3: "Jul", 4: "Oct"}
df["MonAbbr"] = df["QtrNum"].map(q2mon)
df["YY"]      = df["Year"] % 100
df["Date"]    = df["MonAbbr"] + " " + df["YY"].apply(lambda x: f"{x:02d}")

# 6) SELECT & WRITE the final table
final = df[["Quarter", "Date", "Billions"]]
final.columns = [
    "Quarter",
    "Date",
    "GDP in billions of current dollars"
]

final.to_csv(CSV_PATH, index=False)
print(f"Done!  CSV written to: {CSV_PATH}")
