# This script merges various economic and transportation datasets into a single DataFrame.

import os
import pandas as pd

BASE_DIR = os.getenv('AIRFLOW_HOME', '/opt/airflow')
DATA_DIR = os.path.join(BASE_DIR, 'data')

file_paths = {
    'state_miles': os.getenv('STATE_MILES_CSV',
        os.path.join(DATA_DIR, 'tvt', 'processed', 'merged_tvt_state_miles.csv')
    ),
    'tvt_data': os.getenv('NATIONAL_VMT_CSV',
        os.path.join(DATA_DIR, 'tvt', 'processed', 'merged_tvt_data.csv')
    ),
    'gdp': os.path.join(DATA_DIR, 'bea', 'gdp', 'gdp_current_bil.csv'),
    'lfs': os.path.join(DATA_DIR, 'bls', 'labor_participation', 'lfs_participation_1948_2025.csv'),
    'cpiu': os.path.join(DATA_DIR, 'bls', 'cpi', 'cpiu_1913_2025.csv'),
    'unemp': os.path.join(DATA_DIR, 'bls', 'unemployment', 'unemployment_rate.csv')
}

# Helper for GDP date parsing
def parse_gdp_date(s):
    dt = pd.to_datetime(s, format='%b %y', errors='coerce')
    if pd.notnull(dt) and dt.year > pd.Timestamp.now().year:
        dt = dt.replace(year=dt.year - 100)
    return dt

# Load DataFrames
dfs = {}

# State-level VMT data
df_state = pd.read_csv(file_paths['state_miles'], parse_dates=['Date'])
# Drop Month/Year here; we'll derive later
df_state = df_state.drop(columns=['Month', 'Year'])
dfs['state_miles'] = df_state

# TVT national data
df_tvt = pd.read_csv(file_paths['tvt_data'], parse_dates=['Date'])
df_tvt = df_tvt.drop(columns=['Month', 'Year'], errors='ignore')
dfs['tvt_data'] = df_tvt

# GDP
df_gdp = pd.read_csv(file_paths['gdp'])
df_gdp['Date'] = df_gdp['Date'].apply(parse_gdp_date)
df_gdp = df_gdp.drop(columns=['Quarter'], errors='ignore')
dfs['gdp'] = df_gdp

# Labor force participation
df_lfs = pd.read_csv(file_paths['lfs'], parse_dates=['Date'], dayfirst=False)
df_lfs = df_lfs.drop(columns=['Monthn', 'Year'], errors='ignore')
dfs['lfs'] = df_lfs

# CPI-U
df_cpiu = pd.read_csv(file_paths['cpiu'], parse_dates=['Date'], dayfirst=False)
df_cpiu = df_cpiu.drop(columns=['Monthn', 'Year'], errors='ignore')
dfs['cpiu'] = df_cpiu

# Unemployment rate
df_unemp = pd.read_csv(file_paths['unemp'], parse_dates=['Date'], dayfirst=False)
df_unemp = df_unemp.drop(columns=['Monthn', 'Year'], errors='ignore')
dfs['unemp'] = df_unemp

# Perform full outer merge
merged = None
for name, df in dfs.items():
    if merged is None:
        merged = df
    else:
        # Determine merge keys
        keys = ['Date']
        # If both have 'State', merge on State too
        if 'State' in merged.columns and 'State' in df.columns:
            keys.append('State')
        merged = merged.merge(df, on=keys, how='outer')

# Derive Month and Year from Date
merged['Month'] = merged['Date'].dt.strftime('%b')
merged['Year'] = merged['Date'].dt.year

# Reorder columns: Date, Month, Year, State, then others
cols = ['Date', 'Month', 'Year', 'State'] + [c for c in merged.columns if c not in ['Date', 'Month', 'Year', 'State']]
merged = merged[cols]

# Fill numeric NaNs with 0
num_cols = merged.select_dtypes(include='number').columns
merged[num_cols] = merged[num_cols].fillna(0)

# Sort by Date, then State
merged.sort_values(['Date', 'State'], inplace=True)

# Save to CSV
OUTPUT_DIR = os.getenv('DB_DIR', os.path.join(DATA_DIR, 'tvt', 'processed'))
os.makedirs(OUTPUT_DIR, exist_ok=True)
output_path = os.path.join(OUTPUT_DIR, 'merged_db.csv')
merged.to_csv(output_path, index=False)

print(f"Database CSV written to: {output_path}")
