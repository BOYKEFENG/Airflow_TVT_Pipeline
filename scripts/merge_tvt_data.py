# Moving 12 Month VMT Data Merger
# This script merges TVT data from multiple Excel files into a single CSV file.
# It reads data from files named like '19apr t vt .xlsx', processes the data, and saves it to a CSV file.
# To run this script, ensure you have the required libraries installed:
# pip install pandas openpyxl xlrd numpy

import os
import re
import pandas as pd
import logging
from collections import defaultdict
from datetime import datetime
import numpy as np

BASE_DIR = os.getenv('AIRFLOW_HOME', '/opt/airflow')
DATA_DIR = os.path.join(BASE_DIR, 'data')
INPUT_DIR = os.getenv('TVT_RAW_DIR', os.path.join(DATA_DIR, 'tvt', 'raw'))
PROCESSED_DIR = os.getenv('TVT_PROC_DIR', os.path.join(DATA_DIR, 'tvt', 'processed'))
os.makedirs(PROCESSED_DIR, exist_ok=True)
OUTPUT_CSV = os.getenv('NATIONAL_VMT_CSV', os.path.join(PROCESSED_DIR, 'merged_tvt_data.csv'))

# Regex to identify files named like '19apr t vt .xlsx':
#   - yy  = two-digit report year (19–25)
#   - mon = month code (jan, feb, mar, …, sept, …)
FNAME_RE = re.compile(
    r'(?P<yy>\d{2})(?P<mon>[a-z]+)tvt\.xlsx$',
    re.IGNORECASE
)

# Map the month substring to a month number
MONTH_MAP = {
    'jan':  1, 'feb':  2, 'mar':  3, 'apr':  4,
    'may':  5, 'jun':  6, 'jul':  7, 'aug':  8,
    'sep':  9, 'oct': 10, 'nov': 11, 'dec': 12
}

merged: dict[str, dict[str, float]] = {}

def read_excel_data(path: str, year: int) -> pd.DataFrame:
    """Read Excel file based on year-specific format."""
    try:
        if year == 2002:
            df = pd.read_excel(
                path,
                sheet_name='Trend',
                header=None,
                usecols='D:F',
                skiprows=15,  # start from row 16
                nrows=33,     # read until row 48
                names=['year_record', 'tmonth', 'yearToDate'],
                engine='openpyxl'
            )
            df['moving'] = float('nan')  # Add moving column as NaN
            
        elif year == 2003:
            month = os.path.basename(path)[2:5].lower()  # Extract month from filename
            if month in ['may', 'jun']:
                skiprows = 14  # start from row 15
                nrows = 34     # read until row 48
            else:
                skiprows = 15  # start from row 16
                nrows = 34     # read until row 49
                
            df = pd.read_excel(
                path,
                sheet_name='Trend',
                header=None,
                usecols='D:F',
                skiprows=skiprows,
                nrows=nrows,
                names=['year_record', 'tmonth', 'yearToDate'],
                engine='openpyxl'
            )
            df['moving'] = float('nan')  # Add moving column as NaN
            
        elif year == 2004:
            month = os.path.basename(path)[2:5].lower()
            if month in ['apr', 'feb', 'jan', 'mar', 'may']:
                df = pd.read_excel(
                    path,
                    sheet_name='Page 2',
                    header=None,
                    usecols=[2,4,6,8],  # C,E,G,I columns
                    skiprows=21,  # start from row 22
                    nrows=26,     # read until row 47
                    names=['year_record', 'tmonth', 'yearToDate', 'moving'],
                    engine='openpyxl'
                )
            elif month == 'aug':
                df = pd.read_excel(
                    path,
                    sheet_name='Page 2',
                    header=None,
                    usecols='C:F',
                    skiprows=24,  # start from row 25
                    nrows=26,     # read until row 50
                    names=['year_record', 'tmonth', 'yearToDate', 'moving'],
                    engine='openpyxl'
                )
            elif month in ['dec', 'nov', 'oct', 'sep']:
                df = pd.read_excel(
                    path,
                    sheet_name='Page 2',
                    header=None,
                    usecols='E:H',
                    skiprows=24,  # start from row 25
                    nrows=26,     # read until row 50
                    names=['year_record', 'tmonth', 'yearToDate', 'moving'],
                    engine='openpyxl'
                )
            else:  # jul, jun
                df = pd.read_excel(
                    path,
                    sheet_name='Page 2',
                    header=None,
                    usecols='C:F',
                    skiprows=18,  # start from row 19
                    nrows=26,     # read until row 44
                    names=['year_record', 'tmonth', 'yearToDate', 'moving'],
                    engine='openpyxl'
                )

        elif year == 2005:
            month = os.path.basename(path)[2:5].lower()
            if month == 'apr':
                skiprows = 25  # start from row 26
                nrows = 26     # read until row 51
            else:
                skiprows = 24  # start from row 25
                nrows = 26     # read until row 50
                
            df = pd.read_excel(
                path,
                sheet_name='Page 2',
                header=None,
                usecols='E:H',
                skiprows=skiprows,
                nrows=nrows,
                names=['year_record', 'tmonth', 'yearToDate', 'moving'],
                engine='openpyxl'
            )

        elif year == 2006:
            month = os.path.basename(path)[2:5].lower()
            if month == 'dec':
                skiprows = 15  # start from row 16
                nrows = 26     # read until row 41
            else:
                skiprows = 24  # start from row 25
                nrows = 26     # read until row 50
                
            df = pd.read_excel(
                path,
                sheet_name='Page 2',
                header=None,
                usecols='E:H',
                skiprows=skiprows,
                nrows=nrows,
                names=['year_record', 'tmonth', 'yearToDate', 'moving'],
                engine='openpyxl'
            )

        elif year == 2007:
            month = os.path.basename(path)[2:5].lower()
            if month == 'jan':
                skiprows = 16  # start from row 17
                nrows = 26     # read until row 42
            else:
                skiprows = 17  # start from row 18
                nrows = 26     # read until row 43
                
            df = pd.read_excel(
                path,
                sheet_name='Page 2',
                header=None,
                usecols='E:H',
                skiprows=skiprows,
                nrows=nrows,
                names=['year_record', 'tmonth', 'yearToDate', 'moving'],
                engine='openpyxl'
            )

        else:  # 2008-2025
            df = pd.read_excel(
                path,
                sheet_name='Data',
                header=None,
                usecols='A:D',
                skiprows=8,  # start from row 9
                nrows=26,    # read until row 34
                names=['year_record', 'tmonth', 'yearToDate', 'moving'],
                engine='openpyxl'
            )

        # Clean up and convert numeric columns
        df = df[pd.to_numeric(df['year_record'], errors='coerce').notna()]
        df['year_record'] = df['year_record'].astype(float).astype(int)
        df['tmonth'] = pd.to_numeric(df['tmonth'], errors='coerce')
        df['yearToDate'] = pd.to_numeric(df['yearToDate'], errors='coerce')
        if 'moving' in df.columns:
            df['moving'] = pd.to_numeric(df['moving'], errors='coerce')
        
        return df.dropna(subset=['year_record'])
        
    except Exception as e:
        raise RuntimeError(f"Failed to read Excel file '{path}' for year {year}: {e}")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Error tracking
stats = {
    'total_files': 0,
    'processed': 0,
    'errors': defaultdict(list)
}

# Update the year range check in the main loop
for fn in os.listdir(INPUT_DIR):
    stats['total_files'] += 1
    m = FNAME_RE.match(fn)
    if not m:
        stats['errors']['filename_format'].append(fn)
        continue

    yy = int(m.group('yy'))
    if not (2 <= yy <= 25):
        stats['errors']['year_range'].append(fn)
        continue
    
    # Convert two-digit year to full year
    full_year = 2000 + yy

    # Get month from filename and convert to number
    mon_code = m.group('mon').lower()
    mon = MONTH_MAP.get(mon_code)
    if mon is None:
        stats['errors']['month_code'].append(fn)
        continue

    path = os.path.join(INPUT_DIR, fn)
    try:
        df = read_excel_data(path, full_year)
        stats['processed'] += 1
        
        # Iterate over each row (each historic year) and update our merged dict
        for _, row in df.iterrows():
            # Skip rows with invalid numeric data
            if pd.isna(row['year_record']) or pd.isna(row['tmonth']):
                continue
                
            year_rec = int(row['year_record'])
            total_vmt = float(row['tmonth'])
            ytd_vmt = float(row['yearToDate']) if not pd.isna(row['yearToDate']) else float('nan')
            mov_vmt = float(row['moving']) if not pd.isna(row['moving']) else float('nan')

            key = f'{year_rec}-{mon:02d}'  # e.g. "2000-03" for March 2000
            merged[key] = {
                'total_vmt': total_vmt,
                'yearToDate': ytd_vmt,
                'moving': mov_vmt
            }
    except Exception as e:
        stats['errors']['read_error'].append((fn, str(e)))
        continue

# Convert merged dict into a DataFrame
out_df = pd.DataFrame.from_dict(
    merged,
    orient = 'index'
).rename_axis('year_month').reset_index()

# Split 'year_month' into separate 'Year' and 'Month_Num' columns
out_df[['Year', 'Month_Num']] = out_df['year_month'].str.split('-', expand=True).astype(int)


out_df['Date_TS'] = pd.to_datetime(
    out_df['Year'].astype(str) + '-' +
    out_df['Month_Num'].astype(str).str.zfill(2) + '-01'
)

out_df['Date'] = out_df['Date_TS'].dt.strftime('%#m/%#d/%Y')   # e.g. '4/1/1970'
out_df['Month'] = out_df['Date_TS'].dt.strftime('%b')          # e.g. 'Apr'
out_df['Year']  = out_df['Date_TS'].dt.year                    # e.g. 1970

# Sort by date to ensure correct calculations
out_df = out_df.sort_values('Date_TS')

# Calculate monthly change rate using 'moving' instead of 'total_vmt'
out_df['VMT_Change_Rate_Monthly'] = (
    100 * (out_df['moving'] - out_df['moving'].shift(1)) / 
    out_df['moving'].shift(1)
).round(2)

# Calculate annual change rate using 'moving' instead of 'total_vmt'
out_df['VMT_Change_Rate_Annually'] = (
    100 * (out_df['moving'] - out_df['moving'].shift(12)) / 
    out_df['moving'].shift(12)
).round(2)

# Replace NaN values with empty string for better CSV output
out_df['VMT_Change_Rate_Monthly'] = out_df['VMT_Change_Rate_Monthly'].replace({pd.NA: '', np.nan: ''})
out_df['VMT_Change_Rate_Annually'] = out_df['VMT_Change_Rate_Annually'].replace({pd.NA: '', np.nan: ''})

# Update final_df creation to include new columns
final_df = out_df.loc[:, [
    'Date',
    'Month',
    'Year',
    'total_vmt',
    'yearToDate',
    'moving',
    'VMT_Change_Rate_Monthly',
    'VMT_Change_Rate_Annually'
]].rename(columns={
    'total_vmt': 'Total VMT (Million)',
    'yearToDate': 'Year To Date VMT (Million)',
    'moving': 'Moving 12-Month VMT (Million)',
    'VMT_Change_Rate_Monthly': 'VMT Change Rate(Monthly)',
    'VMT_Change_Rate_Annually': 'VMT Change Rate (Annually)'
})

# Summary report
print("\nProcessing Summary:")
print(f"Total files found: {stats['total_files']}")
print(f"Successfully processed: {stats['processed']}")
if stats['errors']:
    print("\nErrors encountered:")
    for error_type, files in stats['errors'].items():
        print(f"\n{error_type}:")
        for f in files:
            if isinstance(f, tuple):
                print(f"  {f[0]}: {f[1]}")
            else:
                print(f"  {f}")

print(f"\nExpected files:") 
for year in range(2002, 2026):
    yy = str(year)[2:]
    for month in MONTH_MAP.keys():
        expected = f"{yy}{month}tvt.xlsx"
        if not any(f.lower().startswith(f"{yy}{month}".lower()) for f in os.listdir(INPUT_DIR)):
            print(f"  Missing: {expected}")

# Sort by Date from oldest to newest
final_df['Date'] = pd.to_datetime(final_df['Date'])
final_df = final_df.sort_values('Date')
final_df['Date'] = final_df['Date'].dt.strftime('%#m/%#d/%Y')

# Save to CSV
final_df.to_csv(OUTPUT_CSV, index=False)
print(f'✅ Merged {len(final_df)} rows → {OUTPUT_CSV}')
