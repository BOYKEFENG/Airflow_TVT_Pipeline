# This script merges state mileage data from multiple Excel files into a single CSV file.
# It reads state mileage data from TVT Excel files, processes them according to the year and month, and merges the data into a single CSV file.
# To run this script, ensure you have the required libraries installed:
# pip install pandas openpyxl tqdm

import os
import re
import pandas as pd
import logging
from collections import defaultdict
from datetime import datetime
from tqdm import tqdm

BASE_DIR = os.getenv('AIRFLOW_HOME', '/opt/airflow')
DATA_DIR = os.path.join(BASE_DIR, 'data')
INPUT_DIR = os.getenv('TVT_RAW_DIR', os.path.join(DATA_DIR, 'tvt', 'raw'))
PROCESSED_DIR = os.getenv('TVT_PROC_DIR', os.path.join(DATA_DIR, 'tvt', 'processed'))
os.makedirs(PROCESSED_DIR, exist_ok=True)
OUTPUT_CSV = os.getenv('STATE_MILES_CSV', os.path.join(PROCESSED_DIR, 'merged_tvt_state_miles.csv'))


FNAME_RE = re.compile(
    r'(?P<yy>\d{2})(?P<mon>[a-z]+)tvt\.xlsx$',
    re.IGNORECASE
)

MONTH_MAP = {
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
    'may': 5, 'jun': 6, 'jul': 7, 'aug': 8,
    'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
}

MONTH_NAMES = {
    1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr',
    5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug',
    9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'
}

def get_excluded_rows(year: int) -> list:
    """Return rows to exclude based on year, adjusted for skiprows."""
    if year <= 2002:
        return [10, 11, 12, 22, 23, 24, 37, 38, 39, 48, 49, 50]
    elif year == 2003:
        return [10, 11, 12, 22, 23, 24, 25, 38, 39, 40, 49, 50, 51]
    elif 2004 <= year <= 2007:
        return [10, 11, 21, 22, 35, 36, 45, 46]
    else:  # 2008-present
        return [10, 11, 21, 22, 35, 36, 45, 46]

def read_state_miles(path: str, year: int, month: int) -> pd.DataFrame:
    """Read state mileage and station data from Excel file based on year-specific format."""
    excluded_rows = get_excluded_rows(year)
    
    if year <= 2003:
        sheet_name = 3
        # Special case for December 2002
        if year == 2002 and month == 12:
            all_cols = [1, 2, 3, 4, 6, 7, 8]  # Shifted one column left for December 2002
        else:
            all_cols = [2, 3, 4, 5, 7, 8, 9]  # Regular column structure
            
        skiprows = 7
        max_row = 70 if year == 2002 else 71
        
        df = pd.read_excel(
            path,
            sheet_name=sheet_name,
            usecols=all_cols,
            skiprows=skiprows,
            nrows=max_row-skiprows,
            header=None,
            engine='openpyxl'
        )
        df = df[~df.index.isin(excluded_rows)]
        
        df.columns = [
            'State',
            'Rural_Stations',
            'Rural_Current', 'Rural_Previous',
            'Rural_LastMonth_Stations',
            'Rural_LastMonth_Current', 'Rural_LastMonth_Previous'
        ]
        return df

    else:  # 2004-2025
        sheets = [3, 4, 5]
        sheet_types = ['Rural', 'Urban', 'All']
        
        if year == 2007 and month >= 4:
            all_cols = [0, 1, 2, 3, 5, 6, 7]  # State, Station1, Miles1, Miles2, Station2, Miles3, Miles4
        else:
            all_cols = [0, 3, 4, 5, 7, 8, 9]  # State, Station1, Miles1, Miles2, Station2, Miles3, Miles4
            
        skiprows = 6 if 2004 <= year <= 2007 else 8
        max_row = 65 if 2004 <= year <= 2007 else 67
        
        dfs = []
        for sheet, sheet_type in zip(sheets, sheet_types):
            df = pd.read_excel(
                path,
                sheet_name=sheet,
                usecols=all_cols,
                skiprows=skiprows,
                nrows=max_row-skiprows,
                header=None,
                engine='openpyxl'
            )
            df = df[~df.index.isin(excluded_rows)]
            df.columns = [
                'State',
                f'{sheet_type}_Stations',
                f'{sheet_type}_Current', f'{sheet_type}_Previous',
                f'{sheet_type}_LastMonth_Stations',
                f'{sheet_type}_LastMonth_Current', f'{sheet_type}_LastMonth_Previous'
            ]
            dfs.append(df)

        final_df = dfs[0]
        for df in dfs[1:]:
            final_df = pd.merge(final_df, df, on='State', how='outer')
        final_df['Year'] = year
        final_df['Month'] = month
        return final_df

def get_previous_month_year(month: int, year: int) -> tuple:
    """Get previous month and year given current month and year."""
    if month == 1:
        return 12, year - 1
    else:
        return month - 1, year

def update_data_with_newer_values(consolidated_data: dict, new_df: pd.DataFrame, 
                                current_year: int, current_month: int) -> dict:
    """
    Update consolidated data with newer values from current file.
    
    Here is our Logic:
    - Use next month's col 8 (LastMonth_Current) to replace previous month's col 4 (Current) - Monthly Update
    - Use next month's col 9 (LastMonth_Previous) to replace previous month previous year's col 4 (Current) - Yearly Update

    Example Flow
    For a file from March 2023:
    1. Updates March 2023 data (current)
    2. Updates February 2023 data (previous month correction)
    3. Updates February 2022 data (previous month previous year correction)
    """
    
    # Get previous month info for monthly update
    prev_month, prev_year = get_previous_month_year(current_month, current_year)
    
    # Process each state in the new data
    for _, row in new_df.iterrows():
        state = row['State']
        if pd.isna(state) or state == '':
            continue
            
        # Ensure state exists in consolidated data
        if state not in consolidated_data:
            consolidated_data[state] = {}
        
        # Store current month data
        current_key = (current_year, current_month)
        if current_key not in consolidated_data[state]:
            consolidated_data[state][current_key] = {}
        
        # Store current month data
        for col_type in ['Rural', 'Urban', 'All']:
            if f'{col_type}_Current' in row:
                consolidated_data[state][current_key][f'{col_type}_Current'] = row[f'{col_type}_Current']
            if f'{col_type}_Stations' in row:
                consolidated_data[state][current_key][f'{col_type}_Stations'] = row[f'{col_type}_Stations']
        
        # MONTHLY UPDATE
        prev_month_key = (prev_year, prev_month)
        if prev_month_key not in consolidated_data[state]:
            consolidated_data[state][prev_month_key] = {}
        
        for col_type in ['Rural', 'Urban', 'All']:
            if f'{col_type}_LastMonth_Current' in row and not pd.isna(row[f'{col_type}_LastMonth_Current']):
                consolidated_data[state][prev_month_key][f'{col_type}_Current'] = row[f'{col_type}_LastMonth_Current']
            if f'{col_type}_LastMonth_Stations' in row and not pd.isna(row[f'{col_type}_LastMonth_Stations']):
                consolidated_data[state][prev_month_key][f'{col_type}_Stations'] = row[f'{col_type}_LastMonth_Stations']

        # YEARLY UPDATE
        prev_month_prev_year_key = (prev_year - 1, prev_month)  
        if prev_month_prev_year_key not in consolidated_data[state]:
            consolidated_data[state][prev_month_prev_year_key] = {}
        
        for col_type in ['Rural', 'Urban', 'All']:
            if f'{col_type}_LastMonth_Previous' in row and not pd.isna(row[f'{col_type}_LastMonth_Previous']):
                consolidated_data[state][prev_month_prev_year_key][f'{col_type}_Current'] = row[f'{col_type}_LastMonth_Previous']
    
    return consolidated_data

def get_month_num(month_str: str) -> int:
    """Convert month string to number, handling both full and abbreviated names."""
    return MONTH_MAP.get(month_str.lower()[:3], 0)

# Main processing logic
consolidated_data = {}  # Structure: {state: {(year, month): {col_type_current: value, col_type_previous: value}}}
stats = {'total_files': 0, 'processed': 0, 'errors': defaultdict(list)}

# Get list of files and sort chronologically from 2002 to present
files = [f for f in os.listdir(INPUT_DIR) if FNAME_RE.match(f)]
files.sort(key=lambda x: (
    int(FNAME_RE.match(x).group('yy')), 
    MONTH_MAP[FNAME_RE.match(x).group('mon').lower()]
))  # Remove reverse=True to process chronologically
stats['total_files'] = len(files)

print(f"Processing {len(files)} files chronologically from 2002 to present...")

# Process files with progress bar
for fn in tqdm(files, desc="Processing files", unit="file"):
    m = FNAME_RE.match(fn)
    yy = int(m.group('yy'))
    if not (2 <= yy <= 25):
        stats['errors']['year_range'].append(fn)
        continue
    
    full_year = 2000 + yy
    mon_code = m.group('mon').lower()
    mon = MONTH_MAP.get(mon_code)
    
    if mon is None:
        stats['errors']['month_code'].append(fn)
        continue

    path = os.path.join(INPUT_DIR, fn)
    try:
        df = read_state_miles(path, full_year, mon)
        
        # Update consolidated data with newer values
        consolidated_data = update_data_with_newer_values(
            consolidated_data, df, full_year, mon
        )
        
        stats['processed'] += 1
    except Exception as e:
        stats['errors']['read_error'].append((fn, str(e)))
        continue

# Print processing summary
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

# Convert consolidated data to DataFrame
if not consolidated_data:
    print("\n❌ No data was successfully processed. No output CSV will be created.")
    print("Please check the file paths and formats.")
else:
    # Build final DataFrame
    final_rows = []
    
    for state, date_data in consolidated_data.items():
        for (year, month), values in date_data.items():
            row = {
                'Date': f"{month}/1/{year}",
                'Month': MONTH_NAMES[month], 
                'Year': year,
                'State': state
            }
            
            # Add rural, urban, and all miles and stations
            for col_type in ['Rural', 'Urban', 'All']:
                # Add miles
                current_key = f'{col_type}_Current'
                if current_key in values:
                    if col_type == 'Rural':
                        row['Rural Arterial Miles'] = values[current_key]
                    elif col_type == 'Urban':
                        row['Urban Arterial Miles'] = values[current_key]
                    elif col_type == 'All':
                        row['All Miles'] = values[current_key]
                
                # Add stations
                station_key = f'{col_type}_Stations'
                if station_key in values:
                    if col_type == 'Rural':
                        row['Rural Arterial Stations'] = values[station_key]
                    elif col_type == 'Urban':
                        row['Urban Arterial Stations'] = values[station_key]
                    elif col_type == 'All':
                        row['All Stations'] = values[station_key]
            
            # Calculate Other Miles and Stations
            if all(key in row for key in ['Rural Arterial Miles', 'Urban Arterial Miles', 'All Miles']):
                try:
                    rural = pd.to_numeric(row['Rural Arterial Miles'])
                    urban = pd.to_numeric(row['Urban Arterial Miles'])
                    all_miles = pd.to_numeric(row['All Miles'])
                    if pd.notna(rural) and pd.notna(urban) and pd.notna(all_miles):
                        row['Other Miles'] = all_miles - rural - urban
                except (ValueError, TypeError):
                    row['Other Miles'] = None
            
            if all(key in row for key in ['Rural Arterial Stations', 'Urban Arterial Stations', 'All Stations']):
                try:
                    rural = pd.to_numeric(row['Rural Arterial Stations'])
                    urban = pd.to_numeric(row['Urban Arterial Stations'])
                    all_stations = pd.to_numeric(row['All Stations'])
                    if pd.notna(rural) and pd.notna(urban) and pd.notna(all_stations):
                        row['Other Stations'] = all_stations - rural - urban
                except (ValueError, TypeError):
                    row['Other Stations'] = None
            
            final_rows.append(row)
    
    # Create DataFrame and sort
    final_df = pd.DataFrame(final_rows)
    
    # Define column order
    column_order = [
        'Date', 'Month', 'Year', 'State',
        'Rural Arterial Miles', 'Urban Arterial Miles', 'All Miles',
        'Rural Arterial Stations', 'Urban Arterial Stations', 'All Stations',
        'Other Miles', 'Other Stations'
    ]
    
    # Convert Date to proper format
    final_df['Date'] = pd.to_datetime(final_df['Date'])
    final_df = final_df.sort_values(['Date', 'State'])
    final_df['Date'] = final_df['Date'].dt.strftime('%#m/%#d/%Y')
    
    # Remove any rows where all mile values are NaN
    final_df = final_df.dropna(subset=['Rural Arterial Miles', 'Urban Arterial Miles', 'All Miles'], how='all')
    
    # Reorder columns
    final_df = final_df[column_order]
    
    # Save to CSV
    final_df.to_csv(OUTPUT_CSV, index=False)
    print(f'\n✅ Merged {len(final_df)} rows → {OUTPUT_CSV}')
    
    # Convert dates to datetime for accurate comparison
    final_df['Date'] = pd.to_datetime(final_df['Date'])
    min_date = final_df['Date'].min().strftime('%#m/%#d/%Y')
    max_date = final_df['Date'].max().strftime('%#m/%#d/%Y')
    print(f"Data spans from {min_date} to {max_date}")
    
    # Convert back to string format if needed for CSV
    final_df['Date'] = final_df['Date'].dt.strftime('%#m/%#d/%Y')
    
    print(f"States included: {final_df['State'].nunique()}")
    print(f"Unique year-month combinations: {final_df.groupby(['Year', 'Month']).ngroups}")