# This script downloads, renames, and converts TVT Excel files from the FHWA website. To run it, ensure you have the required libraries installed:
# pip install requests beautifulsoup4 pandas openpyxl xlrd

import os
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import pandas as pd

# -- Configuration: adjust these paths/URLs if needed
BASE_PAGE = 'https://www.fhwa.dot.gov/policyinformation/travel_monitoring/tvt.cfm'
BASE_DIR = os.getenv('AIRFLOW_HOME', '/opt/airflow')
DATA_DIR = os.path.join(BASE_DIR, 'data')
DOWNLOAD_DIR = os.getenv('TVT_RAW_DIR', os.path.join(DATA_DIR, 'tvt', 'raw'))

# Pattern to match names like tvt<mon><yy>.xls or .xlsx
FILENAME_PATTERN = re.compile(
    r'^tvt(?P<mon>[a-z]{3,4})(?P<yy>\d{2})\.(?P<ext>xls[x]?)$',
    re.IGNORECASE
)


def ensure_dir(path: str) -> None:
    """
    Ensure that a directory exists; create it if it doesn’t.
    """
    if not os.path.isdir(path):
        os.makedirs(path)
        print(f'▶ Created directory: {path}')


def download_files() -> None:
    """
    Download all .xls/.xlsx links from BASE_PAGE into DOWNLOAD_DIR.
    """
    ensure_dir(DOWNLOAD_DIR)
    resp = requests.get(BASE_PAGE)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')

    for a in soup.find_all('a', href=True):
        href = a['href'].strip()
        if not href.lower().endswith(('.xls', '.xlsx')):
            continue

        file_url = urljoin(BASE_PAGE, href)
        filename = os.path.basename(href)
        dest_path = os.path.join(DOWNLOAD_DIR, filename)

        if os.path.exists(dest_path):
            print(f'⚠ Skipping download (exists): {filename}')
            continue

        resp2 = requests.get(file_url)
        if resp2.status_code == 200:
            with open(dest_path, 'wb') as f:
                f.write(resp2.content)
            print(f'✔ Downloaded: {filename}')
        else:
            print(f'✖ Failed to download {filename}: HTTP {resp2.status_code}')


def rename_files() -> None:
    """
    Rename files matching tvt<mon><yy> to <yy><mon>tvt (preserving extension).
    """
    for filename in os.listdir(DOWNLOAD_DIR):
        m = FILENAME_PATTERN.match(filename)
        if not m:
            continue

        mon = m.group('mon').lower()
        yy = m.group('yy')
        ext = m.group('ext').lower()
        old_path = os.path.join(DOWNLOAD_DIR, filename)
        new_name = f"{yy}{mon}tvt.{ext}"
        new_path = os.path.join(DOWNLOAD_DIR, new_name)

        if os.path.exists(new_path):
            print(f'⚠ Skipping rename for {filename} → {new_name} (target exists)')
            continue

        os.rename(old_path, new_path)
        print(f'✔ Renamed: {filename} → {new_name}')


def convert_xls_to_xlsx() -> None:
    """
    Convert all .xls (but not .xlsx) files in DOWNLOAD_DIR to .xlsx preserving sheets.
    """
    for fname in os.listdir(DOWNLOAD_DIR):
        if fname.lower().endswith('.xls') and not fname.lower().endswith('.xlsx'):
            xls_path = os.path.join(DOWNLOAD_DIR, fname)
            base_name = os.path.splitext(fname)[0]
            xlsx_name = base_name + '.xlsx'
            xlsx_path = os.path.join(DOWNLOAD_DIR, xlsx_name)

            if os.path.exists(xlsx_path):
                print(f'⚠ Skipping conversion (xlsx exists): {xlsx_name}')
                continue

            try:
                sheets = pd.read_excel(xls_path, sheet_name=None, engine='xlrd')
                with pd.ExcelWriter(xlsx_path, engine='openpyxl') as writer:
                    for sheet_name, df in sheets.items():
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f'✔ Converted: {fname} → {xlsx_name}')
            except Exception as e:
                print(f'✖ Failed to convert {fname}: {e}')


def main() -> None:
    download_files()
    rename_files()
    convert_xls_to_xlsx()


if __name__ == '__main__':
    main()
