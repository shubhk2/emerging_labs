import os
import time
import random
import psycopg2
from bs4 import BeautifulSoup
import requests
from dotenv import load_dotenv
import re
import calendar
from datetime import datetime

load_dotenv()

TICKERS = [
    "HEROMOTOCO", "TATAMOTORS", "JSWSTEEL", "SHRIRAMFIN", "ADANIPORTS", "ADANIENT",
    "HINDALCO", "HCLTECH", "TRENT", "TATACONSUM", "ASIANPAINT", "TECHM", "BAJAJ-AUTO",
    "TATASTEEL", "BEL", "SBILIFE", "JIOFIN", "ULTRACEMCO", "M&M", "GRASIM", "NESTLEIND",
    "ETERNAL", "MARUTI", "HDFCBANK", "LT", "HDFCLIFE", "TITAN", "ICICIBANK", "RELIANCE",
    "ITC", "AXISBANK", "BAJAJFINSV", "TCS", "SUNPHARMA", "BHARTIARTL", "BAJFINANCE",
    "COALINDIA", "WIPRO", "HINDUNILVR", "APOLLOHOSP", "EICHERMOT", "SBIN", "DRREDDY",
    "INFY", "ONGC", "KOTAKBANK", "INDUSINDBK", "CIPLA", "NTPC","POWERGRID"
]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

PL_ROWS = ["Expenses", "Operating Profit"]
BS_ROWS = ["Total Assets", "Fixed Assets", "Total Liabilities"]

def format_date(date_str):
    """
    Convert date string to YYYY-MM-DD format with last day of month.
    Examples:
        "Mar 2017" -> "2017-03-31"
        "2022-09-30" -> "2022-09-30"
    """
    if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
        return date_str
    try:
        if re.match(r'[A-Za-z]{3}\s+\d{4}', date_str):  # e.g., "Mar 2017"
            dt = datetime.strptime(date_str, '%b %Y')
            last_day = calendar.monthrange(dt.year, dt.month)[1]
            return f"{dt.year}-{dt.month:02d}-{last_day:02d}"
        else:
            dt = pd.to_datetime(date_str)
            return dt.strftime('%Y-%m-%d')
    except Exception as e:
        print(f"Warning: Could not format date '{date_str}': {e}")
        return date_str

def format_value(value_str):
    """
    Format value by removing commas and ensuring it's a float.
    """
    if value_str in (None, '', '-', '—'):
        return None
    try:
        if isinstance(value_str, (int, float)):
            return float(value_str)
        if isinstance(value_str, str):
            clean_value = re.sub(r'[^\d.-]', '', value_str)
            return float(clean_value)
        return value_str
    except Exception as e:
        print(f"Warning: Could not format value '{value_str}': {e}")
        return None

def get_pg_conn():
    conn_string = os.getenv("POSTGRES_URL")
    return psycopg2.connect(conn_string)

def get_company_id(cursor, company_name):
    cursor.execute("SELECT id FROM company_detail WHERE ticker=%s", (company_name,))
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute("INSERT INTO company_detail (ticker) VALUES (%s) RETURNING id", (company_name,))
    print("lode lag gayi")
    return cursor.fetchone()[0]

def parse_table_rows(soup, section_id, wanted_rows):
    section = soup.find(id=section_id)
    if not section:
        return []
    rows = []
    for tr in section.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        row_name = tds[0].get_text(strip=True)
        if row_name in wanted_rows:
            values = [td.get_text(strip=True).replace(",", "") for td in tds[1:]]
            rows.append((row_name, values))
    return rows

def get_report_dates(soup, section_id):
    section = soup.find(id=section_id)
    if not section:
        return []
    header = section.find("thead")
    if not header:
        return []
    ths = header.find_all("th")
    # Skip the first header (usually "Particulars" or similar)
    return [th.get_text(strip=True) for th in ths[1:]]

def insert_rows(cursor, table, company_id, report_dates, row_name, values):
    for date, value in zip(report_dates, values):
        if value in ("", "-", "—"):
            continue
        formatted_date = format_date(date)
        formatted_value = format_value(value)
        if not formatted_date or formatted_value is None:
            continue
        try:
            cursor.execute(
                f"""INSERT INTO {table} (company_id, report_date, parameter, value)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (company_id, report_date, parameter)
                    DO UPDATE SET value = EXCLUDED.value""",
                (company_id, formatted_date, row_name, formatted_value)
            )
        except Exception as e:
            print(f"Insert error for {table}, {row_name}, {date}: {e}")

def main():
    conn = get_pg_conn()
    cursor = conn.cursor()
    random.shuffle(TICKERS)
    for idx, ticker in enumerate(TICKERS):
        url = f"https://www.screener.in/company/{ticker}/consolidated"
        print(f"[{idx+1}/{len(TICKERS)}] Processing {ticker}")
        try:
            time.sleep(random.uniform(2, 5))
            resp = requests.get(url, headers=HEADERS)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            company_id = get_company_id(cursor, ticker)

            # Profit and Loss
            pl_dates = get_report_dates(soup, "profit-loss")
            pl_rows = parse_table_rows(soup, "profit-loss", PL_ROWS)
            for row_name, values in pl_rows:
                insert_rows(cursor, "profit_and_loss", company_id, pl_dates, row_name, values)

            # Balance Sheet
            bs_dates = get_report_dates(soup, "balance-sheet")
            bs_rows = parse_table_rows(soup, "balance-sheet", BS_ROWS)
            for row_name, values in bs_rows:
                insert_rows(cursor, "balance_sheet", company_id, bs_dates, row_name, values)

            conn.commit()
            print(f"Inserted rows for {ticker}")
        except Exception as e:
            print(f"Error processing {ticker}: {e}")
            conn.rollback()
        time.sleep(random.uniform(5, 10))
    cursor.close()
    conn.close()
    print("Done.")

if __name__ == "__main__":
    main()