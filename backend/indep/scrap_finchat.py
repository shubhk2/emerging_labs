import time
import re
from datetime import datetime, date
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import execute_batch
from app.db_setup import connect_to_db


class FinChatBalanceSheetScraper:
    def __init__(self, headless=True, timeout=30):
        self.timeout = timeout
        self.base_url = "https://finchat.io/company/NSEI-{ticker}/financials/balance-sheet/"

        # Chrome options
        self.chrome_options = Options()
        if headless:
            self.chrome_options.add_argument("--headless")

        # Essential Chrome options for stability
        self.chrome_options.add_argument("--no-sandbox")
        self.chrome_options.add_argument("--disable-dev-shm-usage")
        self.chrome_options.add_argument("--disable-gpu")
        self.chrome_options.add_argument("--disable-extensions")
        self.chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        self.chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.chrome_options.add_experimental_option('useAutomationExtension', False)

        # User agent to avoid detection
        self.chrome_options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36")

        self.driver = None
        self.setup_driver()

    def setup_driver(self):
        """Initialize Chrome driver with automatic driver management"""
        try:
            # Let Selenium handle ChromeDriver automatically
            self.driver = webdriver.Chrome(options=self.chrome_options)
            self.driver.set_page_load_timeout(self.timeout)

            # Execute script to avoid detection
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            print("✓ ChromeDriver initialized successfully with automatic management")

        except WebDriverException as e:
            print(f"❌ ChromeDriver setup failed: {e}")
            print("\n🔧 Trying alternative solutions...")
            self.try_alternative_setup()

    def try_alternative_setup(self):
        """Try alternative ChromeDriver setup methods"""
        try:
            # Solution 2: Use webdriver-manager
            from webdriver_manager.chrome import ChromeDriverManager
            from selenium.webdriver.chrome.service import Service

            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=self.chrome_options)
            self.driver.set_page_load_timeout(self.timeout)
            print("✓ ChromeDriver initialized with webdriver-manager")

        except ImportError:
            print("📦 Installing webdriver-manager...")
            import subprocess
            subprocess.check_call(["pip", "install", "webdriver-manager"])

            from webdriver_manager.chrome import ChromeDriverManager
            from selenium.webdriver.chrome.service import Service

            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=self.chrome_options)
            self.driver.set_page_load_timeout(self.timeout)
            print("✓ ChromeDriver initialized with webdriver-manager (newly installed)")

        except Exception as e:
            print(f"❌ Alternative setup also failed: {e}")
            raise

    def get_companies_from_db(self):
        """Fetch companies using your specific query"""
        conn = connect_to_db()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT id, ticker FROM company_detail 
                WHERE ticker IS NOT NULL and ticker in ('HEROMOTOCO', 'TATAMOTORS', 'JSWSTEEL', 'SHRIRAMFIN', 'ADANIPORTS', 'ADANIENT',
        'HINDALCO', 'HCLTECH', 'TRENT', 'TATACONSUM', 'ASIANPAINT', 'TECHM', 'BAJAJ-AUTO',
        'TATASTEEL', 'BEL', 'SBILIFE', 'JIOFIN', 'ULTRACEMCO', 'M&M', 'GRASIM', 'NESTLEIND',
        'ETERNAL', 'MARUTI', 'HDFCBANK', 'LT', 'HDFCLIFE', 'TITAN', 'ICICIBANK', 'RELIANCE',
        'ITC', 'AXISBANK', 'BAJAJFINSV', 'TCS', 'SUNPHARMA', 'BHARTIARTL', 'BAJFINANCE',
        'COALINDIA', 'WIPRO', 'HINDUNILVR', 'APOLLOHOSP', 'EICHERMOT', 'SBIN', 'DRREDDY',
        'INFY', 'ONGC', 'KOTAKBANK', 'INDUSINDBK', 'CIPLA', 'NTPC', 'POWERGRID')
            """)
            companies = cursor.fetchall()
            print(f"✓ Found {len(companies)} companies to process")
            return companies

        except Exception as e:
            print(f"❌ Database query failed: {e}")
            return []
        finally:
            cursor.close()
            conn.close()

    def wait_for_page_load(self):
        """Wait for FinChat page to fully load"""
        try:
            # Wait for skeleton loaders to disappear
            WebDriverWait(self.driver, 20).until_not(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".mantine-Skeleton-root[data-visible='true']"))
            )

            # Additional wait for content to stabilize
            time.sleep(3)

            # Check if financial data is present
            WebDriverWait(self.driver, 10).until(
                EC.any_of(
                    EC.presence_of_element_located((By.TAG_NAME, "table")),
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[role='table']")),
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".table-wrapper"))
                )
            )
            return True

        except TimeoutException:
            print("⚠️ Page load timeout - content may not be fully loaded")
            return False

    def extract_financial_data(self):
        """Extract balance sheet data from the page"""
        try:
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')

            # Strategy 1: Look for standard HTML tables
            tables = soup.find_all('table')
            if tables:
                return self.parse_html_tables(tables)

            # Strategy 2: Look for ARIA tables
            aria_tables = soup.find_all(attrs={'role': 'table'})
            if aria_tables:
                return self.parse_aria_tables(aria_tables)

            # Strategy 3: Look for FinChat-specific structures
            table_wrappers = soup.find_all('div', class_='table-wrapper')
            if table_wrappers:
                return self.parse_table_wrappers(table_wrappers)

            print("❌ No recognizable table structure found")
            return []

        except Exception as e:
            print(f"❌ Data extraction error: {e}")
            return []

    def parse_html_tables(self, tables):
        """Parse standard HTML tables"""
        all_data = []

        for table in tables:
            try:
                # Get headers (dates)
                header_row = table.find('thead', recursive=True) or table.find('tr', recursive=True)
                if not header_row:
                    continue

                headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
                date_headers = [h for h in headers[1:] if h and not h.lower() in ['', 'parameter', 'metric']]

                # Get data rows
                rows = table.find_all('tr')[1:]  # Skip header

                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) < 2:
                        continue

                    parameter = cells[0].get_text(strip=True)
                    if not parameter or parameter.lower() in ['assets', 'liabilities', 'equity']:
                        continue

                    # Extract values for each date
                    for i, date_header in enumerate(date_headers, 1):
                        if i < len(cells):
                            value_text = cells[i].get_text(strip=True)

                            # Parse date and value
                            report_date = self.parse_date(date_header)
                            value = self.parse_financial_value(value_text)

                            if report_date and value is not None:
                                all_data.append({
                                    'parameter': parameter,
                                    'report_date': report_date,
                                    'value': value
                                })

            except Exception as e:
                print(f"⚠️ Error parsing table: {e}")
                continue

        return all_data

    def parse_aria_tables(self, aria_tables):
        """Parse ARIA table structures"""
        all_data = []

        for table in aria_tables:
            try:
                # Find header row
                header_row = table.find(attrs={'role': 'row'})
                if not header_row:
                    continue

                headers = [cell.get_text(strip=True) for cell in header_row.find_all(attrs={'role': 'columnheader'})]
                date_headers = headers[1:] if len(headers) > 1 else []

                # Find data rows
                rows = table.find_all(attrs={'role': 'row'})[1:]  # Skip header

                for row in rows:
                    cells = row.find_all(attrs={'role': ['cell', 'rowheader']})
                    if len(cells) < 2:
                        continue

                    parameter = cells[0].get_text(strip=True)
                    if not parameter:
                        continue

                    for i, date_header in enumerate(date_headers, 1):
                        if i < len(cells):
                            value_text = cells[i].get_text(strip=True)

                            report_date = self.parse_date(date_header)
                            value = self.parse_financial_value(value_text)

                            if report_date and value is not None:
                                all_data.append({
                                    'parameter': parameter,
                                    'report_date': report_date,
                                    'value': value
                                })

            except Exception as e:
                print(f"⚠️ Error parsing ARIA table: {e}")
                continue

        return all_data

    def parse_table_wrappers(self, table_wrappers):
        """Parse FinChat-specific table wrapper structures"""
        all_data = []

        for wrapper in table_wrappers:
            try:
                # Look for any tabular data within the wrapper
                rows = wrapper.find_all('tr') or wrapper.find_all(attrs={'role': 'row'})

                if not rows:
                    continue

                # Process similar to HTML tables
                header_row = rows[0]
                headers = [cell.get_text(strip=True) for cell in
                           header_row.find_all(['th', 'td']) or header_row.find_all(
                               attrs={'role': ['columnheader', 'cell']})]

                for row in rows[1:]:
                    cells = row.find_all(['td', 'th']) or row.find_all(attrs={'role': ['cell', 'rowheader']})
                    if len(cells) < 2:
                        continue

                    parameter = cells[0].get_text(strip=True)
                    if not parameter:
                        continue

                    for i, header in enumerate(headers[1:], 1):
                        if i < len(cells):
                            value_text = cells[i].get_text(strip=True)

                            report_date = self.parse_date(header)
                            value = self.parse_financial_value(value_text)

                            if report_date and value is not None:
                                all_data.append({
                                    'parameter': parameter,
                                    'report_date': report_date,
                                    'value': value
                                })

            except Exception as e:
                print(f"⚠️ Error parsing table wrapper: {e}")
                continue

        return all_data

    def parse_date(self, date_str):
        """Parse various date formats to standard date"""
        if not date_str:
            return None

        try:
            # Clean the date string
            date_str = date_str.strip()

            # Handle formats like "Q4 2024", "Mar 2024", "2024"
            patterns = [
                (r'Q[1-4]\s+(\d{4})', lambda m: date(int(m.group(1)), 3, 31)),  # Q4 -> March 31
                (r'Mar\s+(\d{4})', lambda m: date(int(m.group(1)), 3, 31)),
                (r'(\d{4})', lambda m: date(int(m.group(1)), 3, 31)),
                (r'(\d{1,2})/(\d{1,2})/(\d{4})', lambda m: date(int(m.group(3)), int(m.group(1)), int(m.group(2))))
            ]

            for pattern, converter in patterns:
                match = re.search(pattern, date_str)
                if match:
                    return converter(match)

            return None

        except Exception:
            return None

    def parse_financial_value(self, value_str):
        """Parse financial values to numeric format"""
        if not value_str or value_str in ['—', '-', 'N/A', '']:
            return None

        try:
            # Remove common formatting
            clean_value = re.sub(r'[₹$,\s]', '', value_str)

            # Handle parentheses (negative values)
            if '(' in clean_value and ')' in clean_value:
                clean_value = '-' + clean_value.replace('(', '').replace(')', '')

            # Handle percentage
            if '%' in clean_value:
                clean_value = clean_value.replace('%', '')
                return float(clean_value) / 100

            # Handle multipliers (Cr, L, K)
            multipliers = {'cr': 10000000, 'l': 100000, 'k': 1000}
            for suffix, multiplier in multipliers.items():
                if clean_value.lower().endswith(suffix):
                    clean_value = clean_value[:-len(suffix)]
                    return float(clean_value) * multiplier

            return float(clean_value)

        except Exception:
            return None

    def scrape_company_balance_sheet(self, ticker):
        """Scrape balance sheet for a single company"""
        url = self.base_url.format(ticker=ticker)

        try:
            print(f"🌐 Loading: {url}")
            self.driver.get(url)

            if not self.wait_for_page_load():
                print(f"⚠️ Page load issues for {ticker}")
                return []

            data = self.extract_financial_data()
            print(f"✓ Extracted {len(data)} data points for {ticker}")
            return data

        except Exception as e:
            print(f"❌ Scraping failed for {ticker}: {e}")
            return []

    def store_balance_sheet_data(self, company_id, data):
        """Store data in database using your connection method"""
        if not data:
            return

        conn = connect_to_db()
        cursor = conn.cursor()

        try:
            # Prepare records for batch insert
            records = [
                (company_id, item['report_date'], item['parameter'], item['value'])
                for item in data
            ]

            # UPSERT query
            query = """
                INSERT INTO balance_sheet (company_id, report_date, parameter, value)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (company_id, report_date, parameter) 
                DO UPDATE SET 
                    value = EXCLUDED.value,
                    updated_at = CURRENT_TIMESTAMP
            """

            execute_batch(cursor, query, records)
            conn.commit()
            print(f"✓ Stored {len(records)} records for company {company_id}")

        except Exception as e:
            conn.rollback()
            print(f"❌ Database error for company {company_id}: {e}")
        finally:
            cursor.close()
            conn.close()



    def process_all_companies(self, batch_size=3):
        """Process all companies with rate limiting"""
        companies = self.get_companies_from_db()
        if not companies:
            print("❌ No companies found to process")
            return

        self.create_balance_sheet_table()

        for i, (company_id, ticker) in enumerate(companies):
            print(f"\n📊 Processing {ticker} (ID: {company_id}) [{i + 1}/{len(companies)}]")

            # Scrape data
            balance_sheet_data = self.scrape_company_balance_sheet(ticker)

            # Store data
            if balance_sheet_data:
                self.store_balance_sheet_data(company_id, balance_sheet_data)
            else:
                print(f"⚠️ No data extracted for {ticker}")

            # Rate limiting
            if (i + 1) % batch_size == 0 and i + 1 < len(companies):
                print(f"⏸️ Batch complete. Sleeping for 10 seconds...")
                time.sleep(10)
            else:
                time.sleep(5)  # Short delay between requests

    def test_single_company(self, ticker="RELIANCE"):
        """Test scraper with a single company"""
        print(f"🧪 Testing scraper with {ticker}")

        # Get company ID
        conn = connect_to_db()
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM company_detail WHERE ticker = %s", (ticker,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        if not result:
            print(f"❌ Company {ticker} not found in database")
            return False

        company_id = result[0]

        # Test scraping
        data = self.scrape_company_balance_sheet(ticker)
        if data:
            print(f"✓ Successfully extracted {len(data)} data points")
            print("Sample data:")
            for item in data[:5]:
                print(f"  {item['parameter']}: {item['value']} ({item['report_date']})")

            # Test storage
            self.create_balance_sheet_table()
            self.store_balance_sheet_data(company_id, data)
            return True
        else:
            print("❌ No data extracted")
            return False

    def close(self):
        """Clean up resources"""
        if self.driver:
            self.driver.quit()
            print("✓ Browser closed")


# Usage Example
if __name__ == "__main__":
    print("🚀 FinChat Balance Sheet Scraper")
    print("=" * 50)

    try:
        # Initialize scraper
        scraper = FinChatBalanceSheetScraper(headless=True)

        # Test with single company first
        test_success = scraper.test_single_company("RELIANCE")

        if test_success:
            proceed = input("\n✅ Test successful! Proceed with all companies? (y/n): ")
            if proceed.lower() == 'y':
                scraper.process_all_companies(batch_size=3)
            else:
                print("👋 Scraping cancelled")
        else:
            print("❌ Test failed. Please check the setup")

    except Exception as e:
        print(f"❌ Fatal error: {e}")
    finally:
        if 'scraper' in locals():
            scraper.close()
