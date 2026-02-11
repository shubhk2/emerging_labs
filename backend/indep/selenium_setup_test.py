# Verification script to test setup before running the main scraper

from app.db_setup import connect_to_db
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import sys


def test_database_connection():
    """Test database connection and verify table structure"""
    print("Testing database connection...")
    try:
        conn = connect_to_db()
        cursor = conn.cursor()

        # Test the filtered query
        cursor.execute("""
            SELECT id, ticker FROM company_detail 
            WHERE ticker IS NOT NULL AND ticker IN ('HEROMOTOCO', 'TATAMOTORS', 'JSWSTEEL', 'SHRIRAMFIN', 'ADANIPORTS', 'ADANIENT',
            'HINDALCO', 'HCLTECH', 'TRENT', 'TATACONSUM', 'ASIANPAINT', 'TECHM', 'BAJAJ-AUTO',
            'TATASTEEL', 'BEL', 'SBILIFE', 'JIOFIN', 'ULTRACEMCO', 'M&M', 'GRASIM', 'NESTLEIND',
            'ETERNAL', 'MARUTI', 'HDFCBANK', 'LT', 'HDFCLIFE', 'TITAN', 'ICICIBANK', 'RELIANCE',
            'ITC', 'AXISBANK', 'BAJAJFINSV', 'TCS', 'SUNPHARMA', 'BHARTIARTL', 'BAJFINANCE',
            'COALINDIA', 'WIPRO', 'HINDUNILVR', 'APOLLOHOSP', 'EICHERMOT', 'SBIN', 'DRREDDY',
            'INFY', 'ONGC', 'KOTAKBANK', 'INDUSINDBK', 'CIPLA', 'NTPC', 'POWERGRID')
            LIMIT 5
        """)

        companies = cursor.fetchall()
        print(f"✓ Database connection successful")
        print(f"✓ Found {len(companies)} companies in test query")

        if companies:
            print("Sample companies:")
            for company_id, ticker in companies:
                print(f"  - ID: {company_id}, Ticker: {ticker}")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        return False


def test_selenium_setup():
    """Test Selenium and ChromeDriver setup"""
    print("\nTesting Selenium setup...")
    try:
        # Set up Chrome options
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")

        # Try to initialize Chrome driver
        driver = webdriver.Chrome(options=chrome_options)
        driver.get("https://www.google.com")

        if "Google" in driver.title:
            print("✓ Selenium and ChromeDriver working correctly")
            driver.quit()
            return True
        else:
            print("✗ Selenium setup issue - Google page didn't load properly")
            driver.quit()
            return False

    except Exception as e:
        print(f"✗ Selenium setup failed: {e}")
        print("Please ensure ChromeDriver is installed and in your PATH")
        return False


def test_finchat_access():
    """Test access to FinChat.io"""
    print("\nTesting FinChat.io access...")
    try:
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

        driver = webdriver.Chrome(options=chrome_options)
        test_url = "https://finchat.io/company/NSEI-RELIANCE/financials/balance-sheet/"

        driver.get(test_url)

        if "FinChat" in driver.title or "balance" in driver.page_source.lower():
            print("✓ FinChat.io accessible")
            driver.quit()
            return True
        else:
            print("✗ FinChat.io not accessible or blocked")
            print(f"Page title: {driver.title}")
            driver.quit()
            return False

    except Exception as e:
        print(f"✗ FinChat access test failed: {e}")
        return False


def check_balance_sheet_table():
    """Check if balance_sheet table exists"""
    print("\nChecking balance_sheet table...")
    try:
        conn = connect_to_db()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'balance_sheet'
            );
        """)

        table_exists = cursor.fetchone()[0]

        if table_exists:
            print("✓ balance_sheet table exists")

            # Check table structure
            cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'balance_sheet'
                ORDER BY ordinal_position;
            """)

            columns = cursor.fetchall()
            print("Table structure:")
            for col_name, col_type in columns:
                print(f"  - {col_name}: {col_type}")

        else:
            print("✗ balance_sheet table does not exist")
            print("Run create_balance_sheet_table() to create it")

        cursor.close()
        conn.close()
        return table_exists

    except Exception as e:
        print(f"✗ Error checking balance_sheet table: {e}")
        return False


def main():
    """Run all verification tests"""
    print("FinChat Scraper Setup Verification")
    print("=" * 40)

    tests_passed = 0
    total_tests = 4

    # Test 1: Database connection
    if test_database_connection():
        tests_passed += 1

    # Test 2: Selenium setup
    if test_selenium_setup():
        tests_passed += 1

    # Test 3: FinChat access
    if test_finchat_access():
        tests_passed += 1

    # Test 4: Database table
    if check_balance_sheet_table():
        tests_passed += 1

    print(f"\n{'-' * 40}")
    print(f"Tests passed: {tests_passed}/{total_tests}")

    if tests_passed == total_tests:
        print("✓ All tests passed! You're ready to run the scraper.")
    else:
        print("✗ Some tests failed. Please fix the issues before running the scraper.")
        print("\nTroubleshooting tips:")
        if tests_passed < 1:
            print("- Check your database connection settings")
        if tests_passed < 2:
            print("- Install ChromeDriver: pip install webdriver-manager")
        if tests_passed < 3:
            print("- Check your internet connection")
        if tests_passed < 4:
            print("- Run create_balance_sheet_table() to create the table")


if __name__ == "__main__":
    main()