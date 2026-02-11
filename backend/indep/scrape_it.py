from bs4 import BeautifulSoup
import requests
import re
import json
import os
from json_to_csv_converter import process_api_json_response
import time
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def get_html_content(url, headers):
    """Fetch HTML with retry logic and rate limiting"""
    # Configure retry strategy
    retry_strategy = Retry(
        total=5,  # Maximum number of retries
        status_forcelist=[429, 500, 502, 503, 504],
        backoff_factor=1,  # Exponential backoff factor
        respect_retry_after_header=True
    )
    
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    try:
        # Add random delay before request (2-5 seconds)
        delay = random.uniform(2, 5)
        print(f"Waiting {delay:.2f} seconds before requesting {url}")
        time.sleep(delay)
        
        response = session.get(url, headers=headers)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching HTML content: {e}")
        # Additional manual backoff for 429 errors
        if "429" in str(e):
            extra_delay = random.uniform(30, 60)
            print(f"Rate limited! Waiting {extra_delay:.2f} seconds before continuing...")
            time.sleep(extra_delay)
        return None

def get_company_id_from_html(html_content):
    """
    Tries to extract the company ID from the given HTML content.
    It checks for 'data-row-company-id' attributes and then for IDs in hrefs.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    company_id = None

    # 1. Try 'data-row-company-id' attribute (often on <tr> or other elements)
    element_with_data_attr = soup.find(attrs={"data-row-company-id": True})
    if element_with_data_attr:
        company_id = element_with_data_attr.get('data-row-company-id')
        if company_id and company_id.isdigit():
            print(f"Found Company ID via data-row-company-id: {company_id}")
            return company_id

    # 2. Try extracting from href attributes of <a> tags
    # Pattern 1: /company/source/quarter/ID/month/year/
    link_tag_pattern1 = soup.find('a', href=re.compile(r'/company/source/quarter/(\d+)/\d+/\d+/'))
    if link_tag_pattern1:
        match = re.search(r'/company/source/quarter/(\d+)/\d+/\d+/', link_tag_pattern1['href'])
        if match:
            company_id = match.group(1)
            if company_id and company_id.isdigit():
                print(f"Found Company ID via href pattern 1: {company_id}")
                return company_id

    # Pattern 2: General /company/ID/ or /api/company/ID/ in any <a> tag
    # This is broader and might catch it if the first specific pattern misses
    all_links = soup.find_all('a', href=True) # Get all links with an href
    for link_tag in all_links:
        href_value = link_tag['href']
        # Regex for /company/ID/... or /api/company/ID/...
        # The ID is expected to be a sequence of digits
        match = re.search(r'/(?:company|api/company)/(\d+)/', href_value)
        if match:
            potential_id = match.group(1)
            if potential_id and potential_id.isdigit():
                # To avoid very short numbers that might not be IDs,
                # you could add a length check, e.g., if len(potential_id) > 2
                company_id = potential_id
                print(f"Found Company ID via href general pattern: {company_id} in {href_value}")
                return company_id # Return the first valid ID found this way

    if not company_id:
        print("Company ID could not be reliably extracted from the HTML.")
    return None


def get_expandable_items(html_content):
    """
    Parses the HTML content to find all expandable items (rows with a "+")
    and extracts their 'parent' and 'section' parameters from the onclick attribute.
    Filters for specific sections: 'quarters', 'profit-loss', 'balance-sheet', 'cash-flow'.

    Args:
        html_content (str): The HTML content of the main company page.

    Returns:
        list: A list of dictionaries, where each dictionary contains
              'parent' and 'section' for an expandable item.
              Example: [{'parent': 'Sales', 'section': 'quarters'}, ...]
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    expandable_items = []

    # Define the sections you are interested in
    target_sections = ['quarters', 'profit-loss', 'balance-sheet', 'cash-flow']

    # Find all tags that have an 'onclick' attribute
    # The relevant 'onclick' attributes call 'Company.showSchedule(...)'
    tags_with_onclick = soup.find_all(attrs={"onclick": re.compile(r"Company\.showSchedule\(")})

    for tag in tags_with_onclick:
        onclick_value = tag['onclick']

        # Use regex to extract parent and section from the onclick attribute
        # Example: Company.showSchedule('Sales', 'quarters', this)
        match = re.search(r"Company\.showSchedule\('([^']+)',\s*'([^']+)',", onclick_value)

        if match:
            parent = match.group(1)
            section = match.group(2)

            # Check if the extracted section is one of the targeted sections
            if section in target_sections:
                expandable_items.append({'parent': parent, 'section': section})

    return expandable_items

def extract_company_symbol_from_url(url):
    """
    Extract the company symbol/name from the screener.in URL
    Example: https://www.screener.in/company/TCS/consolidated/ -> TCS
    """
    match = re.search(r'/company/([^/]+)/', url)
    if match:
        company_symbol = match.group(1)
        print(f"Extracted company symbol from URL: {company_symbol}")
        return company_symbol
    print("Could not extract company symbol from URL")
    return None

if __name__ == "__main__":
    tickers = [
        "HEROMOTOCO", "TATAMOTORS", "JSWSTEEL", "SHRIRAMFIN", "ADANIPORTS", "ADANIENT",
        "HINDALCO", "HCLTECH", "TRENT", "TATACONSUM", "ASIANPAINT", "TECHM", "BAJAJ-AUTO",
        "TATASTEEL", "BEL", "SBILIFE", "JIOFIN", "ULTRACEMCO", "M&M", "GRASIM", "NESTLEIND",
        "ETERNAL", "MARUTI", "HDFCBANK", "LT", "HDFCLIFE", "TITAN", "ICICIBANK", "RELIANCE",
        "ITC", "AXISBANK", "BAJAJFINSV", "TCS", "SUNPHARMA", "BHARTIARTL", "BAJFINANCE",
        "COALINDIA", "WIPRO", "HINDUNILVR", "APOLLOHOSP", "EICHERMOT", "SBIN", "DRREDDY",
        "INFY", "ONGC", "KOTAKBANK", "INDUSINDBK", "CIPLA", "NTPC","POWERGRID"
    ]
    
    # Randomize the order of tickers to avoid sequential scraping patterns
    random.shuffle(tickers)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    # Set default output directory for processed data
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)
    output_dir = os.path.join(parent_dir, "processed_data_scrap")
    os.makedirs(output_dir, exist_ok=True)
    
    # Track progress
    processed_count = 0
    error_count = 0
    
    for ticker in tickers:
        main_company_page_url = f"https://www.screener.in/company/{ticker}/consolidated"
        
        # Extract the company symbol from the URL
        company_symbol = extract_company_symbol_from_url(main_company_page_url)
        
        print(f"\n[{processed_count + 1}/{len(tickers)}] Processing: {company_symbol}")
        
        # Use the new get_html_content function with retry logic
        html_content_for_scraping = get_html_content(main_company_page_url, headers)

        if html_content_for_scraping:
            try:
                # 1. Get Company ID from the fetched HTML
                COMPANY_ID_STR = get_company_id_from_html(html_content_for_scraping)

                if not COMPANY_ID_STR:
                    print("CRITICAL: Failed to obtain Company ID. Cannot proceed for this company.")
                    error_count += 1
                else:
                    print(f"Successfully obtained Company ID: {COMPANY_ID_STR}")

                    # 2. Get the expandable items from the same fetched HTML
                    items_to_scrape = get_expandable_items(html_content_for_scraping)

                    if items_to_scrape:
                        print(f"Found {len(items_to_scrape)} expandable items for the target sections")

                        for item in items_to_scrape:
                            api_base_url = f"https://www.screener.in/api/company/{COMPANY_ID_STR}/schedules/"

                            # Parameters for the GET request
                            params = {
                                'parent': item['parent'],
                                'section': item['section'],
                                'consolidated': ''
                            }

                            print(f"Fetching API data for: Parent='{item['parent']}', Section='{item['section']}'")
                            
                            try:
                                # Use session with retry logic for API calls too
                                retry_strategy = Retry(
                                    total=5,  # Maximum number of retries
                                    status_forcelist=[429, 500, 502, 503, 504],
                                    backoff_factor=1,  # Exponential backoff factor
                                    respect_retry_after_header=True
                                )
                                session = requests.Session()
                                retry_adapter = HTTPAdapter(max_retries=retry_strategy)
                                session.mount("http://", retry_adapter)
                                session.mount("https://", retry_adapter)
                                
                                # Add random delay between API requests
                                api_delay = random.uniform(1, 3)
                                time.sleep(api_delay)
                                
                                api_response = session.get(api_base_url, params=params, headers=headers)
                                api_response.raise_for_status()
                                json_data = api_response.json()

                                # Process JSON data and save to CSV
                                saved_file = process_api_json_response(
                                    json_data=json_data,
                                    screener_company_id=int(COMPANY_ID_STR),
                                    company_symbol=company_symbol,
                                    parent_name=item['parent'],
                                    section=item['section'],
                                    output_dir=output_dir
                                )
                                print(f"  Processed data saved to: {saved_file}")

                            except requests.exceptions.RequestException as e:
                                print(f"  Error fetching API data: {e}")
                                # Additional backoff for API rate limits
                                if "429" in str(e):
                                    api_extra_delay = random.uniform(20, 40)
                                    print(f"  API rate limited! Waiting {api_extra_delay:.2f} seconds...")
                                    time.sleep(api_extra_delay)
                            except json.JSONDecodeError as e:
                                print(f"  Error decoding JSON: {e}")
                    else:
                        print("No expandable items found for the target sections.")
                    
                    processed_count += 1
            except Exception as e:
                print(f"Unexpected error processing {company_symbol}: {e}")
                error_count += 1
        else:
            print(f"Cannot proceed without HTML content from the main page for {company_symbol}")
            error_count += 1
        
        # Add a longer delay between companies to avoid hitting rate limits
        company_delay = random.uniform(5, 15)
        print(f"Waiting {company_delay:.2f} seconds before processing next company...")
        time.sleep(company_delay)
    
    print(f"\nScraping complete! Processed {processed_count}/{len(tickers)} companies successfully.")
    print(f"Encountered errors with {error_count} companies.")
