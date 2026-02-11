from typing import Optional, List, Dict, Any
from backend.db_setup import connect_to_db
from psycopg2.extras import RealDictCursor
import logging

logger = logging.getLogger(__name__)


def get_predefined_ratios(
        company_numbers: List[int],
        start_year: Optional[int] = None,
        end_year: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Get predefined financial ratios for one or more companies from the financial_ratios table.

    Parameters:
    - company_numbers: List of company IDs to fetch ratios for.
    - start_year: The starting year for the data range (e.g., 2018).
    - end_year: The ending year for the data range (e.g., 2022).
    """
    logger.debug(f"Getting ratios for companies: {company_numbers}, start: {start_year}, end: {end_year}")

    conn = connect_to_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    # Determine which year columns to select based on the provided range
    all_year_cols = [f'mar_{yr}' for yr in range(16, 26)]
    selected_year_cols = []
    if start_year or end_year:
        start = start_year - 2000 if start_year else 16
        end = end_year - 2000 if end_year else 25
        for yr in range(start, end + 1):
            selected_year_cols.append(f'mar_{yr}')
    else:
        selected_year_cols = all_year_cols

    if not selected_year_cols:
        selected_year_cols = all_year_cols

    # Fetch company details (name and ticker)
    cursor.execute("SELECT id, full_name, ticker FROM company_detail WHERE id = ANY(%s)", (company_numbers,))
    company_details_map = {row['id']: row for row in cursor.fetchall()}

    # Fetch ratio data for the specified companies
    query_cols = ['company_number', 'name', 'percent_or_not'] + selected_year_cols
    query = f"""
        SELECT {', '.join(query_cols)}
        FROM public.financial_ratios
        WHERE company_number = ANY(%s)
    """
    cursor.execute(query, (company_numbers,))
    ratios_data = cursor.fetchall()

    cursor.close()
    conn.close()

    # Structure the data for the API response
    results = []
    for company_number in company_numbers:
        company_info = company_details_map.get(company_number)
        if not company_info:
            continue

        company_ratios = [row for row in ratios_data if row['company_number'] == company_number]

        headers = ['Ratio'] + [f"Mar {2000 + int(col.split('_')[1])}" for col in selected_year_cols]

        formatted_data = []
        for ratio_row in company_ratios:
            data_item = {'Ratio': ratio_row['name']}
            for col in selected_year_cols:
                header_name = f"Mar {2000 + int(col.split('_')[1])}"
                value = ratio_row.get(col)
                if value is not None and ratio_row.get('percent_or_not'):
                    data_item[header_name] = f"{value}%"
                else:
                    data_item[header_name] = value
            formatted_data.append(data_item)

        results.append({
            "company_name": company_info['full_name'],
            "ticker": company_info['ticker'],
            "company_number": company_number,
            "headers": headers,
            "data": formatted_data
        })

    return results


def get_ratios_by_parameters(
        company_numbers: List[int],
        parameters: List[str],
        start_year: Optional[int] = None,
        end_year: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Get specific financial ratios for one or more companies, filtered by ratio name.

    Parameters:
    - company_numbers: List of company IDs to fetch ratios for.
    - parameters: List of ratio names to filter by (e.g., ["ROCE", "Debt to equity"]).
    - start_year: The starting year for the data range.
    - end_year: The ending year for the data range.
    """
    logger.debug(f"Getting specific ratios {parameters} for companies: {company_numbers}, start: {start_year}, end: {end_year}")

    conn = connect_to_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    # Determine which year columns to select
    all_year_cols = [f'mar_{yr}' for yr in range(16, 26)]
    selected_year_cols = []
    if start_year or end_year:
        start = start_year - 2000 if start_year else 16
        end = end_year - 2000 if end_year else 25
        for yr in range(start, end + 1):
            selected_year_cols.append(f'mar_{yr}')
    else:
        selected_year_cols = all_year_cols

    if not selected_year_cols:
        selected_year_cols = all_year_cols

    # Fetch company details
    cursor.execute("SELECT id, full_name, ticker FROM company_detail WHERE id = ANY(%s)", (company_numbers,))
    company_details_map = {row['id']: row for row in cursor.fetchall()}

    # Fetch ratio data, filtered by parameters
    query_cols = ['company_number', 'name', 'percent_or_not'] + selected_year_cols
    query = f"""
        SELECT {', '.join(query_cols)}
        FROM public.financial_ratios
        WHERE company_number = ANY(%s) AND name = ANY(%s)
    """
    cursor.execute(query, (company_numbers, parameters))
    ratios_data = cursor.fetchall()

    cursor.close()
    conn.close()

    # Structure the data (logic is identical to get_predefined_ratios)
    results = []
    for company_number in company_numbers:
        company_info = company_details_map.get(company_number)
        if not company_info:
            continue

        company_ratios = [row for row in ratios_data if row['company_number'] == company_number]

        headers = ['Ratio'] + [f"Mar {2000 + int(col.split('_')[1])}" for col in selected_year_cols]

        formatted_data = []
        for ratio_row in company_ratios:
            data_item = {'Ratio': ratio_row['name']}
            for col in selected_year_cols:
                header_name = f"Mar {2000 + int(col.split('_')[1])}"
                value = ratio_row.get(col)
                if value is not None and ratio_row.get('percent_or_not'):
                    data_item[header_name] = f"{value}%"
                else:
                    data_item[header_name] = value
            formatted_data.append(data_item)

        results.append({
            "company_name": company_info['full_name'],
            "ticker": company_info['ticker'],
            "company_number": company_number,
            "headers": headers,
            "data": formatted_data
        })

    return results


if __name__ == "__main__":
    # Example usage
    company_numbers = [1, 2, 3]  # Replace with actual company IDs
    ratios = get_predefined_ratios(company_numbers, start_year=2018, end_year=2022)
    for ratio in ratios:
        print(ratio)