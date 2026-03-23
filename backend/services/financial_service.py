from typing import Optional, List, Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor
from backend.db_setup import connect_to_db
import logging

# Set up logger for this module
logger = logging.getLogger(__name__)


def get_financial_data(company_number: int, statement_type: str, start_year: Optional[int] = None,
                       end_year: Optional[int] = None) -> Dict[str, Any]:
    """
    Get financial statement data for a company based on the new yearly column structure.

    Parameters:
    - company_number: ID of the company (from company_detail.id).
    - statement_type: Type of statement (balance_sheet, profit_and_loss, cashflow).
    - start_year: Start year for filtering columns (e.g., 2018).
    - end_year: End year for filtering columns (e.g., 2022).
    """
    logger.debug(
        f"Getting financial data for company_number={company_number}, statement_type={statement_type}, start_year={start_year}, end_year={end_year}")

    if statement_type not in ['balance_sheet', 'profit_and_loss', 'cashflow']:
        raise ValueError("Invalid statement type specified.")

    conn = connect_to_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    # Get company details
    cursor.execute("SELECT full_name, ticker FROM company_detail WHERE id = %s", (company_number,))
    company_details = cursor.fetchone()

    if not company_details:
        logger.warning(f"Company with number {company_number} not found")
        raise ValueError(f"Company with number {company_number} not found")

    # Determine which year columns to select
    selected_year_cols = []
    start = start_year - 2000 if start_year else 16
    end = end_year - 2000 if end_year else 25
    for yr in range(start, end + 1):
        selected_year_cols.append(f'mar_{yr}')

    if not selected_year_cols:
        selected_year_cols = [f'mar_{yr}' for yr in range(16, 26)]

    # Build and execute query
    query_cols = ['account'] + selected_year_cols
    query = f"SELECT {', '.join(query_cols)} FROM public.{statement_type} WHERE company_number = %s"

    logger.debug(f"Executing query: {query} with params: ({company_number},)")
    cursor.execute(query, (company_number,))
    data = cursor.fetchall()
    logger.info(f"Retrieved {len(data)} financial records for {statement_type}")

    cursor.close()
    conn.close()

    # Format headers and data for the response
    headers = ['Account'] + [f"Mar {2000 + int(col.split('_')[1])}" for col in selected_year_cols]
    key_map = {'account': 'Account'}
    for col in selected_year_cols:
        key_map[col] = f"Mar {2000 + int(col.split('_')[1])}"

    formatted_data = [{key_map[k]: v for k, v in row.items()} for row in data]

    return {
        "company_name": company_details['full_name'],
        "ticker": company_details['ticker'],
        "statement_type": statement_type,
        "headers": headers,
        "data": formatted_data
    }


def get_financial_data_by_parameters(
    company_number: int,
    statement_type: str,
    parameters: List[str],
    start_year: Optional[int] = None,
    end_year: Optional[int] = None
) -> Dict[str, Any]:
    """
    Fetch only the rows that match the given parameters from the statement table.

    Parameters:
    - company_number: ID of the company (from company_detail.id).
    - statement_type: Type of statement (balance_sheet, profit_and_loss, cashflow).
    - parameters: List of account parameters to filter the results.
    - start_year: Start year for filtering columns (e.g., 2018).
    - end_year: End year for filtering columns (e.g., 2022).
    """
    logger.debug(
        f"Getting financial data by parameters for company_number={company_number}, "
        f"statement_type={statement_type}, parameters={parameters}, "
        f"start_year={start_year}, end_year={end_year}"
    )

    if statement_type not in ['balance_sheet', 'profit_and_loss', 'cashflow']:
        raise ValueError("Invalid statement type specified.")

    conn = connect_to_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    # Get company details
    cursor.execute("SELECT full_name, ticker FROM company_detail WHERE id = %s", (company_number,))
    company_details = cursor.fetchone()

    if not company_details:
        logger.warning(f"Company with number {company_number} not found")
        raise ValueError(f"Company with number {company_number} not found")

    # Determine which year columns to select
    selected_year_cols = []
    start = start_year - 2000 if start_year else 16
    end = end_year - 2000 if end_year else 25
    for yr in range(start, end + 1):
        selected_year_cols.append(f'mar_{yr}')

    if not selected_year_cols:
        selected_year_cols = [f'mar_{yr}' for yr in range(16, 26)]

    # Build and execute query
    query_cols = ['account'] + selected_year_cols
    query = (
        f"SELECT {', '.join(query_cols)} "
        f"FROM public.{statement_type} "
        f"WHERE company_number = %s AND lower(account) = ANY(%s)"
    )
    logger.debug(f"Executing query: {query}")

    cursor.execute(query, (company_number, parameters,))
    data = cursor.fetchall()
    logger.info(f"Retrieved {len(data)} filtered financial records for {statement_type}")

    cursor.close()
    conn.close()

    # Format headers and data for the response
    headers = ['Account'] + [f"Mar {2000 + int(col.split('_')[1])}" for col in selected_year_cols]
    key_map = {'account': 'Account'}
    for col in selected_year_cols:
        key_map[col] = f"Mar {2000 + int(col.split('_')[1])}"

    formatted_data = [{key_map.get(k, k): v for k, v in row.items()} for row in data]

    return {
        "company_name": company_details['full_name'],
        "ticker": company_details['ticker'],
        "statement_type": statement_type,
        "headers": headers,
        "data": formatted_data
    }


def get_financial_periods() -> List[Dict[str, int]]:
    """
    Get available time periods (years) for financial statements.
    This is a static list based on the database schema columns.
    """
    logger.debug("Getting available financial periods")
    years = list(range(2016, 2026))
    return [{"year": year} for year in years]

if __name__ == "__main__":
    # Example usage
    try:
        financial_data = get_financial_data(1, 'balance_sheet', 2018, 2022)
        print(financial_data)
    except Exception as e:
        logger.error(f"Error retrieving financial data: {e}")

    try:
        financial_data_params = get_financial_data_by_parameters(
            42, 'balance_sheet', ["Accrued Expenses, Total"], 2018, 2022
        )
        print(financial_data_params)
    except Exception as e:
        logger.error(f"Error retrieving financial data by parameters: {e}")