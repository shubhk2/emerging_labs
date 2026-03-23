from backend.db_setup import connect_to_db
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Any, Optional
import logging

# Set up logger
logger = logging.getLogger(__name__)


def search_companies(query: str, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Searches for companies by name or ticker.
    """
    logger.debug(f"Searching companies with query: '{query}', limit: {limit}")
    conn = connect_to_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        search_pattern = f"%{query}%"
        cursor.execute(
            """
            SELECT id, full_name, ticker
            FROM public.company_detail
            WHERE lower(full_name) ILIKE lower(%s) OR lower(ticker) ILIKE lower(%s)
            ORDER BY full_name
            LIMIT %s
            """,
            (search_pattern, search_pattern, limit)
        )
        results = cursor.fetchall()
        logger.info(f"Company search found {len(results)} matches for query '{query}'")
        return results
    except Exception as e:
        logger.error(f"Error searching companies: {str(e)}", exc_info=True)
        raise
    finally:
        cursor.close()
        conn.close()


def search_parameters(query: str, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Searches for financial parameters and ratios across all relevant tables.
    """
    logger.debug(f"Searching parameters with query: '{query}', limit: {limit}")
    conn = connect_to_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        search_pattern = f"%{query}%"
        query_sql = """
            WITH all_parameters AS (
                SELECT DISTINCT account AS parameter, 'profit_and_loss' AS source_table
                FROM public.profit_and_loss WHERE lower(account) ILIKE lower(%s)
                UNION
                SELECT DISTINCT account AS parameter, 'balance_sheet' AS source_table
                FROM public.balance_sheet WHERE lower(account) ILIKE lower(%s)
                UNION
                SELECT DISTINCT account AS parameter, 'cashflow' AS source_table
                FROM public.cashflow WHERE lower(account) ILIKE lower(%s)
                UNION
                SELECT DISTINCT name AS parameter, 'financial_ratios' AS source_table
                FROM public.financial_ratios WHERE lower(name) ILIKE lower(%s)
            )
            SELECT * FROM all_parameters
            ORDER BY parameter
            LIMIT %s;
        """
        cursor.execute(query_sql, (search_pattern, search_pattern, search_pattern, search_pattern, limit))
        results = cursor.fetchall()
        logger.info(f"Parameter search found {len(results)} total matches for query '{query}'")
        return results
    except Exception as e:
        logger.error(f"Error searching parameters: {str(e)}", exc_info=True)
        raise
    finally:
        cursor.close()
        conn.close()


def search_company_by_id(company_id: int) -> Optional[Dict[str, Any]]:
    """
    Retrieves a single company by its primary key ID.
    """
    logger.debug(f"Searching company with ID: {company_id}")
    conn = connect_to_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(
            """
            SELECT id, full_name, ticker
            FROM public.company_detail
            WHERE id = %s
            """,
            (company_id,)
        )
        result = cursor.fetchone()
        if result:
            logger.info(f"Company found for ID {company_id}")
        else:
            logger.info(f"No company found for ID {company_id}")
        return result
    except Exception as e:
        logger.error(f"Error searching company by ID: {str(e)}", exc_info=True)
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    # Configure logging for standalone execution
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Example usage
    logger.info("Testing search functionality")
    print("--- Searching company by ID 90 ---")
    print(search_company_by_id(90))
    print("\n--- Searching companies for 'Adani' ---")
    print(search_companies("Adani"))
    print("\n--- Searching companies for ticker 'RELI' ---")
    print(search_companies("RELI"))
    print("\n--- Searching parameters for 'debt' ---")
    print(search_parameters("debt"))