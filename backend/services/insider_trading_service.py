import logging
from typing import Dict, Any, List
from psycopg2.extras import RealDictCursor
from backend.db_setup import connect_to_db

logger = logging.getLogger(__name__)

def get_insider_trading_data(company_number: int) -> Dict[str, Any]:
    """
    Returns insider trading data for a given company_number, excluding 'id' and 'company_no'.
    """
    conn = connect_to_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # Get company name from company_detail
        cursor.execute("""
            SELECT full_name FROM public.company_detail WHERE id = %s LIMIT 1
        """, (company_number,))
        company = cursor.fetchone()
        if not company:
            logger.warning(f"No company found for company_number={company_number}")
            return {"error": f"No company found for {company_number}"}
        company_name = company["full_name"]

        headers = [
            "company_name",
            "person_name",
            "category_of_person",
            "security_type",
            "buy_shares_num",
            "buy_shares_pct",
            "buy_shares_value",
            "sale_shares_num",
            "sale_shares_pct",
            "sale_shares_value",
            "pledge_invocation_num",
            "pledge_invocation_pct",
            "pledge_invocation_value",
            "pledge_creation_num",
            "pledge_creation_pct",
            "pledge_creation_value",
            "pledge_release_num",
            "pledge_release_pct",
            "pledge_release_value"
        ]

        cursor.execute(f"""
            SELECT {', '.join(headers)}
            FROM public.pit
            WHERE company_no = %s
        """, (company_number,))
        rows = cursor.fetchall()

        formatted_data: List[Dict[str, Any]] = []
        for row in rows:
            formatted_data.append({key: row.get(key) for key in headers})

        return {
            "company_name": company_name,
            "headers": headers,
            "data": formatted_data,
        }
    except Exception as e:
        logger.error(f"Error in get_insider_trading_data: {e}", exc_info=True)
        return {"error": str(e)}
    finally:
        cursor.close()
        conn.close()