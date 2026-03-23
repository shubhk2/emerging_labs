import logging
from typing import Dict, Any, List
from psycopg2.extras import RealDictCursor
from backend.db_setup import connect_to_db

logger = logging.getLogger(__name__)

def get_pledged_data(company_number: int) -> Dict[str, Any]:
    """
    Returns pledged data for a given company_number, excluding 'id' and 'company_no'.
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

        # Select all columns except id and company_no
        headers = [
            "company_name",
            "total_issued_shares",
            "promoter_shares",
            "percent_promoter",
            "total_public_holding",
            "shares_encumbered",
            "percent_promoter_encumbered",
            "percent_total_encumbered",
            "value_encumbered",
            "disclosure",
            "shares_pledged",
            "total_demat_shares",
            "pledge_demat_percentage",
            "value_pledged"
        ]

        cursor.execute(f"""
            SELECT {', '.join(headers)}
            FROM public.pledged_data
            WHERE company_name = %s
        """, (company_name,))
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
        logger.error(f"Error in get_pledged_data: {e}", exc_info=True)
        return {"error": str(e)}
    finally:
        cursor.close()
        conn.close()