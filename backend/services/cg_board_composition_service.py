import logging
from typing import Dict, Any, List
from psycopg2.extras import RealDictCursor
from backend.db_setup import connect_to_db

logger = logging.getLogger(__name__)


def get_cg_board_composition(company_number: int) -> Dict[str, Any]:
    """
    Returns Board Composition details for a given company_number excluding 'id' and 'company_no'.
    """
    conn = connect_to_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # Fetch company details (take the first row if multiple)
        cursor.execute(
            """
            SELECT full_name, ticker 
            FROM public.company_detail
            WHERE id = %s
            ORDER BY id
            LIMIT 1
            """,
            (company_number,),
        )
        company_details = cursor.fetchone()
        if not company_details:
            logger.warning(f"No company found for company_number={company_number}")
            return {"error": f"No company found for {company_number}"}

        # Fetch board composition data excluding id and company_no
        cursor.execute(
            """
            SELECT director_name, din, pan, category, designation, appointment_date,
                   reappointment_date, cessation_date, tenure, date_of_birth,
                   directorships_in_listed_entities, memberships_in_committees,
                   chairmanships_in_committees, reason_for_cessation
            FROM public.cg_board_composition
            WHERE company_no = %s
            ORDER BY id
            """,
            (company_number,),
        )
        rows = cursor.fetchall()

        headers: List[str] = [
            "director_name",
            "din",
            "pan",
            "category",
            "designation",
            "appointment_date",
            "reappointment_date",
            "cessation_date",
            "tenure",
            "date_of_birth",
            "directorships_in_listed_entities",
            "memberships_in_committees",
            "chairmanships_in_committees",
            "reason_for_cessation",
        ]

        formatted_data: List[Dict[str, Any]] = []
        for row in rows:
            formatted_data.append({key: row.get(key) for key in headers})

        return {
            "company_name": company_details["full_name"],
            "ticker": company_details["ticker"],
            "headers": headers,
            "data": formatted_data,
        }
    except Exception as e:
        logger.error(f"Error in get_cg_board_composition: {e}", exc_info=True)
        return {"error": str(e)}
    finally:
        cursor.close()
        conn.close()

