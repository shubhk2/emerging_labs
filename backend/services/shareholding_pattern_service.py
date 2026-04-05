import logging
from typing import Dict, Any, List
from psycopg2.extras import RealDictCursor
from backend.db_setup import connect_to_db

logger = logging.getLogger(__name__)

# Commenting out the old function returning a GDrive link:
# def get_shareholding_pattern_file_id(company_number: int) -> str:
#    """
#    Returns a string representing the file id for the given company_number (shareholding pattern).
#    You can implement your own logic or mapping here.
#    """
#    conn = connect_to_db()
#    cursor = conn.cursor()
#    cursor.execute("SELECT ticker FROM company_overview WHERE company_number = %s", (company_number,))
#    ticker = cursor.fetchone()
#    # Example mapping (replace with your actual mapping)
#    tl =  {
#            "ADANIPORTS": "1lPVJDIxQpM7JGvWMuOMAk0mEi4B5TxpR",
#            "ADANIPOWER": "1xPZG5S2ry4l74tF4r887d46LAP8vdV0L",
#            "APOLLOHOSP": "1t1vc0psFPO6Q0K80cnYF9RJSR_FqoBvl",
#            "ASIANPAINT": "1Q7riXe7RgCb4A2KNkRrxe9Z4iIK8yL5F",
#            "AUBANK": "1jEYh5oJ2k9K-Uuj2CnDAniQEjR82eczz",
#            "AXISBANK": "1M9e5_1n23wrYRYXYVwcNqRGUpKgJy9F6",
#            "BAJAJ-AUTO": "1zPp5aUj7FZocw7L6sneqzQhHateXbep2",
#            "BAJAJFINSV": "1W-zkEWQ1oQpWSYBBLbf-4I8cfRtlUsOp",
#            "BEL": "1eueqljAe4wa2uUwqHBvTfIQmM7mVIGPg",
#            "BHARTIARTL": "1ofuuO-m2IkRv25qTdLo1lmjteearPmlZ",
#            "CIPLA": "16IOwXVZ4IXOnFEw_u7cdegpbHGKwKyx3",
#            "COALINDIA": "1JfGOnPJegBE8z5RVPbWoGOAZB-CjzLtP",
#            "DRREDDY": "1x1zHwDpxsb2SvcMLO-m6s09_7kTjS5Wx",
#            "EICHERMOT": "1KET9GU4XTwO9i3fK9iQnpx28BMxbANhP",
#            "GRASIM": "1TvMYdfNQtMZ9_u80mihJwfNb430FH2M8",
#            "HCLTECH": "1ikuEy_XzcpBfeqRrxlrXJ4i68vnVPht1",
#            "HDFCBANK": "1yfKx8vkNTfwuA-tB8lDeg0b-XFrOhS1B",
#            "HDFCLIFE": "1SklBtcKOeAP-WJpugdrz4lvGjgwhp43U",
#            "HEROMOTOCO": "16Haf_iw4bsg-ZmK6qU8qHir7oFspl_wR",
#            "HINDALCO": "1v73hi95xNzZ3Q2h74_DdwNdxvTlVriPf",
#            "HINDUNILVR": "1O7RgRvCskOW2OswPWk9FHvcXdBfWmCq6",
#            "ICICIBANK": "1-yMG4mqs0TuZ9BYcDWyqeWPB0N3aqY_W",
#            "INDUSINDBK": "1P-esQ-OhcS_kxdKlhVl9S6VpgC3BzYT2",
#            "INFY": "1rq_mW5oSlN_vWJ1s7PwWS665wGspQc5E",
#            "ITC": "1liEcdg6gYQjgfe6wjXx-iM7H9HbKCenZ",
#            "JIOFIN": "1CK5YWrWicnYmm7e2_onnqc6DBvHGKFzq",
#            "JSWSTEEL": "1E_iVRFqiH6Y2oBeFc_1cROZvy5GHwKIH",
#            "KOTAKBANK": "1QpLdoAR30K0ktG3qQZgZ_ckOred18p0G",
#            "LT": "1ow8zw_SIcxktrK7tq17rIfl5QO27bgvi",
#            "M&M": "1j_JwQSZ7nWLdn5h5a1b-w_k19jPEqtG5",
#            "MARUTI": "1KKCJrrJOXUN3TvvgCHHEsp3k7AxqNC6J",
#            "NESTLEIND": "1PD_5MXtTbaApO25s9zPZSPkvOkm3oSFe",
#            "NTPC": "1KQ4SF--aqCn3wvubrMP4b2ImDrBLF_LM",
#            "ONGC": "1WWRSWXyQVhYYEaPS2dCUcQ9FaUXHdPLS",
#            "POWERGRID": "19ZjjXcu-IOFbLONfxPagnE7U-5b_TA_H",
#            "RELIANCE": "1Pt69R294vDW7LCFaH2CHAGFprATtB3es",
#            "SBILIFE": "1qNc0hwaLxNhYdxi5l0VoWPuZIHlvlVhv",
#            "SBIN": "1my1AEaO8C826f1X3Ud0TYIsD8fGTxfuP",
#            "SHRIRAMFIN": "1cJ8XSu-HzdxEoNNiRtIjhqwcUK0-r29b",
#            "SUNPHARMA": "1Hm2z2Yq0VuJElCo77oN69kwYDQroWQkO",
#            "TATACONSUM": "1r2MC6aj1HvxPx8fCQzQ0ND3UE9-NQqbk",
#            "TATAMOTORS": "1YL9MiRS-DUQxLHOhF_efHoNMnGhHA8MA",
#            "TATASTEEL": "1_QXSQMTGjqpEz5XzBYP19fM4Hlh-9JIn",
#            "TCS": "1fwOncZ7DhYLmn__ciHBHYf4oalxiUDhp",
#            "TECHM": "1CrYRDMYSonAOxKg2Ms3fraIaQFgeqTWL",
#            "TITAN": "1Z-hD0xzhscyHFeAdc32oEl2QvJ_DBO8V",
#            "TRENT": "1sv7MH9eAfuBQ13_WB2f6BMlb-XG8UhQ3",
#            "ULTRACEMCO": "1fJrtXSa0W5Q9u19qKhSM6R4ARVP0n54C",
#            "WIPRO": "1Ksvcr7_gCw5MgwE6lR5lceNDofg39mHo"
#        }


def get_shareholding_data(company_number: int) -> Dict[str, Any]:
    """
    Returns shareholding details for a given company_number (excluding 'context' column).
    """
    conn = connect_to_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # Fetch company details (take the first row if multiple)
        cursor.execute("""
            SELECT full_name, ticker 
            FROM public.company_detail
            WHERE id = %s
            ORDER BY id
            LIMIT 1
        """, (company_number,))
        company_details = cursor.fetchone()
        if not company_details:
            logger.warning(f"No company found for company_number={company_number}")
            return {"error": f"No company found for {company_number}"}

        # Fetch shareholding data; skip context column
        cursor.execute("""
            SELECT investor, date, owned, marketvalue, shares, chgshares, chgsharesperc, portfolioperc
            FROM public.share_holder
            WHERE company_no = %s
            ORDER BY id
        """, (company_number,))
        rows = cursor.fetchall()

        # Prepare headers
        headers: List[str] = [
            "Investor", "Date", "Owned", "MarketValue",
            "Shares", "ChgShares", "ChgShares%", "Portfolio%"
        ]

        # Format data
        formatted_data = []
        for row in rows:
            formatted_data.append({
                "Investor": row["investor"],
                "Date": row["date"],
                "Owned": row["owned"],
                "MarketValue": row["marketvalue"],
                "Shares": row["shares"],
                "ChgShares": row["chgshares"],
                "ChgShares%": row["chgsharesperc"],
                "Portfolio%": row["portfolioperc"]
            })

        return {
            "company_name": company_details["full_name"],
            "ticker": company_details["ticker"],
            "headers": headers,
            "data": formatted_data
        }
    except Exception as e:
        logger.error(f"Error in get_shareholding_data: {e}", exc_info=True)
        return {"error": str(e)}
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # Example usage
    try:
        company_number7 = 90  # Replace with a valid company number
        file_id = get_shareholding_data(company_number7)
        print(f"Shareholding Pattern Data for company number {company_number7}: {file_id}")
    except Exception as e:
        print(f"Error: {e}")
