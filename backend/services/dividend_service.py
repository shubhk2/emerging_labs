import psycopg2
from psycopg2.extras import RealDictCursor
from backend.db_setup import connect_to_db

def get_dividend_data(company_number: int):
    """
    Returns dividend details for the given company_number, including company full_name and ticker.
    Data is formatted and mapped to keys in headers, similar to shareholding_pattern_service.py.
    """
    conn = connect_to_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    # Fetch company full_name and ticker
    cursor.execute("SELECT full_name, ticker FROM company_detail WHERE id = %s", (company_number,))
    company = cursor.fetchone()
    if not company:
        cursor.close()
        conn.close()
        raise ValueError("Company number not found in database.")

    # Fetch dividend data
    cursor.execute("""
        SELECT
            symbol, company_name, series, purpose, face_value,
            ex_date, record_date,
            book_closure_start_date, book_closure_end_date
        FROM dividend
        WHERE company_no = %s
        """, (company_number,))
    dividend_rows = cursor.fetchall()
    cursor.close()
    conn.close()

    headers = [
        "Symbol", "Company Name", "Series", "Purpose", "Face Value",
        "Declaration Date", "Record Date",
        "Book Closure Start Date", "Book Closure End Date"
    ]

    # Format data to map to headers
    formatted_data = []
    for row in dividend_rows:
        formatted_data.append({
            "Symbol": row["symbol"],
            "Company Name": row["company_name"],
            "Series": row["series"],
            "Purpose": row["purpose"],
            "Face Value": row["face_value"],
            "Declaration Date": row["ex_date"],
            "Record Date": row["record_date"],
            "Book Closure Start Date": row["book_closure_start_date"],
            "Book Closure End Date": row["book_closure_end_date"]
        })

    return {
        "company_name": company["full_name"],
        "ticker": company["ticker"],
        "headers": headers,
        "data": formatted_data
    }


if __name__ == "__main__":
    # Example usage
    try:
        company_number = 42 # Replace with a valid company number
        file_id = get_dividend_data(company_number)
        print(f"File ID for company number {company_number}: {file_id}")
    except Exception as e:
        print(f"Error: {e}")