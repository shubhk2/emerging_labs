import sys
import os

print("Current working directory:", os.getcwd())
print("sys.path:", sys.path)

from backend.db_setup import connect_to_db
from psycopg2.extras import RealDictCursor


def get_company_overview(company_number):
    """
    Get overview text for the specified company_number
    """
    conn = connect_to_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "SELECT overview_text FROM company_overview WHERE company_number = %s",
                (company_number,)
            )
            result = cursor.fetchone()
            if not result:
                return {"overview_text": "No overview available"}
            return result
    finally:
        conn.close()


def get_company_stats(company_number):
    """
    Fetch metrics from the various *_metrics tables for the given company_number,
    grouped by table name.
    """
    conn = connect_to_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            table_names = [
                "profile_metrics",
                "margins_metrics",
                "returns_5yr_avg_metrics",
                "valuation_ttm_metrics",
                "valuation_ntm_metrics",
                "financial_health_metrics",
                "growth_cagr_metrics"
            ]
            stats = {}  # Initialize as an empty dictionary

            for table in table_names:
                query = f"""
                    SELECT metric, value, unit, as_of
                    FROM {table}
                    WHERE company_number = %s
                """
                cursor.execute(query, (company_number,))
                rows = cursor.fetchall()

                table_values = []
                for row in rows:
                    table_values.append([
                        row["metric"],
                        float(row["value"]) if row["value"] is not None else None,
                        row["unit"] or "",
                        row["as_of"].strftime("%Y-%m-%d") if row["as_of"] else None
                    ])

                # Add data for the current table to the stats dictionary
                # Only add if there are values for that table
                if table_values:
                    stats[table] = {
                        "columns": ["metric", "value", "unit", "as_of"],
                        "values": table_values
                    }
            return stats
    finally:
        conn.close()


if __name__ == "__main__":
    # Example usage
    company_number = 5  # Example company number
    overview = get_company_overview(company_number)
    print("Company Overview:")
    print(overview)

    print("\nCompany Stats:")
    stats = get_company_stats(company_number)
    print(stats)
    import json

    # print(json.dumps(stats, indent=4))
