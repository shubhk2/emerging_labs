import os
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()

def connect_to_db():
    """Connect to PostgreSQL database using environment variables."""
    conn_string = os.getenv("POSTGRES_URL")
    conn = psycopg2.connect(conn_string)
    return conn

def sanitize_column_name(col):
    """Sanitize column name for SQL - replace special chars with underscores"""
    sql_col = ''.join(c if c.isalnum() else '_' for c in col).lower()
    # Avoid duplicate underscores
    while '__' in sql_col:
        sql_col = sql_col.replace('__', '_')
    # Remove leading/trailing underscores
    return sql_col.strip('_')
#

#
# def main():
#     """Main function to set up database and load data"""
#     try:
#         # Connect to database
#         conn = connect_to_db()
#         print("Connected to PostgreSQL database")
#
#         # Create tables
#         create_tables(conn)
#
#         # Load data
#         load_data(conn)
#
#         # Close connection
#         conn.close()
#         print("Database setup complete!")
#
#     except Exception as e:
#         print(f"Error: {str(e)}")
#
# if __name__ == "__main__":
#     main()


    # map_company_details()