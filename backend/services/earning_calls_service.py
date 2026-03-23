import os
from backend.db_setup import connect_to_db
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "financial_documents")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "classified_texts")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

def get_earning_call_file(company_number: int, quarter: int, year: int = 2025):
    # Get ticker from SQL DB
    conn = connect_to_db()
    cursor = conn.cursor()
    cursor.execute("SELECT ticker FROM company_detail WHERE id = %s", (company_number,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    if not row:
        return None
    ticker = row[0]
    quarter_str = f"Q{quarter}"
    # Query MongoDB
    doc = collection.find_one({
        "ticker": ticker.upper(),
        "quarter": quarter_str,
        "year": year
    }, {"_id": 0})
    return doc

if __name__ == "__main__":
    # Example usage
    company_number = 4  # Replace with actual company number
    quarter = 4  # Replace with actual quarter
    year = 2025  # Replace with actual year
    earning_call = get_earning_call_file(company_number, quarter, year)
    if earning_call:
        print(f"Earning call for {earning_call['ticker']} - {earning_call['quarter']} {earning_call['year']}:")
        print(earning_call['text'])
    else:
        print("Earning call not found.")

