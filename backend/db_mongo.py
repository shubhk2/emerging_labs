import os
from pymongo import MongoClient
from typing import List, Dict, Optional
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# MongoDB connection
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "financial_documents")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "classified_texts")

client = None
db = None
collection = None


def get_database():
    """Get MongoDB database connection"""
    global client, db, collection
    if client is None:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
    return collection

def get_tickers():
    """Retrieve distinct tickers from the MongoDB collection"""
    collection = get_database()
    # Get distinct tickers
    tickers = collection.distinct("ticker")
    print(tickers)
def save_to_mongodb(ticker, date, classified_text, source_file=""):
    collection = get_database()
    # Convert date to year/month fields
    year = int(date[:4]) if len(date) >= 4 else 9999
    month = int(date[4:6]) if len(date) >= 6 else 1
    document = {
        "ticker": ticker,
        "year": year,
        "month": month,
        "source_file": source_file,
        "metadata": {
            "classification_model": "our model",
            "classified_on": datetime.now().isoformat()
        }
    }
    # Insert each category into the doc
    for category, details in classified_text.items():
        document[category] = details

    existing = collection.find_one({
        "ticker": ticker, "year": year, "month": month, "source_file": source_file
    })
    if existing:
        collection.update_one({"_id": existing["_id"]}, {"$set": document})
    else:
        collection.insert_one(document)
    return document


def get_classified_data(ticker: Optional[str] = None, date: Optional[str] = None):
    """Retrieve classified data from MongoDB, optionally filtered"""
    collection = get_database()

    # Build query
    query = {}
    if ticker:
        query["ticker"] = ticker
    if date and len(date) >= 6:
        query["year"] = int(date[:4])
        query["month"] = int(date[4:6])

    # Retrieve documents
    results = list(collection.find(query, {"_id": 0}))
    return results
if __name__=="__main__":
    get_tickers()

