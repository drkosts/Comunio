from pymongo.mongo_client import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()
mongo_uri = os.getenv("MONGO_URI")

# Singleton MongoClient instance for connection pooling
_client: MongoClient | None = None


def get_client() -> MongoClient:
    """Return the singleton MongoClient, creating it if necessary."""
    global _client
    if _client is None:
        _client = MongoClient(mongo_uri)
    return _client


def get_db():
    """Return the default database from the shared MongoClient."""
    client = get_client()
    db = client.test
    return db

