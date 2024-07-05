from pymongo.mongo_client import MongoClient
import os

mongo_uri = os.getenv("MONGO_URI")


def get_db():
    client = MongoClient(mongo_uri)
    db = client.test
    return db
