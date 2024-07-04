from pymongo.mongo_client import MongoClient


def get_db():
    client = MongoClient(
        "mongodb+srv://samuelkost:Jmspce52j12@comunio.hhihf3e.mongodb.net/?retryWrites=true&w=majority"
    )
    db = client.test
    return db
