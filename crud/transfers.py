"""Transfer-related CRUD operations."""

from pymongo.mongo_client import MongoClient
import pandas as pd
from datetime import datetime
import logging

from .base import get_date_range


def get_transfers(db: MongoClient, spielzeit: str = "2024/2025") -> pd.DataFrame:
    date_from, date_to = get_date_range(spielzeit)
    transfers_collection = db["Transfers"]

    transfers = list(
        transfers_collection.find(
            {"buy.date": {"$gte": date_from, "$lte": date_to}}, {"_id": 0}
        )
    )
    transfer_array = []
    for transfer in transfers:
        buy_price = transfer["buy"]["price"]
        sell_price = transfer["sell"]["price"] if transfer["sell"] else None
        buy_date = datetime.strptime(transfer["buy"]["date"], "%Y-%m-%d")
        sell_date = (
            datetime.strptime(transfer["sell"]["date"], "%Y-%m-%d")
            if transfer["sell"]
            else None
        )
        days = (sell_date - buy_date).days if sell_date else None
        profit = sell_price - buy_price if sell_price else None
        profit_percentage = round(profit / buy_price * 100) if profit else None
        profit_per_day = round(profit / days) if profit and days else None
        transfer_array.append(
            {
                "ID": transfer["player_id"],
                "Spieler": transfer["player_name"],
                "Mitspieler": transfer["member_name"],
                "Kaufdatum": buy_date,
                "Kaufpreis": buy_price,
                "Von": transfer["buy"]["from_name"],
                "Verkaufsdatum": sell_date,
                "Verkaufspreis": sell_price,
                "An": transfer["sell"]["to_name"] if transfer["sell"] else None,
                "Gewinn/Verlust": profit,
                "Gewinn %": profit_percentage,
                "Gewinn/Verlust pro Tag": profit_per_day,
            }
        )
    logging.info(transfer_array)
    return pd.DataFrame(transfer_array)


def count_second_bids(db: MongoClient, spielzeit: str = "2024/2025"):
    date_from, date_to = get_date_range(spielzeit)

    transfers_collection = db["Transfers"]
    pipeline = [
        {"$match": {"buy.date": {"$gt": date_from, "$lt": date_to}}},
        {"$group": {"_id": "$buy.second_highest_bidder", "count": {"$sum": 1}}},
        {"$project": {"_id": 0, "second_highest_bidder": "$_id", "count": 1}},
    ]

    results = list(transfers_collection.aggregate(pipeline))
    df = pd.DataFrame(results)
    df = df.dropna(subset=["second_highest_bidder"])
    df = df.sort_values(by="count", ascending=False)
    df.rename(
        columns={"second_highest_bidder": "Mitspieler", "count": "Zweitgebote"},
        inplace=True,
    )
    return df


def count_transfers_buys(db: MongoClient, spielzeit: str = "2024/2025"):
    date_from, date_to = get_date_range(spielzeit)
    transfers_collection = db["Transfers"]
    pipeline = [
        {"$match": {"buy.date": {"$gt": date_from, "$lt": date_to}}},
        {"$group": {"_id": "$member_name", "count": {"$sum": 1}}},
        {"$project": {"_id": 0, "member_name": "$_id", "count": 1}},
    ]

    results = list(transfers_collection.aggregate(pipeline))
    df = pd.DataFrame(results)
    df = df.dropna(subset=["member_name"])
    df = df.sort_values(by="count", ascending=False)
    df.rename(
        columns={"member_name": "Mitspieler", "count": "Transfers"},
        inplace=True,
    )
    return df
