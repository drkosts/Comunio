from pymongo.mongo_client import MongoClient
import time
from pymongo.collection import Collection
import pandas as pd
from datetime import datetime
import logging
from typing import Dict, Tuple

# Season date ranges configuration
SEASON_DATE_RANGES: Dict[str, Tuple[str, str]] = {
    "2024/2025": ("2024-07-01", "2025-06-30"),
    "2023/2024": ("2023-06-01", "2024-06-30"),
    "2025/2026": ("2025-06-30", "2026-06-30"),
}

DEFAULT_DATE_RANGE = ("2000-01-01", "2030-01-01")

def get_date_range(spielzeit: str) -> Tuple[str, str]:
    """Get date range for a given spielzeit (season)"""
    return SEASON_DATE_RANGES.get(spielzeit, DEFAULT_DATE_RANGE)


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


def get_player_market_value(db: MongoClient, player_id: str):
    players = db["Players"]
    player = players.find_one({"id": int(player_id)})
    if not player:
        return None
    price_history = player["price_history"]
    data = {
        "Datum": [entry["timestamp"] for entry in price_history],
        "Marktwert": [entry["quotedPrice"] for entry in price_history],
    }
    df = pd.DataFrame(data)
    # Fix: Add utc=True to handle mixed timezones properly
    df["Datum"] = pd.to_datetime(df["Datum"], utc=True)
    df.sort_values(by="Datum", inplace=True)
    return df


def get_player_market_values_df(db: MongoClient):
    players = db["Players"]

    # Define the aggregation pipeline
    pipeline = [
        {"$unwind": "$price_history"},
        {
            "$project": {
                "Datum": "$price_history.timestamp",
                "Marktwert": "$price_history.quotedPrice",
                "Spieler": "$name",
                "ID": "$id",
            }
        },
    ]

    # Execute the aggregation pipeline
    result = list(players.aggregate(pipeline))

    # Convert the result to a DataFrame
    df = pd.DataFrame(result)

    return df


def get_player_points_df(db: MongoClient):
    players = db["Players"]

    # Measure time to define the aggregation pipeline
    start_time = time.time()
    pipeline = [
        {"$unwind": "$point_history"},
        {"$match": {"point_history.matchday.timestamp": {"$gt": "2024-07-12"}}},
        {
            "$project": {
                "Datum": "$point_history.matchday.timestamp",
                "Punkte": "$point_history.points",
                "Spieler": "$name",
                "ID": "$id",
                "Preis": "$price",
            }
        },
    ]
    end_time = time.time()
    print(f"Time to define aggregation pipeline: {end_time - start_time:.2f} seconds")

    # Measure time to execute the aggregation pipeline
    start_time = time.time()
    points = players.aggregate(pipeline)
    end_time = time.time()
    print(f"Time to execute aggregation pipeline: {end_time - start_time:.2f} seconds")

    # Measure time to execute the aggregation pipeline
    start_time = time.time()
    points = list(points)
    end_time = time.time()
    print(len(points))
    print(
        f"Time to make aggregation result a list: {end_time - start_time:.2f} seconds"
    )

    # Measure time to convert the result to a DataFrame
    start_time = time.time()
    df = pd.DataFrame(list(points))
    end_time = time.time()
    print(f"Time to convert result to DataFrame: {end_time - start_time:.2f} seconds")

    # Measure time to filter entries
    start_time = time.time()
    df = df[df["Datum"] > "2024-07-01"]
    end_time = time.time()
    print(f"Time to filter entries: {end_time - start_time:.2f} seconds")

    # Measure time to make Punkte numeric
    start_time = time.time()
    df["Punkte"] = pd.to_numeric(df["Punkte"], downcast="integer")
    end_time = time.time()
    print(f"Time to make Punkte numeric: {end_time - start_time:.2f} seconds")

    # Measure time to group by ID and aggregate points
    start_time = time.time()
    df = (
        df.groupby(["ID", "Spieler", "Preis"])["Punkte"]
        .agg(["sum", "count"])
        .rename(columns={"sum": "Punkte", "count": "Spiele"})
        .reset_index()
    )
    end_time = time.time()
    print(
        f"Time to group by ID and aggregate points: {end_time - start_time:.2f} seconds"
    )

    # Measure time to calculate points per game
    start_time = time.time()
    df["PpS"] = round(df["Punkte"] / df["Spiele"], 2)
    end_time = time.time()
    print(f"Time to calculate points per game: {end_time - start_time:.2f} seconds")

    return df


def get_player_points(db: MongoClient, player_id: str):
    players = db["Players"]
    player = players.find_one({"id": int(player_id)})
    if not player:
        return None
    points_history = player["point_history"]
    data = {
        "Datum": [entry["matchday"]["timestamp"] for entry in points_history],
        "Punkte": [
            int(entry["points"]) if entry["points"] else 0 for entry in points_history
        ],
        "Spieltag": [entry["matchday"]["key"] for entry in points_history],
    }
    df = pd.DataFrame(data)
    df["Datum"] = pd.to_datetime(df["Datum"])
    df.sort_values(by="Datum", inplace=True)
    return df


def get_player_points_between_dates(
    db: MongoClient,
    player_id: str,
    start_date: str,
    end_date: str = pd.to_datetime("2030-01-01").date().strftime("%Y-%m-%d"),
):
    start_date = pd.to_datetime(start_date).strftime("%Y-%m-%d")
    end_date = (
        pd.to_datetime(end_date).strftime("%Y-%m-%d")
        if pd.notna(end_date)
        else "2030-01-01"
    )
    # Get the PlayerPoints collection
    player_points_collection = db["PlayerPoints"]

    # Define the query to fetch points between the dates
    pipeline = [
        {"$match": {"player_id": player_id}},  # Match the player_id
        {"$unwind": "$point_history"},  # Unwind the matchdays array
        {
            "$match": {
                "point_history.matchday.timestamp": {
                    "$gte": start_date,
                    "$lte": end_date,
                }
            }
        },  # Match the date range
        {
            "$group": {
                "_id": "$player_id",
                "total_points": {"$sum": "$point_history.points"},
                "matchdays_count": {"$sum": 1},
            }
        },
        {"$project": {"_id": 0, "total_points": 1, "matchdays_count": 1}},
    ]

    # Fetch the points data
    results = list(player_points_collection.aggregate(pipeline))

    if not results:
        return 0, 0

    # Extract the total points and matchdays count
    total_points = results[0]["total_points"]
    matchdays_count = results[0]["matchdays_count"]

    return total_points, matchdays_count


def get_player_current_market_values_df(db: MongoClient):
    """Get the current (most recent) market value for each player"""
    players = db["Players"]

    pipeline = [
        {
            "$project": {
                "id": 1,
                "name": 1,
                # Get the last element of price_history array (most recent market value)
                "latest_price": {"$last": "$price_history.quotedPrice"},
                "latest_timestamp": {"$last": "$price_history.timestamp"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "ID": "$id",
                "Spieler": "$name",
                "Aktueller_Marktwert": "$latest_price",
                "Letztes_Update": "$latest_timestamp"
            }
        }
    ]

    result = list(players.aggregate(pipeline))
    df = pd.DataFrame(result)
    
    if not df.empty:
        df["Letztes_Update"] = pd.to_datetime(df["Letztes_Update"])
    
    return df


def get_player_points_with_market_value_df(db: MongoClient):
    """Get player points data combined with current market value"""
    players = db["Players"]

    # Optimized pipeline that combines both operations without memory-intensive sorting
    pipeline = [
        {
            "$project": {
                "id": 1,
                "name": 1,
                "price": 1,
                "point_history": 1,
                # Get the last element of price_history array (most recent market value)
                "latest_market_value": {"$last": "$price_history.quotedPrice"}
            }
        },
        {"$unwind": "$point_history"},
        {"$match": {"point_history.matchday.timestamp": {"$gt": "2024-07-12"}}},
        {
            "$group": {
                "_id": "$id",
                "Spieler": {"$first": "$name"},
                "ID": {"$first": "$id"},
                "Preis": {"$first": "$price"},
                "Aktueller_Marktwert": {"$first": "$latest_market_value"},
                "total_points": {"$sum": "$point_history.points"},
                "games_count": {"$sum": 1}
            }
        },
        {
            "$project": {
                "_id": 0,
                "ID": 1,
                "Spieler": 1,
                "Preis": 1,
                "Aktueller_Marktwert": 1,
                "Punkte": "$total_points",
                "Spiele": "$games_count",
                "PpS": {
                    "$round": [
                        {"$divide": ["$total_points", "$games_count"]}, 
                        2
                    ]
                }
            }
        }
    ]
    
    # Execute the optimized pipeline
    result = list(players.aggregate(pipeline))
    df = pd.DataFrame(result)
    
    if not df.empty:
        df["Punkte"] = pd.to_numeric(df["Punkte"], downcast="integer")
    
    return df
