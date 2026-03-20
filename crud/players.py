"""Player-related CRUD operations."""

from pymongo.mongo_client import MongoClient
import pandas as pd
import time

from .base import get_date_range, SEASON_DATE_RANGES


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


def get_current_market_values_bulk(db: MongoClient, player_ids: list) -> dict:
    """Get the most recent market value for multiple players in a single MongoDB aggregation.

    Args:
        db: MongoDB database connection.
        player_ids: List of player IDs (int or str).

    Returns:
        Dictionary mapping player_id (int) -> latest quotedPrice (float/int).
    """
    players = db["Players"]
    int_ids = [int(pid) for pid in player_ids]

    pipeline = [
        {"$match": {"id": {"$in": int_ids}}},
        {
            "$project": {
                "_id": 0,
                "id": 1,
                "latest_price": {"$last": "$price_history.quotedPrice"},
            }
        },
    ]

    result = players.aggregate(pipeline)
    return {doc["id"]: doc.get("latest_price", 0) for doc in result}


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


def get_player_points_df(db: MongoClient, spielzeit: str = "2024/2025"):
    """Get aggregated player points for a given season."""
    players = db["Players"]

    # Derive season_start from SEASON_DATE_RANGES, fall back to safe default
    season_range = SEASON_DATE_RANGES.get(spielzeit)
    if season_range:
        season_start = f"{season_range[0]}T00:00:00+00:00"
        filter_date = season_range[0]
    else:
        season_start = "2000-01-01T00:00:00+00:00"
        filter_date = "2000-01-01"

    # Measure time to define the aggregation pipeline
    start_time = time.time()
    pipeline = [
        {"$unwind": "$point_history"},
        {"$match": {"point_history.matchday.timestamp": {"$gte": season_start}}},
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

    start_time = time.time()
    points = list(points)
    end_time = time.time()
    print(len(points))
    print(
        f"Time to make aggregation result a list: {end_time - start_time:.2f} seconds"
    )

    start_time = time.time()
    df = pd.DataFrame(list(points))
    end_time = time.time()
    print(f"Time to convert result to DataFrame: {end_time - start_time:.2f} seconds")

    start_time = time.time()
    df = df[df["Datum"] > filter_date]
    end_time = time.time()
    print(f"Time to filter entries: {end_time - start_time:.2f} seconds")

    start_time = time.time()
    df["Punkte"] = pd.to_numeric(df["Punkte"], downcast="integer")
    end_time = time.time()
    print(f"Time to make Punkte numeric: {end_time - start_time:.2f} seconds")

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


def get_player_points_with_market_value_df(db: MongoClient, spielzeit: str = "2024/2025"):
    """Get player points data combined with current market value"""
    players = db["Players"]

    # Derive season_start from SEASON_DATE_RANGES
    season_range = SEASON_DATE_RANGES.get(spielzeit)
    if season_range:
        season_start = f"{season_range[0]}T00:00:00+00:00"
    else:
        season_start = "2000-01-01T00:00:00+00:00"

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
        {"$match": {"point_history.matchday.timestamp": {"$gte": season_start}}},
        {
            "$group": {
                "_id": "$id",
                "Spieler": {"$first": "$name"},
                "ID": {"$first": "$id"},
                "Preis": {"$first": "$price"},
                "Aktueller_Marktwert": {"$first": "$latest_market_value"},
                "total_points": {
                    "$sum": {
                        "$toInt": {
                            "$ifNull": ["$point_history.points", 0]
                        }
                    }
                },
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
