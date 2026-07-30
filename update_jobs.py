"""Self-contained Wrapper um die Comunio-API + Mongo-Writes.

Spiegelt die Logik aus ``backend/structure_data_utils.py`` (Player- und
Transfer-Updates) sowie ``backend/services.login_to_comunio``. Diese Datei
hält das Streamlit-Deployment unabhängig vom ``backend/``-Ordner — die
Funktionen können direkt aus ``modules/admin.py`` aufgerufen werden, ohne
dass der Streamlit-Cloud-Runner Zugriff auf den Backend-Code braucht.

Fortschritt wird über einen optionalen ``log``-Callable ausgegeben
(typischerweise ein ``st.empty()``-Container, der per ``.text(...)``
aktualisiert wird).
"""

from datetime import datetime

import requests
from pymongo.errors import DuplicateKeyError


# Comunio-Konstanten — Community/User-IDs aus backend/structure_data.py.
COMUNIO_API = "https://comunio.de/api"
COMMUNITY_ID = "857661"
USER_ID = "5763843"


def login_to_comunio(username: str, password: str) -> str:
    """Loggt sich bei Comunio ein und liefert das access_token (Bearer).

    Raises:
        requests.HTTPError: Bei falschen Credentials oder API-Fehlern.
    """
    response = requests.post(
        f"{COMUNIO_API}/login",
        data={"username": username, "password": password},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def _make_request(url: str, token: str) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def refresh_players(db, token: str, log=print) -> dict:
    """Holt Spielerliste + Price/Point-History und merged in Players.

    Spiegelt ``backend/structure_data_utils.process_players_information``.
    Schreibt pro Spieler ~3 sequenzielle Requests → bei ~800 Spielern
    dauert das einige Minuten; in Streamlit Cloud auf das Timeout achten.
    """
    players = db.get_collection("Players")
    players_list = _make_request(
        f"https://www.comunio.de/api/communities/{COMMUNITY_ID}"
        f"/players?start=0&limit=800",
        token,
    )
    players_comunio = players_list["tradables"]
    total = len(players_comunio)
    log(f"Players: {total} Spieler zu aktualisieren")

    for i, player in enumerate(players_comunio, start=1):
        player_info = _make_request(
            f"https://www.comunio.de/api/communities/{COMMUNITY_ID}"
            f"/users/{USER_ID}/players/{player['id']}",
            token,
        )
        price_history = _make_request(
            f"https://www.comunio.de/api/players/{player['id']}/quote-history",
            token,
        )
        points_history = _make_request(
            f"https://www.comunio.de/api/players/{player['id']}"
            f"/match-statistics-history",
            token,
        )

        # Existierende History holen und deduplizieren.
        existing_player = players.find_one({"id": player["id"]})
        existing_price_history = (
            existing_player.get("price_history", []) if existing_player else []
        )
        existing_price_history.sort(key=lambda x: x["timestamp"])

        existing_timestamps = {
            datetime.strptime(e["timestamp"][:10], "%Y-%m-%d")
            for e in existing_price_history
        }

        prev_date = None
        for entry in existing_price_history:
            current_date = datetime.strptime(entry["timestamp"][:10], "%Y-%m-%d")
            if prev_date == current_date:
                existing_price_history.remove(entry)
            prev_date = current_date

        new_price_history = [
            e for e in price_history["quoteCollection"]
            if datetime.strptime(e["timestamp"][:10], "%Y-%m-%d")
            not in existing_timestamps
        ]
        combined_price_history = existing_price_history + new_price_history

        existing_points_history = (
            existing_player.get("point_history", []) if existing_player else []
        )
        existing_matchdays = {
            e["matchday"]["id"] for e in existing_points_history
        }
        new_points_history = [
            e for e in points_history["matchStatisticsCollection"]
            if e["matchday"]["id"] not in existing_matchdays
        ]
        combined_points_history = existing_points_history + new_points_history

        player_info["price_history"] = combined_price_history
        player_info["point_history"] = combined_points_history

        players.update_one(
            {"id": player["id"]}, {"$set": player_info}, upsert=True
        )
        if i % 25 == 0 or i == total:
            log(f"  Players: {i}/{total} verarbeitet")

    log(f"Players: fertig — {total} Spieler aktualisiert")
    return {"players_updated": total}


def refresh_transfers(db, log=print) -> dict:
    """Liest neue Transactions und merged sie in die Transfers-Collection.

    Spiegelt ``backend/structure_data_utils.process_transfer_raw_data``.
    Kein Token nötig (Rohdaten liegen schon in ``TRANSACTION_TRANSFER``).
    """
    transactions = db.get_collection("TRANSACTION_TRANSFER")
    transfers = db.get_collection("Transfers")
    members = db.get_collection("Members")

    latest_transfer = transfers.find_one(sort=[("buy.date", -1)])
    if latest_transfer is None:
        log("Transfers: noch keine Transfers in DB — Abbruch")
        return {"transfers_added": 0}
    latest_transfer_date = latest_transfer["buy"]["date"]

    documents = list(
        transactions.find({"date": {"$gte": latest_transfer_date}}).sort("date", 1)
    )
    total = len(documents)
    log(f"Transfers: {total} neue Dokumente zu verarbeiten")

    added = 0
    message_types = ["FROM_COMPUTER", "TO_COMPUTER", "BETWEEN_USERS"]
    for document in documents:
        date = document["date"]
        for message_type in message_types:
            message_content = document["message"].get(message_type)
            if message_content is None:
                continue
            for transfer in message_content:
                player_id = transfer["tradable"]["id"]
                player_name = transfer["tradable"]["name"]

                if message_type == "FROM_COMPUTER":
                    member_id = transfer["to"]["id"]
                    member_name = transfer["to"]["name"]
                    buy = {
                        "datetime": date,
                        "date": date[:10],
                        "price": transfer["price"],
                        "from_name": transfer["from"]["name"],
                        "from_id": transfer["from"]["id"],
                    }
                    sell = None
                    if transfer["secondHighestBid"].get("name") is not None:
                        buy["second_highest_bid"] = transfer["secondHighestBid"]["price"]
                        buy["second_highest_bidder"] = transfer["secondHighestBid"]["name"]
                        buyer_doc = members.find_one(
                            {"firstName": transfer["secondHighestBid"]["name"]}
                        )
                        if buyer_doc:
                            buy["second_highest_bidder_id"] = buyer_doc["id"]
                    try:
                        transfers.insert_one({
                            "player_id": player_id,
                            "player_name": player_name,
                            "member_id": member_id,
                            "member_name": member_name,
                            "buy": buy,
                            "sell": sell,
                        })
                        added += 1
                    except DuplicateKeyError:
                        pass

                elif message_type == "TO_COMPUTER":
                    member_id = transfer["from"]["id"]
                    existing_transfer_entry = transfers.find_one({
                        "member_id": member_id,
                        "player_id": player_id,
                        "sell": None,
                        "buy.date": {"$gt": "2024-07-10"},
                    })
                    if existing_transfer_entry is None:
                        continue
                    sell = {
                        "datetime": date,
                        "date": date[:10],
                        "price": transfer["price"],
                        "to_name": transfer["to"]["name"],
                        "to_id": transfer["to"]["id"],
                    }
                    transfers.update_one(
                        {"_id": existing_transfer_entry["_id"]},
                        {"$set": {"sell": sell}},
                    )

                elif message_type == "BETWEEN_USERS":
                    # Buy-Seite des Käufers anlegen
                    member_id = transfer["to"]["id"]
                    member_name = transfer["to"]["name"]
                    buy = {
                        "datetime": date,
                        "date": date[:10],
                        "price": transfer["price"],
                        "from_name": transfer["from"]["name"],
                        "from_id": transfer["from"]["id"],
                    }
                    try:
                        transfers.insert_one({
                            "player_id": player_id,
                            "player_name": player_name,
                            "member_id": member_id,
                            "member_name": member_name,
                            "buy": buy,
                            "sell": None,
                        })
                        added += 1
                    except DuplicateKeyError:
                        pass

                    # Sell-Seite des Verkäufers aktualisieren
                    existing_transfer_entry = transfers.find_one({
                        "member_id": transfer["from"]["id"],
                        "player_id": player_id,
                        "sell": None,
                    })
                    if not existing_transfer_entry:
                        continue
                    sell = {
                        "datetime": date,
                        "date": date[:10],
                        "price": transfer["price"],
                        "to_name": transfer["to"]["name"],
                        "to_id": transfer["to"]["id"],
                    }
                    transfers.update_one(
                        {"_id": existing_transfer_entry["_id"]},
                        {"$set": {"sell": sell}},
                    )

    log(f"Transfers: fertig — {added} neue Buy-Transfers eingefügt")
    return {"transfers_added": added}
