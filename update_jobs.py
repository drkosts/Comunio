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

from datetime import datetime, timedelta

import requests
from pymongo.errors import DuplicateKeyError


# Comunio-Konstanten — Community/User-IDs aus backend/structure_data.py.
COMUNIO_API = "https://comunio.de/api"
COMMUNITY_ID = "857661"
USER_ID = "5763843"

# News-Rückblick für die Cron-Pipeline. 10 Tage deckt Wochenenden ab und
# holt typische Bot-/Cron-Fehlläufe selbständig nach. Duplikate sind über
# die News-ID als Upsert-Schlüssel abgefangen, daher re-runsicher.
NEWS_DAYS_BACK = 10

# Sicherheitsnetz gegen Endlos-Pagination (z.B. wenn die API einmal einen
# Gruppen-Datums-Parser-Edge-Case trifft).
NEWS_MAX_PAGES = 200

# Cutoff für die Verarbeitung der Roh-Transfers — analog zu NEWS_DAYS_BACK.
TRANSFER_DAYS_BACK = 10

# Spieler mit `buy.date` älter als dieser Floor (ISO-String) werden nie
# berücksichtigt. Schützt vor Daten aus vor-strukturierten Epochen.
SEASON_FLOOR_DATE = "2024-07-10"


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


def _attach_sell(transfers, member_id, player_id, sell, log=print):
    """Schreibt einen Verkauf an den passenden offenen Kauf. Idempotent.

    Spiegelung der Logik in backend/structure_data_utils.py. Drei
    Schutzmechanismen:

    1. Doppelattach verhindern — existiert dieser Verkauf (gleiche
       sell.datetime) bereits, wird er übersprungen.
    2. Bei mehreren offenen Käufen desselben Spielers wird der ÄLTESTE
       zuerst geschlossen (FIFO via Sort). Sonst hätte MongoDB je nach
       Speicherreihenfolge einen beliebigen genommen und damit falsche
       Verkäufe erfunden.
    3. Fehlt der offene Kauf, wird der Verkauf stumm verworfen statt zu
       knallen.
    """
    already = transfers.find_one(
        {
            "member_id": member_id,
            "player_id": player_id,
            "sell.datetime": sell["datetime"],
        }
    )
    if already is not None:
        return False

    buy = transfers.find_one(
        {
            "member_id": member_id,
            "player_id": player_id,
            "sell": None,
        },
        sort=[("buy.datetime", 1)],
    )
    if buy is None:
        log(
            f"  sell skipped: player_id={player_id}, member_id={member_id}, "
            f"sell.date={sell['date']} — kein offener Kauf"
        )
        return False

    transfers.update_one({"_id": buy["_id"]}, {"$set": {"sell": sell}})
    log(
        f"  sell attached: player_id={player_id}, member_id={member_id}, "
        f"sell.date={sell['date']}, kaufdatum={buy['buy']['date']}"
    )
    return True


def refresh_news(db, token, days_back=NEWS_DAYS_BACK, log=print) -> int:
    """Holt News-Items der letzten ``days_back`` Tage und schreibt sie nach Mongo.

    Paginiert ``/communities/{cid}/users/{uid}/news`` und schreibt jedes Item
    per Upsert in seine ``type``-Collection (z.B. TRANSACTION_TRANSFER).
    Identisch zur Backend-Logik in ``crud.save_news_entry``, aber hier ohne
    ``crud``-Import, damit der Cron-Pfad self-contained bleibt.

    Idempotent: Upsert-Schlüssel ist die News-ID; wiederholte Aufrufe fügen
    nichts doppelt ein.

    Returns:
        Anzahl geschriebener / aktualisierter News-Items.
    """
    headers = {"Authorization": f"Bearer {token}"}
    url = (
        f"https://www.comunio.de/api/communities/{COMMUNITY_ID}"
        f"/users/{USER_ID}/news?group=true&originaltypes=true"
    )
    limit = 20
    cutoff = datetime.utcnow().date() - timedelta(days=days_back)
    log(f"Hole News ab {cutoff} (Rückblick: {days_back} Tage)")

    written = 0
    for page in range(NEWS_MAX_PAGES):
        final_url = f"{url}&start={page * limit}&limit={limit}"
        response = requests.get(final_url, headers=headers, timeout=30)
        response.raise_for_status()
        groups = response.json().get("newsList", {}).get("groups", {})
        if not groups:
            break

        # Eine Seite kann sowohl Tages-Gruppen INNERHALB als auch AUSSERHALB
        # des Cutoffs enthalten. Erst ALLES scannen, dann erst entscheiden,
        # ob weitergeblättert wird — sonst bricht eine einzelne alte Gruppe
        # auf der Seite die Schleife ab, obwohl rechts noch relevante Tage
        # stehen.
        reached_cutoff = False
        for _, single_news in groups.items():
            try:
                news_date = datetime.strptime(
                    single_news["name"], "%Y-%m-%d"
                ).date()
            except (ValueError, KeyError):
                continue
            if news_date < cutoff:
                reached_cutoff = True
                continue
            if news_date < datetime.strptime(SEASON_FLOOR_DATE, "%Y-%m-%d").date():
                continue
            for entry in single_news.get("entries", []):
                collection = db[entry["type"]]
                collection.update_one(
                    {"id": entry["id"]}, {"$set": entry}, upsert=True
                )
                written += 1

        if reached_cutoff:
            break
    else:
        log(f"WARN: News-Pagination nach {NEWS_MAX_PAGES} Seiten abgebrochen")

    log(f"News aktualisiert: {written} Einträge")
    return written


def refresh_transfers(db, token=None, days_back=TRANSFER_DAYS_BACK, log=print) -> dict:
    """Liest neue Transactions und merged sie in die Transfers-Collection.

    Spiegelung von ``backend/structure_data_utils.process_transfer_raw_data``.

    Vor der Verarbeitung wird (falls ``token`` gegeben) automatisch die
    News-Pipeline (``refresh_news``) angestoßen, damit die Roh-Daten
    überhaupt in TRANSACTION_TRANSFER liegen. Das macht den Cron-Pfad
    unabhängig vom ``backend/``-Skript.

    Verarbeitet die letzten ``days_back`` Tage neu (idempotent — Käufe
    über den Unique-Index dedup-geschützt, Verkäufe über ``_attach_sell``).
    Frühere Runs mit kaputten Ankern (``latest_transfer["buy"]["date"]``)
    konnten Käufe verschlucken — der feste Rückblick heilt das ab.
    """
    if token is not None:
        n = refresh_news(db, token, days_back=days_back, log=log)
        log(f"News vorgeladen: {n}")

    transactions = db.get_collection("TRANSACTION_TRANSFER")
    transfers = db.get_collection("Transfers")
    members = db.get_collection("Members")

    cutoff_date = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    log(f"Verarbeite Roh-Transfers ab {cutoff_date} (Rückblick: {days_back} Tage)")

    documents = list(
        transactions.find({"date": {"$gte": cutoff_date}}).sort("date", 1)
    )
    total = len(documents)
    log(f"Transfers: {total} Dokumente zu verarbeiten")

    added = 0
    message_types = ["FROM_COMPUTER", "TO_COMPUTER", "BETWEEN_USERS"]
    for document in documents:
        date = document["date"]
        for message_type in message_types:
            message_content = document.get("message", {}).get(message_type)
            if not message_content:
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
                    sell = {
                        "datetime": date,
                        "date": date[:10],
                        "price": transfer["price"],
                        "to_name": transfer["to"]["name"],
                        "to_id": transfer["to"]["id"],
                    }
                    _attach_sell(transfers, member_id, player_id, sell, log=log)

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
                            "sell": sell,
                        })
                        added += 1
                    except DuplicateKeyError:
                        pass

                    # Sell-Seite des Verkäufers aktualisieren
                    sell = {
                        "datetime": date,
                        "date": date[:10],
                        "price": transfer["price"],
                        "to_name": transfer["to"]["name"],
                        "to_id": transfer["to"]["id"],
                    }
                    _attach_sell(
                        transfers, transfer["from"]["id"], player_id, sell, log=log
                    )

    log(f"Transfers: fertig — {added} neue Buy-Transfers eingefügt")
    return {"transfers_added": added}
