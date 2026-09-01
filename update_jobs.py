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

# --- Season-Bonus Konstanten ---------------------------------------------
# Comunio schüttet nach jedem Spieltag zwei Bonusarten aus:
#   1. per-point:  €10.000 pro Matchday-Punkt eines Mitspielers
#   2. day_first / day_last:  €250.000 für Erst- und Letztplatzierten,
#      gleichmäßig aufgeteilt bei Ties (z.B. 2 geteilt → je €125.000).
# Die Raten werden hier zentral gepflegt — wenn der Communityleiter sie
# ändert, ist das die einzige Stelle, die angefasst werden muss.
PER_POINT_BONUS_EUR = 10_000
DAY_FIRST_BONUS_EUR = 250_000
DAY_LAST_BONUS_EUR = 250_000
SEASON_BONUS_KIND_PER_POINT = "per_point"
SEASON_BONUS_KIND_DAY_FIRST = "day_first"
SEASON_BONUS_KIND_DAY_LAST = "day_last"


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


def _make_request_with_retry(url, get_token, log=print):
    """GET mit EINEM Retry bei 401.

    ``get_token`` ist ein Callable, das einen frischen Bearer liefert —
    typischerweise ein Closure, das bei Bedarf neu einloggt. Bei 401 wird
    genau EIN weiterer Versuch mit dem frischen Token unternommen; gibt
    es erneut 401, wird die HTTPError durchgelassen.
    """
    response = requests.get(url, headers={"Authorization": f"Bearer {get_token()}"}, timeout=30)
    if response.status_code == 401:
        log(f"  401 — Token erneuern und Retry für {url[:80]}…")
        response = requests.get(
            url, headers={"Authorization": f"Bearer {get_token(force_refresh=True)}"}, timeout=30
        )
    response.raise_for_status()
    return response.json()


def refresh_players(db, token, username=None, password=None,
                    refresh_interval=300, log=print) -> dict:
    """Holt Spielerliste + Price/Point-History und merged in Players.

    Spiegelt ``backend/structure_data_utils.process_players_information``.
    Schreibt pro Spieler ~3 sequenzielle Requests → bei ~800 Spielern
    dauert das einige Minuten; in Streamlit Cloud auf das Timeout achten.

    Token-Refresh:
      Comunio-Tokens laufen nach ~30 min ab. Bei großen Ligen (~800
      Spieler) reicht das knapp nicht. Wir erneuern den Token
      präventiv alle ``refresh_interval`` Fetches (Default 300) UND
      reagieren zusätzlich auf 401-Antworten mit einem einzelnen
      Retry. Beides setzt ``username``/``password`` voraus. Ohne
      Credentials läuft die Funktion wie vorher — der Aufrufer
      trägt dann das Risiko eines Token-Ablaufs.
    """
    players = db.get_collection("Players")

    # Closure: gibt aktuellen Token zurück, loggt bei Bedarf neu ein.
    # ``fetches_since_login`` zählt die seit dem letzten Login
    # durchgeführten Spieler-Updates; alle 3 Calls pro Spieler zählen
    # als EIN Update (sonst würde der Refresh viel zu früh kommen).
    fetches_since_login = 0
    current_token = {"value": token}

    def get_token(force_refresh=False):
        nonlocal fetches_since_login
        if force_refresh and username and password:
            current_token["value"] = login_to_comunio(username, password)
            fetches_since_login = 0
            log("  Token erneuert (Re-Login).")
        return current_token["value"]

    def maybe_proactive_refresh():
        nonlocal fetches_since_login
        if (
            username
            and password
            and fetches_since_login >= refresh_interval
        ):
            current_token["value"] = login_to_comunio(username, password)
            fetches_since_login = 0
            log(f"  Token präventiv erneuert (alle {refresh_interval} Fetches).")

    players_list = _make_request_with_retry(
        f"https://www.comunio.de/api/communities/{COMMUNITY_ID}"
        f"/players?start=0&limit=800",
        get_token, log=log,
    )
    players_comunio = players_list["tradables"]
    total = len(players_comunio)
    log(f"Players: {total} Spieler zu aktualisieren")

    for i, player in enumerate(players_comunio, start=1):
        maybe_proactive_refresh()
        player_info = _make_request_with_retry(
            f"https://www.comunio.de/api/communities/{COMMUNITY_ID}"
            f"/users/{USER_ID}/players/{player['id']}",
            get_token, log=log,
        )
        price_history = _make_request_with_retry(
            f"https://www.comunio.de/api/players/{player['id']}/quote-history",
            get_token, log=log,
        )
        points_history = _make_request_with_retry(
            f"https://www.comunio.de/api/players/{player['id']}"
            f"/match-statistics-history",
            get_token, log=log,
        )
        fetches_since_login += 1

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

        # Owner-Feld sauber mitschreiben: ist im Response `owner: null`
        # (Spieler verkauft, wieder auf Transfermarkt), müssen wir es
        # explizit `$unset`en — sonst bleibt der alte Owner aus der
        # Vorperiode im Dokument stehen und führt zu falschen
        # Audit-Treffern ("SOLD_BUT_STALE"). Bei gesetztem Owner reicht
        # `$set` wie gehabt.
        owner_present = "owner" in player_info
        owner_value   = player_info.get("owner")
        if owner_present and owner_value:
            update_doc = {"$set": player_info}
        else:
            # `owner` aus dem `$set` raushalten und separat unsetten.
            update_doc = {
                "$set": {k: v for k, v in player_info.items() if k != "owner"},
                "$unset": {"owner": ""},
            }

        players.update_one(
            {"id": player["id"]}, update_doc, upsert=True
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


def refresh_season_bonuses(db, token, season=None, log=print) -> dict:
    """Holt Boni (per-point + first/last) aus Comunio-Standings und schreibt
    sie in die `SeasonBonus`-Collection.

    Idempotent: Upsert-Schlüssel (season, matchday, member_id, kind) ist
    unique. Re-Runs aktualisieren nur Felder wie `amount` und
    `fetched_at`, fügen nichts doppelt ein.

    Args:
        db:      Mongo-DB-Handle.
        token:   Bearer-Token von Comunio.
        season:  z.B. "2026/2027" — Default fällt auf "2026/2027".
        log:     Logging-Callable.

    Returns:
        Dict mit Counts pro kind + skipped.
    """
    headers = {"Authorization": f"Bearer {token}"}

    # 1) Eventliste vom Wurzel-Endpoint holen (Matchday-IDs)
    root_url = (
        f"https://www.comunio.de/api/communities/{COMMUNITY_ID}"
        f"/standings?wpe=true"
    )
    root = requests.get(root_url, headers=headers, timeout=30)
    root.raise_for_status()
    events = (
        root.get("_embedded", {})
        .get("formerEventsWithPoints", {})
        .get("events", [])
    )

    season = season or "2026/2027"
    season_year = int(season.split("/")[0])

    # Nur MATCHDAY-Events (kein SEASON_END, kein MATCHDAY_SHIFTED) der
    # aktuellen Saison.
    matchday_events = [
        e for e in events
        if e.get("type") == "MATCHDAY"
        and e.get("year") == season_year
        and int(e.get("matchdayKey", 0)) >= 1
    ]

    log(
        f"SeasonBonus: {len(matchday_events)} Matchdays für Saison "
        f"{season_year}/{season_year+1}"
    )

    sb = db["SeasonBonus"]
    counts = {"per_point": 0, "day_first": 0, "day_last": 0, "skipped": 0}

    for ev in matchday_events:
        period_id = ev["id"]
        matchday_key = ev["matchdayKey"]

        # 2) Per-Matchday-Standings holen
        url = (
            f"https://www.comunio.de/api/communities/{COMMUNITY_ID}"
            f"/standings?period={period_id}&wpe=true"
        )
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            log(f"SeasonBonus: Matchday {matchday_key} fetch failed ({e}), skip")
            counts["skipped"] += 1
            continue

        data = resp.json()
        items = data.get("items") or []
        if not items:
            log(f"SeasonBonus: Matchday {matchday_key} has no items, skip")
            counts["skipped"] += 1
            continue

        # 3) Per-Point für jeden Mitspieler
        positions = []  # für Tie-Detection
        for it in items:
            user = (it.get("_embedded") or {}).get("user") or {}
            member_id = user.get("id")
            member_name = (user.get("name") or "").strip()
            try:
                points = int(it.get("lastPoints") or 0)
            except (TypeError, ValueError):
                points = 0
            try:
                pos = int(it.get("position") or 0)
            except (TypeError, ValueError):
                pos = 0
            if member_id is None or points <= 0:
                continue
            amount = points * PER_POINT_BONUS_EUR
            sb.update_one(
                {
                    "season": season,
                    "matchday": matchday_key,
                    "member_id": member_id,
                    "kind": SEASON_BONUS_KIND_PER_POINT,
                },
                {
                    "$set": {
                        "matchday_id": period_id,
                        "matchday_label": ev.get("event", ""),
                        "member_name": member_name,
                        "amount": amount,
                        "points": points,
                        "source": "standings",
                        "fetched_at": datetime.utcnow().isoformat(),
                    },
                },
                upsert=True,
            )
            counts["per_point"] += 1
            positions.append((member_id, member_name, pos, points))

        # 4) First / Last — Tie-Detection per Position
        if positions:
            best_pos = min(p[2] for p in positions)
            worst_pos = max(p[2] for p in positions)
            best_count = sum(1 for p in positions if p[2] == best_pos)
            worst_count = sum(1 for p in positions if p[2] == worst_pos)
            best_each = DAY_FIRST_BONUS_EUR // best_count if best_count else 0
            worst_each = DAY_LAST_BONUS_EUR // worst_count if worst_count else 0
            for member_id, member_name, pos, _ in positions:
                if pos == best_pos and best_each > 0:
                    sb.update_one(
                        {
                            "season": season,
                            "matchday": matchday_key,
                            "member_id": member_id,
                            "kind": SEASON_BONUS_KIND_DAY_FIRST,
                        },
                        {
                            "$set": {
                                "matchday_id": period_id,
                                "matchday_label": ev.get("event", ""),
                                "member_name": member_name,
                                "amount": best_each,
                                "points": None,
                                "source": "standings",
                                "fetched_at": datetime.utcnow().isoformat(),
                            },
                        },
                        upsert=True,
                    )
                    counts["day_first"] += 1
                if pos == worst_pos and worst_each > 0:
                    sb.update_one(
                        {
                            "season": season,
                            "matchday": matchday_key,
                            "member_id": member_id,
                            "kind": SEASON_BONUS_KIND_DAY_LAST,
                        },
                        {
                            "$set": {
                                "matchday_id": period_id,
                                "matchday_label": ev.get("event", ""),
                                "member_name": member_name,
                                "amount": worst_each,
                                "points": None,
                                "source": "standings",
                                "fetched_at": datetime.utcnow().isoformat(),
                            },
                        },
                        upsert=True,
                    )
                    counts["day_last"] += 1

    # 5) Unique compound index sicherstellen (idempotent)
    sb.create_index(
        [("season", 1), ("matchday", 1), ("member_id", 1), ("kind", 1)],
        unique=True,
        name="season_matchday_member_kind_unique",
    )

    log(
        f"SeasonBonus fertig: per_point={counts['per_point']} "
        f"day_first={counts['day_first']} day_last={counts['day_last']} "
        f"skipped={counts['skipped']}"
    )
    return counts
