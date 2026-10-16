"""Regression test: sell-Dict darf nicht zwischen Iterationen leaken.

Vor dem Fix enthielt der BETWEEN_USERS-Branch in
``update_jobs.refresh_transfers`` KEIN ``sell = None``-Reset vor dem
``insert_one`` für die Käuferseite. Folge: das ``sell``-Dict aus dem
vorherigen TO_COMPUTER bzw. BETWEEN_USERS blieb in der nächsten
Buyer-Insert hängen. Symptom war am 2026-09-10:

  Schuckinho/Gosens        → sell 7,600,000 → Schuckinho (war Orbán's Preis)
  Schuckinho/W. Orbán      → sell 1 → Ballerdasdarein (war Rothe's Preis)
  Ballerdasdarein/Rothe    → sell 1,013,300 → Computer (war Schnellbacher's Preis)

Stattdessen hätte sell überall None sein müssen, weil zwischen
Mitspielern gekaufte Spieler noch keinen Sell haben.

Die Tests laufen gegen die echte MongoDB-Atlas-Instanz, benutzen aber
ausschliesslich Test-Player-IDs (>= 1_000_000) und Member-IDs
(>= 9_900_000), damit sie nichts in der Produktiv-Collection
verändern. Cleanup am Ende jedes Tests entfernt die Test-Rows wieder.
"""

import os
from datetime import datetime, timedelta, timezone

import pytest
from dotenv import load_dotenv
from pymongo import MongoClient

import update_jobs


TEST_PLAYER_BASE = 1_000_000
TEST_MEMBER_BASE = 9_900_000
TEST_DATE = "2026-09-10T05:29:55+02:00"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def test_db():
    """Live-Mongo-Verbindung; benutzt die gleiche DB wie der Cron."""
    load_dotenv()
    uri = os.environ.get("MONGO_URI")
    if not uri:
        pytest.skip("MONGO_URI nicht gesetzt — Test braucht Live-Mongo")
    client = MongoClient(uri)
    try:
        db = client.get_default_database()
    except Exception:
        db = client["test"]
    yield db
    client.close()


@pytest.fixture
def test_doc(test_db):
    """Baut ein einzelnes Test-TRANSACTION_TRANSFER-Doc in einem
    abgeschirmten Namespace und gibt (db, doc_id) zurück. Räumt danach auf.
    """
    import uuid
    test_id = f"test-{uuid.uuid4().hex}"
    test_db["TRANSACTION_TRANSFER"].insert_one({
        "id": test_id,
        "date": TEST_DATE,
        "type": "TRANSACTION_TRANSFER",
        "message": {
            "FROM_COMPUTER": [],
            "TO_COMPUTER": [
                # Beliebiges TO_COMPUTER, dessen sell-Dict im BUG-Fall
                # in den ersten BETWEEN_USERS-Buyer-Insert leaken würde.
                {
                    "tradable": {"id": TEST_PLAYER_BASE + 1, "name": "_t_schnell"},
                    "from": {"id": TEST_MEMBER_BASE + 1, "name": "_t_seller1"},
                    "to": {"id": 1, "name": "Computer"},
                    "price": 1013300,
                    "secondHighestBid": {"price": 0, "name": None},
                },
                {
                    "tradable": {"id": TEST_PLAYER_BASE + 2, "name": "_t_terhorst"},
                    "from": {"id": TEST_MEMBER_BASE + 2, "name": "_t_seller2"},
                    "to": {"id": 1, "name": "Computer"},
                    "price": 2060300,
                    "secondHighestBid": {"price": 0, "name": None},
                },
            ],
            "BETWEEN_USERS": [
                # Erstes BETWEEN_USERS, dessen Buyer-Insert den Leak
                # der vorherigen TO_COMPUTER zeigen würde.
                {
                    "tradable": {"id": TEST_PLAYER_BASE + 10, "name": "_t_rothe"},
                    "from": {"id": TEST_MEMBER_BASE + 1, "name": "_t_seller1"},
                    "to": {"id": TEST_MEMBER_BASE + 11, "name": "_t_buyer1"},
                    "price": 1,
                    "secondHighestBid": None,
                },
                # Zweites BETWEEN_USERS, dessen Buyer-Insert den Leak
                # des ersten zeigen würde (off-by-one).
                {
                    "tradable": {"id": TEST_PLAYER_BASE + 11, "name": "_t_orban"},
                    "from": {"id": TEST_MEMBER_BASE + 2, "name": "_t_seller2"},
                    "to": {"id": TEST_MEMBER_BASE + 12, "name": "_t_buyer2"},
                    "price": 7600000,
                    "secondHighestBid": None,
                },
                # Drittes BETWEEN_USERS, dessen Buyer-Insert den Leak
                # des zweiten zeigen würde.
                {
                    "tradable": {"id": TEST_PLAYER_BASE + 12, "name": "_t_player3"},
                    "from": {"id": TEST_MEMBER_BASE + 3, "name": "_t_seller3"},
                    "to": {"id": TEST_MEMBER_BASE + 12, "name": "_t_buyer2"},
                    "price": 4500000,
                    "secondHighestBid": None,
                },
            ],
        },
    })
    yield test_db

    # Cleanup
    test_db["TRANSACTION_TRANSFER"].delete_one({"id": test_id})
    test_db["Transfers"].delete_many({
        "player_id": {"$gte": TEST_PLAYER_BASE, "$lt": TEST_PLAYER_BASE + 1000}
    })


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_between_users_buyer_insert_has_sell_none(test_doc):
    """Beweisstück A: jeder Buyer in BETWEEN_USERS bekommt sell=None —
    das Sell-Dict aus dem vorherigen TO_COMPUTER / BETWEEN_USERS darf
    nicht durchschlagen.
    """
    # Arrange
    db = test_doc
    transfers = db["Transfers"]

    # Pre-seed: die "Seller" haben bereits offene Buys für die Spieler,
    # damit _attach_sell im Testlauf was zum Aktualisieren findet.
    for pid_offset, seller_offset in [
        (10, 1),  # _t_rothe / _t_seller1
        (11, 2),  # _t_orban / _t_seller2
        (12, 3),  # _t_player3 / _t_seller3
    ]:
        transfers.insert_one({
            "player_id": TEST_PLAYER_BASE + pid_offset,
            "player_name": f"_t_player{pid_offset}",
            "member_id": TEST_MEMBER_BASE + seller_offset,
            "member_name": f"_t_seller{seller_offset}",
            "buy": {
                "datetime": "2026-01-01T00:00:00+01:00",
                "date": "2026-01-01",
                "price": 999,
                "from_name": "Computer",
                "from_id": 1,
            },
            "sell": None,
        })

    # Act: Aufruf ohne Token (verhindert Live-News-Refresh) — wir wollen
    # nur den Verarbeitungsschritt testen, nicht die Comunio-API.
    update_jobs.refresh_transfers(db, token=None, days_back=2, log=lambda *a, **k: None)

    # Assert
    for buyer_id, player_id in [
        (TEST_MEMBER_BASE + 11, TEST_PLAYER_BASE + 10),
        (TEST_MEMBER_BASE + 12, TEST_PLAYER_BASE + 11),
        (TEST_MEMBER_BASE + 12, TEST_PLAYER_BASE + 12),
    ]:
        entry = transfers.find_one({
            "member_id": buyer_id,
            "player_id": player_id,
            "buy.datetime": TEST_DATE,
        })
        assert entry is not None, f"Buyer {buyer_id}/{player_id} fehlt"
        assert entry["sell"] is None, (
            f"Buyer {buyer_id}/{player_id} hat sell={entry['sell']!r} — "
            "das ist der Leak. sell MUSS None sein für BETWEEN_USERS-Buyer."
        )

    # Außerdem: die Sellers haben jetzt den korrekten Sell angehängt bekommen.
    for seller_offset, player_id, expected_price, expected_to_id in [
        (1, TEST_PLAYER_BASE + 10, 1, TEST_MEMBER_BASE + 11),
        (2, TEST_PLAYER_BASE + 11, 7600000, TEST_MEMBER_BASE + 12),
    ]:
        entry = transfers.find_one({
            "member_id": TEST_MEMBER_BASE + seller_offset,
            "player_id": player_id,
        })
        assert entry is not None
        assert entry["sell"] is not None, (
            f"Seller {TEST_MEMBER_BASE + seller_offset}/{player_id} hat "
            f"keinen Sell — _attach_sell hat nicht angehängt."
        )
        assert entry["sell"]["price"] == expected_price
        assert entry["sell"]["to_id"] == expected_to_id


def test_between_users_does_not_leak_into_between_users(test_doc):
    """Beweisstück B: konkretes Off-by-one — wenn der Fix auskommt,
    würde das Sell-Dict aus BETWEEN_USERS #1 (Rothe) im Buyer-Insert
    für BETWEEN_USERS #2 (Orbán) landen. Mit Fix: alle sells auf Buyer-
    Seiten sind None.
    """
    db = test_doc
    transfers = db["Transfers"]

    # Pre-seed: nur Seller1 hat einen offenen Rothe-Buy (für _attach_sell).
    transfers.insert_one({
        "player_id": TEST_PLAYER_BASE + 10,
        "player_name": "_t_rothe",
        "member_id": TEST_MEMBER_BASE + 1,
        "member_name": "_t_seller1",
        "buy": {
            "datetime": "2026-01-01T00:00:00+01:00",
            "date": "2026-01-01",
            "price": 999,
            "from_name": "Computer",
            "from_id": 1,
        },
        "sell": None,
    })

    update_jobs.refresh_transfers(db, token=None, days_back=2, log=lambda *a, **k: None)

    # Buyer1 (Rothe) darf KEINEN sell angehängt bekommen.
    b1 = transfers.find_one({
        "member_id": TEST_MEMBER_BASE + 11,
        "player_id": TEST_PLAYER_BASE + 10,
    })
    assert b1 is not None
    assert b1["sell"] is None, "Buyer1 (Rothe) hat sell — Leak aus TO_COMPUTER"

    # Buyer2 (Orbán) — auch hier sell=None, NICHT Rothe's Daten.
    b2 = transfers.find_one({
        "member_id": TEST_MEMBER_BASE + 12,
        "player_id": TEST_PLAYER_BASE + 11,
    })
    assert b2 is not None
    assert b2["sell"] is None, "Buyer2 (Orbán) hat sell — Leak aus BETWEEN_USERS #1"


def test_attach_sell_idempotent():
    """Beweisstück C: _attach_sell mit identischem (member, player, sell.datetime)
    ist idempotent — Re-Runs dürfen keinen zweiten Sell anlegen.
    """
    # Nutzt das echte Mongo, aber mit Test-IDs außerhalb der Produktiv-Range.
    load_dotenv()
    uri = os.environ.get("MONGO_URI")
    if not uri:
        pytest.skip("MONGO_URI nicht gesetzt")
    client = MongoClient(uri)
    try:
        db = client.get_default_database()
    except Exception:
        db = client["test"]
    try:
        transfers = db["Transfers"]
        test_pid = TEST_PLAYER_BASE + 999
        test_mid = TEST_MEMBER_BASE + 999

        # Cleanup vor dem Test
        transfers.delete_many({"player_id": test_pid})

        transfers.insert_one({
            "player_id": test_pid,
            "player_name": "_t_idem",
            "member_id": test_mid,
            "member_name": "_t_idem_seller",
            "buy": {"datetime": "2026-01-01", "date": "2026-01-01", "price": 100, "from_name": "Computer", "from_id": 1},
            "sell": None,
        })

        sell = {"datetime": "2026-09-10T00:00:00+02:00", "date": "2026-09-10", "price": 200, "to_name": "Computer", "to_id": 1}

        first = update_jobs._attach_sell(transfers, test_mid, test_pid, sell, log=lambda *a, **k: None)
        second = update_jobs._attach_sell(transfers, test_mid, test_pid, sell, log=lambda *a, **k: None)

        assert first is True, "_attach_sell sollte beim ersten Mal anlegen"
        assert second is False, "_attach_sell muss idempotent sein (zweiter Aufruf = False)"

        entry = transfers.find_one({"player_id": test_pid})
        assert entry["sell"]["price"] == 200
    finally:
        transfers.delete_many({"player_id": test_pid})
        client.close()


def test_attach_sell_lifo_picks_most_recent_buy_before_sell():
    """Beweisstück D: Wenn ein Spieler mehrfach gekauft wurde, geht der Sell an
    den JÜNGSTEN offenen Kauf VOR dem Sell — nicht an den ältesten, nicht an
    einen späteren Buy. Symptom war: Hansi Flick Cash-Dashboard zeigte 3,39M€
    statt 3,87M€ (480K zu wenig — fehlende per_point-Boni), weil bei
    Mehrfach-Käufen der falsche Buy getroffen wurde und der Cash-Flow
    systematisch verschoben war.
    """
    load_dotenv()
    uri = os.environ.get("MONGO_URI")
    if not uri:
        pytest.skip("MONGO_URI nicht gesetzt")
    client = MongoClient(uri)
    try:
        db = client.get_default_database()
    except Exception:
        db = client["test"]
    try:
        transfers = db["Transfers"]
        test_pid = TEST_PLAYER_BASE + 42
        test_mid = TEST_MEMBER_BASE + 42

        transfers.delete_many({"player_id": test_pid})

        # Drei Buys: alt, mittel, neu. Der Sell soll am mittleren landen
        # (jüngster Buy VOR dem Sell-Datum).
        for ts, price in [("2025-09-01", 1000), ("2026-03-15", 2000), ("2026-08-20", 3000)]:
            transfers.insert_one({
                "player_id": test_pid,
                "player_name": "_t_lifo",
                "member_id": test_mid,
                "member_name": "_t_lifo_member",
                "buy": {"datetime": ts, "date": ts[:10], "price": price, "from_name": "Computer", "from_id": 1},
                "sell": None,
            })

        # Sell-Datum liegt ZWISCHEN dem mittleren und dem neuen Buy
        sell_dt = "2026-07-15T05:00:00+02:00"
        sell = {"datetime": sell_dt, "date": "2026-07-15", "price": 1500, "to_name": "Computer", "to_id": 1}
        ok = update_jobs._attach_sell(transfers, test_mid, test_pid, sell, log=lambda *a, **k: None)
        assert ok is True

        # Der Sell muss am mittleren (2000) Buy hängen — nicht am alten (1000), nicht am neuen (3000).
        middle = transfers.find_one({"buy.datetime": "2026-03-15"})
        assert middle["sell"] is not None, "Sell muss am 2026-03-15 (jüngster Buy vor Sell) hängen"
        assert middle["sell"]["price"] == 1500

        old = transfers.find_one({"buy.datetime": "2025-09-01"})
        assert old["sell"] is None, "Alter Buy darf KEINEN Sell bekommen (LIFO, nicht FIFO)"

        new = transfers.find_one({"buy.datetime": "2026-08-20"})
        assert new["sell"] is None, "Buy NACH dem Sell-Datum darf den Sell nicht bekommen"
    finally:
        transfers.delete_many({"player_id": test_pid})
        client.close()


def test_incremental_recalc_includes_season_bonuses():
    """Beweisstück E: `calculate_portfolio_timeline_from_date` (die Incremental-
    Update-Funktion des Portfolio-Cache) muss Season-Boni in den Cash-Flow
    einhängen — sonst driftet ein Live-Update um den Bonus-Betrag vom
    Full-Rebuild ab. Symptom war: Hansi Flick Cash-Dashboard zeigte 3,39M€
    statt 3,87M€ (480K zu wenig) nachdem ein Cron-Run den Cache per
    Incremental-Update fortgeschrieben hatte.
    """
    from database import get_db as _get_db
    from crud import portfolio as portfolio_mod

    db = _get_db()
    test_user = "_t_inc_recalc_user"
    test_season = "_t_inc_recalc_season"

    # Setup: a fake cache entry where bonus is the only cash-increasing event
    import datetime as _dt
    today = _dt.date.today()
    season_start = _dt.date(today.year, 1, 1)

    # Bypass get_date_range by writing transfers with buy.date in the season range
    # and a SeasonBonus for that user
    db["Transfers"].delete_many({"member_name": test_user})
    db["SeasonBonus"].delete_many({"member_name": test_user, "season": test_season})
    db["PortfolioCache"].delete_many({"cache_key": f"{test_user}_{test_season}"})

    # Inserts: simple buy
    db["Transfers"].insert_one({
        "player_id": TEST_PLAYER_BASE + 99,
        "player_name": "_t_inc",
        "member_id": TEST_MEMBER_BASE + 99,
        "member_name": test_user,
        "buy": {"datetime": "2026-03-01", "date": "2026-03-01", "price": 100, "from_name": "Computer", "from_id": 1},
        "sell": None,
    })
    # Bonus per_point MD1 = 480,000
    db["SeasonBonus"].insert_one({
        "season": test_season,
        "matchday": 1,
        "member_id": TEST_MEMBER_BASE + 99,
        "member_name": test_user,
        "kind": "per_point",
        "amount": 480000,
        "points": 48,
    })

    # We need to override the date range check — simplest: monkey-patch get_date_range
    import crud.portfolio as portfolio_mod_obj
    original_get_range = portfolio_mod_obj.get_date_range
    portfolio_mod_obj.get_date_range = lambda s: ("2026-01-01", "2026-12-31")

    try:
        # Full recompute should produce 40M - 100 + 480000 = 40,479,900
        df_full = portfolio_mod_obj.calculate_portfolio_timeline_optimized(
            db, test_user, test_season
        )
        full_cash = int(df_full["Verfuegbares_Cash"].iloc[-1])
        assert full_cash == 40_479_900, (
            f"Full recompute should yield 40,479,900 (40M - 100 buy + 480K bonus); got {full_cash}"
        )

        # Incremental path: cache exists with last_cached_date BEFORE today,
        # so from_date_obj triggers the replay path.
        # We simulate that by directly calling calculate_portfolio_timeline_from_date.
        from_date_obj = season_start
        df_inc = portfolio_mod_obj.calculate_portfolio_timeline_from_date(
            db, test_user, test_season, from_date_obj,
        )
        inc_cash = int(df_inc["Verfuegbares_Cash"].iloc[-1])
        assert inc_cash == full_cash, (
            f"Incremental path yields {inc_cash}, full path yields {full_cash} — "
            f"incremental path is missing season bonuses (480K gap expected)"
        )
    finally:
        db["Transfers"].delete_many({"member_name": test_user})
        db["SeasonBonus"].delete_many({"member_name": test_user, "season": test_season})
        db["PortfolioCache"].delete_many({"cache_key": f"{test_user}_{test_season}"})
        portfolio_mod_obj.get_date_range = original_get_range


def test_attach_sell_same_tick_round_trip():
    """Beweisstück E: Kauf und Rückverkauf im selben News-Item haben dieselbe
    datetime. Symptom 2026-10-16: Constantin/Karaman + Raum blieben offen,
    Dashboard zeigte +16M Kaderwert und -16M Cash zu viel.
    """
    load_dotenv()
    uri = os.environ.get("MONGO_URI")
    if not uri:
        pytest.skip("MONGO_URI nicht gesetzt")
    client = MongoClient(uri)
    try:
        db = client.get_default_database()
    except Exception:
        db = client["test"]
    transfers = db["Transfers"]
    test_pid = TEST_PLAYER_BASE + 998
    test_mid = TEST_MEMBER_BASE + 998
    try:
        transfers.delete_many({"player_id": test_pid})
        transfers.insert_one({
            "player_id": test_pid, "player_name": "_t_tick",
            "member_id": test_mid, "member_name": "_t_tick_member",
            "buy": {"datetime": TEST_DATE, "date": TEST_DATE[:10], "price": 100, "from_name": "X", "from_id": 2},
            "sell": None,
        })
        sell = {"datetime": TEST_DATE, "date": TEST_DATE[:10], "price": 90, "to_name": "X", "to_id": 2}
        assert update_jobs._attach_sell(transfers, test_mid, test_pid, sell, log=lambda *a, **k: None) is True
        assert transfers.find_one({"player_id": test_pid})["sell"]["price"] == 90
    finally:
        transfers.delete_many({"player_id": test_pid})
        client.close()
