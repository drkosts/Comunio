"""
Heuristisches Empfehlungsmodell für Comunio-Käufe und -Verkäufe.

Wichtig: Das ist kein ML-Modell, sondern eine transparente, gewichtete
Score-Funktion. Die User-Vorgabe ist "schlägt vor, Mensch bestätigt" —
Heuristik ist leichter zu validieren und zu debuggen als ein Black-Box-Modell.

Datenquellen (vorausgesetzt, die Cronjob-Pipeline läuft):
    LiveExchangemarket     — aktuelle Marktangebote inkl. recommendedPrice
    LigaInsiderInjuries    — Verletzungen/Sperren mit liga_insider_player_id
    LigaInsiderNews        — News-Stream (z.B. "Angeschlagen"-Meldungen)
    OpenLigaMatches        — Bundesliga-Spielplan mit teamId
    OpenLigaTable          — Ligatabelle (Gegnerstärke)
    Players.price_history  — historische Marktwerte (für Trendanalyse)
    Players.point_history  — historische Punkteleistung (für Form)
    Transfers              — Community-Kaufdaten (second_highest_bid etc.)

Player-Mapping:
    Comunio → LigaInsider: über player_name (nicht 100% zuverlässig, aber
    pragmatisch für Start). TODO: Mapping-Tabelle pflegen.
    Comunio-Club → OpenLigaDB: über club_mapping.py.

Output:
    Pro Spieler: buy_score (0–100), sell_score (0–100), Reasoning-Strings,
    vorgeschlagener bid_price (für Käufe) und ask_price (für Verkäufe).
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from statistics import mean, median
from typing import Iterable

import club_mapping
from database import get_db
import live_endpoints
import live_data_utils
import seasonality_analysis

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Konfiguration — Gewichte für die Score-Berechnung
# ---------------------------------------------------------------------------
# Total pro Score: 100 Punkte. Justieren nach den ersten Erfahrungswerten.

BUY_WEIGHTS = {
    "undervalued": 25,        # Preis vs recommendedPrice
    "form": 20,               # letzte 3 Spiele Punkte
    "opponent_difficulty": 15, # nächste Gegner (günstig = hoher Score)
    "availability": 15,       # fit (kein LigaInsider-Eintrag)
    "resell_potential": 10,   # wie oft in Community gehandelt
    "price_stability": 10,    # niedrige Volatilität = sicherer
    "value_per_point": 5,     # Punkterwartung pro Million €
}

SELL_WEIGHTS = {
    "price_at_peak": 25,      # Markt nahe 30d-Hoch
    "upcoming_difficulty": 20, # schwere Gegner kommen
    "injury_risk": 20,        # verletzt/angeschlagen
    "playing_time_decay": 15, # drop-out detection
    "form_decline": 20,       # Form schlechter als Saisonschnitt
}

# Preis-Buckets (in €). Der Bucket beeinflusst, wie wir den Wert
# einschätzen: billige Spieler = wenig upside, aber wenig Risiko.
# Premium-Spieler = viel upside, aber viel Risiko.
PRICE_BUCKETS = [
    (500_000, "budget"),       # < 500k
    (2_000_000, "low"),        # 500k - 2M
    (8_000_000, "mid"),        # 2M - 8M
    (20_000_000, "high"),      # 8M - 20M
    (float("inf"), "premium"), # > 20M
]

# Erwarteter Return pro Bucket (Median über die nächsten 30 Tage, geschätzt
# aus Erfahrung). Bucket-Multiplier fließt in den Score mit ein.
BUCKET_EXPECTED_RETURN = {
    "budget": 0.10,
    "low": 0.15,
    "mid": 0.12,
    "high": 0.08,
    "premium": 0.05,
}

# LigaInsider-/OpenLigaDB-Daten sind X Tage alt. Wie stark wir dem
# Faktor "Verletzungs-/News-Aktualität" vertrauen, hängt davon ab.
NEWS_FRESHNESS_DAYS = 7


# ---------------------------------------------------------------------------
# Datenstrukturen
# ---------------------------------------------------------------------------

@dataclass
class PlayerFeatures:
    """Roh-Features pro Spieler, bevor sie zu einem Score aggregiert werden."""
    player_id: int
    player_name: str
    club_name: str
    quoted_price: int
    recommended_price: int | None
    minimum_bid: int | None = None  # Marktwert am Tag des Markteintrags
    last_3_points: list[float] = field(default_factory=list)
    season_avg_points: float | None = None
    next_opponent_team_id: int | None = None
    next_opponent_rank: int | None = None
    injured: bool = False
    injury_reason: str | None = None
    recent_news_categories: list[str] = field(default_factory=list)
    median_community_buy_premium: float | None = None  # (buy_price - second_highest) / second_highest
    price_history_30d: list[tuple[datetime, int]] = field(default_factory=list)


@dataclass
class Recommendation:
    """Endprodukt pro Spieler."""
    player_id: int
    player_name: str
    club_name: str
    quoted_price: int
    minimum_bid: int | None
    price_bucket: str           # "budget" | "low" | "mid" | "high" | "premium"
    buy_score: int
    sell_score: int
    buy_reasons: list[str]
    sell_reasons: list[str]
    suggested_bid_price: int | None
    suggested_ask_price: int | None


# ---------------------------------------------------------------------------
# Feature-Extraktion
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc)

def _ensure_live_snapshot(db, token: str) -> None:
    """Holt einen frischen LiveExchangemarket-Snapshot, wenn der letzte
    älter als 6 Stunden ist (Cronjob läuft alle 2h, wir wollen nicht strikt
    nur darauf warten)."""
    from datetime import timedelta
    coll = db.get_collection("LiveExchangemarket")
    latest = coll.find_one(sort=[("fetched_at", -1)])
    if latest:
        ts = latest["fetched_at"]
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if (_now() - ts) > timedelta(hours=6):
            logger.info("LiveExchangemarket ist älter als 6h — hole frischen Snapshot")
            live_data_utils.process_exchangemarket_snapshot(db, token)
    else:
        live_data_utils.process_exchangemarket_snapshot(db, token)


def _get_table_lookup(db) -> dict[int, dict]:
    """Liefert teamId → Tabelleneintrag (für Gegnerstärke)."""
    out = {}
    for entry in db.get_collection("OpenLigaTable").find():
        out[entry["team_id"]] = entry
    return out


def _get_upcoming_matches(db, team_id: int, limit: int = 3) -> list[dict]:
    """Liefert die nächsten `limit` Spiele eines Teams (Heim oder Auswärts)."""
    coll = db.get_collection("OpenLigaMatches")
    return list(
        coll.find({
            "$or": [{"team1_id": team_id}, {"team2_id": team_id}],
            "match_date_time": {"$gte": _now().strftime("%Y-%m-%dT%H:%M:%S")},
            "is_finished": False,
        })
        .sort("match_date_time", 1)
        .limit(limit)
    )


def _team_of_player(player: dict, club_lookup: dict) -> int | None:
    """Comunio-Club → OpenLigaDB-Team-ID."""
    p = player.get("_embedded", {}).get("player") or player.get("player") or player
    club_name = (p.get("club") or {}).get("name", "")
    entry = club_mapping.lookup(club_name)
    return entry["openligadb_team_id"] if entry else None


def _next_opponent_for(player: dict, db, club_lookup: dict) -> tuple[int | None, int | None]:
    """Gibt (opponent_team_id, opponent_rank) für den nächsten Gegner zurück."""
    from datetime import timedelta
    team_id = _team_of_player(player, club_lookup)
    if team_id is None:
        return None, None
    matches = _get_upcoming_matches(db, team_id, limit=1)
    if not matches:
        return None, None
    match = matches[0]
    opponent_id = match["team2_id"] if match["team1_id"] == team_id else match["team1_id"]
    table = _get_table_lookup(db)
    return opponent_id, (table.get(opponent_id) or {}).get("rank")


def _player_history(player_id: int, db) -> dict:
    """Holt price_history und point_history aus Players-Collection."""
    return db.get_collection("Players").find_one(
        {"id": player_id}, {"price_history": 1, "point_history": 1}
    ) or {}


def _minimum_bid_for(player_id: int, market_date_str: str | None, db) -> int | None:
    """Fallback-Schätzung des Mindestgebots, falls die API es nicht liefert.

    Comunio-API-Lookup (verifiziert via Browser-MCP, GET
    /api/communities/{cid}/users/{uid}/exchangemarket):
        Jedes Market-Item hat _embedded.player.recommendedPrice — das
        ist exakt das Mindestgebot (= Marktwert am Tag des
        Markteintrags, intern aus dem damaligen price_history-Eintrag
        gefüllt). Es kann über oder unter dem aktuellen Marktwert
        (quotedPrice) liegen. Wer weniger bietet, wird abgelehnt.

    Diese Funktion bleibt als Fallback, falls recommendedPrice fehlt:
        nimm den letzten price_history-Eintrag vor oder an 'date'.
    In der Praxis wird sie nicht mehr aufgerufen — extract_features
    zieht das Mindestgebot direkt aus recommendedPrice.
    """
    if not market_date_str:
        return None

    # market_date_str könnte ISO-Format mit oder ohne tz sein
    from datetime import datetime, timezone
    try:
        if market_date_str.endswith("Z"):
            market_date_str = market_date_str.replace("Z", "+00:00")
        market_dt = datetime.fromisoformat(market_date_str)
        if market_dt.tzinfo is None:
            market_dt = market_dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None

    history = _player_history(player_id, db).get("price_history") or []
    candidates = []
    for entry in history:
        ts_str = entry.get("timestamp") or entry.get("date")
        if not ts_str:
            continue
        try:
            if ts_str.endswith("Z"):
                ts_str = ts_str.replace("Z", "+00:00")
            ts = datetime.fromisoformat(ts_str)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        if ts <= market_dt:
            price = entry.get("quotedPrice")
            if price is not None and price > 0:
                candidates.append((ts, int(price)))

    if not candidates:
        return None
    # Letzter Eintrag vor oder an market_dt
    candidates.sort(key=lambda x: x[0])
    return candidates[-1][1]


def _last_n_points(history: list, n: int) -> list[float]:
    """Letzte n Punkte aus point_history (in zeitlicher Reihenfolge)."""
    if not history:
        return []
    sorted_hist = sorted(history, key=lambda e: e.get("matchday", {}).get("id", 0))
    pts = []
    for entry in sorted_hist[-n:]:
        # Comunio-API hat verschiedene Felder — robust parsen
        v = entry.get("points")
        if v is None:
            v = entry.get("totalPoints")
        if v is not None:
            pts.append(float(v))
    return pts


def _season_avg_points(history: list) -> float | None:
    if not history:
        return None
    pts = []
    for entry in history:
        v = entry.get("points") or entry.get("totalPoints")
        if v is not None:
            pts.append(float(v))
    return mean(pts) if pts else None


def _price_history_30d(history: list) -> list[tuple[datetime, int]]:
    """Letzte 30 Tage Preisentwicklung."""
    from datetime import timedelta
    cutoff = _now() - timedelta(days=30)
    out = []
    for entry in history or []:
        try:
            ts = datetime.fromisoformat(entry["timestamp"].replace("Z", "+00:00"))
        except (KeyError, ValueError):
            continue
        if ts < cutoff:
            continue
        price = entry.get("price") or entry.get("quote")
        if price is None:
            continue
        out.append((ts, int(price)))
    return sorted(out)


def _community_buy_premium(player_id: int, db) -> float | None:
    """Median der (buy_price - second_highest_bid) / second_highest_bid
    aus den letzten 20 Transfers dieses Spielers.

    Positiv: Spieler wird typischerweise über Mindestgebot gekauft (begehrt).
    Negativ: Spieler wird oft zum Mindestpreis gekauft.
    """
    coll = db.get_collection("Transfers")
    recent = list(
        coll.find({"player_id": player_id, "buy.second_highest_bid": {"$exists": True}})
        .sort("buy.date", -1)
        .limit(20)
    )
    premiums = []
    for t in recent:
        buy_price = t.get("buy", {}).get("price")
        second = t.get("buy", {}).get("second_highest_bid")
        if buy_price and second and second > 0:
            premiums.append((buy_price - second) / second)
    if not premiums:
        return None
    return median(premiums)


def _is_injured(player_name: str, db) -> tuple[bool, str | None]:
    """Prüft, ob der Spieler in LigaInsiderInjuries auftaucht."""
    name_lower = player_name.strip().lower()
    coll = db.get_collection("LigaInsiderInjuries")
    # neueste Einträge zuerst
    for entry in coll.find().sort("fetched_at", -1).limit(200):
        if (entry.get("player_name") or "").strip().lower() == name_lower:
            return True, entry.get("reason")
    return False, None


def _recent_news_for(player_name: str, db, days: int = 7) -> list[str]:
    """News-Kategorien der letzten X Tage für diesen Spieler."""
    from datetime import timedelta
    cutoff = _now() - timedelta(days=days)
    name_lower = player_name.strip().lower()
    out = []
    for entry in db.get_collection("LigaInsiderNews").find():
        if (entry.get("player_name") or "").strip().lower() != name_lower:
            continue
        ts = entry.get("published_at")
        if ts is None:
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if ts >= cutoff:
            cat = entry.get("category")
            if cat:
                out.append(cat)
    return out


def extract_features(player: dict, db, club_lookup: dict) -> PlayerFeatures:
    """Baut die PlayerFeatures aus allen Datenquellen.

    Items können aus exchangemarket (mit _embedded.player) oder squad
    (mit eingebetteten Spieler-Daten) kommen. Wir normalisieren intern.
    """
    # Comunio-API HAL-Format: Spieler liegt unter _embedded.player
    p = player.get("_embedded", {}).get("player") or player.get("player") or player
    player_id = p.get("id")
    player_name = p.get("name", "Unbekannt")
    club_name = (p.get("club") or {}).get("name", "")
    quoted_price = int(p.get("quotedPrice") or player.get("quotedPrice") or 0)
    recommended_price = p.get("recommendedPrice") or player.get("recommendedPrice")

    # Mindestgebot (minimum_bid) = recommendedPrice aus dem Market-Item.
    # Verifiziert über Browser-MCP: GET /api/communities/.../exchangemarket
    # liefert für _embedded.player.recommendedPrice EXAKT den Wert, der im
    # UI als "Mindestgebot" angezeigt wird — kein QuotingPrice, kein
    # price_history-Roundtrip nötig. Für eigene Kaderspieler ohne Markt-
    # Eintrag gibt es kein Mindestgebot, dort bleibt recommended_price
    # der einzige Anker.
    minimum_bid = recommended_price if recommended_price else None

    # Form, Saisonschnitt
    history = _player_history(player_id, db)
    last_3 = _last_n_points(history.get("point_history", []), 3)
    season_avg = _season_avg_points(history.get("point_history", []))
    price_hist_30d = _price_history_30d(history.get("price_history", []))

    # Nächster Gegner
    opponent_id, opponent_rank = _next_opponent_for(player, db, club_lookup)

    # Verletzungen
    injured, injury_reason = _is_injured(player_name, db)

    # News
    recent_news = _recent_news_for(player_name, db)

    # Community-Buy-Premium
    premium = _community_buy_premium(player_id, db)

    return PlayerFeatures(
        player_id=player_id,
        player_name=player_name,
        club_name=club_name,
        quoted_price=quoted_price,
        recommended_price=recommended_price,
        minimum_bid=minimum_bid,
        last_3_points=last_3,
        season_avg_points=season_avg,
        next_opponent_team_id=opponent_id,
        next_opponent_rank=opponent_rank,
        injured=injured,
        injury_reason=injury_reason,
        recent_news_categories=recent_news,
        median_community_buy_premium=premium,
        price_history_30d=price_hist_30d,
    )


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------


def _clamp(value: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, value))


def _bucket(price: int) -> str:
    """Preis-Bucket für einen Spieler. BUDGET/LOW/MID/HIGH/PREMIUM."""
    for threshold, name in PRICE_BUCKETS:
        if price < threshold:
            return name
    return "premium"


def _seasonal_multiplier() -> tuple[float, int | None]:
    """Berechnet den saisonalen Multiplikator und die Wochen vor Saisonstart.

    Logik (kalibriert mit unseren historischen Daten aus 854 Spielern):
        - Wochen 5-8 vor Start: Preise im Tief (-9.1% zu Start), Faktor 1.10
          (gute Zeit zum Einsteigen)
        - Wochen 3-4 vor Start: Preise steigen wieder, Faktor 0.95
        - Wochen 0-2 vor Start: nahe Peak, Faktor 0.85 (zurückhalten)
        - In Saison: Preise schwanken performance-basiert, Faktor 1.0

    Returns:
        (multiplier, weeks_before_season_start)
    """
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    season_year, season_start = seasonality_analysis._closest_season_start(now)
    weeks_before = (season_start - now).days / 7

    if weeks_before < 0:
        return 1.0, None
    if weeks_before >= 5:
        return 1.10, int(weeks_before)
    if weeks_before >= 3:
        return 0.95, int(weeks_before)
    return 0.85, max(0, int(weeks_before))


def _score_buy(f: PlayerFeatures) -> tuple[int, list[str]]:
    """Berechnet buy_score (0–100) und die Begründungen.

    Score-Semantik:
        70-100: starke Kauf-Empfehlung (guter Preis, gute Form, leichter Gegner)
        40-69:  solide Empfehlung
        15-39:  Vorsicht
        0-14:   Finger weg
    """
    reasons = []
    score = 0.0

    # 1) Undervalued (max 25)
    if f.recommended_price and f.quoted_price > 0:
        ratio = f.quoted_price / f.recommended_price
        if ratio <= 0.85:
            score += BUY_WEIGHTS["undervalued"]
            reasons.append(f"deutlich unter Listenpreis ({ratio:.0%} von recommendedPrice)")
        elif ratio <= 0.95:
            score += BUY_WEIGHTS["undervalued"] * 0.7
            reasons.append(f"leicht unter Listenpreis ({ratio:.0%})")
        elif ratio <= 1.05:
            score += BUY_WEIGHTS["undervalued"] * 0.3
        elif ratio >= 1.2:
            reasons.append(f"deutlich über Listenpreis ({ratio:.0%})")

    # 2) Form (max 20)
    if f.last_3_points:
        avg = mean(f.last_3_points)
        if avg >= 6:
            score += BUY_WEIGHTS["form"]
            reasons.append(f"starke Form (ø {avg:.1f} Pkt letzte 3 Spiele)")
        elif avg >= 3:
            score += BUY_WEIGHTS["form"] * 0.6
            reasons.append(f"solide Form (ø {avg:.1f} Pkt)")
        elif avg <= 0.5:
            reasons.append(f"Formschwäche (ø {avg:.1f} Pkt letzte 3)")

    # 3) Opponent difficulty (max 15) — niedrige Tabellenposition = günstig
    if f.next_opponent_rank is not None:
        if f.next_opponent_rank >= 15:
            score += BUY_WEIGHTS["opponent_difficulty"]
            reasons.append(f"nächster Gegner auf Platz {f.next_opponent_rank} (leicht)")
        elif f.next_opponent_rank >= 11:
            score += BUY_WEIGHTS["opponent_difficulty"] * 0.6
            reasons.append(f"nächster Gegner auf Platz {f.next_opponent_rank}")
        elif f.next_opponent_rank <= 4:
            score -= 5  # Strafe für Top-Gegner
            reasons.append(f"nächster Gegner auf Platz {f.next_opponent_rank} (schwer)")

    # 4) Availability (max 15)
    if not f.injured:
        score += BUY_WEIGHTS["availability"]
        if "Angeschlagen" in f.recent_news_categories:
            score -= BUY_WEIGHTS["availability"] * 0.4
            reasons.append("aber: in den letzten Tagen 'angeschlagen' gemeldet")
    else:
        score -= BUY_WEIGHTS["availability"]
        reasons.append(f"verletzt/gesperrt ({f.injury_reason}) — Finger weg")

    # 5) Resell potential (max 10)
    if f.median_community_buy_premium is not None:
        if f.median_community_buy_premium > 0.3:
            score += BUY_WEIGHTS["resell_potential"]
            reasons.append("begehrt in der Community (Median-Aufschlag >30%)")
        elif f.median_community_buy_premium > 0.1:
            score += BUY_WEIGHTS["resell_potential"] * 0.6

    # 6) Price stability (max 10)
    if len(f.price_history_30d) >= 5:
        prices = [p for _, p in f.price_history_30d]
        if max(prices) / max(min(prices), 1) < 1.3:
            score += BUY_WEIGHTS["price_stability"]
        elif max(prices) / max(min(prices), 1) > 2.0:
            reasons.append("Preis stark schwankend (Risk Faktor)")

    # 7) Value per point (max 5)
    if f.quoted_price > 0 and f.last_3_points and f.season_avg_points:
        expected_pts = max(mean(f.last_3_points), f.season_avg_points)
        value_per_million = (expected_pts * 1_000_000) / f.quoted_price
        if value_per_million >= 5:
            score += BUY_WEIGHTS["value_per_point"]
            reasons.append(f"gutes Pkt/Mio-Verhältnis ({value_per_million:.1f})")

    # 8) Bucket-spezifische Bucket-Sicherheit (klein, da Weight bereits verteilt)
    bucket = _bucket(f.quoted_price)
    if bucket == "budget" and f.quoted_price > 0:
        # Billige Spieler: leichter Bonus, wenn überhaupt was drin ist
        if f.last_3_points and mean(f.last_3_points) >= 2:
            score += 3  # kleiner Bucket-Bonus
            reasons.append(f"Bucket: budget ({f.quoted_price:,}€), günstige Punktesammler")

    # 9) Saison-Multiplier: timing-aware
    season_mult, weeks_before = _seasonal_multiplier()
    if weeks_before is not None:
        if season_mult > 1.0:
            reasons.append(f"Saison-Timing günstig ({weeks_before} Wo vor Start, Tiefphase)")
        elif season_mult < 1.0:
            reasons.append(f"Saison-Timing ungünstig ({weeks_before} Wo vor Start, Peak-Phase)")
        score *= season_mult

    return int(_clamp(score)), reasons


def _score_sell(f: PlayerFeatures, in_own_squad: bool) -> tuple[int, list[str]]:
    """Berechnet sell_score (0–100). Nur sinnvoll für eigene Kaderspieler."""
    if not in_own_squad:
        return 0, []

    reasons = []
    score = 0.0

    # 1) Price at peak (max 25)
    if len(f.price_history_30d) >= 5:
        prices = [p for _, p in f.price_history_30d]
        current, peak = f.quoted_price, max(prices)
        if current >= peak * 0.95:
            score += SELL_WEIGHTS["price_at_peak"]
            reasons.append(f"Preis nahe 30d-Hoch ({current} vs peak {peak})")
        elif current <= min(prices) * 1.05:
            reasons.append(f"Preis nahe 30d-Tief — Tiefpunkt-Gefahr")

    # 2) Upcoming difficulty (max 20)
    if f.next_opponent_rank is not None and f.next_opponent_rank <= 1:
        # Tabellenführer ausgenommen — die sind nicht immer leicht
        pass
    if f.next_opponent_rank is not None:
        if f.next_opponent_rank <= 4:
            score += SELL_WEIGHTS["upcoming_difficulty"]
            reasons.append(f"schwerer Gegner kommt (Platz {f.next_opponent_rank})")
        elif f.next_opponent_rank <= 8:
            score += SELL_WEIGHTS["upcoming_difficulty"] * 0.5

    # 3) Injury risk (max 20)
    if f.injured:
        score += SELL_WEIGHTS["injury_risk"]
        reasons.append(f"verletzt/gesperrt ({f.injury_reason})")
    elif "Angeschlagen" in f.recent_news_categories:
        score += SELL_WEIGHTS["injury_risk"] * 0.3
        reasons.append("angeschlagen gemeldet")

    # 4) Playing time decay (max 15) — drop-out detection
    if f.last_3_points and len(f.last_3_points) >= 3:
        if all(p == 0 for p in f.last_3_points):
            score += SELL_WEIGHTS["playing_time_decay"]
            reasons.append("0 Punkte in letzten 3 Spielen — wahrscheinlich Bank/Verletzt")
        elif (
            f.season_avg_points is not None
            and mean(f.last_3_points) < f.season_avg_points * 0.3
        ):
            score += SELL_WEIGHTS["playing_time_decay"] * 0.7
            reasons.append("drastischer Punkte-Abfall im Vergleich zur Saison")

    # 5) Form decline (max 20)
    if f.last_3_points and f.season_avg_points is not None:
        if f.season_avg_points > 0:
            form_ratio = mean(f.last_3_points) / f.season_avg_points
            if form_ratio < 0.4:
                score += SELL_WEIGHTS["form_decline"]
                reasons.append(f"Form {form_ratio:.0%} des Saisonschnitts")
            elif form_ratio < 0.7:
                score += SELL_WEIGHTS["form_decline"] * 0.5

    return int(_clamp(score)), reasons


def _suggest_bid_price(f: PlayerFeatures, buy_score: int) -> int | None:
    """Schlägt einen Gebotspreis vor.

    Comunio-Regel: Das MINDESTGEBOT (minimum_bid = Marktwert am Tag, an dem
    der Spieler auf den Markt kam) ist die Untergrenze. Du kannst nicht
    darunter bieten. Der aktuelle Marktwert (quotedPrice) ist nur ein
    Referenzwert und kann über oder unter dem Mindestgebot liegen.

    Fallback: wenn minimum_bid nicht ableitbar ist, verwende quotedPrice
    als sichere Untergrenze.

    Logik:
        1. Untergrenze = max(minimum_bid, recommendedPrice * 0.95)
           - Immer ≥ Mindestgebot (Comunio-Pflicht)
           - Wenn recommendedPrice deutlich über Mindestgebot: trotzdem
             nicht viel höher als 95% davon bieten (Deal-Puffer)
        2. Obergrenze = recommendedPrice * 1.3 (Sicherheits-Cap)
        3. Score-basierter Anteil zwischen Unter- und Obergrenze
        4. Saisonfaktor: in Pre-Season-Peak-Phasen höher bieten
        5. Community-Buy-Premium: wenn Community historisch überzahlt
    """
    if f.quoted_price <= 0:
        return None

    # Untergrenze = echtes Mindestgebot, nicht aktueller MW
    true_min = f.minimum_bid if f.minimum_bid is not None else f.quoted_price

    # 1) Untergrenze
    if f.recommended_price:
        lower = max(true_min, int(f.recommended_price * 0.95))
        upper = int(f.recommended_price * 1.3)
    else:
        lower = true_min
        upper = int(f.quoted_price * 1.5)

    # 2) Score-basierter Anteil an upper vs lower
    score_fraction = _clamp(buy_score, 0, 100) / 100
    base = int(lower + (upper - lower) * score_fraction)

    # 3) Saisonfaktor
    season_mult, weeks_before = _seasonal_multiplier()
    if weeks_before is not None and weeks_before <= 2:
        base = int(base * 1.05)

    # 4) Community-Buy-Premium
    if f.median_community_buy_premium is not None and f.median_community_buy_premium > 0.1:
        bump = min(f.median_community_buy_premium, 0.4) * 0.5
        base = int(base * (1 + bump))

    # In jedem Fall ≥ Mindestgebot (Comunio-Pflicht)
    return max(base, true_min)


def _suggest_ask_price(f: PlayerFeatures) -> int | None:
    """Schlägt einen Verkaufspreis vor.

    Logik: über aktuellem Marktwert, aber unter 30d-Peak.
    """
    if f.quoted_price <= 0:
        return None
    base = int(f.quoted_price * 1.1)
    if f.price_history_30d:
        peak = max(p for _, p in f.price_history_30d)
        cap = int(peak * 0.95)
        base = min(base, cap)
    return base


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def run_recommendations(db, token: str | None = None) -> dict[str, list[Recommendation]]:
    """Berechnet Empfehlungen für alle Markt-Spieler und die eigenen Spieler.

    Returns:
        {"market": [...], "own_squad": [...]} mit Recommendation-Objekten.
    """
    if db is None:
        db = get_db()

    _ensure_live_snapshot(db, token)

    # Markt holen
    market = live_endpoints.get_exchangemarket(token)
    market_items = market.get("items", [])

    # Eigener Kader
    squad = live_endpoints.get_squad(token)
    squad_items = squad.get("items", [])
    own_player_ids = {p.get("id") for p in squad_items}

    # Club-Lookup einmalig
    club_lookup = {}  # cache, falls derselbe Club mehrfach kommt

    out_market: list[Recommendation] = []
    out_own: list[Recommendation] = []

    for player in market_items:
        p = player.get("_embedded", {}).get("player") or player.get("player") or player
        in_squad = p.get("id") in own_player_ids
        features = extract_features(player, db, club_lookup)
        buy_score, buy_reasons = _score_buy(features)
        sell_score, sell_reasons = _score_sell(features, in_squad)
        bid = _suggest_bid_price(features, buy_score)
        rec = Recommendation(
            player_id=features.player_id,
            player_name=features.player_name,
            club_name=features.club_name,
            quoted_price=features.quoted_price,
            minimum_bid=features.minimum_bid,
            price_bucket=_bucket(features.quoted_price),
            buy_score=buy_score,
            sell_score=sell_score,
            buy_reasons=buy_reasons,
            sell_reasons=sell_reasons,
            suggested_bid_price=bid,
            suggested_ask_price=None,
        )
        out_market.append(rec)

    # Eigenen Kader separat prüfen (jemand im Kader kann auf dem Markt sein,
    # aber Spieler im Kader ist nicht zwangsläufig auf dem Markt)
    for player in squad_items:
        p = player.get("_embedded", {}).get("player") or player.get("player") or player
        if p.get("id") in {r.player_id for r in out_market}:
            continue
        # In ein exchangemarket-Item-Format bringen
        wrapped = {
            "_embedded": {"player": p},
            "quotedPrice": player.get("quotedPrice") or player.get("value"),
            "recommendedPrice": player.get("recommendedPrice"),
        }
        features = extract_features(wrapped, db, club_lookup)
        buy_score, buy_reasons = _score_buy(features)
        sell_score, sell_reasons = _score_sell(features, in_own_squad=True)
        ask = _suggest_ask_price(features)
        rec = Recommendation(
            player_id=features.player_id,
            player_name=features.player_name,
            club_name=features.club_name,
            quoted_price=features.quoted_price,
            minimum_bid=features.minimum_bid,
            price_bucket=_bucket(features.quoted_price),
            buy_score=buy_score,
            sell_score=sell_score,
            buy_reasons=buy_reasons,
            sell_reasons=sell_reasons,
            suggested_bid_price=None,
            suggested_ask_price=ask,
        )
        out_own.append(rec)

    out_market.sort(key=lambda r: r.buy_score, reverse=True)
    out_own.sort(key=lambda r: r.sell_score, reverse=True)

    return {"market": out_market, "own_squad": out_own}
