"""
Saisonalitäts-Analyse: Wie verhalten sich Marktwerte im Verlauf einer
Saison-Vorbereitung?

Hypothese (vom User): 4-6 Wochen vor Saisonstart steigen die Preise
(Spekulation), peak 2-3 Wochen vor Start, dann fallen sie wieder.

Methode:
    Für jeden Spieler in der Players-Collection:
        - Hole price_history
        - Bestimme für jeden Preis-Eintrag, zu welcher Saison er gehört
          (anhand Bundesliga-Saison-Start-Daten)
        - Berechne Wochen-vor-Saisonstart
        - Normalisiere den Preis auf den Saisonstart-Wert (price_at_start = 1.0)
        - Aggregiere über alle Spieler pro Saison und Woche

Output:
    - Tabelle: Woche-vor-Start → Median-Preis-Faktor
    - Aktuelle Wochen-Empfehlung: "Jetzt kaufen", "Warten", "Top-Zeit"
"""

import json
import logging
import os
import statistics
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

from database import get_db

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

logger = logging.getLogger(__name__)

# Bundesliga Saison-Start-Daten (1. Spieltag)
# Diese kannst du bei Bedarf anpassen, falls die tatsächlichen Termine anders waren.
SEASON_STARTS = {
    2024: datetime(2024, 8, 24, tzinfo=timezone.utc),  # 2024/25
    2025: datetime(2025, 8, 22, tzinfo=timezone.utc),  # 2025/26
    2026: datetime(2026, 8, 22, tzinfo=timezone.utc),  # 2026/27 (geplant)
}

# Wie viele Wochen vor Saisonstart wollen wir betrachten?
WEEKS_BEFORE_WINDOW = 8


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _closest_season_start(t: datetime) -> tuple[int, datetime]:
    """Gibt (season_year, season_start) für den nächsten Saisonstart relativ
    zu t zurück. Vor Saisonbeginn → diese Saison, nach Beginn → nächste."""
    for year, start in sorted(SEASON_STARTS.items()):
        if t <= start:
            return year, start
    # t liegt nach allen bekannten Starts
    return max(SEASON_STARTS), SEASON_STARTS[max(SEASON_STARTS)]


def _parse_price_timestamp(entry: dict) -> datetime | None:
    """Robuster Parser für unterschiedliche Zeitstempel-Formate."""
    ts = entry.get("timestamp") or entry.get("date")
    if not ts:
        return None
    try:
        # ISO-Format mit oder ohne tz
        if isinstance(ts, str):
            if ts.endswith("Z"):
                ts = ts.replace("Z", "+00:00")
            return datetime.fromisoformat(ts)
        if isinstance(ts, datetime):
            return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    except (ValueError, AttributeError):
        return None
    return None


def collect_player_data(db) -> list[dict]:
    """Sammelt pro Spieler die auf Saisonstart normalisierten Preise.

    Returns:
        Liste von Dicts:
            {season, weeks_before, normalized_price} pro Preis-Eintrag.
    """
    players = db.get_collection("Players")
    out = []

    for player in players.find({}, {"price_history": 1, "id": 1}):
        history = player.get("price_history") or []
        if not history:
            continue

        # Gruppiere nach Saison: jedes Season-Start-Datum hat einen "Anker"
        # für die Wochen-Berechnung
        for season_year, season_start in SEASON_STARTS.items():
            # Hole den ersten Preis-Eintrag in der Saison-Vorbereitung
            season_history = []
            for entry in history:
                ts = _parse_price_timestamp(entry)
                if ts is None:
                    continue
                # Nur Einträge zwischen WEEKS_BEFORE_WINDOW Wochen vor Start
                # und 1 Woche nach Start (für Drop-Beobachtung)
                window_start = season_start - timedelta(weeks=WEEKS_BEFORE_WINDOW)
                window_end = season_start + timedelta(weeks=1)
                if not (window_start <= ts <= window_end):
                    continue
                # Comunio-API nutzt "quotedPrice" im price_history-Eintrag
                price = entry.get("quotedPrice") or entry.get("price") or entry.get("quote")
                if price is None or price <= 0:
                    continue
                season_history.append((ts, int(price)))

            if len(season_history) < 3:
                continue  # zu wenig Daten, auslassen

            season_history.sort(key=lambda x: x[0])
            anchor_price = season_history[-1][0]  # Preis am nächsten am Start
            # Besser: nimm den letzten Preis vor oder am Start
            pre_start_prices = [p for ts, p in season_history if ts <= season_start]
            if not pre_start_prices:
                continue
            anchor_price = pre_start_prices[-1]  # letzter Preis direkt vor Start

            for ts, price in season_history:
                weeks_before = (season_start - ts).days / 7
                # Runde auf nächste ganze Woche (positiv = Wochen vor Start)
                weeks_before = round(weeks_before)
                if weeks_before < 0 or weeks_before > WEEKS_BEFORE_WINDOW:
                    continue
                normalized = price / anchor_price
                out.append({
                    "player_id": player["id"],
                    "season": season_year,
                    "weeks_before": int(weeks_before),
                    "normalized_price": round(normalized, 4),
                })

    return out


def aggregate_by_week(data: list[dict]) -> dict[int, dict]:
    """Berechnet pro Woche (vor Saisonstart) den Median, Mittelwert und
    die Stichprobengröße."""
    by_week = defaultdict(list)
    for entry in data:
        by_week[entry["weeks_before"]].append(entry["normalized_price"])

    out = {}
    for week, prices in sorted(by_week.items(), reverse=True):
        if len(prices) < 5:
            continue  # zu wenig Daten
        out[week] = {
            "median": round(statistics.median(prices), 4),
            "mean": round(statistics.mean(prices), 4),
            "stdev": round(statistics.stdev(prices), 4) if len(prices) > 1 else 0,
            "n_samples": len(prices),
        }
    return out


def seasonal_factor(weeks_before: int) -> float:
    """Multiplikator für den buy_score, abhängig vom aktuellen Saisonzeitpunkt.

    Logik:
        - Wochen 6-8 vor Start: Preise steigen noch (Spekulation) → Faktor 0.95
          (wir wollen noch nicht kaufen, Markt ist noch zu heiß)
        - Wochen 3-5 vor Start: Peak erwartet → Faktor 0.85
        - Wochen 1-2 vor Start: Preise fallen → Faktor 1.10
          (guter Zeitpunkt zum Einsteigen)
        - Bei Saisonstart: 1.0
    """
    if weeks_before >= 6:
        return 0.95
    if weeks_before >= 3:
        return 0.85
    if weeks_before >= 1:
        return 1.10
    return 1.0


def current_timing() -> str:
    """Gibt eine textliche Empfehlung basierend auf dem aktuellen Zeitpunkt."""
    now = _now()
    season_year, season_start = _closest_season_start(now)
    weeks_before = (season_start - now).days / 7
    if weeks_before < 0:
        return f"Saison läuft ({season_year}/{season_year+1})"
    if weeks_before >= 6:
        return f"Wochen {int(weeks_before)} vor Saisonstart — Spekulationsphase, Preise steigen noch"
    if weeks_before >= 3:
        return f"Wochen {int(weeks_before)} vor Saisonstart — Peak-Phase, zurückhalten"
    if weeks_before >= 1:
        return f"Wochen {int(weeks_before)} vor Saisonstart — Preise fallen, guter Einstiegszeitpunkt"
    return "Saisonstart steht unmittelbar bevor"


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    db = get_db()

    print(f"Aktueller Zeitpunkt: {_now().isoformat()}")
    print(f"Timing-Empfehlung: {current_timing()}")
    print()

    data = collect_player_data(db)
    if not data:
        print("Keine Daten gefunden. Läuft der Cronjob? Sind die player_history-Einträge da?")
        return

    print(f"{len(data)} Preis-Einträge aus {len({e['player_id'] for e in data})} Spielern geladen")
    print()

    by_week = aggregate_by_week(data)
    print("Wochen vor Saisonstart → Median Preis-Faktor (1.0 = Saisonstart-Preis)")
    print("Wochen   Median   Mittel   StdAbw   n")
    print("─" * 40)
    for week in sorted(by_week.keys(), reverse=True):
        entry = by_week[week]
        print(f"  -{week:>2}    {entry['median']:>5}    {entry['mean']:>5}    {entry['stdev']:>5}    {entry['n_samples']:>4}")
    print()

    # Pattern erkennen
    if 5 in by_week and 1 in by_week:
        peak_to_late = (by_week[5]["median"] - by_week[1]["median"]) / by_week[5]["median"]
        if peak_to_late > 0.05:
            print(f"→ Peak 5-6 Wochen vor Start, fällt dann um {peak_to_late*100:.1f}% bis 1-2 Wochen vor Start")
        elif peak_to_late < -0.05:
            print(f"→ Ansteigendes Muster: Preise {peak_to_late*100:.1f}% — keine Peak-Phase erkennbar")
        else:
            print("→ Stabiles Muster, kein klarer Peak")

    # Empfehlung
    print()
    factors = {week: seasonal_factor(week) for week, _ in by_week.items()}
    print("Seasonal factors (für Empfehlungs-Engine):")
    for week, factor in sorted(factors.items(), reverse=True):
        print(f"  -{week:>2} Wochen: ×{factor}")


if __name__ == "__main__":
    main()
