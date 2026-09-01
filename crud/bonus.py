"""Read-side queries for season bonuses (per-point + first/last).

Schema siehe `update_jobs.refresh_season_bonuses`. Diese Datei ist
bewusst klein — sie wird sowohl vom Streamlit-Frontend (Home-Metriken,
Portfolio-Timeline) als auch vom Audit-Skript und vom Backfill-Skript
konsumiert.
"""

from datetime import datetime
from typing import Optional

import pandas as pd
from pymongo.mongo_client import MongoClient


def _norm_first(s: str) -> str:
    """Erstes Wort, getrimmt und lowercased — für Owner-Name-Match
    auf 'Kevin Wache' vs 'Kevin', 'Schuckinho ' vs 'Schuckinho' etc."""
    if not s:
        return ""
    return s.strip().split(" ", 1)[0].lower()


def get_season_bonuses(
    db: MongoClient,
    season: str = "2026/2027",
    member_name: Optional[str] = None,
) -> pd.DataFrame:
    """Holt alle Boni einer Saison (gefiltert optional auf Mitspieler).

    Returns:
        DataFrame mit Spalten: matchday, kind, amount, points,
        member_name, matchday_label, matchday_id.
    """
    query = {"season": season}
    if member_name:
        # Case-insensitive Prefix-Match (MongoDB $regex ist sonst
        # case-sensitive — würde "Hansi Flick" nicht auf "hansi"
        # matchen).
        query["member_name"] = {
            "$regex": f"^{_norm_first(member_name)}",
            "$options": "i",
        }

    rows = list(
        db["SeasonBonus"].find(query, {"_id": 0}).sort([("matchday", 1)])
    )
    if not rows:
        return pd.DataFrame(
            columns=[
                "matchday", "kind", "amount", "points",
                "member_name", "matchday_label", "matchday_id",
            ]
        )
    df = pd.DataFrame(rows)
    if "matchday_label" in df.columns:
        df["matchday_label"] = df["matchday_label"].fillna("")
    return df


def get_member_bonus_total(
    db: MongoClient,
    season: str = "2026/2027",
    member_name: Optional[str] = None,
) -> int:
    """Summe aller Boni (per_point + first + last) für einen Mitspieler."""
    df = get_season_bonuses(db, season=season, member_name=member_name)
    if df.empty:
        return 0
    return int(df["amount"].sum())


def get_matchday_total_for_member(
    db: MongoClient,
    season: str,
    member_name: str,
    matchday: int,
) -> int:
    """Summe aller Boni eines Mitspielers für ein einzelnes Matchday."""
    df = get_season_bonuses(db, season=season, member_name=member_name)
    if df.empty:
        return 0
    return int(df.loc[df["matchday"] == matchday, "amount"].sum())


def get_member_matchday_history(
    db: MongoClient,
    season: str,
    member_name: str,
) -> pd.DataFrame:
    """Per-Matchday-Aufstellung der Boni: matchday → {per_point, day_first,
    day_last, total} für Portfolio-Timeline."""
    df = get_season_bonuses(db, season=season, member_name=member_name)
    if df.empty:
        return pd.DataFrame(
            columns=["matchday", "per_point", "day_first", "day_last", "total"]
        )
    pivot = df.pivot_table(
        index="matchday",
        columns="kind",
        values="amount",
        aggfunc="sum",
        fill_value=0,
    )
    for col in ("per_point", "day_first", "day_last"):
        if col not in pivot.columns:
            pivot[col] = 0
    pivot["total"] = pivot[["per_point", "day_first", "day_last"]].sum(axis=1)
    return pivot.reset_index()


def clear_season_bonus_cache(
    db: MongoClient,
    season: str,
    member_name: Optional[str] = None,
) -> int:
    """Löscht Bonus-Rows (für manuelles Re-Backfill).

    Returns Anzahl gelöschter Rows.
    """
    query = {"season": season}
    if member_name:
        query["member_name"] = {
            "$regex": f"^{_norm_first(member_name)}",
            "$options": "i",
        }
    return db["SeasonBonus"].delete_many(query).deleted_count
