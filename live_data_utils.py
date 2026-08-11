"""
Snapshot-Helfer für den Transfermarkt — minimaler Stand für die Empfehlungs-
Engine. Die Engine ruft process_exchangemarket_snapshot(), wenn der letzte
Snapshot älter als 6h ist (siehe _ensure_live_snapshot in recommendation_engine).
"""

import logging
from datetime import datetime, timezone

import live_endpoints

logger = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def process_exchangemarket_snapshot(db, token: str) -> int:
    """Holt den aktuellen Transfermarkt und schreibt jeden Eintrag als Snapshot.

    Returns:
        Anzahl der geschriebenen Snapshot-Datensätze.
    """
    market = live_endpoints.get_exchangemarket(token)
    items = market.get("items", [])
    if not items:
        logger.info("Transfermarkt-Snapshot: leer (Saisonpause?)")
        return 0

    fetched_at = {"fetched_at": _now()}
    docs = [{**entry, **fetched_at} for entry in items]

    collection = db.get_collection("LiveExchangemarket")
    collection.insert_many(docs)
    logger.info("Transfermarkt-Snapshot: %d Angebote geschrieben", len(docs))
    return len(docs)
