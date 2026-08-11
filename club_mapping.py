"""
Mapping Comunio-Vereinsnamen → OpenLigaDB-Team-IDs.

Hintergrund: Comunio und OpenLigaDB verwenden unterschiedliche Vereinsnamen
(z.B. "BVB" vs. "Borussia Dortmund"). Damit der Empfehlungs-Mapper von
Comunio-Spielername + Verein auf den nächsten Bundesliga-Gegner kommt,
braucht es eine Übersetzungstabelle.

Diese Datei ist absichtlich ein separates JSON, damit du sie ohne Python-
Kenntnisse pflegen kannst. Wenn der Empfänger in der openligadb.de-Liste
einen Verein nicht findet, ergänze ihn hier.

Format:
    COMMUNIO_CLUB_MAPPING = {
        "comunio_name_lower": {
            "openligadb_team_id": 40,
            "openligadb_team_name": "FC Bayern München",
            "aliases": ["FC Bayern", "Bayern München", "FCB"]
        },
        ...
    }

TODO vom User zu pflegen:
    - Eigene Comunio-API-Aufruf machen, alle einzigartigen Club-Namen
      aus den Players.price_history.club.name extrahieren
    - Pro Club den openligadb.de-Team aus
      https://api.openligadb.de/getavailableteams/bl1/2026 heraussuchen
    - Hier eintragen
"""

import json
import os
from pathlib import Path

# Bekannte Defaults — die häufigsten Bundesliga-Vereine. Diese Liste ist
# Vorbefüllung; jeder Club, den Comunio zurückgibt, sollte hier landen.
DEFAULT_MAPPING = {
    "fc bayern münchen": {
        "openligadb_team_id": 40,
        "openligadb_team_name": "FC Bayern München",
        "aliases": ["fc bayern", "bayern münchen", "fcb"],
    },
    "borussia dortmund": {
        "openligadb_team_id": 7,
        "openligadb_team_name": "Borussia Dortmund",
        "aliases": ["bvb", "dortmund"],
    },
    "rb leipzig": {
        "openligadb_team_id": 123,
        "openligadb_team_name": "RB Leipzig",
        "aliases": ["leipzig", "rbleipzig"],
    },
    "bayer 04 leverkusen": {
        "openligadb_team_id": 6,
        "openligadb_team_name": "Bayer 04 Leverkusen",
        "aliases": ["leverkusen", "bayer leverkusen"],
    },
    "fc augsburg": {
        "openligadb_team_id": 5001,
        "openligadb_team_name": "FC Augsburg",
        "aliases": ["augsburg", "fca"],
    },
    "vfb stuttgart": {
        "openligadb_team_id": 16,
        "openligadb_team_name": "VfB Stuttgart",
        "aliases": ["stuttgart", "vfb"],
    },
    "eintracht frankfurt": {
        "openligadb_team_id": 91,
        "openligadb_team_name": "Eintracht Frankfurt",
        "aliases": ["frankfurt", "sge"],
    },
    "sc freiburg": {
        "openligadb_team_id": 25,
        "openligadb_team_name": "SC Freiburg",
        "aliases": ["freiburg", "scf"],
    },
    "tsg 1899 hoffenheim": {
        "openligadb_team_id": 2,
        "openligadb_team_name": "TSG 1899 Hoffenheim",
        "aliases": ["tsg hoffenheim", "hoffenheim"],
    },
    "1. fc heidenheim 1846": {
        "openligadb_team_id": 5009,
        "openligadb_team_name": "1. FC Heidenheim 1846",
        "aliases": ["heidenheim", "fch"],
    },
    "vfl wolfsburg": {
        "openligadb_team_id": 15,
        "openligadb_team_name": "VfL Wolfsburg",
        "aliases": ["wolfsburg", "vfl"],
    },
    "1. fsv mainz 05": {
        "openligadb_team_id": 81,
        "openligadb_team_name": "1. FSV Mainz 05",
        "aliases": ["mainz", "fsv mainz"],
    },
    "borussia mönchengladbach": {
        "openligadb_team_id": 87,
        "openligadb_team_name": "Borussia Mönchengladbach",
        "aliases": ["gladbach", "mönchengladbach", "bmg"],
    },
    "1. fc union berlin": {
        "openligadb_team_id": 100,
        "openligadb_team_name": "1. FC Union Berlin",
        "aliases": ["union berlin", "union"],
    },
    "sv werder bremen": {
        "openligadb_team_id": 134,
        "openligadb_team_name": "SV Werder Bremen",
        "aliases": ["werder bremen", "werder"],
    },
    "fc st. pauli": {
        "openligadb_team_id": 5002,
        "openligadb_team_name": "FC St. Pauli",
        "aliases": ["st. pauli", "pauli"],
    },
    "hamburger sv": {
        "openligadb_team_id": 1000,
        "openligadb_team_name": "Hamburger SV",
        "aliases": ["hsv", "hamburg"],
    },
    "1. fc köln": {
        "openligadb_team_id": 65,
        "openligadb_team_name": "1. FC Köln",
        "aliases": ["köln", "fc köln", "effzeh"],
    },
}


def _mapping_file_path() -> Path:
    """Pfad zur Mapping-Override-Datei (optional). Liegt eine user_data/club_mapping.json,
    wird die statt DEFAULT_MAPPING geladen — so kann man ohne Code-Edit pflegen."""
    return Path(__file__).parent / "user_data" / "club_mapping.json"


def load_mapping() -> dict:
    """Lädt das Vereins-Mapping. Override-Datei hat Vorrang."""
    path = _mapping_file_path()
    if path.exists():
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return DEFAULT_MAPPING


def lookup(comunio_club_name: str) -> dict | None:
    """Schlägt einen Comunio-Vereinsnamen nach und gibt das OpenLigaDB-Pendant zurück.

    Args:
        comunio_club_name: Name aus der Comunio-API, z.B. "1. FC Union Berlin".

    Returns:
        dict mit "openligadb_team_id" + "openligadb_team_name" oder None,
        wenn nicht im Mapping.
    """
    if not comunio_club_name:
        return None
    key = comunio_club_name.strip().lower()
    mapping = load_mapping()
    if key in mapping:
        return mapping[key]
    # Alias-Fallback
    for entry in mapping.values():
        if key in [a.lower() for a in entry.get("aliases", [])]:
            return entry
    # Fallback: numerische Präfixe und Punkte entfernen, damit
    # "1. FC Bayern München" zu "fc bayern münchen" passt.
    import re
    normalised = re.sub(r"^\d+\.\s*", "", key).strip()
    if normalised in mapping:
        return mapping[normalised]
    return None
