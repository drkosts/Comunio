"""
Stripped Comunio-API-Helfer für die Empfehlungs-Seite.

Nur die zwei Endpunkte, die recommendation_engine braucht:
  - get_exchangemarket()  — aktueller Transfermarkt
  - get_squad()           — eigener Kader

Login passiert upstream in update_jobs.login_to_comunio(); diese Datei
erwartet nur einen gültigen Bearer-Token.
"""

import requests

COMMUNITY_ID = "857661"
USER_ID = "5763843"

BASE_URL = "https://www.comunio.de/api"


def _get(url: str, token: str) -> dict:
    response = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    response.raise_for_status()
    return response.json()


def get_squad(token: str, user_id: str = USER_ID, state: str = "lineup") -> dict:
    """Holt den aktuellen Kader eines Users."""
    url = f"{BASE_URL}/users/{user_id}/squad?state={state}"
    return _get(url, token)


def get_exchangemarket(
    token: str, community_id: str = COMMUNITY_ID, user_id: str = USER_ID
) -> dict:
    """Holt den aktuellen Transfermarkt."""
    url = f"{BASE_URL}/communities/{community_id}/users/{user_id}/exchangemarket"
    return _get(url, token)
