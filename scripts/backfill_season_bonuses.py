"""One-shot backfill for season bonuses.

Nutzt nach Datenkorrekturen oder Schema-Updates, um die komplette
Saison (per default 26/27) neu zu berechnen. Idempotent — bestehende
Rows werden via Upsert aktualisiert, nicht dupliziert.

Aufruf:
    MONGO_URI=... python scripts/backfill_season_bonuses.py
    # Optional:
    # --season 2026/2027
"""

import argparse
import os
import sys

from dotenv import load_dotenv
from pymongo import MongoClient


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--season", help="Default: AUDIT_SEASON oder 2026/2027")
    args = p.parse_args()

    # .env im Projekt-Root (eine Ebene über scripts/), nicht im CWD.
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv(os.path.join(project_root, ".env"))

    uri = os.environ.get("MONGO_URI")
    if not uri:
        print("ERROR: MONGO_URI nicht gesetzt", file=sys.stderr)
        sys.exit(2)
    db = MongoClient(uri)["test"]
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    import update_jobs

    username = os.environ["COMUNIO_USERNAME"]
    password = os.environ["COMUNIO_PASSWORD"]
    season = args.season or os.environ.get("AUDIT_SEASON") or "2026/2027"

    print(f"Login als '{username}' …")
    token = update_jobs.login_to_comunio(username, password)
    print(f"OK. Starte Backfill für Saison {season} …")

    counts = update_jobs.refresh_season_bonuses(db, token, season=season, log=print)
    print(f"\nBackfill fertig: {counts}")
    sys.exit(0)


if __name__ == "__main__":
    main()
