"""Standalone-Update-Skript für den täglichen Cronjob (GitHub Actions).

Läuft OHNE Streamlit — wird vom Workflow unter .github/workflows/
daily_update.yml täglich um 10:00 UTC ausgelöst. Schreibt einen kurzen
Log-Report auf stdout (GitHub Actions zeigt das in der Job-Übersicht).

Credentials kommen aus Umgebungsvariablen:
  MONGO_URI            — Pflicht
  COMUNIO_USERNAME     — Pflicht für Player-Update
  COMUNIO_PASSWORD     — Pflicht für Player-Update

Aufruf:
  python cron_update.py                # beide Updates
  python cron_update.py --only players # nur Player-Update
  python cron_update.py --only transfers
  python cron_update.py --skip-players # Transfer-Update ohne Login
"""

import argparse
import os
import sys
import time

from dotenv import load_dotenv

import update_jobs


def _connect_db():
    """Mongo-Verbindung über MONGO_URI aufbauen.

    Folgt dem Pattern aus backend/database/base.py: wenn die URI einen
    DB-Namen enthält, wird der benutzt, sonst Fallback "test".
    """
    from pymongo import MongoClient

    uri = os.environ.get("MONGO_URI")
    if not uri:
        print("ERROR: MONGO_URI nicht gesetzt", file=sys.stderr)
        sys.exit(2)
    client = MongoClient(uri)
    try:
        return client.get_default_database()
    except Exception:
        return client["test"]


def _login():
    """Login zu Comunio. Liefert (token, username)."""
    username = os.environ.get("COMUNIO_USERNAME")
    password = os.environ.get("COMUNIO_PASSWORD")
    if not username or not password:
        print(
            "ERROR: COMUNIO_USERNAME / COMUNIO_PASSWORD nicht gesetzt",
            file=sys.stderr,
        )
        sys.exit(2)
    print(f"Login als '{username}' …")
    token = update_jobs.login_to_comunio(username, password)
    print("Login OK.")
    return token


def _log(msg: str) -> None:
    """Log-Zeile mit Zeitstempel."""
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def run(args):
    load_dotenv()  # lokal vorhandene .env wird mitgelesen (lokal/Dev)
    db = _connect_db()

    only = args.only
    skip_players = args.skip_players or only == "transfers"
    only_players = only == "players"

    summary = {}

    # Token wird vorgehalten, damit das nachfolgende Transfer-Update denselben
    # Login wiederverwenden kann (sonst zwei logins pro Lauf). Wird vor dem
    # Player-Update angelegt; wird beim Transfer-Update ggf. neu aufgesetzt,
    # falls vorher kein Player-Update lief (z.B. via --skip-players).
    token_for_transfer = None

    if not skip_players:
        if only_players or not only:
            _log("Starte Player-Update …")
            t0 = time.time()
            try:
                token = _login()
                summary["players"] = update_jobs.refresh_players(db, token, log=_log)
                token_for_transfer = token
                _log(
                    f"Player-Update OK in {time.time() - t0:.1f}s: "
                    f"{summary['players']}"
                )
            except Exception as e:
                _log(f"Player-Update FEHLGESCHLAGEN: {e}")
                summary["players_error"] = str(e)
                if not args.continue_on_error:
                    sys.exit(1)
    else:
        _log("Player-Update übersprungen (--skip-players / --only transfers).")

    if not only_players:
        _log("Starte Transfer-Update …")
        t0 = time.time()
        try:
            # Transfer-Update holt seine eigenen News (10 Tage Rückblick) —
            # dafür braucht's einen Token. Reuse wenn vorhanden, sonst
            # frisch einloggen.
            if token_for_transfer is None:
                token_for_transfer = _login()
            summary["transfers"] = update_jobs.refresh_transfers(
                db, token=token_for_transfer, log=_log
            )
            _log(
                f"Transfer-Update OK in {time.time() - t0:.1f}s: "
                f"{summary['transfers']}"
            )
        except Exception as e:
            _log(f"Transfer-Update FEHLGESCHLAGEN: {e}")
            summary["transfers_error"] = str(e)
            if not args.continue_on_error:
                sys.exit(1)
    else:
        _log("Transfer-Update übersprungen (--only players).")

    _log(f"Fertig. Zusammenfassung: {summary}")
    return 0 if not any(k.endswith("_error") for k in summary) else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--only",
        choices=["players", "transfers"],
        help="Nur dieses Update ausführen.",
    )
    parser.add_argument(
        "--skip-players",
        action="store_true",
        help="Player-Update überspringen (kombiniert gut mit Transfer-Only).",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Bei Fehlern weiterlaufen statt sofort zu beenden.",
    )
    args = parser.parse_args()
    sys.exit(run(args))


if __name__ == "__main__":
    main()
