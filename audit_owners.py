"""Audit-Skript: gleicht Comunio-Spielereigentümer gegen die Transfers-Collection ab.

Zweck:
    Frisches Beispiel war: Hansi Flick besaß auf Comunio 15 Spieler, in der
    DB waren aber nur 13 davon als offene Käufe in `Transfers` erfasst.
    Zwei `FROM_COMPUTER` Buy-News-Einträge waren nicht in die `Transfers`-
    Collection durchgereicht worden (Cron-Run abgebrochen, spätere Runs
    haben die Lücke nie gefüllt). Solche Drifts sollen früh auffallen.

Aufruf:
    python audit_owners.py [--season 2026/2027] [--days 60] [--strict]

Was es prüft:
    1. Spieler, die im aktuellen Comunio-Besitz sind UND deren jüngster
       buy.date innerhalb des Saison-Fensters liegt — aber KEINE offene
       Buy-Zeile in `Transfers` haben → "missing_buy".
    2. Spieler, deren `Players.owner` aus der letzten Saison stammt (alter
       Besitz steht noch im Dokument) und NICHT im aktuellen Comunio-
       Bestand sind → reine Geister; nur als Info geloggt, nicht als Fehler.

Exit-Code:
    0 = keine missing_buy-Fälle gefunden
    1 = ≥1 missing_buy gefunden (im `--strict`-Modus FAIL; sonst WARN)

Konfiguration via ENV:
    MONGO_URI            — Pflicht
    AUDIT_SEASON         — Default "2026/2027" (übersteuerbar per CLI)
    AUDIT_COMMUNITY_ID   — Default "857661"
    AUDIT_USER_ID        — Default "5763843"
"""

import argparse
import os
import re
import sys
from datetime import datetime, timedelta

from dotenv import load_dotenv
from pymongo import MongoClient


_WS_RE = re.compile(r"\s+")


def _norm_name(s):
    """Strip + lower-case whitespace-normalisiert Namen.

    Comunio schreibt `owner.name` mit inkonsistenten trailing Spaces
    ("Kumpel und Malocherclub  ") und Members heißen teilweise mit
    Mittelnamen, aber `member_name` in Transfers schreibt nur den
    Vornamen ("Kevin" statt "Kevin Wache"). Wir matchen deshalb auf den
    ersten Token (Vorname), case-insensitive.
    """
    if not s:
        return ""
    return _WS_RE.sub(" ", s).strip().lower()


def parse_args():
    p = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    p.add_argument("--season", help="z.B. 2026/2027 — sonst AUDIT_SEASON")
    p.add_argument("--days", type=int, default=60,
                   help="Spieler mit jüngstem buy.date innerhalb dieser Tage werden auditiert (Default: 60)")
    p.add_argument("--strict", action="store_true",
                   help="Mit Exit-Code 1 abbrechen bei Funden")
    return p.parse_args()


def get_db(args):
    load_dotenv()
    uri = os.environ.get("MONGO_URI")
    if not uri:
        print("ERROR: MONGO_URI nicht gesetzt", file=sys.stderr)
        sys.exit(2)
    return MongoClient(uri)["test"]


def main():
    args = parse_args()
    season = args.season or os.environ.get("AUDIT_SEASON") or "2026/2027"
    db = get_db(args)

    try:
        start_y, end_y = season.split("/")
        start_y = int(start_y); end_y = int(end_y)
        # Saison-Start/-Ende heuristisch (1. Juli → 30. Juni)
        date_from = f"{start_y}-07-01"
        date_to   = f"{end_y}-06-30"
    except Exception:
        print(f"WARN: Saison '{season}' nicht parsbar, fallback auf dynamisches Fenster", file=sys.stderr)
        date_from = (datetime.utcnow() - timedelta(days=400)).strftime("%Y-%m-%d")
        date_to   = (datetime.utcnow() + timedelta(days=30)).strftime("%Y-%m-%d")

    print(f"Audit-Fenster: {date_from} .. {date_to}  (Saison={season})")

    Transfers = db["Transfers"]
    Players   = db["Players"]

    # 1) Aktive Spieler pro Mitspieler — über `Transfers` mit offenen Buys.
    #    Zusätzlich: Spieler ohne Sell, dessen buy.date im Fenster liegt.
    open_buys = list(Transfers.find(
        {
            "sell": None,
            "buy.date": {"$gte": date_from, "$lte": date_to},
        },
        {"player_id": 1, "member_id": 1, "member_name": 1, "buy.date": 1, "_id": 0},
    ))

    # Map: normalised member_name -> set(player_id). Wir matchen später
    # per Vorname (erstes Token), da Transfers den Vornamen ohne Mittel-
    # name speichert, Comunio aber teils voll schreibt.
    by_member_norm = {}
    for row in open_buys:
        nm = _norm_name(row.get("member_name") or "?")
        by_member_norm.setdefault(nm, set()).add(row["player_id"])

    # 2) Aktuelle Comunio-Eigentümer per Players.owner.id != null
    owned = list(Players.find(
        {"owner.id": {"$ne": None}},
        {"id": 1, "name": 1, "owner": 1, "price_history": {"$slice": -1}, "_id": 0},
    ))

    # 3) Für jeden Eigentümer: prüfen, ob alle aktuell besessenen Spieler
    #    in `by_member_norm` auftauchen. Spieler mit `buy.date` außerhalb
    #    des Fensters ignorieren wir (echte Saison-Geister).
    cutoff_recent = datetime.utcnow() - timedelta(days=args.days)
    cutoff_iso = cutoff_recent.strftime("%Y-%m-%d")

    members = set()
    missing = []
    ghosts_seen = 0

    for p in owned:
        owner = p.get("owner") or {}
        owner_id = owner.get("id")
        owner_name = (owner.get("name") or "?").strip()
        if owner_id is None:
            continue
        members.add((owner_id, owner_name))

        owner_first = _norm_name(owner_name).split(" ", 1)[0]
        owned_id = p["id"]

        # Suche in allen Member-Varianten, deren Vorname passt. Damit
        # decken wir "Kumpel und Malocherclub  " vs.
        # "Kumpel und Malocherclub" genauso ab wie "Kevin" vs. "Kevin Wache".
        owned_ids = set()
        for nm, ids in by_member_norm.items():
            if nm.split(" ", 1)[0] == owner_first:
                owned_ids |= ids

        if owned_id in owned_ids:
            continue

        # Geist? — wir schauen auf das jüngste `price_history.timestamp`.
        # Wenn das innerhalb der letzten `args.days` Tage liegt UND die
        # jüngste Buy-Zeile für diesen Spieler in Transfers liegt AUF
        # oder NACH unserem cutoff, dann wäre er unserer DB normalerweise
        # bekannt. Wenn er fehlt, ist das ein Fund.
        last_ph = (p.get("price_history") or [{}])[-1]
        last_ts = last_ph.get("timestamp")
        if not last_ts or last_ts[:10] < cutoff_iso:
            ghosts_seen += 1
            continue

        # Hat er überhaupt jemals eine offene Buy-Zeile? Wenn nein → echter
        # Geist (letzte Saison). Wenn ja → im aktuellen Fenster fehlt er.
        ever_open = Transfers.find_one({
            "player_id": owned_id, "buy.date": {"$gte": date_from, "$lte": date_to},
        })
        if ever_open is None:
            ghosts_seen += 1
            continue

        # Spieler auf dem Transfermarkt (Owner = "Computer") zählt NICHT
        # als "missing buy" — er gehört niemandem im aktuellen Squad. Nur
        # reale Mitspieler-Eigentümer interessieren uns hier.
        if owner_name.strip().lower() == "computer":
            continue

        missing.append({
            "owner": owner_name,
            "player_id": owned_id,
            "player_name": p.get("name"),
            "last_buy_date": ever_open.get("buy", {}).get("date"),
        })

    # 4) Report
    print(f"\nAudited members: {len(members)}")
    print(f"  Open buys in DB pro member:")
    for m_name, ids in sorted(by_member_norm.items()):
        print(f"    {m_name:40s} {len(ids):>3} Spieler")

    print(f"\nMissing buys (in Comunio-Squad, nicht in Transfers): "
          f"{len(missing)}")
    for m in missing:
        print(f"  - {m['owner']:30s} {m['player_name']:25s} "
              f"(player_id={m['player_id']}, last_buy={m['last_buy_date']})")

    print(f"\nGhosts ignored (kein aktueller Buy im Fenster): {ghosts_seen}")

    if missing:
        print("\nWARN: Spieler im aktiven Comunio-Besitz ohne offene Buy-Zeile.")
        if args.strict:
            print("--strict gesetzt, Exit 1")
            sys.exit(1)
    else:
        print("\nOK: kein Drift festgestellt.")


if __name__ == "__main__":
    main()
