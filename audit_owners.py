"""Audit-Skript: gleicht Comunio-Spielereigentümer gegen die Transfers-Collection ab.

Zweck:
    Frisches Beispiel war: Hansi Flick besaß auf Comunio 15 Spieler, in der
    DB waren aber nur 13 davon als offene Käufe in `Transfers` erfasst.
    Zwei `FROM_COMPUTER` Buy-News-Einträge waren nicht in die `Transfers`-
    Collection durchgereicht worden (Cron-Run abgebrochen, spätere Runs
    haben die Lücke nie gefüllt). Solche Drifts sollen früh auffallen.

Buckets:
    MISSING_BUY      — Spieler hat `Players.owner.id` (Comunio-Eigentümer
                       gesetzt) aber KEINEN einzigen `Transfers`-Eintrag
                       in der aktuellen Saison. → Echter Daten-Gap, Aktion
                       nötig (Cron-Insert oder manueller Backfill).
    SOLD_BUT_STALE   — Spieler hat `Players.owner.id` gesetzt UND es gibt
                       in `Transfers` einen Buy MIT Sell (also geschlosse-
                       ner Trade), aber KEINEN offenen Buy. → Kosmetik:
                       `refresh_players` hätte beim Verkauf das Owner-
                       Feld löschen müssen, hat es aber nicht. Hat keine
                       Auswirkung auf das Dashboard (das aus `Transfers`
                       liest), ist aber audit-mäßig falsch.
    OWNER_MISMATCH   — `Players.owner` zeigt auf Mitglied X, aber `Transfers`
                       hat einen offenen Buy von Mitglied Y für diesen
                       Spieler. → Echte Inkonsistenz, manuelle Prüfung.
    GHOST            — `Players.owner.id` ist gesetzt, aber weder aktiver
                       Buy in der Saison noch jüngste price_history-
                       Aktualisierung. → Altlast aus Vor-Saison. Info.
    TRANSFER_MARKET  — `Players.owner.name == "Computer"`. Kein reale-
                       Mitspieler-Besitz, daher kein Fehler.

Aufruf:
    python audit_owners.py [--season 2026/2027] [--days 60] [--strict]

    --strict verlässt das Programm mit Exit-Code 1 bei jeglichem Fund
    (auch nur kosmetische `SOLD_BUT_STALE`).

Konfiguration via ENV:
    MONGO_URI            — Pflicht
    AUDIT_SEASON         — Default "2026/2027" (übersteuerbar per CLI)
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


def get_db():
    load_dotenv()
    uri = os.environ.get("MONGO_URI")
    if not uri:
        print("ERROR: MONGO_URI nicht gesetzt", file=sys.stderr)
        sys.exit(2)
    return MongoClient(uri)["test"]


def main():
    args = parse_args()
    season = args.season or os.environ.get("AUDIT_SEASON") or "2026/2027"
    db = get_db()

    try:
        start_y, end_y = season.split("/")
        start_y = int(start_y); end_y = int(end_y)
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
    open_buys = list(Transfers.find(
        {
            "sell": None,
            "buy.date": {"$gte": date_from, "$lte": date_to},
        },
        {"player_id": 1, "member_id": 1, "member_name": 1, "buy.date": 1, "_id": 0},
    ))
    by_member_norm = {}
    for row in open_buys:
        nm = _norm_name(row.get("member_name") or "?")
        by_member_norm.setdefault(nm, set()).add(row["player_id"])

    # 2) Alle Spieler mit `Players.owner.id` gesetzt — die "Comunio-Squad-
    #    Kandidaten". Außerdem: Alle Spieler mit aktiver price_history (für
    #    Ghost-Heuristik).
    owned = list(Players.find(
        {"owner.id": {"$ne": None}},
        {"id": 1, "name": 1, "owner": 1,
         "price_history": {"$slice": -1}, "_id": 0},
    ))

    cutoff_recent = datetime.utcnow() - timedelta(days=args.days)
    cutoff_iso = cutoff_recent.strftime("%Y-%m-%d")

    members = set()
    missing_buy   = []
    sold_stale    = []
    owner_mismatch = []
    ghosts_seen    = 0
    on_market      = 0

    for p in owned:
        owner = p.get("owner") or {}
        owner_id = owner.get("id")
        owner_name = (owner.get("name") or "?").strip()
        if owner_id is None:
            continue
        members.add((owner_id, owner_name))

        # Spieler auf dem Transfermarkt: `owner.name == "Computer"`. Kein
        # Mitspieler-Besitz → ignorieren.
        if owner_name.lower() == "computer":
            on_market += 1
            continue

        owner_first = _norm_name(owner_name).split(" ", 1)[0]
        owned_id = p["id"]

        # Such-Mengen: alle Spieler-IDs, die laut Transfers offen im
        # Squad sind UND deren Mitspieler-Vorname mit unserem
        # DB-Owner-Vornamen übereinstimmt. Treffer → alles konsistent.
        owned_ids_for_owner = set()
        for nm, ids in by_member_norm.items():
            if nm.split(" ", 1)[0] == owner_first:
                owned_ids_for_owner |= ids

        if owned_id in owned_ids_for_owner:
            continue

        # Nicht im offenen Buy-Set. Jetzt: gibt es ÜBERHAUPT einen
        # `Transfers`-Eintrag für diesen Spieler in der aktuellen Saison?
        # Genau das unterscheidet "echter Daten-Gap" von "DB-Owner ist
        # stale, weil refresh_players das Owner-Feld beim Verkauf nicht
        # gelöscht hat".
        season_rows = list(Transfers.find({
            "player_id": owned_id,
            "buy.date": {"$gte": date_from, "$lte": date_to},
        }))

        if not season_rows:
            # Komplett keine Einträge in dieser Saison → ECHTER Daten-Gap.
            # ODER: Spieler ist ein Geist aus Vor-Saison.
            # Unterscheide anhand jüngster price_history-Aktualisierung.
            last_ph = (p.get("price_history") or [{}])[-1]
            last_ts = last_ph.get("timestamp")
            if last_ts and last_ts[:10] >= cutoff_iso:
                # Kürzlich noch im Spielbetrieb → echter Gap.
                missing_buy.append({
                    "owner": owner_name,
                    "player_id": owned_id,
                    "player_name": p.get("name"),
                })
            else:
                ghosts_seen += 1
            continue

        # Es gibt Transfers-Einträge. Sind alle geschlossen (jeder hat
        # einen Sell)? Dann ist DB-Owner stale, kein Daten-Gap.
        has_open = any(not r.get("sell") for r in season_rows)
        if not has_open:
            sold_stale.append({
                "owner": owner_name,
                "player_id": owned_id,
                "player_name": p.get("name"),
                "closed_trades": len(season_rows),
                "last_buy": sorted(season_rows, key=lambda r: r["buy"]["date"])[-1]["buy"]["date"],
            })
            continue

        # Es gibt einen offenen Buy — aber für einen ANDEREN Mitspieler.
        open_rows = [r for r in season_rows if not r.get("sell")]
        for r in open_rows:
            other_member = r.get("member_name") or "?"
            if _norm_name(other_member).split(" ", 1)[0] != owner_first:
                owner_mismatch.append({
                    "db_owner": owner_name,
                    "transfers_owner": other_member,
                    "player_id": owned_id,
                    "player_name": p.get("name"),
                    "buy_date": r["buy"]["date"],
                })
                break

    # 4) Report — sauber nach Bucket getrennt
    print(f"\nAudited members: {len(members)}")
    print(f"  Open buys in DB pro member:")
    for m_name, ids in sorted(by_member_norm.items()):
        print(f"    {m_name:40s} {len(ids):>3} Spieler")

    def _print_bucket(title, rows, color_hint=""):
        print(f"\n{title}: {len(rows)}")
        for r in rows:
            extras = ", ".join(
                f"{k}={v}" for k, v in r.items()
                if k not in ("owner", "player_name", "player_id")
            )
            extra_str = f"  [{extras}]" if extras else ""
            print(f"  - {r.get('owner', r.get('db_owner', '?')):30s} "
                  f"{r.get('player_name', '?'):25s} "
                  f"(id={r['player_id']}){extra_str}")

    _print_bucket("MISSING_BUY (echter Daten-Gap, Aktion nötig)", missing_buy)
    _print_bucket("OWNER_MISMATCH (DB-Owner ≠ Transfers-Owner)", owner_mismatch)
    _print_bucket("SOLD_BUT_STALE (refresh_players hat Owner nicht gelöscht — kosmetisch)",
                  sold_stale)
    print(f"\nGHOST (Vor-Saison-Besitz, ignoriert): {ghosts_seen}")
    print(f"TRANSFER_MARKET (Owner=Computer, ignoriert): {on_market}")

    action_required = missing_buy + owner_mismatch
    cosmetic = sold_stale

    if action_required:
        print("\nFAIL: echte Daten-Divergenz. Manuell prüfen / Cron-Run triggern.")
        if args.strict:
            sys.exit(1)
    elif cosmetic:
        print("\nWARN: nur kosmetische SOLD_BUT_STALE-Funde — "
              "Dashboard bleibt korrekt. Refresh-Patch sollte das "
              "künftig verhindern.")
    else:
        print("\nOK: kein Drift festgestellt.")


if __name__ == "__main__":
    main()
