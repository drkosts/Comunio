"""Re-warm script for PortfolioCache + MarketValueCache.

Nutzt nach größeren Daten-Fixes (z.B. eingefügte Buy-Records), um den
Cache NICHT im laufenden Betrieb langsam aufzubauen. Iteriert über alle
Kombinationen (member, spielzeit), die in `Transfers` vorkommen, und
schreibt beide Cache-Collections.

Aufruf:
    MONGO_URI=mongodb://... python rewarm_portfolio_cache.py
    # Optional:
    # --seasons 2026/2027,2025/2026
    # --users Hansi Flick,Kumpel und Malocherclub
"""

import argparse
import os
import sys
import time
from datetime import datetime

from dotenv import load_dotenv
from pymongo import MongoClient


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--seasons", help="Komma-separierte Liste, sonst alle")
    p.add_argument("--users", help="Komma-separierte Liste, sonst alle")
    p.add_argument("--portfolio-only", action="store_true")
    p.add_argument("--market-only", action="store_true")
    args = p.parse_args()

    load_dotenv()
    uri = os.environ.get("MONGO_URI")
    if not uri:
        print("ERROR: MONGO_URI nicht gesetzt", file=sys.stderr)
        sys.exit(2)
    db = MongoClient(uri)["test"]

    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from crud.base import get_date_range
    from crud.portfolio import (
        calculate_portfolio_timeline_optimized,
        calculate_market_value_timeline_optimized,
        update_portfolio_cache,
    )

    T = db["Transfers"]
    seasons_filter = set(a.strip() for a in args.seasons.split(",")) if args.seasons else None
    users_filter = set(a.strip() for a in args.users.split(",")) if args.users else None

    # (member, season) Paare bestimmen.
    pairs = set()
    seen = list(T.find({}, {"member_name": 1, "buy.date": 1}))
    for d in seen:
        m = d.get("member_name")
        bd = d.get("buy", {}).get("date") or ""
        if not bd or not m:
            continue
        yr = int(bd[:4])
        mo = int(bd[5:7])
        # Saison-Heuristik: nach Jul–Dez → season yr/(yr+1); Jan–Jun → (yr-1)/yr
        if mo >= 7:
            season = f"{yr}/{yr+1}"
        else:
            season = f"{yr-1}/{yr}"
        if seasons_filter and season not in seasons_filter:
            continue
        if users_filter and m not in users_filter:
            continue
        pairs.add((m, season))

    pairs = sorted(pairs)
    print(f"Re-warming {len(pairs)} (user, season) combinations ...")

    do_portfolio = not args.market_only
    do_market    = not args.portfolio_only
    ok = 0
    fail = 0
    t0 = time.time()
    for i, (user, season) in enumerate(pairs, 1):
        try:
            if do_portfolio:
                df = calculate_portfolio_timeline_optimized(db, user, season)
                if df is not None and not df.empty:
                    cache_key = f"{user}_{season}"
                    update_portfolio_cache(db, cache_key, user, season, df)
            if do_market:
                df_mv = calculate_market_value_timeline_optimized(db, user, season)
                if df_mv is not None and not df_mv.empty:
                    cache_data = df_mv.copy()
                    if 'Datum' in cache_data.columns:
                        cache_data['Datum'] = cache_data['Datum'].astype(str)
                    db["MarketValueCache"].replace_one(
                        {"cache_key": f"{user}_{season}_market"},
                        {
                            "cache_key": f"{user}_{season}_market",
                            "user_name": user,
                            "spielzeit": season,
                            "timeline_data": cache_data.to_dict('records'),
                            "calculated_at": datetime.utcnow().isoformat(),
                        },
                        upsert=True,
                    )
            ok += 1
        except Exception as e:
            print(f"  FAIL {user}/{season}: {e}")
            fail += 1
        if i % 10 == 0:
            print(f"  {i}/{len(pairs)} done in {time.time()-t0:.1f}s")

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s — ok={ok}  fail={fail}")
    sys.exit(0 if fail == 0 else 1)


if __name__ == "__main__":
    main()
