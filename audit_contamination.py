"""Comprehensive contamination audit.

Strategy: for every transfer entry where buy.from_id != 1 (i.e., bought
via BETWEEN_USERS), there should be only TWO legitimate states:
  1. sell is None → buyer still owns the player
  2. sell corresponds to a real raw TRANSACTION_TRANSFER event
     (sell.to_id matches a buyer in that raw event, AND
      sell.datetime matches that event's datetime)

Everything else is contamination from the sell-leak bug.
"""
from collections import defaultdict
from datetime import datetime

from database.base import get_db

db = get_db()

# Step 1: Build lookup of legitimate sells from raw TRANSACTION_TRANSFER
# Map: (player_id, sell.datetime) -> set of legitimate (to_id, price)
legit_sells = defaultdict(set)
all_raw = list(db.TRANSACTION_TRANSFER.find({}))
for d in all_raw:
    raw_dt = d['date']
    msg = d.get('message', {})
    for k in ('FROM_COMPUTER', 'TO_COMPUTER', 'BETWEEN_USERS'):
        for t in (msg.get(k) or []):
            pid = t['tradable']['id']
            to = t['to']
            price = t['price']
            legit_sells[(pid, raw_dt)].add((to['id'], price))

# Step 2: Walk every transfer entry and flag contamination
all_transfers = list(db.Transfers.find({}))
contaminations = []
checked = 0
for t in all_transfers:
    checked += 1
    sell = t.get('sell')
    if sell is None:
        continue
    # Only flag BETWEEN_USERS buys (the bug class)
    if t['buy'].get('from_id') == 1:
        continue
    pid = t['player_id']
    sell_dt = sell.get('datetime')
    if not sell_dt:
        continue
    key = (pid, sell_dt)
    legit_options = legit_sells.get(key, set())
    is_legit = (sell['to_id'], sell['price']) in legit_options
    if not is_legit:
        contaminations.append({
            '_id': t['_id'],
            'player': t['player_name'],
            'member': t['member_name'],
            'buy_date': t['buy']['datetime'],
            'buy_price': t['buy']['price'],
            'sell_to': sell['to_name'],
            'sell_price': sell['price'],
            'sell_datetime': sell_dt,
        })

print(f"Checked {checked} transfer entries total")
print(f"Found {len(contaminations)} contaminated BETWEEN_USERS buyer entries")
print()

# Print all contaminations
contaminations.sort(key=lambda x: x['buy_date'])
groups = defaultdict(list)
for c in contaminations:
    groups[c['buy_date']].append(c)

for dt, items in sorted(groups.items()):
    print(f"=== {dt} ===")
    for c in items:
        print(f"  _id={c['_id']}")
        print(f"    {c['member']:25s} Bought {c['player']:25s} at {c['buy_price']:>10,}")
        print(f"    CONTAMINATED sell: to {c['sell_to']:25s} at {c['sell_price']:>10,} (date {c['sell_datetime']})")
    print()
