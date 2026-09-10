"""Fix duplicate-attached sells using LIFO rule.

Bug: when a player was bought multiple times and sold once, the same
sell got attached to every buy entry (Mongo's natural order, no ordering).
The fix: keep the sell only on the MOST RECENT buy before that sell
(matches user intuition when multiple buys exist), clear from the rest.

Run with --dry-run to preview, --apply to commit.
"""
import argparse
import sys
from collections import defaultdict

# Make sure we use the local database.py module (not the backend one)
sys.path.insert(0, ".")
from database import get_db


def find_duplicates(db):
    """Group transfers by (member_id, player_id, sell.datetime).
    Only groups with >1 entry are duplicates.
    """
    sell_groups = defaultdict(list)
    for t in db.Transfers.find({"sell": {"$ne": None}}):
        sell = t.get("sell")
        if not isinstance(sell, dict):
            continue
        sell_dt = sell.get("datetime", "")
        if not sell_dt:
            continue
        key = (t["member_id"], t["player_id"], sell_dt)
        sell_groups[key].append(t)
    return {k: v for k, v in sell_groups.items() if len(v) > 1}


def plan_fixes(duplicates):
    """For each duplicate group, pick the entry whose buy.datetime is the
    latest one strictly before the sell.datetime. Mark that one to KEEP,
    the others to CLEAR.
    """
    actions = []  # list of (entry, keep_or_clear)
    for (member_id, player_id, sell_dt), entries in sorted(duplicates.items()):
        # Find entries with buy.datetime < sell_dt (the buy constraint)
        eligible = [e for e in entries if e["buy"]["datetime"] < sell_dt]
        if not eligible:
            # No buy before the sell — preserve as-is (not fixable without
            # manual decision). Surface this in the plan.
            actions.append((entries[0], "orphaned"))
            for e in entries[1:]:
                actions.append((e, "orphaned"))
            continue
        # Pick the latest eligible buy
        keep = max(eligible, key=lambda e: e["buy"]["datetime"])
        for e in entries:
            if e["_id"] == keep["_id"]:
                actions.append((e, "keep"))
            else:
                actions.append((e, "clear"))
    return actions


def format_plan(actions):
    """Pretty-print the planned actions grouped by member/player."""
    by_group = defaultdict(list)
    for entry, action in actions:
        by_group[(entry["member_name"], entry["player_name"], entry.get("sell", {}).get("datetime", "?"))].append((entry, action))

    n_keep = sum(1 for _, a in actions if a == "keep")
    n_clear = sum(1 for _, a in actions if a == "clear")
    n_orphaned = sum(1 for _, a in actions if a == "orphaned")

    print(f"Plan summary:")
    print(f"  Keep (no change): {n_keep}")
    print(f"  Clear (sell=None): {n_clear}")
    print(f"  Orphan (manual): {n_orphaned}")
    print()

    for (mname, pname, sell_dt), items in sorted(by_group.items()):
        print(f"-- {mname} / {pname} / sell @ {sell_dt}")
        for entry, action in sorted(items, key=lambda x: x[0]["buy"]["datetime"]):
            marker = {"keep": "KEEP  ", "clear": "CLEAR ", "orphaned": "ORPHAN"}.get(action, "????? ")
            print(
                f"  {marker} BUY {entry['buy']['datetime']} {entry['buy']['price']:>11,} -> "
                f"SELL? {entry.get('sell') is not None}"
            )
        print()


def apply_plan(db, actions):
    """Apply the clear actions."""
    n_cleared = 0
    for entry, action in actions:
        if action == "clear":
            db.Transfers.update_one(
                {"_id": entry["_id"]},
                {"$set": {"sell": None}},
            )
            n_cleared += 1
    return n_cleared


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Show planned changes (default)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually apply the changes to MongoDB",
    )
    args = parser.parse_args()
    is_apply = args.apply

    db = get_db()
    duplicates = find_duplicates(db)
    print(f"Found {len(duplicates)} duplicate (member, player, sell) groups.\n")

    actions = plan_fixes(duplicates)
    format_plan(actions)

    if is_apply:
        confirm = input("Type 'yes' to commit: ").strip()
        if confirm.lower() == "yes":
            n = apply_plan(db, actions)
            print(f"\nApplied: cleared sell on {n} entries.")
        else:
            print("\nAborted — no changes.")
    else:
        print("(Dry run; pass --apply to commit.)")


if __name__ == "__main__":
    main()
