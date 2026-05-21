#!/usr/bin/env python3
"""Freshness check for hardwood-cron snapshot tables.

Verifies that both the odds snapshotter and BDL injury snapshotter have
written rows recently. During WNBA in-season (May-Oct) we require freshness
within 12 hours; off-season the check is informational only.

Required env vars:
  SUPABASE_URL    Postgres connection URL

Exit codes:
  0 — fresh (or off-season pass-through)
  1 — stale during in-season → workflow fails → GitHub emails owner
"""
import os
import sys
from datetime import datetime, timezone

from sqlalchemy import create_engine, text as sql_text

STALE_HOURS = 0.5
IN_SEASON_MONTHS = {5, 6, 7, 8, 9, 10}

TABLES = [
    ("wnba_live_game_line_snapshots", "snapshot_at"),
    ("wnba_live_player_prop_snapshots", "snapshot_at"),
    ("wnba_bdl_injury_snapshots", "snapshot_at"),
]


def check_table(engine, table: str, column: str) -> tuple[float | None, datetime | None]:
    """Return (hours_since_latest, latest_ts). None if table empty or missing."""
    sql = sql_text(f"SELECT MAX({column}) AS latest FROM {table}")
    with engine.connect() as conn:
        row = conn.execute(sql).fetchone()
    latest = row[0] if row else None
    if latest is None:
        return None, None
    if latest.tzinfo is None:
        latest = latest.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    hours = (now - latest).total_seconds() / 3600.0
    return hours, latest


def main() -> int:
    db_url = os.environ.get("SUPABASE_URL")
    if not db_url:
        print("FATAL: SUPABASE_URL env var required", file=sys.stderr)
        return 1

    now_utc = datetime.now(timezone.utc)
    in_season = now_utc.month in IN_SEASON_MONTHS
    print(f"Freshness check @ {now_utc.isoformat()}  (in_season={in_season}, threshold={STALE_HOURS}h)")

    try:
        engine = create_engine(db_url, pool_pre_ping=True)
    except Exception as e:
        print(f"FATAL: DB init failed: {e}", file=sys.stderr)
        return 1

    any_stale = False
    for table, col in TABLES:
        try:
            hours, latest = check_table(engine, table, col)
        except Exception as e:
            print(f"  {table:42s} ERROR: {e}")
            any_stale = True
            continue
        if hours is None:
            print(f"  {table:42s} EMPTY (no rows)")
            if in_season:
                any_stale = True
            continue
        status = "FRESH" if hours <= STALE_HOURS else "STALE"
        print(f"  {table:42s} {status}  (last snap {hours:.1f}h ago at {latest.isoformat()})")
        if hours > STALE_HOURS:
            any_stale = True

    if any_stale and in_season:
        print(f"\nVERDICT: at least one table is stale during in-season ({STALE_HOURS}h threshold)")
        return 1
    if any_stale and not in_season:
        print(f"\nVERDICT: some tables stale but off-season — pass-through (informational only)")
        return 0
    print("\nVERDICT: all tables fresh")
    return 0


if __name__ == "__main__":
    sys.exit(main())
