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
import datetime as _dt
from datetime import datetime, timezone

from sqlalchemy import create_engine, text as sql_text

# 2.0h, not 0.5h: GitHub throttles the */15 snapshotter crons to ~hourly, so a
# 0.5h threshold trips on healthy crons (delta v77). 2.0h gives margin over the
# real throttled cadence while still catching a genuinely stalled writer.
STALE_HOURS = 2.0
DAILY_STALE_HOURS = 30.0   # daily-snapshot tables: 24h cadence + GHA slippage margin
IN_SEASON_MONTHS = {5, 6, 7, 8, 9, 10}

# (table, write-timestamp column, max_hours, hard_fail)
#   hard_fail=True  → staleness fails the workflow (a refresher exists for it)
#   hard_fail=False → WARN-only (surfaced but non-fatal; no refresher deployed yet)
TABLES = [
    ("wnba_live_game_line_snapshots", "snapshot_at", STALE_HOURS, True),
    ("wnba_live_player_prop_snapshots", "snapshot_at", STALE_HOURS, True),
    ("wnba_bdl_injury_snapshots", "snapshot_at", STALE_HOURS, True),
    # Canary was blind to this — the table that actually rotted (frozen 80h, v78);
    # refreshed by availability-refresh.yml.
    ("wnba_player_availability_current", "computed_at", STALE_HOURS, True),
    # KEEP-signal A of the v84 ghost filter. Its CLEAN content is derived from
    # wnba_daily_rosters (raw_payload source='wnba_daily_rosters_espn_match'), NOT a
    # raw BDL /players pull (which is a bloated all-time dump incl. defunct teams —
    # the 856-row 2026-05-19 bug). So its refresh depends on daily_rosters being
    # fresh. WARN-only until a daily_rosters→derive refresher is deployed (v9x recon).
    ("wnba_bdl_active_players", "snapshot_date", DAILY_STALE_HOURS, False),
    # KEEP-signal of the v84 ghost filter; ESPN/wehoop-sourced — NO refresher deployed
    # yet. The real stale ROOT behind signal A. WARN-only until a daily_rosters cron
    # exists (v84 #4). Build a wehoop/ESPN refresher next.
    ("wnba_daily_rosters", "fetched_at", DAILY_STALE_HOURS, False),
]


def check_table(engine, table: str, column: str) -> tuple[float | None, datetime | None]:
    """Return (hours_since_latest, latest_ts). None if table empty or missing.
    Handles DATE columns (e.g. snapshot_date) by anchoring at midnight UTC."""
    sql = sql_text(f"SELECT MAX({column}) AS latest FROM {table}")
    with engine.connect() as conn:
        row = conn.execute(sql).fetchone()
    latest = row[0] if row else None
    if latest is None:
        return None, None
    # DATE (not datetime) → anchor to midnight UTC so the arithmetic works
    if isinstance(latest, _dt.date) and not isinstance(latest, _dt.datetime):
        latest = datetime(latest.year, latest.month, latest.day, tzinfo=timezone.utc)
    elif latest.tzinfo is None:
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
    print(f"Freshness check @ {now_utc.isoformat()}  (in_season={in_season})")

    try:
        engine = create_engine(db_url, pool_pre_ping=True)
    except Exception as e:
        print(f"FATAL: DB init failed: {e}", file=sys.stderr)
        return 1

    any_stale = False
    for table, col, max_hours, hard_fail in TABLES:
        tag = "" if hard_fail else " [warn-only]"
        try:
            hours, latest = check_table(engine, table, col)
        except Exception as e:
            print(f"  {table:42s} ERROR: {e}{tag}")
            if hard_fail:
                any_stale = True
            continue
        if hours is None:
            print(f"  {table:42s} EMPTY (no rows){tag}")
            if in_season and hard_fail:
                any_stale = True
            continue
        status = "FRESH" if hours <= max_hours else "STALE"
        print(f"  {table:42s} {status}  (last {hours:.1f}h ago at {latest.isoformat()}, thresh {max_hours:.0f}h){tag}")
        if hours > max_hours and hard_fail:
            any_stale = True

    if any_stale and in_season:
        print(f"\nVERDICT: at least one hard-fail table is stale during in-season")
        return 1
    if any_stale and not in_season:
        print(f"\nVERDICT: some tables stale but off-season — pass-through (informational only)")
        return 0
    print("\nVERDICT: all tables fresh")
    return 0


if __name__ == "__main__":
    sys.exit(main())
