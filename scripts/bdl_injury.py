#!/usr/bin/env python3
"""BDL WNBA injury snapshotter — appends current league-wide injury state to Supabase.

Each run pulls the live /player_injuries snapshot and writes one row per
(snapshot_at, player) into wnba_bdl_injury_snapshots. Over time this builds
the dense time-series used for "status at tipoff" labels in The Hardwood.

Required env vars:
  SUPABASE_URL   Postgres connection URL (sqlalchemy psycopg2 format)
  BDL_API_KEY    Ball Don't Lie API key

Exit codes:
  0 — wrote N rows (incl. zero-row marker for "no injuries reported")
  1 — API or DB error

Local test (PowerShell):
  $env:SUPABASE_URL = "postgresql+psycopg2://..."
  $env:BDL_API_KEY  = "..."
  python scripts/bdl_injury.py
"""
import json
import os
import sys
from datetime import datetime, timezone

import pandas as pd
import requests
from sqlalchemy import create_engine

BDL_INJURIES_URL = "https://api.balldontlie.io/wnba/v1/player_injuries"
TARGET_TABLE = "wnba_bdl_injury_snapshots"


def main() -> int:
    bdl_key = os.environ.get("BDL_API_KEY")
    db_url = os.environ.get("SUPABASE_URL")
    if not bdl_key:
        print("FATAL: BDL_API_KEY env var required", file=sys.stderr)
        return 1
    if not db_url:
        print("FATAL: SUPABASE_URL env var required", file=sys.stderr)
        return 1

    headers = {"Authorization": bdl_key}

    all_records = []
    cursor = None
    page = 0
    while True:
        params = {"per_page": 100}
        if cursor:
            params["cursor"] = cursor
        try:
            r = requests.get(BDL_INJURIES_URL, headers=headers, params=params, timeout=30)
        except Exception as e:
            print(f"FATAL: request failed at page {page}: {e}", file=sys.stderr)
            return 1
        if r.status_code != 200:
            print(f"FATAL: HTTP {r.status_code} at page {page}: {r.text[:200]}", file=sys.stderr)
            return 1
        d = r.json()
        all_records.extend(d.get("data", []))
        cursor = (d.get("meta") or {}).get("next_cursor")
        page += 1
        if not cursor or page > 30:
            break

    print(f"Pulled {len(all_records)} records across {page} pages")

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    engine = create_engine(db_url, pool_pre_ping=True)

    if not all_records:
        marker = pd.DataFrame([{
            "snapshot_at": now,
            "bdl_player_id": None, "player_first_name": None, "player_last_name": None,
            "team_id": None, "team_abbreviation": None,
            "status": "NO_INJURIES", "return_date": None, "description": None,
            "raw_payload": "{}",
        }])
        try:
            marker.to_sql(TARGET_TABLE, engine, if_exists="append", index=False, method="multi")
        except Exception as e:
            print(f"FATAL: DB write failed (marker): {e}", file=sys.stderr)
            return 1
        print(f"Captured 0 rows from BDL injuries at {now.isoformat()} (wrote NO_INJURIES marker)")
        return 0

    rows = []
    for rec in all_records:
        player = rec.get("player") or {}
        team = player.get("team") or {}
        rows.append({
            "snapshot_at": now,
            "bdl_player_id": player.get("id"),
            "player_first_name": player.get("first_name"),
            "player_last_name": player.get("last_name"),
            "team_id": team.get("id"),
            "team_abbreviation": team.get("abbreviation"),
            "status": rec.get("status"),
            "return_date": rec.get("return_date"),
            "description": rec.get("comment"),
            "raw_payload": json.dumps(rec),
        })

    try:
        df = pd.DataFrame(rows)
        df.to_sql(TARGET_TABLE, engine, if_exists="append", index=False, method="multi", chunksize=200)
    except Exception as e:
        print(f"FATAL: DB write failed: {e}", file=sys.stderr)
        return 1

    print(f"Captured {len(df)} rows from BDL injuries at {now.isoformat()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
