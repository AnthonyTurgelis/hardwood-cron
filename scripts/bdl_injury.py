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
  1 — API or DB error (always returns non-zero on any failure path)

Local test (PowerShell):
  $env:SUPABASE_URL = "postgresql+psycopg2://..."
  $env:BDL_API_KEY  = "..."
  python scripts/bdl_injury.py
"""
import json
import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from sqlalchemy import create_engine

BDL_INJURIES_URL = "https://api.balldontlie.io/wnba/v1/player_injuries"
TARGET_TABLE = "wnba_bdl_injury_snapshots"

MAX_PAGES = 30
RETRY_STATUS = {429, 500, 502, 503, 504}
RETRY_ATTEMPTS = 4   # 1 initial + 3 retries
RETRY_BACKOFF_SEC = (2, 5, 15)


def _log_response_diag(r: requests.Response, page: int) -> None:
    """Dump headers + body excerpt so workflow logs reveal the real failure cause."""
    diag_headers = {k: r.headers.get(k) for k in (
        "Content-Type", "X-RateLimit-Remaining", "X-RateLimit-Limit",
        "X-RateLimit-Reset", "Retry-After",
    )}
    print(f"  diag@page{page}: status={r.status_code} headers={diag_headers}",
          file=sys.stderr)
    print(f"  diag@page{page}: body[:1000]={r.text[:1000]!r}", file=sys.stderr)


def fetch_page(headers, cursor, page):
    """Fetch one page with retry on 429/5xx. Returns parsed JSON dict or raises RuntimeError."""
    params = {"per_page": 100}
    if cursor:
        params["cursor"] = cursor
    last_err = None
    for attempt in range(RETRY_ATTEMPTS):
        try:
            r = requests.get(BDL_INJURIES_URL, headers=headers, params=params, timeout=30)
        except Exception as e:
            last_err = e
            if attempt < RETRY_ATTEMPTS - 1:
                wait = RETRY_BACKOFF_SEC[min(attempt, len(RETRY_BACKOFF_SEC) - 1)]
                print(f"  page{page} attempt{attempt + 1}: request exc {e!r} — retry in {wait}s",
                      file=sys.stderr)
                time.sleep(wait)
                continue
            raise RuntimeError(f"request failed at page {page} after {RETRY_ATTEMPTS} attempts: {e!r}")

        if r.status_code == 200:
            try:
                return r.json()
            except Exception as e:
                _log_response_diag(r, page)
                raise RuntimeError(f"200 with non-JSON body at page {page}: {e!r}")

        if r.status_code in RETRY_STATUS and attempt < RETRY_ATTEMPTS - 1:
            wait = RETRY_BACKOFF_SEC[min(attempt, len(RETRY_BACKOFF_SEC) - 1)]
            print(f"  page{page} attempt{attempt + 1}: HTTP {r.status_code} — retry in {wait}s",
                  file=sys.stderr)
            _log_response_diag(r, page)
            time.sleep(wait)
            continue

        _log_response_diag(r, page)
        raise RuntimeError(f"HTTP {r.status_code} at page {page} (non-retryable or retries exhausted)")

    raise RuntimeError(f"page {page} exhausted retries; last exc: {last_err!r}")


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
        try:
            d = fetch_page(headers, cursor, page)
        except RuntimeError as e:
            print(f"FATAL: {e}", file=sys.stderr)
            return 1
        all_records.extend(d.get("data", []))
        cursor = (d.get("meta") or {}).get("next_cursor")
        page += 1
        if not cursor:
            break
        if page >= MAX_PAGES:
            # Never silently truncate. If we hit this cap with cursor still set,
            # BDL paginated beyond what we expected — fail loud so the cap can be raised.
            print(f"FATAL: pagination cap {MAX_PAGES} hit with cursor still set "
                  f"(have {len(all_records)} records so far) — raise MAX_PAGES and re-run",
                  file=sys.stderr)
            return 1

    print(f"Pulled {len(all_records)} records across {page} pages")

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    try:
        engine = create_engine(db_url, pool_pre_ping=True)
    except Exception as e:
        print(f"FATAL: DB init failed: {e!r}", file=sys.stderr)
        return 1

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
            print(f"FATAL: DB write failed (marker): {e!r}", file=sys.stderr)
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
        print(f"FATAL: DB write failed: {e!r}", file=sys.stderr)
        return 1

    print(f"Captured {len(df)} rows from BDL injuries at {now.isoformat()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
