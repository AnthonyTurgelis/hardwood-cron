#!/usr/bin/env python3
"""WNBA live odds snapshotter — captures game lines + player props for upcoming events.

Pulls current odds for every upcoming WNBA event and appends one row per
(event, snapshot_at, bookmaker, market, outcome) into:
  wnba_live_game_line_snapshots    h2h / spreads / totals
  wnba_live_player_prop_snapshots  player_points / rebounds / assists / threes

Each run is idempotent on the primary keys above — re-running within the same
second is a no-op (ON CONFLICT DO NOTHING). Scheduled every 4 hours, this
accumulates 6-10 snapshots per game covering the open→close line evolution.

Required env vars:
  SUPABASE_URL    Postgres connection URL (sqlalchemy psycopg2 format)
  ODDS_API_KEY    The Odds API key

Optional env vars:
  ODDS_BASE       Defaults to https://api.the-odds-api.com/v4

Exit codes:
  0 — success (incl. "no events upcoming today")
  1 — API or DB error
"""
import os
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import requests
from sqlalchemy import create_engine, text as sql_text

GAME_TABLE = "wnba_live_game_line_snapshots"
PROP_TABLE = "wnba_live_player_prop_snapshots"
REGIONS = "us,us2"
GAME_MARKETS = "h2h,spreads,totals"
PROP_MARKETS = "player_points,player_rebounds,player_assists,player_threes"


def py(v):
    """numpy/pandas scalar -> native Python (sqlalchemy params)."""
    if v is None:
        return None
    if isinstance(v, (np.floating, float)):
        return None if pd.isna(v) else float(v)
    if isinstance(v, (np.integer, int)):
        return int(v)
    if isinstance(v, (np.bool_, bool)):
        return bool(v)
    if isinstance(v, (pd.Timestamp, datetime)):
        return v.to_pydatetime() if hasattr(v, "to_pydatetime") else v
    if isinstance(v, str):
        return v
    return str(v)


def ensure_tables(engine):
    with engine.begin() as c:
        c.execute(sql_text(f"""
            CREATE TABLE IF NOT EXISTS {GAME_TABLE} (
                event_id TEXT NOT NULL,
                commence_time TIMESTAMPTZ NOT NULL,
                home_team TEXT, away_team TEXT,
                snapshot_at TIMESTAMPTZ NOT NULL,
                bookmaker TEXT NOT NULL,
                market TEXT NOT NULL,
                outcome_label TEXT NOT NULL,
                outcome_name TEXT,
                point DOUBLE PRECISION,
                price DOUBLE PRECISION,
                pulled_at TIMESTAMPTZ DEFAULT now(),
                PRIMARY KEY (event_id, snapshot_at, bookmaker, market, outcome_label)
            )
        """))
        c.execute(sql_text(f"""
            CREATE TABLE IF NOT EXISTS {PROP_TABLE} (
                event_id TEXT NOT NULL,
                commence_time TIMESTAMPTZ NOT NULL,
                home_team TEXT, away_team TEXT,
                snapshot_at TIMESTAMPTZ NOT NULL,
                bookmaker TEXT NOT NULL,
                market TEXT NOT NULL,
                player_name TEXT NOT NULL,
                side TEXT NOT NULL,
                point DOUBLE PRECISION,
                price DOUBLE PRECISION,
                pulled_at TIMESTAMPTZ DEFAULT now(),
                PRIMARY KEY (event_id, snapshot_at, bookmaker, market, player_name, side)
            )
        """))
        c.execute(sql_text(f"CREATE INDEX IF NOT EXISTS idx_{GAME_TABLE}_event ON {GAME_TABLE}(event_id)"))
        c.execute(sql_text(f"CREATE INDEX IF NOT EXISTS idx_{GAME_TABLE}_commence ON {GAME_TABLE}(commence_time)"))
        c.execute(sql_text(f"CREATE INDEX IF NOT EXISTS idx_{PROP_TABLE}_event ON {PROP_TABLE}(event_id)"))
        c.execute(sql_text(f"CREATE INDEX IF NOT EXISTS idx_{PROP_TABLE}_commence ON {PROP_TABLE}(commence_time)"))


def fetch_events(api_key: str, base: str) -> tuple[list, str]:
    r = requests.get(f"{base}/sports/basketball_wnba/events",
                     params={"apiKey": api_key}, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"events endpoint HTTP {r.status_code}: {r.text[:200]}")
    quota = r.headers.get("X-Requests-Remaining", "?")
    return r.json(), quota


def fetch_event_odds(api_key: str, base: str, event_id: str, markets: str) -> dict | None:
    r = requests.get(
        f"{base}/sports/basketball_wnba/events/{event_id}/odds",
        params={"apiKey": api_key, "regions": REGIONS,
                "markets": markets, "oddsFormat": "american"},
        timeout=30,
    )
    if r.status_code != 200:
        return None
    return r.json()


def insert_chunked(engine, sql: str, rows: list, chunk: int = 200) -> int:
    if not rows:
        return 0
    inserted = 0
    with engine.begin() as conn:
        for cs in range(0, len(rows), chunk):
            batch = rows[cs:cs + chunk]
            conn.execute(sql_text(sql), [{k: py(v) for k, v in r.items()} for r in batch])
            inserted += len(batch)
    return inserted


def main() -> int:
    api_key = os.environ.get("ODDS_API_KEY")
    db_url = os.environ.get("SUPABASE_URL")
    base = os.environ.get("ODDS_BASE", "https://api.the-odds-api.com/v4")
    if not api_key:
        print("FATAL: ODDS_API_KEY env var required", file=sys.stderr)
        return 1
    if not db_url:
        print("FATAL: SUPABASE_URL env var required", file=sys.stderr)
        return 1

    snapshot_at = datetime.now(timezone.utc)
    print(f"Snapshot run @ {snapshot_at.isoformat()}")

    try:
        engine = create_engine(db_url, pool_pre_ping=True)
        ensure_tables(engine)
    except Exception as e:
        print(f"FATAL: DB init failed: {e}", file=sys.stderr)
        return 1

    try:
        events, quota = fetch_events(api_key, base)
    except Exception as e:
        print(f"FATAL: fetch_events: {e}", file=sys.stderr)
        return 1

    print(f"Upcoming events: {len(events)}; quota remaining: {quota}")
    if not events:
        print("Captured 0 rows from Odds API (no upcoming events)")
        return 0

    game_sql = f"""
        INSERT INTO {GAME_TABLE}
            (event_id, commence_time, home_team, away_team, snapshot_at,
             bookmaker, market, outcome_label, outcome_name, point, price)
        VALUES
            (:event_id, :commence_time, :home_team, :away_team, :snapshot_at,
             :bookmaker, :market, :outcome_label, :outcome_name, :point, :price)
        ON CONFLICT DO NOTHING
    """
    prop_sql = f"""
        INSERT INTO {PROP_TABLE}
            (event_id, commence_time, home_team, away_team, snapshot_at,
             bookmaker, market, player_name, side, point, price)
        VALUES
            (:event_id, :commence_time, :home_team, :away_team, :snapshot_at,
             :bookmaker, :market, :player_name, :side, :point, :price)
        ON CONFLICT DO NOTHING
    """

    game_rows: list = []
    prop_rows: list = []
    api_calls_game = 0
    api_calls_prop = 0
    failed_events: list = []

    for e in events:
        eid = e["id"]
        commence = e["commence_time"]
        home = e["home_team"]
        away = e["away_team"]

        d = fetch_event_odds(api_key, base, eid, GAME_MARKETS)
        api_calls_game += 1
        if d is None:
            failed_events.append((eid, "game"))
        else:
            for b in d.get("bookmakers", []):
                for m in b.get("markets", []):
                    for o in m.get("outcomes", []):
                        name = o.get("name", "")
                        if m["key"] == "spreads":
                            label = "home" if name == home else "away"
                        elif m["key"] == "totals":
                            label = name.lower()
                        else:
                            label = "home" if name == home else "away"
                        game_rows.append({
                            "event_id": eid, "commence_time": commence,
                            "home_team": home, "away_team": away,
                            "snapshot_at": snapshot_at,
                            "bookmaker": b["key"], "market": m["key"],
                            "outcome_label": label, "outcome_name": name,
                            "point": float(o["point"]) if o.get("point") is not None else None,
                            "price": float(o["price"]) if o.get("price") is not None else None,
                        })

        d2 = fetch_event_odds(api_key, base, eid, PROP_MARKETS)
        api_calls_prop += 1
        if d2 is None:
            failed_events.append((eid, "prop"))
        else:
            for b in d2.get("bookmakers", []):
                for m in b.get("markets", []):
                    for o in m.get("outcomes", []):
                        side = o.get("name", "")
                        player_name = o.get("description", "")
                        if not player_name:
                            continue
                        prop_rows.append({
                            "event_id": eid, "commence_time": commence,
                            "home_team": home, "away_team": away,
                            "snapshot_at": snapshot_at,
                            "bookmaker": b["key"], "market": m["key"],
                            "player_name": player_name, "side": side,
                            "point": float(o["point"]) if o.get("point") is not None else None,
                            "price": float(o["price"]) if o.get("price") is not None else None,
                        })

    try:
        n_game = insert_chunked(engine, game_sql, game_rows)
        n_prop = insert_chunked(engine, prop_sql, prop_rows)
    except Exception as e:
        print(f"FATAL: DB insert failed: {e}", file=sys.stderr)
        return 1

    total = n_game + n_prop
    print(f"Captured {n_game} game-line rows + {n_prop} prop rows = {total} rows from Odds API at {snapshot_at.isoformat()}")
    print(f"  api calls: {api_calls_game} game + {api_calls_prop} prop = {api_calls_game + api_calls_prop}")
    print(f"  failed event fetches: {len(failed_events)}")

    # Don't fail the run for transient per-event 404s if we got SOMETHING.
    # Only fail if we got nothing AND there were upcoming events.
    if total == 0 and events:
        print("FATAL: zero rows captured despite upcoming events", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
