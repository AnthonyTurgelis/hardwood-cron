#!/usr/bin/env python3
"""WNBA daily-rosters refresher (ESPN) + bdl_active_players re-derive.

The REAL fix for the v84 ghost filter's KEEP-signal A (delta v95 recon):
  - wnba_daily_rosters (ESPN current rosters) is the stale ROOT — no refresher.
  - wnba_bdl_active_players' clean ~213 snapshot is DERIVED from it
    (raw_payload source='wnba_daily_rosters_espn_match'), NOT a BDL /players pull
    (which returns 856 incl. defunct Houston Comets — the documented bug).

This cron:
  1. Pulls the 15 current WNBA team rosters from ESPN's public JSON API
     (site.api.espn.com/.../teams/{id}/roster — pure Python, no wehoop/R).
  2. Writes a fresh wnba_daily_rosters snapshot (DELETE-today + INSERT).
  3. Re-derives the clean wnba_bdl_active_players snapshot from it via the
     cell_35v3 espn_athlete_id + accent-normalized-name bridge.

Idempotency (three layers, Section 2.F):
  1. pre-flight: skip if today's snapshot already written (unless DRY_RUN).
  2. DELETE WHERE snapshot_date=today before INSERT (both tables).
  3. SANITY GUARD: assert ~150-300 rows / ~15 teams before each write; FATAL if
     >300 (catches the 856-bloat regression) or <120 (catches an under-pull).

Required env: SUPABASE_URL.   Optional: DRY_RUN (skip writes; print counts).
Exit: 0 ok / 1 any error (ESPN 401/403/429/5xx hard-stop, empty/oversized pull, DB).
"""
import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from sqlalchemy import create_engine, text as sql_text

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba"
UA = {"User-Agent": "Mozilla/5.0"}
# espn_team_id -> canonical abbr (the 15 current franchises; matches the existing
# wnba_daily_rosters + wnba_bdl_team_map.team_abbr_canonical). Pinned so we never
# pull defunct/all-star teams (the 856-bloat trap).
ESPN_TEAM_MAP = {
    20: "ATL", 19: "CHI", 18: "CON", 3: "DAL", 129689: "GSV", 5: "IND",
    6: "LAS", 17: "LVA", 8: "MIN", 9: "NYL", 11: "PHO", 132052: "POR",
    14: "SEA", 131935: "TOR", 16: "WAS",
}
HARD_STOP = {401, 403, 429, 500, 502, 503, 504}
COLS = ["snapshot_date", "team_abbr", "espn_team_id", "espn_player_id", "player_name",
        "first_name", "last_name", "position", "jersey", "height_display", "height_inches",
        "weight_lbs", "age", "experience_years", "college", "birth_city", "birth_country",
        "headshot_url", "status", "fetched_at"]


def hr(t):
    print("\n" + "=" * 78 + f"\n  {t}\n" + "=" * 78, flush=True)


def get_json(url, tries=3):
    for k in range(tries):
        try:
            r = requests.get(url, headers=UA, timeout=20)
        except Exception as e:
            if k < tries - 1:
                time.sleep(2 + k); continue
            raise RuntimeError(f"request failed: {url} :: {e!r}")
        if r.status_code in HARD_STOP:
            raise RuntimeError(f"HARD STOP HTTP {r.status_code}: {url} :: {r.text[:160]!r}")
        if r.status_code == 200:
            return r.json()
        if k < tries - 1:
            time.sleep(2 + k); continue
        raise RuntimeError(f"HTTP {r.status_code}: {url}")
    raise RuntimeError(f"exhausted: {url}")


def _g(d, *path, default=None):
    cur = d
    for p in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p)
    return cur if cur is not None else default


def pull_rosters(today, now):
    rows, seen = [], set()
    for tid, abbr in ESPN_TEAM_MAP.items():
        data = get_json(f"{ESPN_BASE}/teams/{tid}/roster")
        ath = data.get("athletes", [])
        # flat list (observed) or position-grouped {items:[...]} — handle both
        flat = []
        for a in ath:
            flat.extend(a["items"]) if isinstance(a, dict) and "items" in a else flat.append(a)
        for a in flat:
            pid = a.get("id")
            if pid is None or (tid, pid) in seen:
                continue
            seen.add((tid, pid))
            rows.append({
                "snapshot_date": today, "team_abbr": abbr, "espn_team_id": str(tid),
                "espn_player_id": str(pid), "player_name": a.get("displayName"),
                "first_name": a.get("firstName"), "last_name": a.get("lastName"),
                "position": _g(a, "position", "abbreviation"), "jersey": a.get("jersey"),
                "height_display": a.get("displayHeight"),
                "height_inches": float(a["height"]) if a.get("height") is not None else None,
                "weight_lbs": int(a["weight"]) if a.get("weight") is not None else None,
                "age": int(a["age"]) if a.get("age") is not None else None,
                "experience_years": _g(a, "experience", "years"),
                "college": _g(a, "college", "name"),
                "birth_city": _g(a, "birthPlace", "city"),
                "birth_country": _g(a, "birthPlace", "country"),
                "headshot_url": _g(a, "headshot", "href"),
                "status": _g(a, "status", "name"),
                "fetched_at": now,
            })
        time.sleep(0.4)
    return rows


REDERIVE_ESPN = """
    INSERT INTO wnba_bdl_active_players (snapshot_date, bdl_player_id, team_id, raw_payload)
    SELECT :d, m.bdl_player_id, t.bdl_team_id,
        jsonb_build_object('source','wnba_daily_rosters_espn_match',
            'roster_snapshot_date',dr.snapshot_date::text,'team_abbr',dr.team_abbr,
            'player_name',dr.player_name,'position',dr.position,'status',dr.status,
            'espn_player_id',dr.espn_player_id)
    FROM wnba_daily_rosters dr
    JOIN wnba_bdl_player_map m ON m.espn_athlete_id = dr.espn_player_id::bigint
    JOIN wnba_bdl_team_map t ON t.team_abbr_canonical = dr.team_abbr
    WHERE dr.snapshot_date = (SELECT MAX(snapshot_date) FROM wnba_daily_rosters)
    ON CONFLICT (snapshot_date, bdl_player_id) DO NOTHING
"""
REDERIVE_NAME = """
    INSERT INTO wnba_bdl_active_players (snapshot_date, bdl_player_id, team_id, raw_payload)
    WITH unbridged AS (
        SELECT dr.team_abbr, dr.player_name, dr.position, dr.status, dr.espn_player_id, dr.snapshot_date,
               LOWER(TRANSLATE(dr.player_name,
                 'àáâãäåèéêëìíîïòóôõöùúûüñçÀÁÂÃÄÅÈÉÊËÌÍÎÏÒÓÔÕÖÙÚÛÜÑÇ',
                 'aaaaaaeeeeiiiiooooouuuuncAAAAAAEEEEIIIIOOOOOUUUUNC')) AS name_norm
        FROM wnba_daily_rosters dr
        WHERE dr.snapshot_date = (SELECT MAX(snapshot_date) FROM wnba_daily_rosters)
          AND NOT EXISTS (SELECT 1 FROM wnba_bdl_player_map m WHERE m.espn_athlete_id = dr.espn_player_id::bigint)
    ),
    bdl_names AS (
        SELECT bdl_player_id, canonical_name,
               LOWER(TRANSLATE(canonical_name,
                 'àáâãäåèéêëìíîïòóôõöùúûüñçÀÁÂÃÄÅÈÉÊËÌÍÎÏÒÓÔÕÖÙÚÛÜÑÇ',
                 'aaaaaaeeeeiiiiooooouuuuncAAAAAAEEEEIIIIOOOOOUUUUNC')) AS name_norm
        FROM wnba_bdl_player_map WHERE canonical_name IS NOT NULL
    ),
    matched AS (
        SELECT DISTINCT ON (u.team_abbr, u.player_name)
            u.team_abbr, u.player_name, u.position, u.status, u.espn_player_id, u.snapshot_date, b.bdl_player_id
        FROM unbridged u JOIN bdl_names b ON b.name_norm = u.name_norm
        ORDER BY u.team_abbr, u.player_name, b.bdl_player_id
    )
    SELECT :d, mm.bdl_player_id, t.bdl_team_id,
        jsonb_build_object('source','wnba_daily_rosters_name_fallback','team_abbr',mm.team_abbr,
            'player_name',mm.player_name,'position',mm.position,'status',mm.status,
            'espn_player_id',mm.espn_player_id)
    FROM matched mm JOIN wnba_bdl_team_map t ON t.team_abbr_canonical = mm.team_abbr
    ON CONFLICT (snapshot_date, bdl_player_id) DO NOTHING
"""
# COUNT-only preview of the re-derive (for DRY_RUN) against the latest daily_rosters
REDERIVE_PREVIEW = """
    WITH espn AS (
        SELECT m.bdl_player_id FROM wnba_daily_rosters dr
        JOIN wnba_bdl_player_map m ON m.espn_athlete_id = dr.espn_player_id::bigint
        JOIN wnba_bdl_team_map t ON t.team_abbr_canonical = dr.team_abbr
        WHERE dr.snapshot_date = (SELECT MAX(snapshot_date) FROM wnba_daily_rosters)
    )
    SELECT COUNT(DISTINCT bdl_player_id) FROM espn
"""


def main() -> int:
    db_url = os.environ.get("SUPABASE_URL")
    if not db_url:
        print("FATAL: SUPABASE_URL required", file=sys.stderr); return 1
    dry = bool(os.environ.get("DRY_RUN"))
    today = datetime.now(timezone.utc).date()
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    print(f"DAILY ROSTERS REFRESH @ {now.isoformat()}  snapshot_date={today}  DRY_RUN={dry}")
    try:
        engine = create_engine(db_url, pool_pre_ping=True)
    except Exception as e:
        print(f"FATAL: DB init failed: {e!r}", file=sys.stderr); return 1

    # Layer-1 idempotency
    try:
        existing = pd.read_sql(sql_text(
            "SELECT COUNT(*) n FROM wnba_daily_rosters WHERE snapshot_date=:d"),
            engine, params={"d": today}).iloc[0]["n"]
    except Exception as e:
        print(f"FATAL: pre-flight failed: {e!r}", file=sys.stderr); return 1
    if existing and not dry:
        print(f"  daily_rosters already has {int(existing)} rows for {today} — no-op."); return 0

    # -- Pull ESPN rosters --
    hr("Step 1: pull 15 current-team rosters (ESPN)")
    try:
        rows = pull_rosters(today, now)
    except RuntimeError as e:
        print(f"FATAL: {e}", file=sys.stderr); return 1
    df = pd.DataFrame(rows)
    n_teams = df["team_abbr"].nunique() if len(df) else 0
    print(f"  pulled {len(df)} players across {n_teams} teams")
    if len(df):
        print("  per-team:", df.groupby("team_abbr").size().to_dict())

    # SANITY GUARD (catches bloat / under-pull)
    if not (120 <= len(df) <= 300) or n_teams != 15:
        print(f"FATAL: sanity gate — {len(df)} rows / {n_teams} teams (want ~150-220 / 15). "
              f"Refusing to write.", file=sys.stderr)
        return 1

    if dry:
        hr("DRY_RUN — skipping writes; previewing re-derive against current daily_rosters")
        try:
            prev = pd.read_sql(sql_text(REDERIVE_PREVIEW), engine).iloc[0, 0]
            print(f"  re-derive (espn-match) would yield ~{int(prev)} bdl_active rows (vs the 856 bloat)")
        except Exception as e:
            print(f"  preview err: {e!r}")
        print(f"  would write {len(df)} daily_rosters rows for {today}")
        return 0

    # -- Write daily_rosters (DELETE-today + INSERT) --
    hr("Step 2: write wnba_daily_rosters snapshot")
    try:
        with engine.begin() as c:
            c.execute(sql_text("DELETE FROM wnba_daily_rosters WHERE snapshot_date=:d"), {"d": today})
        df[COLS].to_sql("wnba_daily_rosters", engine, if_exists="append", index=False, method="multi", chunksize=200)
    except Exception as e:
        print(f"FATAL: daily_rosters write failed: {e!r}", file=sys.stderr); return 1
    print(f"  wrote {len(df)} rows for {today}")

    # -- Re-derive bdl_active_players (cell_35v3 bridge) --
    hr("Step 3: re-derive wnba_bdl_active_players (espn_athlete_id + name fallback)")
    try:
        with engine.begin() as c:
            c.execute(sql_text("DELETE FROM wnba_bdl_active_players WHERE snapshot_date=:d"), {"d": today})
            n1 = c.execute(sql_text(REDERIVE_ESPN), {"d": today}).rowcount
            n2 = c.execute(sql_text(REDERIVE_NAME), {"d": today}).rowcount
            total = pd.read_sql(sql_text(
                "SELECT COUNT(*) n FROM wnba_bdl_active_players WHERE snapshot_date=:d"),
                c, params={"d": today}).iloc[0]["n"]
            # GUARD inside txn: rollback the bloat regression
            if int(total) > 300 or int(total) < 120:
                raise RuntimeError(f"re-derive sanity: {int(total)} bdl_active rows (want ~150-220) — rolling back")
        print(f"  bdl_active re-derived: {n1} via espn_id + {n2} via name = {int(total)} rows")
    except Exception as e:
        print(f"FATAL: bdl_active re-derive failed/rolled back: {e!r}", file=sys.stderr); return 1

    print(f"\n  DONE — daily_rosters + bdl_active fresh for {today}. Ghost filter signal A future-proofed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
