#!/usr/bin/env python3
"""WNBA 2026 score feeder — the LINCHPIN that makes the live-strength chain move.

PROBLEM (delta v165 diagnosis): wnba_games carries the full 2026 schedule (335
rows through 2026-09-25) but scores were frozen at 2026-05-14 — NO automated
process fills home_score/away_score as games are played. The chain
  games scores -> series_extend_refresh -> strength_live_refresh -> front-end
is therefore inert: with no fresh scores, the Kalman/Glicko-2 series never extend
and the live surface never moves.

THIS feeder fills played 2026 scores from ESPN's public scoreboard JSON — the
SAME ingest pattern already used by daily_rosters_refresh.py (site.api.espn.com,
pure Python, no API key). wnba_games.game_id for 2026 IS the ESPN event id
(verified: 401867793-style; historical 2024 used a different synthetic id, but
2026 was seeded with ESPN ids), so the match is exact and safe.

Existing-first: the schedule rows already exist; we only UPDATE scores, never
INSERT games. Idempotent: only fills rows where home_score IS NULL (WHERE-guarded
in the UPDATE too), so a re-run on the same slate is a net no-op and already-final
scores are never rewritten. ESPN events that do not match a scheduled game_id are
reported for human review, never guessed.

CHAINING: runs FIRST in the daily workflow, before series_extend_refresh.py.

Required env: SUPABASE_URL.   Optional: DRY_RUN (fetch + match + print, no write).
Exit: 0 ok / 1 any error (DB init, ESPN hard-stop, sanity gate).
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
HARD_STOP = {401, 403, 429, 500, 502, 503, 504}
SEASON = 2026
SCORE_MIN, SCORE_MAX = 1, 250   # sanity band for a final WNBA team score

# ESPN abbreviation -> canonical wnba_games.home_team_abbr (mirrors src.utils.canonical_team)
ESPN_ABBR = {
    "ATL": "ATL", "CHI": "CHI", "CONN": "CON", "CON": "CON", "DAL": "DAL",
    "GS": "GSV", "GSV": "GSV", "IND": "IND", "LV": "LVA", "LVA": "LVA",
    "LA": "LAS", "LAS": "LAS", "MIN": "MIN", "NY": "NYL", "NYL": "NYL",
    "PHX": "PHO", "PHO": "PHO", "POR": "POR", "SEA": "SEA", "TOR": "TOR",
    "WAS": "WAS", "WSH": "WAS",
}


def hr(t):
    print("\n" + "=" * 78 + f"\n  {t}\n" + "=" * 78, flush=True)


def canon(abbr):
    if not abbr:
        return None
    return ESPN_ABBR.get(str(abbr).upper().strip(), str(abbr).upper().strip())


def get_json(url, params=None, tries=3):
    for k in range(tries):
        try:
            r = requests.get(url, params=params, headers=UA, timeout=20)
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


def fetch_scoreboard(yyyymmdd):
    """Return list of completed games: {game_id, home_abbr, away_abbr, home_score, away_score}."""
    js = get_json(f"{ESPN_BASE}/scoreboard", params={"dates": yyyymmdd})
    out = []
    for ev in js.get("events", []):
        comps = ev.get("competitions", [])
        if not comps:
            continue
        comp = comps[0]
        st = (comp.get("status") or ev.get("status") or {}).get("type", {})
        if not (st.get("completed") or st.get("state") == "post"):
            continue  # not final yet — skip (revisited on a later run)
        home = away = None
        for cc in comp.get("competitors", []):
            abbr = canon((cc.get("team") or {}).get("abbreviation"))
            try:
                score = int(cc.get("score"))
            except (TypeError, ValueError):
                score = None
            if cc.get("homeAway") == "home":
                home = (abbr, score)
            elif cc.get("homeAway") == "away":
                away = (abbr, score)
        if home and away and home[1] is not None and away[1] is not None:
            out.append({"game_id": str(ev.get("id")), "home_abbr": home[0], "away_abbr": away[0],
                        "home_score": home[1], "away_score": away[1]})
    return out


def main() -> int:
    db_url = os.environ.get("SUPABASE_URL")
    if not db_url:
        print("FATAL: SUPABASE_URL required", file=sys.stderr); return 1
    dry = bool(os.environ.get("DRY_RUN"))
    today = datetime.now(timezone.utc).date()
    print(f"GAMES SCORE FEEDER @ {datetime.now(timezone.utc).isoformat()}  season={SEASON}  "
          f"today={today}  DRY_RUN={dry}")
    try:
        engine = create_engine(db_url, pool_pre_ping=True)
    except Exception as e:
        print(f"FATAL: DB init failed: {e!r}", file=sys.stderr); return 1

    # the fillable gap: scheduled 2026 rows, in the past, still missing a score
    try:
        gap = pd.read_sql(sql_text("""
            SELECT game_id, game_date, home_team_abbr, away_team_abbr
            FROM wnba_games
            WHERE season=:s AND home_score IS NULL AND game_date::date <= :t
            ORDER BY game_date
        """), engine, params={"s": SEASON, "t": today})
    except Exception as e:
        print(f"FATAL: gap query failed: {e!r}", file=sys.stderr); return 1

    hr("GAP — past 2026 games missing a score")
    print(f"  unplayed-but-past rows: {len(gap)}")
    if gap.empty:
        print("  ZERO gap — every past 2026 game already has a score. No-op.")
        return 0
    gids = set(gap["game_id"].astype(str))
    dates = sorted({str(d)[:10].replace("-", "") for d in gap["game_date"]})
    print(f"  dates to fetch from ESPN: {len(dates)} ({dates[0]}..{dates[-1]})")

    # fetch ESPN scoreboard per gap-date, match by exact game_id (ESPN event id)
    updates, unmatched = [], []
    for d in dates:
        try:
            evs = fetch_scoreboard(d)
        except Exception as e:
            print(f"  ESPN fetch {d} failed: {e!r}", file=sys.stderr)
            return 1
        for ev in evs:
            if ev["game_id"] in gids:
                if not (SCORE_MIN <= ev["home_score"] <= SCORE_MAX and
                        SCORE_MIN <= ev["away_score"] <= SCORE_MAX):
                    print(f"  SKIP implausible score {ev}", file=sys.stderr); continue
                ev["home_wl"] = "W" if ev["home_score"] > ev["away_score"] else "L"
                updates.append(ev)
            else:
                unmatched.append(ev)

    hr("MATCH")
    print(f"  ESPN completed games matched to scheduled game_id: {len(updates)}")
    if unmatched:
        print(f"  ESPN completed games NOT matching any scheduled 2026 game_id "
              f"(review, not written): {len(unmatched)}")
        for ev in unmatched[:8]:
            print(f"    espn {ev['game_id']}: {ev['away_abbr']}@{ev['home_abbr']} "
                  f"{ev['away_score']}-{ev['home_score']}")
    if not updates:
        print("\n  No matched final scores to write. No-op.")
        return 0

    sample = updates[: min(10, len(updates))]
    print("\n  would fill:")
    for ev in sample:
        print(f"    {ev['game_id']}: {ev['away_abbr']}@{ev['home_abbr']} "
              f"{ev['away_score']}-{ev['home_score']} (home {ev['home_wl']})")

    if dry:
        hr("DRY_RUN — skipping writes")
        print(f"  would UPDATE {len(updates)} wnba_games rows (home_score IS NULL guard; idempotent).")
        return 0

    hr("PERSIST (UPDATE, NULL-guarded, idempotent)")
    n = 0
    try:
        with engine.begin() as c:
            for ev in updates:
                res = c.execute(sql_text("""
                    UPDATE wnba_games
                    SET home_score=:hs, away_score=:as_, home_wl=:wl
                    WHERE game_id=:gid AND season=:s AND home_score IS NULL
                """), {"hs": int(ev["home_score"]), "as_": int(ev["away_score"]),
                       "wl": ev["home_wl"], "gid": ev["game_id"], "s": SEASON})
                n += res.rowcount
    except Exception as e:
        print(f"FATAL: update failed: {e!r}", file=sys.stderr); return 1
    print(f"  filled {n} rows (of {len(updates)} matched).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
