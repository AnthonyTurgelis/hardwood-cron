#!/usr/bin/env python3
"""WNBA LIVE 2026 team-strength surface refresher (Kalman + uncertainty band).

Standalone cron port of cells/cell_strength_live_02_build_surface.py (delta v151,
forward-build v152). Rebuilds wnba_team_strength_live_2026 @ kalman_live_2026_05_25
from the walk-forward Kalman series (wnba_team_kalman) + Glicko RD
(wnba_glicko2_ratings) + 2026 standings (wnba_games). The front-end 2026 power-
ranking widget reads this table at its latest snapshot_date (build hw-v16-0525a).

Underlying Kalman ratings were built strict-< walk-forward (predict-then-update,
v142), so each as-of value reflects only games already played — no look-ahead.

Idempotency (Section 2.F, per-model_version):
  1. CREATE TABLE IF NOT EXISTS (coexists; never TRUNCATE).
  2. DELETE WHERE model_version=:v, then full rebuild+append of the (team x date)
     grid from ALL played 2026 games. Re-running on the SAME slate is a no-op in
     net state (same rows back). As the slate advances, the rebuild EXTENDS the
     surface with new snapshot_dates under the SAME model_version. Other
     model_versions and wnba_power_rankings_v3 are never touched.
  3. SANITY GUARD: assert 15 teams and >=1 snapshot date before writing; refuse
     on an empty/degenerate pull.

Required env: SUPABASE_URL.   Optional: DRY_RUN (skip writes; print the surface).
Exit: 0 ok / 1 any error (DB init, empty pull, sanity gate).
"""
import math
import os
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text as sql_text

MODEL_VERSION = "kalman_live_2026_05_25"
KAL_MV = "kalman_walkforward_2026_05_24"
GLI_MV = "glicko2_walkforward_2026_05_24"
DEBUT_SIGMA = math.sqrt(150.0)   # Kalman P0 -> entering-season band for debut teams
OFFSEASON_VAR = 50.0             # season-bump var added to returning teams' band

# Inlined from src/utils.py canonical_team (cron repo has no src/ package).
TEAM_ABBR_VARIANTS = {
    'LV': 'LVA', 'NY': 'NYL', 'CONN': 'CON', 'CT': 'CON',
    'PHX': 'PHO', 'LA': 'LAS', 'LOS': 'LAS',
    'SA': 'SAN', 'SAS': 'SAN', 'GS': 'GSV', 'WSH': 'WAS', 'UTA': 'UTH',
    'LVA': 'LVA', 'NYL': 'NYL', 'CON': 'CON', 'PHO': 'PHO', 'LAS': 'LAS',
    'SAN': 'SAN', 'GSV': 'GSV', 'WAS': 'WAS', 'TOR': 'TOR', 'POR': 'POR',
    'MIN': 'MIN', 'IND': 'IND', 'ATL': 'ATL', 'CHI': 'CHI', 'DAL': 'DAL', 'SEA': 'SEA',
}


def canonical_team(abbr):
    if pd.isna(abbr):
        return None
    return TEAM_ABBR_VARIANTS.get(str(abbr).upper().strip(), str(abbr).upper().strip())


def py(v):
    """numpy/pandas scalar -> native Python (psycopg2 param safety)."""
    if v is None:
        return None
    if isinstance(v, (np.floating, float)):
        return None if (isinstance(v, float) and math.isnan(v)) else float(v)
    if isinstance(v, (np.integer, int)):
        return int(v)
    if isinstance(v, (np.bool_, bool)):
        return bool(v)
    return v


def hr(t):
    print("\n" + "=" * 78 + f"\n  {t}\n" + "=" * 78, flush=True)


def build_surface(engine):
    with engine.connect() as c:
        games = pd.read_sql(sql_text("""
            SELECT game_date, season, home_team_abbr AS home, away_team_abbr AS away,
                   home_score, away_score
            FROM wnba_games WHERE season=2026 AND home_score IS NOT NULL
        """), c)
        kal = pd.read_sql(sql_text("""
            SELECT team_abbr, game_date, season, rating, rating_uncertainty
            FROM wnba_team_kalman WHERE model_version=:v AND season IN (2025, 2026)
        """), c, params={"v": KAL_MV})
        gli = pd.read_sql(sql_text("""
            SELECT team_abbr, game_date, rd FROM wnba_glicko2_ratings
            WHERE model_version=:v AND season IN (2025, 2026)
        """), c, params={"v": GLI_MV})

    games["team_home"] = games["home"].map(canonical_team)
    games["team_away"] = games["away"].map(canonical_team)
    kal["team"] = kal["team_abbr"].map(canonical_team)
    gli["team"] = gli["team_abbr"].map(canonical_team)
    kal["d"] = pd.to_datetime(kal["game_date"])
    gli["d"] = pd.to_datetime(gli["game_date"])

    teams_2026 = sorted(set(games.team_home) | set(games.team_away))
    dates = sorted(pd.to_datetime(games.game_date).unique())
    hr("INPUTS")
    print(f"  2026 teams = {len(teams_2026)}  |  played snapshot dates = {len(dates)} "
          f"({pd.Timestamp(dates[0]).date()} .. {pd.Timestamp(dates[-1]).date()})")

    # carry-in (entering 2026): end-2025 rating + inflated band; debut=neutral/wide
    end25 = (kal[kal.season == 2025].sort_values("d").groupby("team")
             .agg(rating=("rating", "last"), unc=("rating_uncertainty", "last")).reset_index())
    carry = {}
    for t in teams_2026:
        row = end25[end25.team == t]
        if len(row):
            carry[t] = (float(row.rating.iloc[0]),
                        float(math.sqrt(row.unc.iloc[0] ** 2 + OFFSEASON_VAR)))
        else:
            carry[t] = (0.0, DEBUT_SIGMA)   # TOR/POR debut

    kal26 = (kal[kal.season == 2026].sort_values("d")
             .groupby(["team", "d"]).agg(rating=("rating", "last"),
                                         unc=("rating_uncertainty", "last")).reset_index())

    long = pd.concat([
        games[["game_date", "team_home"]].assign(won=(games.home_score > games.away_score).astype(int))
            .rename(columns={"team_home": "team"}),
        games[["game_date", "team_away"]].assign(won=(games.away_score > games.home_score).astype(int))
            .rename(columns={"team_away": "team"}),
    ], ignore_index=True)
    long["d"] = pd.to_datetime(long["game_date"])

    rows = []
    for t in teams_2026:
        kt = kal26[kal26.team == t].set_index("d")["rating"]
        ut = kal26[kal26.team == t].set_index("d")["unc"]
        gt = gli[gli.team == t].sort_values("d")
        base_r, base_u = carry[t]
        for D in dates:
            r_hist = kt[kt.index <= D]
            u_hist = ut[ut.index <= D]
            if len(r_hist):
                rating, unc = float(r_hist.iloc[-1]), float(u_hist.iloc[-1])
            else:
                rating, unc = base_r, base_u
            rd_hist = gt[gt.d <= D]["rd"]
            rd = float(rd_hist.iloc[-1]) if len(rd_hist) else 350.0
            rec = long[(long.team == t) & (long.d <= D)]
            w = int(rec["won"].sum()); l = int((rec["won"] == 0).sum())
            rows.append({"snapshot_date": pd.Timestamp(D).date(), "season": 2026, "team": t,
                         "kalman_rating": rating, "kalman_sigma": unc,
                         "rating_lo": rating - unc, "rating_hi": rating + unc,
                         "glicko_rd": rd, "games_played": w + l, "wins": w, "losses": l})

    S = pd.DataFrame(rows)
    S["power_rating"] = S.groupby("snapshot_date")["kalman_rating"].transform(
        lambda x: (x - x.mean()) / (x.std(ddof=0) if x.std(ddof=0) else 1.0))
    S["rank_in_season"] = S.groupby("snapshot_date")["kalman_rating"].rank(
        ascending=False, method="min").astype(int)
    S["model_version"] = MODEL_VERSION
    return S


def main() -> int:
    db_url = os.environ.get("SUPABASE_URL")
    if not db_url:
        print("FATAL: SUPABASE_URL required", file=sys.stderr); return 1
    dry = bool(os.environ.get("DRY_RUN"))
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    print(f"STRENGTH-LIVE REFRESH @ {now.isoformat()}  model_version={MODEL_VERSION}  DRY_RUN={dry}")
    try:
        engine = create_engine(db_url, pool_pre_ping=True)
    except Exception as e:
        print(f"FATAL: DB init failed: {e!r}", file=sys.stderr); return 1

    try:
        S = build_surface(engine)
    except Exception as e:
        print(f"FATAL: build failed: {e!r}", file=sys.stderr); return 1

    n_teams = S.team.nunique()
    n_dates = S.snapshot_date.nunique()
    hr("CURRENT SURFACE (latest snapshot)")
    cur = S[S.snapshot_date == S.snapshot_date.max()].sort_values("kalman_rating", ascending=False)
    show = cur[["rank_in_season", "team", "kalman_rating", "kalman_sigma", "rating_lo",
                "rating_hi", "glicko_rd", "wins", "losses", "power_rating"]]
    print(show.to_string(index=False, formatters={
        "kalman_rating": lambda v: f"{v:+.2f}", "rating_lo": lambda v: f"{v:+.2f}",
        "rating_hi": lambda v: f"{v:+.2f}", "power_rating": lambda v: f"{v:+.2f}"}))

    # SANITY GUARD — refuse to write a degenerate surface
    if n_teams != 15 or n_dates < 1:
        print(f"FATAL: sanity gate — {n_teams} teams / {n_dates} dates (want 15 / >=1). "
              f"Refusing to write.", file=sys.stderr)
        return 1

    if dry:
        hr("DRY_RUN — skipping writes")
        print(f"  would APPEND-rebuild {len(S)} rows ({n_teams} teams x {n_dates} dates) "
              f"@ {MODEL_VERSION}")
        print("  idempotent: DELETE WHERE model_version + full rebuild from all played "
              "2026 games; same slate -> same rows (net no-op); new games -> new "
              "snapshot_dates appended under the same model_version; PR-v3 / other "
              "versions untouched.")
        return 0

    hr("PERSIST (APPEND-only, per model_version)")
    out = pd.DataFrame([{k: py(v) for k, v in r.items()} for r in S.to_dict("records")])
    try:
        with engine.begin() as c:
            c.execute(sql_text("""
                CREATE TABLE IF NOT EXISTS wnba_team_strength_live_2026 (
                    snapshot_date date, season bigint, team text,
                    kalman_rating double precision, kalman_sigma double precision,
                    rating_lo double precision, rating_hi double precision,
                    glicko_rd double precision, games_played bigint, wins bigint, losses bigint,
                    power_rating double precision, rank_in_season bigint, model_version text )"""))
            c.execute(sql_text("DELETE FROM wnba_team_strength_live_2026 WHERE model_version=:v"),
                      {"v": MODEL_VERSION})
            out.to_sql("wnba_team_strength_live_2026", c, if_exists="append", index=False,
                       method="multi", chunksize=200)
    except Exception as e:
        print(f"FATAL: persist failed: {e!r}", file=sys.stderr); return 1
    print(f"  wrote {len(out)} rows ({n_teams} teams x {n_dates} dates) @ {MODEL_VERSION}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
