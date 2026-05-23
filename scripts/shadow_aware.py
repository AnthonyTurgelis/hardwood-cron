#!/usr/bin/env python3
"""Shadow AWARE predictor + clean training-set logger (self-contained for cron).

Mirrors cells/cell_151_shadow_aware_predictions.py from the workspace, but
inlined to avoid src.* imports. See v59 delta for full rationale.

Three play_prob schemes tested in parallel; v12 BLIND production untouched.
The wnba_forward_play_prob_log table is the clean training set that, by end
of 2026, will let us fit the P(plays | status, hours_to_tip) model the
historical Wayback data couldn't support.

Required env vars:  SUPABASE_URL
Exit codes:         0 success / 1 DB error
"""
import os
import sys
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text as sql_text


# v12 BLIND OLS coefs (frozen — see cell_146)
V12_INTERCEPT = +0.7215
V12_DIFF      = +115.8490
V12_CHEM      = +0.9745
V12_HOME_STR  = +74.7916
V12_AWAY_STR  = -41.0575

SCHEMES = ["scheme_a_crude", "scheme_b_reversal", "scheme_c_decayed"]
LOOKAHEAD_HOURS = 48

# wnba_forward_play_prob_log has PK (game_id, player_id, snapshot_at) where snapshot_at =
# wnba_player_availability_current.computed_at. If upstream refresh stalls, every INSERT
# silently no-ops on ON CONFLICT and the table goes stale without any error surfacing.
# Fail loud past this age so the workflow goes red and the operator gets paged. Discovered
# 2026-05-23 when play_prob_log lagged 26h while aware_shadow (same script) stayed fresh.
AVAILABILITY_STALE_HOURS = 6.0

TEAM_NAME_TO_ABBR = {
    "Atlanta Dream":"ATL","Chicago Sky":"CHI","Connecticut Sun":"CON",
    "Dallas Wings":"DAL","Golden State Valkyries":"GSV","Indiana Fever":"IND",
    "Las Vegas Aces":"LVA","Los Angeles Sparks":"LAS","Minnesota Lynx":"MIN",
    "New York Liberty":"NYL","Phoenix Mercury":"PHO","Seattle Storm":"SEA",
    "Washington Mystics":"WAS","Portland Fire":"POR","Toronto Tempo":"TOR",
}


def compute_play_prob(status, hours_to_tip, scheme):
    s = (status or "healthy").lower()
    if scheme == "scheme_a_crude":
        return {"healthy": 0.95, "day-to-day": 0.147, "out": 0.022}.get(s, 0.95)
    if scheme == "scheme_b_reversal":
        return {"healthy": 0.95, "day-to-day": 0.66, "out": 0.33}.get(s, 0.95)
    if scheme == "scheme_c_decayed":
        h = hours_to_tip if hours_to_tip is not None else 24.0
        if s == "healthy": return 0.95
        if s == "day-to-day": return 0.66
        if s == "out":
            if h < 2:   return 0.022
            if h >= 24: return 0.33
            return 0.022 + (0.33 - 0.022) * (h - 2) / 22.0
        return 0.95
    raise ValueError(f"unknown scheme: {scheme}")


def ensure_tables(engine):
    with engine.begin() as c:
        c.execute(sql_text("""
            CREATE TABLE IF NOT EXISTS wnba_forward_aware_shadow (
                game_id text NOT NULL, scheme text NOT NULL,
                computed_at timestamptz NOT NULL, model_version text NOT NULL,
                home_str_aware double precision, away_str_aware double precision,
                strength_diff_aware double precision, pred_margin_aware double precision,
                PRIMARY KEY (game_id, scheme)
            )
        """))
        c.execute(sql_text("""
            CREATE TABLE IF NOT EXISTS wnba_forward_play_prob_log (
                game_id text NOT NULL, player_id bigint NOT NULL,
                snapshot_at timestamptz NOT NULL, logged_at timestamptz NOT NULL,
                player_name text, team_abbr text, status_norm text,
                hours_to_tip double precision, roll10_min double precision,
                rating double precision,
                actual_played int NULL, actual_minutes double precision NULL,
                graded_at timestamptz NULL,
                PRIMARY KEY (game_id, player_id, snapshot_at)
            )
        """))


def pull_target_games(engine, now_utc):
    horizon = now_utc + timedelta(hours=LOOKAHEAD_HOURS)
    return pd.read_sql(sql_text("""
        SELECT g.game_id::text AS game_id, g.game_date::date AS game_date,
               g.home_team_abbr AS home_abbr, g.away_team_abbr AS away_abbr,
               gp.chem_diff
        FROM wnba_games g
        LEFT JOIN wnba_game_predictions_2026 gp ON gp.game_id::text = g.game_id::text
        WHERE g.game_date::date BETWEEN :d0 AND :d1
          AND g.season::int = 2026 AND COALESCE(gp.played, false) = false
        ORDER BY g.game_date, g.game_id
    """), engine, params={"d0": now_utc.date(), "d1": horizon.date()})


def pull_commence_time_per_game(engine, games_df):
    if not len(games_df):
        return games_df.assign(commence_time=pd.NaT)
    sl = pd.read_sql(sql_text("""
        SELECT DISTINCT home_team, away_team, commence_time
        FROM wnba_live_game_line_snapshots WHERE market='spreads'
    """), engine)
    sl["home_abbr"] = sl["home_team"].map(TEAM_NAME_TO_ABBR)
    sl["away_abbr"] = sl["away_team"].map(TEAM_NAME_TO_ABBR)
    sl["commence_date"] = pd.to_datetime(sl["commence_time"]).dt.date
    sl = sl.dropna(subset=["home_abbr","away_abbr"]).sort_values("commence_time").drop_duplicates(
        ["home_abbr","away_abbr","commence_date"], keep="last")
    out = games_df.merge(sl[["home_abbr","away_abbr","commence_date","commence_time"]],
                         left_on=["home_abbr","away_abbr","game_date"],
                         right_on=["home_abbr","away_abbr","commence_date"],
                         how="left").drop(columns=["commence_date"])
    out["commence_time"] = pd.to_datetime(out["commence_time"], utc=True)
    fb = out["commence_time"].isna()
    if fb.any():
        out.loc[fb, "commence_time"] = pd.to_datetime(out.loc[fb, "game_date"]).dt.tz_localize("UTC") + pd.Timedelta(hours=23)
    return out


def pull_roster(engine, team_abbrs):
    if not team_abbrs: return pd.DataFrame()
    return pd.read_sql(sql_text("""
        SELECT a.player_id::bigint AS player_id, a.player_name, a.team_abbr,
               a.rating, a.status_norm, a.computed_at
        FROM wnba_player_availability_current a
        WHERE a.computed_at = (SELECT MAX(computed_at) FROM wnba_player_availability_current)
          AND a.team_abbr = ANY(:teams)
          AND a.player_id IS NOT NULL AND a.rating IS NOT NULL
    """), engine, params={"teams": list(team_abbrs)})


def pull_latest_roll10(engine, player_ids):
    if not player_ids: return pd.DataFrame()
    return pd.read_sql(sql_text("""
        SELECT DISTINCT ON (player_id) player_id::bigint AS player_id, roll10_min
        FROM wnba_player_rolling_features
        WHERE roll10_min IS NOT NULL AND player_id = ANY(:pids)
        ORDER BY player_id, game_date DESC
    """), engine, params={"pids": [int(p) for p in player_ids]})


def main() -> int:
    db_url = os.environ.get("SUPABASE_URL")
    if not db_url:
        print("FATAL: SUPABASE_URL required", file=sys.stderr); return 1
    now_utc = datetime.now(timezone.utc)
    print(f"SHADOW AWARE TRACKER @ {now_utc.isoformat()}")
    try:
        engine = create_engine(db_url, pool_pre_ping=True)
        ensure_tables(engine)
    except Exception as e:
        print(f"FATAL: DB init failed: {e}", file=sys.stderr); return 1

    games = pull_target_games(engine, now_utc)
    print(f"  unplayed 2026 games in next {LOOKAHEAD_HOURS}h: {len(games)}")

    availability_age_h = None
    log_attempted = 0
    log_inserted = 0

    if len(games):
        games = pull_commence_time_per_game(engine, games)
        teams = sorted(set(games["home_abbr"]) | set(games["away_abbr"]))
        roster = pull_roster(engine, teams)
        roll = pull_latest_roll10(engine, roster["player_id"].tolist() if len(roster) else [])
        snap_at = roster["computed_at"].iloc[0] if len(roster) else now_utc
        snap_at = pd.Timestamp(snap_at).tz_localize("UTC") if snap_at.tzinfo is None else pd.Timestamp(snap_at).tz_convert("UTC")
        print(f"  BDL snapshot_at: {snap_at}, roster: {len(roster)}")
        rost = roster.merge(roll, on="player_id", how="left")
        rost["roll10_min"] = rost["roll10_min"].fillna(0.0)

        shadow_rows, log_rows = [], []
        mv = f"v12_aware_shadow_{now_utc.strftime('%Y_%m_%d_%H%M')}"
        for _, gm in games.iterrows():
            ct = gm["commence_time"]
            htt = (ct - snap_at).total_seconds()/3600.0 if pd.notna(ct) else None
            for scheme in SCHEMES:
                h = rost[rost["team_abbr"]==gm["home_abbr"]].copy()
                a = rost[rost["team_abbr"]==gm["away_abbr"]].copy()
                for r in [h, a]:
                    r["pp"] = r["status_norm"].apply(lambda s: compute_play_prob(s, htt, scheme))
                    r["contrib"] = r["rating"] * r["roll10_min"] * r["pp"]
                hs = h["contrib"].sum()/40.0; as_ = a["contrib"].sum()/40.0
                sd = hs - as_; cd = float(gm["chem_diff"]) if pd.notna(gm["chem_diff"]) else 0.0
                pm = V12_INTERCEPT + V12_DIFF*sd + V12_CHEM*cd + V12_HOME_STR*hs + V12_AWAY_STR*as_
                shadow_rows.append(dict(game_id=str(gm["game_id"]), scheme=scheme,
                    computed_at=now_utc, model_version=mv,
                    home_str_aware=float(hs), away_str_aware=float(as_),
                    strength_diff_aware=float(sd), pred_margin_aware=float(pm)))
            for _, p in rost.iterrows():
                if p["team_abbr"] not in (gm["home_abbr"], gm["away_abbr"]): continue
                log_rows.append(dict(game_id=str(gm["game_id"]), player_id=int(p["player_id"]),
                    snapshot_at=snap_at.to_pydatetime(), logged_at=now_utc,
                    player_name=str(p["player_name"]), team_abbr=str(p["team_abbr"]),
                    status_norm=str(p["status_norm"]) if pd.notna(p["status_norm"]) else None,
                    hours_to_tip=float(htt) if htt is not None else None,
                    roll10_min=float(p["roll10_min"]), rating=float(p["rating"])))

        log_attempted = len(log_rows)
        with engine.begin() as c:
            for r in shadow_rows:
                c.execute(sql_text("""
                    INSERT INTO wnba_forward_aware_shadow VALUES
                      (:game_id, :scheme, :computed_at, :model_version,
                       :home_str_aware, :away_str_aware, :strength_diff_aware, :pred_margin_aware)
                    ON CONFLICT (game_id, scheme) DO UPDATE SET
                      computed_at=EXCLUDED.computed_at, model_version=EXCLUDED.model_version,
                      home_str_aware=EXCLUDED.home_str_aware, away_str_aware=EXCLUDED.away_str_aware,
                      strength_diff_aware=EXCLUDED.strength_diff_aware,
                      pred_margin_aware=EXCLUDED.pred_margin_aware
                """), r)
            for r in log_rows:
                res = c.execute(sql_text("""
                    INSERT INTO wnba_forward_play_prob_log (
                      game_id, player_id, snapshot_at, logged_at, player_name,
                      team_abbr, status_norm, hours_to_tip, roll10_min, rating
                    ) VALUES (:game_id, :player_id, :snapshot_at, :logged_at, :player_name,
                      :team_abbr, :status_norm, :hours_to_tip, :roll10_min, :rating)
                    ON CONFLICT (game_id, player_id, snapshot_at) DO NOTHING
                """), r)
                log_inserted += (res.rowcount or 0)

        availability_age_h = (now_utc - snap_at.to_pydatetime().astimezone(timezone.utc)).total_seconds() / 3600.0
        print(f"  shadow predictions written/updated: {len(shadow_rows)}")
        print(f"  log rows: {log_attempted} attempted, {log_inserted} NEW, "
              f"{log_attempted - log_inserted} duplicates (PK collision on snapshot_at)")
        print(f"  availability snap_at={snap_at.isoformat()}  age={availability_age_h:.2f}h")

    # Grade settled
    actuals = pd.read_sql(sql_text("""
        SELECT game_id::text AS game_id, player_id::bigint AS player_id, min AS actual_minutes
        FROM wnba_game_logs WHERE season::int=2026
    """), engine)
    if len(actuals):
        pending = pd.read_sql(sql_text("""
            SELECT game_id, player_id FROM wnba_forward_play_prob_log WHERE actual_played IS NULL
        """), engine)
        if len(pending):
            pending["player_id"] = pending["player_id"].astype(int)
            grade = pending.merge(actuals, on=["game_id","player_id"], how="inner")
            grade["actual_played"] = (grade["actual_minutes"].fillna(0) > 0).astype(int)
            if len(grade):
                with engine.begin() as c:
                    for _, r in grade.iterrows():
                        c.execute(sql_text("""
                            UPDATE wnba_forward_play_prob_log
                            SET actual_played=:p, actual_minutes=:m, graded_at=:t
                            WHERE game_id=:g AND player_id=:pi AND actual_played IS NULL
                        """), dict(p=int(r["actual_played"]), m=float(r["actual_minutes"]),
                                   t=now_utc, g=r["game_id"], pi=int(r["player_id"])))
                print(f"  graded {len(grade)} log entries")
    n_log = pd.read_sql(sql_text("SELECT COUNT(*) FROM wnba_forward_play_prob_log"), engine).iloc[0,0]
    n_log_graded = pd.read_sql(sql_text("SELECT COUNT(*) FROM wnba_forward_play_prob_log WHERE actual_played IS NOT NULL"), engine).iloc[0,0]
    n_shadow = pd.read_sql(sql_text("SELECT COUNT(*) FROM wnba_forward_aware_shadow"), engine).iloc[0,0]
    print(f"  totals: shadow={n_shadow:,}  log={n_log:,} ({n_log_graded:,} graded)")

    # Fail loud if upstream availability_current is stuck. Shadow predictions have already
    # been written by this point (UPSERT on (game_id, scheme) means they refresh regardless),
    # but play_prob_log silently no-ops on PK collision when snap_at doesn't advance. Without
    # this guard the workflow stays green while the training-set table goes stale.
    if availability_age_h is not None and availability_age_h > AVAILABILITY_STALE_HOURS:
        print(
            f"\nFATAL: wnba_player_availability_current.computed_at is {availability_age_h:.2f}h "
            f"old (threshold {AVAILABILITY_STALE_HOURS}h). wnba_forward_play_prob_log silently "
            f"no-ops on ON CONFLICT (game_id, player_id, snapshot_at) until upstream refresh "
            f"advances snap_at. ROOT CAUSE: fix the wnba_player_availability_current refresh "
            f"job; this script will resume writing log rows automatically once snap_at moves.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
