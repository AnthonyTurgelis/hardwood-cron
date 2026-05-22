#!/usr/bin/env python3
"""WNBA forward paper-bet tracker — emit / persist / grade clean 3-edge bets.

Implements three frozen edges using FROZEN 2023-2025 calibration (see v58 delta):
  - PART 9 spreads:  |pred + home_point| > 5, abs_open >= 2, month != 9, away not on b2b
  - B1 reb edge>=5pp: calibrated_p − implied_p >= 5pp on player_rebounds best-line
  - B2 fg3m edge>=5pp: same on player_threes best-line

Each run is idempotent (UPSERT on PK; first-observed line locked in).

Required env vars:
  SUPABASE_URL    Postgres connection URL (sqlalchemy psycopg2 format)

Exit codes:
  0 — success (incl. "no games in window")
  1 — DB error
"""
import os
import sys
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text as sql_text
from scipy.stats import norm  # noqa: F401  (kept for parity with cell_144; used elsewhere)


# ─── FROZEN constants (must match cell_146 v12 outputs and v58 delta) ─────
FROZEN_CAL_B1_REB = {
    "<-3":    (0.166667,    6),
    "-3..-2": (0.333333,   54),
    "-2..-1": (0.410714,  392),
    "-1..0":  (0.445647, 1941),
    "0..1":   (0.481084, 1586),
    "1..2":   (0.591195,  159),
    "2..3":   (0.555556,    9),
}
FROZEN_CAL_B2_FG3M = {
    "-2..-1": (0.267857,   56),
    "-1..0":  (0.401114, 1436),
    "0..1":   (0.500479, 1043),
    "1..2":   (0.733333,   15),
}
EDGE_THR_PP = 5.0
PART9_SIGNAL_THR = 5.0
PART9_MIN_ABS_OPEN = 2.0
PART9_EXCLUDE_MONTH = 9
STAKE_PER_BET = 1.0
LOOKAHEAD_HOURS = 36
STALE_PRED_HOURS = 24

# Injury-timing rule (per v58 / cell_147)
ROTATION_MIN_THRESHOLD = 20.0
UNCERTAIN_LO = 0.20
UNCERTAIN_HI = 0.80

BUCKETS = [-100, -3, -2, -1, 0, 1, 2, 3, 100]
BUCKET_LABELS = ["<-3","-3..-2","-2..-1","-1..0","0..1","1..2","2..3","3+"]

TEAM_NAME_TO_ABBR = {
    "Atlanta Dream":"ATL","Chicago Sky":"CHI","Connecticut Sun":"CON",
    "Dallas Wings":"DAL","Golden State Valkyries":"GSV","Indiana Fever":"IND",
    "Las Vegas Aces":"LVA","Los Angeles Sparks":"LAS","Minnesota Lynx":"MIN",
    "New York Liberty":"NYL","Phoenix Mercury":"PHO","Seattle Storm":"SEA",
    "Washington Mystics":"WAS","Portland Fire":"POR","Toronto Tempo":"TOR",
}


def hr(t):
    print(f"\n{'='*78}\n  {t}\n{'='*78}", flush=True)


def am_to_dec(p):
    p = float(p)
    return 1 + p/100.0 if p > 0 else 1 + 100.0/abs(p)


def ensure_table(engine):
    with engine.begin() as c:
        c.execute(sql_text("""
            CREATE TABLE IF NOT EXISTS wnba_forward_paper_bets (
                game_id              text        NOT NULL,
                edge_name            text        NOT NULL,
                bet_subject          text        NOT NULL,
                bet_side             text        NOT NULL,
                emitted_at           timestamptz NOT NULL,
                last_updated         timestamptz NOT NULL,
                game_date            date        NOT NULL,
                season               int         NOT NULL,
                player_id            bigint      NULL,
                bet_pt               double precision NOT NULL,
                bet_dec              double precision NOT NULL,
                book                 text        NOT NULL,
                stake                double precision NOT NULL DEFAULT 1.0,
                model_pred           double precision NULL,
                edge_pp              double precision NULL,
                bet_signal           double precision NULL,
                prediction_age_hours double precision NULL,
                timing_flag          text        NULL,
                result               text        NULL,
                actual_value         double precision NULL,
                units_won            double precision NULL,
                graded_at            timestamptz NULL,
                PRIMARY KEY (game_id, edge_name, bet_subject, bet_side)
            )
        """))
        c.execute(sql_text("""
            ALTER TABLE wnba_forward_paper_bets
            ADD COLUMN IF NOT EXISTS timing_flag text NULL
        """))


def compute_team_timing_flags(engine):
    avail = pd.read_sql(sql_text("""
        SELECT a.player_id::bigint AS player_id, a.player_name, a.team_abbr,
               a.play_prob, a.status_norm
        FROM wnba_player_availability_current a
        WHERE a.computed_at = (SELECT MAX(computed_at) FROM wnba_player_availability_current)
          AND a.team_abbr IS NOT NULL AND a.play_prob IS NOT NULL
    """), engine)
    roll = pd.read_sql(sql_text("""
        SELECT DISTINCT ON (player_id) player_id::bigint AS player_id, roll10_min
        FROM wnba_player_rolling_features
        WHERE roll10_min IS NOT NULL
        ORDER BY player_id, game_date DESC
    """), engine)
    df = avail.merge(roll, on="player_id", how="left")
    df["roll10_min"] = df["roll10_min"].fillna(0.0)
    df["is_rotation"] = df["roll10_min"] >= ROTATION_MIN_THRESHOLD
    df["is_uncertain"] = (df["play_prob"] > UNCERTAIN_LO) & (df["play_prob"] < UNCERTAIN_HI)
    df["flag_player"] = df["is_rotation"] & df["is_uncertain"]
    flags = {}
    for team, sub in df.groupby("team_abbr"):
        uncertain_players = sub.loc[sub["flag_player"], "player_name"].tolist()
        if uncertain_players:
            flags[team] = ("HOLD_FOR_1H_PRETIP", uncertain_players)
        else:
            flags[team] = ("FIRE_OPEN", [])
    return flags


def game_timing_flag(home_abbr, away_abbr, team_flags):
    h = team_flags.get(home_abbr, ("FIRE_OPEN", []))
    a = team_flags.get(away_abbr, ("FIRE_OPEN", []))
    if h[0] == "HOLD_FOR_1H_PRETIP" or a[0] == "HOLD_FOR_1H_PRETIP":
        return "HOLD_FOR_1H_PRETIP"
    return "FIRE_OPEN"


def pull_target_games(engine, now_utc):
    horizon = now_utc + timedelta(hours=LOOKAHEAD_HOURS)
    return pd.read_sql(sql_text("""
        SELECT game_id::text AS game_id, game_date::date AS game_date,
               season::int AS season, home_team_abbr AS home_abbr,
               away_team_abbr AS away_abbr, home_score, away_score
        FROM wnba_games
        WHERE game_date::date BETWEEN :d0 AND :d1 AND season::int = 2026
        ORDER BY game_date, game_id
    """), engine, params={"d0": now_utc.date(), "d1": horizon.date()})


def pull_latest_live_lines(engine):
    spreads = pd.read_sql(sql_text("""
        WITH latest AS (
            SELECT event_id, MAX(snapshot_at) AS max_ts
            FROM wnba_live_game_line_snapshots WHERE market='spreads'
            GROUP BY event_id
        )
        SELECT s.event_id, s.commence_time, s.home_team, s.away_team,
               s.snapshot_at, s.bookmaker, s.outcome_label, s.point, s.price
        FROM wnba_live_game_line_snapshots s
        JOIN latest l ON l.event_id=s.event_id AND l.max_ts=s.snapshot_at
        WHERE s.market='spreads' AND s.point IS NOT NULL AND s.price IS NOT NULL
    """), engine)
    props = pd.read_sql(sql_text("""
        WITH latest AS (
            SELECT event_id, MAX(snapshot_at) AS max_ts
            FROM wnba_live_player_prop_snapshots
            WHERE market IN ('player_rebounds','player_threes')
            GROUP BY event_id
        )
        SELECT p.event_id, p.commence_time, p.home_team, p.away_team,
               p.snapshot_at, p.bookmaker, p.market, p.player_name,
               p.side, p.point, p.price
        FROM wnba_live_player_prop_snapshots p
        JOIN latest l ON l.event_id=p.event_id AND l.max_ts=p.snapshot_at
        WHERE p.market IN ('player_rebounds','player_threes')
          AND p.point IS NOT NULL AND p.price IS NOT NULL
    """), engine)
    return spreads, props


def map_events_to_games(events_df, games_df):
    if not len(events_df):
        return events_df.assign(game_id=pd.NA)
    e = events_df.copy()
    e["home_abbr"] = e["home_team"].map(TEAM_NAME_TO_ABBR)
    e["away_abbr"] = e["away_team"].map(TEAM_NAME_TO_ABBR)
    e["game_date"] = pd.to_datetime(e["commence_time"]).dt.date
    out = e.merge(games_df[["game_id","game_date","home_abbr","away_abbr","season"]],
                  on=["home_abbr","away_abbr","game_date"], how="left")
    for off in [-1, 1]:
        um = out["game_id"].isna()
        if not um.any():
            break
        u = out[um].copy()
        u["gdt"] = (pd.to_datetime(u["game_date"]) + pd.Timedelta(days=off)).dt.date
        retry = u.drop(columns=["game_id","season"]).merge(
            games_df[["game_id","game_date","home_abbr","away_abbr","season"]].rename(
                columns={"game_date":"gdt"}),
            on=["home_abbr","away_abbr","gdt"], how="left")
        if retry["game_id"].notna().any():
            out.loc[um, "game_id"] = retry["game_id"].values
            out.loc[um, "season"]  = retry["season"].values
    return out


def emit_part9_bets(engine, games_df, spread_lines, now_utc, team_flags):
    if not len(spread_lines):
        return []
    preds = pd.read_sql(sql_text("""
        SELECT game_id::text AS game_id, pred_margin::float AS pred_margin, model_version
        FROM wnba_game_predictions_2026 WHERE played = false
    """), engine)
    if not len(preds):
        return []
    sl = map_events_to_games(spread_lines, games_df).dropna(subset=["game_id"]).copy()
    sl["game_id"] = sl["game_id"].astype(str)
    sl["decimal"] = sl["price"].apply(am_to_dec)
    h = sl[sl["outcome_label"]=="home"].groupby("game_id").agg(
        home_point=("point","median"), home_dec=("decimal","median")).reset_index()
    a = sl[sl["outcome_label"]=="away"].groupby("game_id").agg(
        away_point=("point","median"), away_dec=("decimal","median")).reset_index()
    lines_med = h.merge(a, on="game_id", how="inner")
    games_meta = games_df[["game_id","game_date","season","home_abbr","away_abbr"]].copy()
    all_played = pd.read_sql(sql_text("""
        SELECT game_id::text AS game_id, game_date::date AS gd,
               home_team_abbr AS h, away_team_abbr AS a
        FROM wnba_games WHERE game_date::date >= :d
    """), engine, params={"d": now_utc.date() - timedelta(days=2)})
    yesterday = now_utc.date() - timedelta(days=1)
    yesterday_teams = set(all_played[all_played["gd"]==yesterday]["h"])
    yesterday_teams |= set(all_played[all_played["gd"]==yesterday]["a"])
    m = preds.merge(lines_med, on="game_id", how="inner").merge(games_meta, on="game_id", how="inner")
    m["bet_signal"] = m["pred_margin"] + m["home_point"]
    m["abs_open"]   = m["home_point"].abs()
    m["month"]      = pd.to_datetime(m["game_date"]).dt.month
    m["away_b2b"]   = m["away_abbr"].isin(yesterday_teams).astype(int)
    bets = []
    for _, r in m.iterrows():
        if abs(r["bet_signal"]) <= PART9_SIGNAL_THR: continue
        if r["abs_open"] < PART9_MIN_ABS_OPEN: continue
        if r["month"] == PART9_EXCLUDE_MONTH: continue
        if r["away_b2b"] == 1: continue
        bet_side = "home" if r["bet_signal"] > 0 else "away"
        bet_pt   = float(r["home_point"]) if bet_side=="home" else float(r["away_point"])
        bet_dec  = float(r["home_dec"])   if bet_side=="home" else float(r["away_dec"])
        flag = game_timing_flag(r["home_abbr"], r["away_abbr"], team_flags)
        bets.append(dict(
            game_id=str(r["game_id"]), edge_name="PART9_spreads",
            bet_subject="team", bet_side=bet_side,
            game_date=r["game_date"], season=int(r["season"]), player_id=None,
            bet_pt=bet_pt, bet_dec=bet_dec, book="median_line",
            stake=STAKE_PER_BET, model_pred=float(r["pred_margin"]),
            edge_pp=None, bet_signal=float(r["bet_signal"]),
            prediction_age_hours=None, timing_flag=flag,
        ))
    return bets


def bucket_for(edge):
    s = pd.cut([edge], bins=BUCKETS, labels=BUCKET_LABELS, include_lowest=True)
    return str(s[0]) if pd.notna(s[0]) else None


def emit_prop_bets(engine, games_df, prop_lines, edge_name, market, pred_type, frozen_cal, now_utc, team_flags):
    if not len(prop_lines): return []
    pl = prop_lines[prop_lines["market"]==market].copy()
    if not len(pl): return []
    pl = map_events_to_games(pl, games_df).dropna(subset=["game_id"])
    pl["game_id"] = pl["game_id"].astype(str)
    pl["decimal"] = pl["price"].apply(am_to_dec)
    pl["name_lower"] = pl["player_name"].str.strip().str.lower()
    med = pl.groupby(["game_id","name_lower","side"]).agg(med_pt=("point","median")).reset_index()
    mo = med[med["side"]=="Over"][["game_id","name_lower","med_pt"]].rename(columns={"med_pt":"mop"})
    mu = med[med["side"]=="Under"][["game_id","name_lower","med_pt"]].rename(columns={"med_pt":"mup"})
    medw = mo.merge(mu, on=["game_id","name_lower"], how="inner")
    medw["median_line"] = (medw["mop"]+medw["mup"])/2.0
    overs = pl[pl["side"]=="Over"].sort_values(
        ["game_id","name_lower","point","decimal"], ascending=[True,True,True,False])
    best_over = overs.groupby(["game_id","name_lower"]).head(1)[
        ["game_id","name_lower","point","decimal","bookmaker","player_name"]
    ].rename(columns={"point":"bo_pt","decimal":"bo_dec","bookmaker":"bo_book","player_name":"player_name_disp"})
    unders = pl[pl["side"]=="Under"].sort_values(
        ["game_id","name_lower","point","decimal"], ascending=[True,True,False,False])
    best_under = unders.groupby(["game_id","name_lower"]).head(1)[
        ["game_id","name_lower","point","decimal","bookmaker"]
    ].rename(columns={"point":"bu_pt","decimal":"bu_dec","bookmaker":"bu_book"})
    preds = pd.read_sql(sql_text("""
        WITH latest AS (
            SELECT odds_event_id, player_id, pred_type, MAX(snapshot_at) AS max_ts
            FROM wnba_predictions_live WHERE pred_type=:pt
              AND EXTRACT(YEAR FROM commence_time)=2026
            GROUP BY odds_event_id, player_id, pred_type
        )
        SELECT p.odds_event_id, p.commence_time, p.home_team_abbr, p.away_team_abbr,
               p.player_id, p.player_name, p.pred_mean, p.snapshot_at, p.model_version
        FROM wnba_predictions_live p
        JOIN latest l ON l.odds_event_id=p.odds_event_id AND l.player_id=p.player_id
                     AND l.pred_type=p.pred_type AND l.max_ts=p.snapshot_at
    """), engine, params={"pt": pred_type})
    if not len(preds): return []
    preds["game_date"] = pd.to_datetime(preds["commence_time"]).dt.date
    preds = preds.merge(games_df[["game_id","game_date","home_abbr","away_abbr","season"]],
                        left_on=["home_team_abbr","away_team_abbr","game_date"],
                        right_on=["home_abbr","away_abbr","game_date"], how="left")
    preds = preds.dropna(subset=["game_id"]).copy()
    preds["game_id"]   = preds["game_id"].astype(str)
    preds["name_lower"] = preds["player_name"].str.strip().str.lower()
    preds["pred_age_h"] = (now_utc - pd.to_datetime(preds["snapshot_at"], utc=True)).dt.total_seconds()/3600.0
    merged = preds.merge(medw, on=["game_id","name_lower"], how="inner")
    merged = merged.merge(best_over,  on=["game_id","name_lower"], how="inner")
    merged = merged.merge(best_under, on=["game_id","name_lower"], how="inner")
    merged["model_edge"] = merged["pred_mean"] - merged["median_line"]
    bets = []
    for _, r in merged.iterrows():
        edge = float(r["model_edge"])
        if edge == 0: continue
        bucket = bucket_for(edge)
        if bucket not in frozen_cal: continue
        over_rate, _ = frozen_cal[bucket]
        side = "over" if edge > 0 else "under"
        calibrated_p = over_rate if side=="over" else 1 - over_rate
        bet_pt  = float(r["bo_pt"])  if side=="over" else float(r["bu_pt"])
        bet_dec = float(r["bo_dec"]) if side=="over" else float(r["bu_dec"])
        book    = str(r["bo_book"])  if side=="over" else str(r["bu_book"])
        implied_p = 1.0 / bet_dec
        edge_pp = (calibrated_p - implied_p) * 100.0
        if edge_pp < EDGE_THR_PP: continue
        bets.append(dict(
            game_id=str(r["game_id"]), edge_name=edge_name,
            bet_subject=str(r["player_name"]), bet_side=side,
            game_date=r["game_date"], season=int(r["season"]),
            player_id=int(r["player_id"]) if pd.notna(r["player_id"]) else None,
            bet_pt=bet_pt, bet_dec=bet_dec, book=book,
            stake=STAKE_PER_BET, model_pred=float(r["pred_mean"]),
            edge_pp=float(edge_pp), bet_signal=None,
            prediction_age_hours=float(r["pred_age_h"]),
            timing_flag="HOLD_FOR_1H_PRETIP",  # props: only closing data validated
        ))
    return bets


def upsert_bets(engine, bets, now_utc):
    if not bets: return 0, 0
    sql = sql_text("""
        INSERT INTO wnba_forward_paper_bets (
            game_id, edge_name, bet_subject, bet_side,
            emitted_at, last_updated, game_date, season, player_id,
            bet_pt, bet_dec, book, stake, model_pred, edge_pp, bet_signal,
            prediction_age_hours, timing_flag
        ) VALUES (
            :game_id, :edge_name, :bet_subject, :bet_side,
            :now, :now, :game_date, :season, :player_id,
            :bet_pt, :bet_dec, :book, :stake, :model_pred, :edge_pp, :bet_signal,
            :prediction_age_hours, :timing_flag
        )
        ON CONFLICT (game_id, edge_name, bet_subject, bet_side) DO UPDATE
        SET last_updated = EXCLUDED.last_updated,
            timing_flag  = EXCLUDED.timing_flag
        RETURNING (xmax = 0) AS is_insert
    """)
    n_new = n_seen = 0
    with engine.begin() as c:
        for b in bets:
            b2 = dict(b); b2["now"] = now_utc
            row = c.execute(sql, b2).first()
            if row and row.is_insert: n_new += 1
            else: n_seen += 1
    return n_new, n_seen


def grade_settled(engine, now_utc):
    pending = pd.read_sql(sql_text("""
        SELECT * FROM wnba_forward_paper_bets WHERE result IS NULL
    """), engine)
    if not len(pending): return 0
    played = pd.read_sql(sql_text("""
        SELECT game_id::text AS game_id, home_score, away_score
        FROM wnba_games WHERE season::int=2026
          AND home_score IS NOT NULL AND away_score IS NOT NULL
    """), engine)
    if not len(played): return 0
    pending = pending.merge(played, on="game_id", how="inner")
    if not len(pending): return 0
    updates = []
    for _, r in pending[pending["edge_name"]=="PART9_spreads"].iterrows():
        ahm = r["home_score"] - r["away_score"]
        if r["bet_side"] == "home":
            sgn = ahm + r["bet_pt"]
        else:
            sgn = -(ahm) + r["bet_pt"]
        if sgn > 0:
            updates.append(("won", float(ahm), float(r["stake"]*(r["bet_dec"]-1)),
                            r["game_id"], r["edge_name"], r["bet_subject"], r["bet_side"]))
        elif sgn < 0:
            updates.append(("lost", float(ahm), float(-r["stake"]),
                            r["game_id"], r["edge_name"], r["bet_subject"], r["bet_side"]))
        else:
            updates.append(("push", float(ahm), 0.0,
                            r["game_id"], r["edge_name"], r["bet_subject"], r["bet_side"]))
    prop_pending = pending[pending["edge_name"].isin(["B1_reb_edge5","B2_fg3m_edge5"])]
    if len(prop_pending):
        actuals = pd.read_sql(sql_text("""
            SELECT game_id::text AS game_id, player_id, reb, fg3m
            FROM wnba_game_logs WHERE season::int=2026
        """), engine)
        prop_pending = prop_pending.merge(actuals, on=["game_id","player_id"], how="left")
        for _, r in prop_pending.iterrows():
            stat_col = "reb" if r["edge_name"]=="B1_reb_edge5" else "fg3m"
            av = r.get(stat_col)
            if pd.isna(av): continue
            av = float(av); pt = float(r["bet_pt"])
            if av == pt:
                updates.append(("push", av, 0.0,
                                r["game_id"], r["edge_name"], r["bet_subject"], r["bet_side"]))
            else:
                won = (av > pt) if r["bet_side"]=="over" else (av < pt)
                if won:
                    updates.append(("won", av, float(r["stake"]*(r["bet_dec"]-1)),
                                    r["game_id"], r["edge_name"], r["bet_subject"], r["bet_side"]))
                else:
                    updates.append(("lost", av, float(-r["stake"]),
                                    r["game_id"], r["edge_name"], r["bet_subject"], r["bet_side"]))
    n = 0
    with engine.begin() as c:
        for res, av, units, gid, en, bs, bsd in updates:
            c.execute(sql_text("""
                UPDATE wnba_forward_paper_bets
                SET result=:r, actual_value=:av, units_won=:u, graded_at=:t
                WHERE game_id=:gid AND edge_name=:en
                  AND bet_subject=:bs AND bet_side=:bsd
            """), dict(r=res, av=av, u=float(units), t=now_utc,
                       gid=gid, en=en, bs=bs, bsd=bsd))
            n += 1
    return n


def print_tally(engine):
    df = pd.read_sql(sql_text("""
        SELECT edge_name, result, units_won, game_id
        FROM wnba_forward_paper_bets
    """), engine)
    if not len(df):
        print("  (no bets emitted yet)"); return
    expectation = {
        "PART9_spreads":  ("+10.60% ROI σ+1.52 (clean backtest)", 184),
        "B1_reb_edge5":   ("+6.65% ROI σ+1.51",  524),
        "B2_fg3m_edge5":  ("+4.31% ROI σ+0.97",  552),
    }
    for en in ["PART9_spreads","B1_reb_edge5","B2_fg3m_edge5"]:
        sub = df[df["edge_name"]==en]
        if not len(sub):
            print(f"  {en:20s}  n=0"); continue
        n = len(sub)
        settled = sub[sub["result"].isin(["won","lost","push"])]
        n_settled = len(settled); n_pending = (sub["result"].isna()).sum()
        if n_settled == 0:
            print(f"  {en:20s}  n={n}  settled=0  pending={n_pending}    (target: {expectation[en][0]})")
            continue
        units = float(settled["units_won"].sum())
        roi = units / n_settled
        sd = settled["units_won"].std() if n_settled > 1 else float("nan")
        sigma = (roi / (sd/np.sqrt(n_settled))) if sd and sd > 0 else float("nan")
        n_games = settled["game_id"].nunique()
        w = (settled["result"]=="won").sum()
        l = (settled["result"]=="lost").sum()
        p = (settled["result"]=="push").sum()
        print(f"  {en:20s}  n={n} settled={n_settled} pending={n_pending} games={n_games} "
              f"W-L-P={w}-{l}-{p} units={units:+.2f}u ROI={roi:+.2%} σ={sigma:+.2f}  "
              f"(target: {expectation[en][0]})")
    # Combined
    settled = df[df["result"].isin(["won","lost","push"])]
    if len(settled):
        n = len(df); n_settled = len(settled)
        units = float(settled["units_won"].sum())
        roi = units / n_settled
        sd = settled["units_won"].std() if n_settled > 1 else float("nan")
        sigma = (roi / (sd/np.sqrt(n_settled))) if sd and sd > 0 else float("nan")
        n_games = settled["game_id"].nunique()
        print(f"  {'COMBINED':20s}  n={n} settled={n_settled} games={n_games} "
              f"units={units:+.2f}u ROI={roi:+.2%} σ={sigma:+.2f}  "
              f"(target: clean 3-edge backtest +6.19% σ+2.17)")


def main() -> int:
    db_url = os.environ.get("SUPABASE_URL")
    if not db_url:
        print("FATAL: SUPABASE_URL env var required", file=sys.stderr)
        return 1
    now_utc = datetime.now(timezone.utc)
    hr(f"FORWARD BET TRACKER @ {now_utc.isoformat()}")
    try:
        engine = create_engine(db_url, pool_pre_ping=True)
        ensure_table(engine)
    except Exception as e:
        print(f"FATAL: DB init failed: {e}", file=sys.stderr)
        return 1

    games = pull_target_games(engine, now_utc)
    print(f"\n  Target window: now → +{LOOKAHEAD_HOURS}h ({now_utc.date()} to {(now_utc+timedelta(hours=LOOKAHEAD_HOURS)).date()})")
    print(f"  Scheduled 2026 games in window: {len(games)}")

    if len(games):
        spreads, props = pull_latest_live_lines(engine)
        print(f"  Latest live spread rows: {len(spreads)} | prop rows: {len(props)}")
        team_flags = compute_team_timing_flags(engine)
        n_hold = sum(1 for _, (f, _) in team_flags.items() if f == "HOLD_FOR_1H_PRETIP")
        print(f"  Team timing flags: {n_hold}/{len(team_flags)} on HOLD")
        for team, (flag, players) in team_flags.items():
            if flag == "HOLD_FOR_1H_PRETIP":
                print(f"    {team}: HOLD — uncertain rotation players: {', '.join(players)}")

        all_bets = []
        all_bets += emit_part9_bets(engine, games, spreads, now_utc, team_flags)
        all_bets += emit_prop_bets(engine, games, props, "B1_reb_edge5",
                                     "player_rebounds", "player_rebounds",
                                     FROZEN_CAL_B1_REB, now_utc, team_flags)
        all_bets += emit_prop_bets(engine, games, props, "B2_fg3m_edge5",
                                     "player_threes", "player_threes",
                                     FROZEN_CAL_B2_FG3M, now_utc, team_flags)
        hr("Emitted bets this run")
        if not all_bets:
            print("  (no edges qualified)")
        else:
            for b in all_bets:
                stale = ""
                if b["prediction_age_hours"] and b["prediction_age_hours"] > STALE_PRED_HOURS:
                    stale = f"  STALE pred_age={b['prediction_age_hours']:.1f}h"
                tag = f" pred={b['model_pred']:+.2f}"
                if b["edge_pp"] is not None: tag += f"  edge_pp={b['edge_pp']:+.1f}"
                if b["bet_signal"] is not None: tag += f"  signal={b['bet_signal']:+.2f}"
                tflag = b.get("timing_flag") or "FIRE_OPEN"
                print(f"  {b['game_date']} {b['edge_name']:15s} game={b['game_id']:>10s}  "
                      f"{b['bet_subject']:25.25s} {b['bet_side']:5s}  pt={b['bet_pt']:+6.1f} "
                      f"@ {b['bet_dec']:.2f} ({b['book']:12.12s}){tag}{stale}  [{tflag}]")
        n_new, n_seen = upsert_bets(engine, all_bets, now_utc)
        print(f"\n  Upsert: {n_new} new bets, {n_seen} already-recorded (idempotent)")
    n_graded = grade_settled(engine, now_utc)
    if n_graded:
        print(f"\n  Graded {n_graded} bet(s) this run.")
    hr("RUNNING TALLY vs backtest expectation")
    print_tally(engine)
    return 0


if __name__ == "__main__":
    sys.exit(main())
