#!/usr/bin/env python3
"""WNBA in-season RATING refresher — walk-forward EXTEND of the Kalman + Glicko-2
team-strength series as new 2026 games land. Standalone cron port of
cells/cell_strength_live_06_series_extend.py (+ cell_strength_series_lib.py),
delta v163.

WHY a full replay: the Kalman filter's state is a JOINT covariance across all
teams; the persisted series stores only the per-team diagonal, so it cannot be
resumed from stored state. The correct continuation is a deterministic REPLAY of
the EXACT v142 walk-forward over ALL played games (Kalman Q,R tuned on immutable
season<2024 -> identical; Glicko ratings are calibration-independent), which
reproduces every existing row byte-for-byte (verified <5e-12 in cell 07) and
yields the new-game rows. We APPEND ONLY rows whose game_id is not already in the
series. The update math below is copied VERBATIM from the v142 builders
(cell_kalman_02_state_space_strength.py, cell_glicko2_01_dynamic_ratings.py) —
coefficients/process-variance are NOT reinvented.

CHAINING: run this BEFORE strength_live_refresh.py in the daily workflow — the
series extends, then the surface rebuilds its date axis off the fresh ratings.

Idempotency (Section 2.F): zero new game_ids -> appends nothing (net no-op).
Same model_versions kept stable so strength_live_refresh.py's read is unchanged.

Required env: SUPABASE_URL.   Optional: DRY_RUN (skip writes; print the plan).
Exit: 0 ok / 1 any error (DB init, empty pull, sanity gate).
"""
import math
import os
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text as sql_text

KAL_MV = "kalman_walkforward_2026_05_24"
GLI_MV = "glicko2_walkforward_2026_05_24"

# ---- Kalman constants (verbatim from cell_kalman_02) ----
P0 = 150.0
SEASON_BUMP = 50.0
SQRT2 = math.sqrt(2.0)
Q_GRID = [0.01, 0.02, 0.05, 0.1, 0.2, 0.4, 0.8]
R_GRID = [100.0, 130.0, 160.0, 200.0, 250.0, 300.0]

# ---- Glicko-2 constants (verbatim from cell_glicko2_01) ----
SCALE = 173.7178
R0, RD0, SIGMA0 = 1500.0, 350.0, 0.06
TAU = 0.5
EPS = 1e-6
OFFSEASON_RD_BUMP = 60.0


def py(v):
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


def load_played_games(engine):
    with engine.connect() as c:
        g = pd.read_sql(sql_text("""
            SELECT game_id, game_date, season,
                   home_team_abbr AS home, away_team_abbr AS away,
                   home_score, away_score
            FROM wnba_games
            WHERE home_score IS NOT NULL AND away_score IS NOT NULL
            ORDER BY game_date, game_id
        """), c)
    g["actual_margin"] = (g["home_score"] - g["away_score"]).astype(int)
    g["home_won"] = (g["actual_margin"] > 0).astype(int)
    return g


# ============================ KALMAN (verbatim) ===============================
def run_kalman(games, Q, R, HCA, snapshot=False):
    teams = sorted(set(games.home) | set(games.away))
    idx = {t: i for i, t in enumerate(teams)}
    n = len(teams)
    m = np.zeros(n)
    P = np.full(n, P0)
    Pmat = np.diag(P).astype(float)
    last_season = {}
    t = 0
    last_t = {}
    rows, snaps = [], []
    for g in games.itertuples(index=False):
        i, j = idx[g.home], idx[g.away]
        for tm, k in ((g.home, i), (g.away, j)):
            elapsed = t - last_t.get(k, t)
            Pmat[k, k] += Q * max(elapsed, 1)
            if last_season.get(k, g.season) < g.season:
                Pmat[k, k] += SEASON_BUMP
            last_season[k] = g.season
            last_t[k] = t
        pred = m[i] - m[j] + HCA
        S = Pmat[i, i] + Pmat[j, j] - 2.0 * Pmat[i, j] + R
        p_home = 0.5 * (1.0 + math.erf(pred / (math.sqrt(S) * SQRT2)))
        rows.append({"game_id": g.game_id, "season": int(g.season), "game_date": g.game_date,
                     "home": g.home, "away": g.away, "pred_margin": pred, "p_home_win": p_home,
                     "actual_margin": int(g.actual_margin), "home_won": int(g.home_won), "S": S})
        innov = g.actual_margin - pred
        Hcov = Pmat[i, :] - Pmat[j, :]
        K = Hcov / S
        m = m + K * innov
        Pmat = Pmat - np.outer(K, Hcov)
        Pmat = 0.5 * (Pmat + Pmat.T)
        t += 1
        if snapshot:
            for tm, k in ((g.home, i), (g.away, j)):
                snaps.append({"team_abbr": tm, "game_id": g.game_id, "game_date": g.game_date,
                              "season": int(g.season), "rating": float(m[k]),
                              "rating_uncertainty": float(math.sqrt(max(Pmat[k, k], 0.0))),
                              "model_version": KAL_MV})
    return pd.DataFrame(rows), pd.DataFrame(snaps)


def _mae(df):
    return (df.pred_margin - df.actual_margin).abs().mean()


def replay_kalman(games):
    HCA = float(games[games.season < 2024]["actual_margin"].mean())
    best = None
    pre24 = games[games.season < 2024]
    for Q in Q_GRID:
        for R in R_GRID:
            pred, _ = run_kalman(pre24, Q, R, HCA, snapshot=False)
            val = pred[pred.season.isin([2022, 2023])]
            mae = _mae(val)
            if best is None or mae < best[0]:
                best = (mae, Q, R)
    _, Q_best, R_best = best
    _, snaps = run_kalman(games, Q_best, R_best, HCA, snapshot=True)
    return snaps


# ============================ GLICKO-2 (verbatim) =============================
def _g(phi):
    return 1.0 / math.sqrt(1.0 + 3.0 * phi * phi / (math.pi * math.pi))


def _E(mu, mu_opp, phi_opp):
    return 1.0 / (1.0 + math.exp(-_g(phi_opp) * (mu - mu_opp)))


def _new_sigma(sigma, delta, phi, v, tau):
    a = math.log(sigma * sigma)
    d2 = delta * delta
    phi2 = phi * phi

    def f(x):
        ex = math.exp(x)
        num = ex * (d2 - phi2 - v - ex)
        den = 2.0 * (phi2 + v + ex) ** 2
        return num / den - (x - a) / (tau * tau)

    A = a
    if d2 > phi2 + v:
        B = math.log(d2 - phi2 - v)
    else:
        k = 1
        while f(a - k * tau) < 0:
            k += 1
        B = a - k * tau
    fA, fB = f(A), f(B)
    while abs(B - A) > EPS:
        C = A + (A - B) * fA / (fB - fA)
        fC = f(C)
        if fC * fB <= 0:
            A, fA = B, fB
        else:
            fA = fA / 2.0
        B, fB = C, fC
    return math.exp(A / 2.0)


def update_team(r, rd, sigma, opp_r, opp_rd, score, hca_elo):
    mu = (r - R0) / SCALE
    phi = rd / SCALE
    mu_opp = (opp_r - R0) / SCALE
    phi_opp = opp_rd / SCALE
    mu_eff = mu + hca_elo / SCALE
    g_opp = _g(phi_opp)
    E = _E(mu_eff, mu_opp, phi_opp)
    v = 1.0 / (g_opp * g_opp * E * (1.0 - E))
    delta = v * g_opp * (score - E)
    sigma_p = _new_sigma(sigma, delta, phi, v, TAU)
    phi_star = math.sqrt(phi * phi + sigma_p * sigma_p)
    phi_p = 1.0 / math.sqrt(1.0 / (phi_star * phi_star) + 1.0 / v)
    mu_p = mu + phi_p * phi_p * g_opp * (score - E)
    return SCALE * mu_p + R0, SCALE * phi_p, sigma_p


def replay_glicko2(games):
    state = {}
    snaps = []

    def get(team, season):
        s = state.get(team)
        if s is None:
            s = {"r": R0, "rd": RD0, "sigma": SIGMA0, "last_season": season}
            state[team] = s
        elif season > s["last_season"]:
            s["rd"] = min(RD0, math.sqrt(s["rd"] ** 2 + OFFSEASON_RD_BUMP ** 2))
            s["last_season"] = season
        return s

    for g in games.itertuples(index=False):
        sh = get(g.home, g.season)
        sa = get(g.away, g.season)
        sh_score = 1.0 if g.home_won == 1 else 0.0
        nh = update_team(sh["r"], sh["rd"], sh["sigma"], sa["r"], sa["rd"], sh_score, +0.0)
        na = update_team(sa["r"], sa["rd"], sa["sigma"], sh["r"], sh["rd"], 1.0 - sh_score, -0.0)
        sh["r"], sh["rd"], sh["sigma"] = nh
        sa["r"], sa["rd"], sa["sigma"] = na
        for tm, st in ((g.home, sh), (g.away, sa)):
            snaps.append({"team_abbr": tm, "game_id": g.game_id, "game_date": g.game_date,
                          "season": int(g.season), "rating": st["r"], "rd": st["rd"],
                          "volatility": st["sigma"], "model_version": GLI_MV})
    return pd.DataFrame(snaps)


def main() -> int:
    db_url = os.environ.get("SUPABASE_URL")
    if not db_url:
        print("FATAL: SUPABASE_URL required", file=sys.stderr); return 1
    dry = bool(os.environ.get("DRY_RUN"))
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    print(f"SERIES EXTEND REFRESH @ {now.isoformat()}  kal={KAL_MV} gli={GLI_MV}  DRY_RUN={dry}")
    try:
        engine = create_engine(db_url, pool_pre_ping=True)
    except Exception as e:
        print(f"FATAL: DB init failed: {e!r}", file=sys.stderr); return 1

    try:
        games = load_played_games(engine)
        with engine.connect() as c:
            kal_have = set(pd.read_sql(sql_text(
                "SELECT DISTINCT game_id FROM wnba_team_kalman WHERE model_version=:v"),
                c, params={"v": KAL_MV})["game_id"])
            gli_have = set(pd.read_sql(sql_text(
                "SELECT DISTINCT game_id FROM wnba_glicko2_ratings WHERE model_version=:v"),
                c, params={"v": GLI_MV})["game_id"])
        kal_snaps = replay_kalman(games)
        gli_snaps = replay_glicko2(games)
    except Exception as e:
        print(f"FATAL: replay failed: {e!r}", file=sys.stderr); return 1

    kal_new = kal_snaps[~kal_snaps.game_id.isin(kal_have)].copy()
    gli_new = gli_snaps[~gli_snaps.game_id.isin(gli_have)].copy()

    hr("EXTEND PLAN")
    print(f"  played games total = {len(games)} | series has {len(kal_have)} game_ids")
    print(f"  Kalman   NEW {kal_new.game_id.nunique()} games ({len(kal_new)} rows)")
    print(f"  Glicko-2 NEW {gli_new.game_id.nunique()} games ({len(gli_new)} rows)")

    # SANITY: the replay must regenerate the full existing series (never fewer)
    if kal_snaps.game_id.nunique() < len(kal_have) or gli_snaps.game_id.nunique() < len(gli_have):
        print("FATAL: replay covers fewer game_ids than the stored series — "
              "refusing to write (possible mid-history mutation).", file=sys.stderr)
        return 1

    if kal_new.empty and gli_new.empty:
        print("\n  ZERO new games -> APPEND-only no-op. Series already current.")
        return 0

    if dry:
        hr("DRY_RUN — skipping writes")
        print(f"  would APPEND Kalman={len(kal_new)} rows, Glicko-2={len(gli_new)} rows "
              f"(new game_ids only; existing rows untouched).")
        return 0

    hr("PERSIST (APPEND-only, new game_ids)")
    kal_out = pd.DataFrame([{k: py(v) for k, v in r.items()} for r in kal_new.to_dict("records")])
    gli_out = pd.DataFrame([{k: py(v) for k, v in r.items()} for r in gli_new.to_dict("records")])
    try:
        with engine.begin() as c:
            if len(kal_out):
                kal_out.to_sql("wnba_team_kalman", c, if_exists="append", index=False,
                               method="multi", chunksize=500)
            if len(gli_out):
                gli_out.to_sql("wnba_glicko2_ratings", c, if_exists="append", index=False,
                               method="multi", chunksize=500)
    except Exception as e:
        print(f"FATAL: persist failed: {e!r}", file=sys.stderr); return 1
    print(f"  appended Kalman={len(kal_out)} rows, Glicko-2={len(gli_out)} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
