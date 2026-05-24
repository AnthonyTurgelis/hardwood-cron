#!/usr/bin/env python3
"""WNBA availability refresh — append a fresh wnba_player_availability_current snapshot.

Self-contained cron port of cells/cell_3_v3_refresh_availability_intl.py
(the source of truth, post-delta v81). Resolves every player's rating through
a 5-layer cascade so accented veterans never silently fall to rating=0 and
international 2026 rookies never silently regress to the flat -0.008 prior:
  1. blend_k15 exact player_name
  2. blend_k15 NFKD+lowercase accent-normalized (dedup-by-|rating|-desc)
  3. wnba_name_map (NFKD-normalized canonical_name -> nba_player_id) -> blend by player_id
  4. v3_intl_fiba prior (wnba_player_priors_v2 @ v3_intl_fiba_2026_05_24, NEW
     in delta v81 — preserves the 15 international cold-start priors that
     would otherwise be overwritten by step 5)
  5. carry-forward: each player's most-recent NON-ZERO rating across ALL history
     (preserves injected rookie/international cold-start priors not in blend_k15)
  6. rating=0 neutralfill ONLY for genuinely unmatched

GHOST FILTER (delta v84): the snapshot universe is restricted to players who
are actually on a current 2026 roster. KEEP UNION =
  - wnba_bdl_active_players latest narrow snapshot (~213 active 2026 players)
  - 2026 wnba_game_logs hits
  - v3_intl_fiba_2026_05_24 prior cohort (15 rookies)
  - latest wnba_bdl_injury_snapshots
  - wnba_daily_rosters latest snapshot
Without this filter, the carry-forward layer preserves any prior non-zero
rating indefinitely — even for retired players (Candace Parker, Maya Moore,
Sylvia Fowles, Elena Delle Donne, etc.), polluting downstream WAR sums and
calibration cohorts.

INSERT-ONLY: appends ONE new computed_at snapshot, never deletes/truncates.
Downstream consumers filter by MAX(computed_at).

This must stay byte-faithful to the cell's cascade — any divergence reintroduces
the rating=0-for-accented-veterans bug (delta v78) OR the -0.008-regression
for intl rookies (delta v81) on every hourly run.

Required env vars:
  SUPABASE_URL   Postgres connection URL (sqlalchemy psycopg2 format)
Optional:
  DRY_RUN        if set (non-empty), do everything EXCEPT the final INSERT;
                 print source_counts + accented-veteran audit + neutralfill count.

Exit codes:
  0 — snapshot appended (or DRY_RUN audit clean)
  1 — any error (empty BDL/blend, DB/API failure, accent-cascade gate failure)

Local dry-run (PowerShell):
  $env:SUPABASE_URL=...; $env:DRY_RUN="1"; python scripts/availability_refresh.py
"""
import os
import sys
import unicodedata
from datetime import datetime

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text as sql_text


PLAY_PROB_MAP = {"healthy": 0.950, "day-to-day": 0.147, "out": 0.022}
FACTOR_SOURCE_DEFAULT = "v4_prior:informed_v1_n:49"
RATING_MV     = "rating_asof_blend_v1_k15"
INTL_MV       = "v3_intl_fiba_2026_05_24"  # v81 cold-start priors source-of-truth
BDL_TO_PROD   = {"LV": "LVA", "GS": "GSV", "PHX": "PHO", "NY": "NYL",
                 "WSH": "WAS", "LA": "LAS", "TOR": "TOR"}

# Known accented veterans — verification gate (must NOT land at rating=0 when
# blend_k15 holds a real non-zero rating for them).
ACCENT_VET_CHECK = ["Temi Fagbenle", "Marieme Badiane", "Azura Stevens",
                    "Marine Johannes", "Gabby Williams"]


def hr(title):
    print()
    print("=" * 78)
    print(f"  {title}")
    print("=" * 78)


def normalize_name(s):
    if s is None or pd.isna(s):
        return None
    s = str(s).strip()
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    return " ".join(s.lower().split())


def py(v):
    if v is None: return None
    if isinstance(v, float) and np.isnan(v): return None
    if pd.isna(v): return None
    if isinstance(v, np.integer):  return int(v)
    if isinstance(v, np.floating): return float(v)
    if isinstance(v, np.bool_):    return bool(v)
    if isinstance(v, pd.Timestamp): return v.to_pydatetime()
    return v


def to_prod_abbr(s):
    """BDL->production team mapping. Idempotent: prod values pass through."""
    if s is None or pd.isna(s): return None
    return BDL_TO_PROD.get(str(s), str(s))


def bdl_status_to_norm(s):
    if s is None or pd.isna(s): return "healthy"
    s = str(s).lower().strip()
    if s == "out": return "out"
    if s in ("day-to-day", "doubtful", "questionable"): return "day-to-day"
    if s in ("available", "probable", "healthy"): return "healthy"
    return "day-to-day"


def safe_set_index_dict(df, key_col, prefer_col=None, prefer_high_abs=None):
    if len(df) == 0:
        return {}
    df = df.copy()
    sort_cols, sort_asc = [key_col], [True]
    if prefer_col and prefer_col in df.columns:
        df["_prefer_null"] = df[prefer_col].isna()
        sort_cols.append("_prefer_null"); sort_asc.append(True)
    if prefer_high_abs and prefer_high_abs in df.columns:
        df["_neg_abs"] = -df[prefer_high_abs].abs()
        sort_cols.append("_neg_abs"); sort_asc.append(True)
    df = df.sort_values(sort_cols, ascending=sort_asc)
    df = df.drop_duplicates(subset=[key_col], keep="first")
    df = df.drop(columns=[c for c in ("_prefer_null", "_neg_abs") if c in df.columns])
    return df.set_index(key_col).to_dict("index")


def main() -> int:
    db_url = os.environ.get("SUPABASE_URL")
    if not db_url:
        print("FATAL: SUPABASE_URL env var required", file=sys.stderr)
        return 1
    dry_run = bool(os.environ.get("DRY_RUN"))
    print(f"AVAILABILITY REFRESH @ {datetime.now().isoformat()}  (DRY_RUN={dry_run})")

    try:
        engine = create_engine(db_url, pool_pre_ping=True)
    except Exception as e:
        print(f"FATAL: DB init failed: {e!r}", file=sys.stderr)
        return 1

    # -- Step 0: schema discovery + idempotency ----------------------------
    hr("Step 0: schema discovery + idempotency")
    try:
        cols = pd.read_sql(sql_text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'wnba_player_availability_current'
            ORDER BY ordinal_position
        """), engine)["column_name"].tolist()
        existing = pd.read_sql(sql_text("""
            SELECT computed_at, COUNT(*) AS n FROM wnba_player_availability_current
            GROUP BY computed_at ORDER BY computed_at DESC LIMIT 5
        """), engine)
    except Exception as e:
        print(f"FATAL: schema/idempotency query failed: {e!r}", file=sys.stderr)
        return 1
    if not cols or existing.empty:
        print("FATAL: availability table missing or empty", file=sys.stderr)
        return 1
    print(f"  Table columns ({len(cols)})")
    print(existing.to_string(index=False))
    latest_at = existing.iloc[0]["computed_at"]
    # DB writes are UTC (GHA + this script use UTC clock). Use UTC reference.
    seconds_ago = (pd.Timestamp.utcnow().tz_localize(None) - pd.Timestamp(latest_at)).total_seconds()
    print(f"\n  >>> MAX(computed_at) BEFORE = {latest_at}  ({seconds_ago/3600.0:.1f}h ago, UTC)")
    if not dry_run and abs(seconds_ago) < 60:
        print("  Last refresh < 60s ago (either direction) — skipping to avoid dup snapshot (no-op).")
        return 0

    # -- Step 1: blend_k15 -------------------------------------------------
    hr("Step 1: load blend_k15")
    try:
        blend = pd.read_sql(sql_text(f"""
            WITH ranked AS (
                SELECT player_id, player_name, team_abbr, rating, n_stints_basis,
                       ROW_NUMBER() OVER (PARTITION BY player_name
                                          ORDER BY game_date DESC) AS rn
                FROM wnba_player_rating_asof
                WHERE model_version = '{RATING_MV}'
            )
            SELECT player_id, player_name, team_abbr, rating, n_stints_basis
            FROM ranked WHERE rn = 1
        """), engine)
    except Exception as e:
        print(f"FATAL: blend_k15 query failed: {e!r}", file=sys.stderr)
        return 1
    if blend.empty:
        print("FATAL: blend_k15 returned 0 rows", file=sys.stderr)
        return 1
    print(f"  blend_k15 players (latest per name): {len(blend)}")

    blend["_nrm"] = blend["player_name"].apply(normalize_name)
    blend_by_exact = safe_set_index_dict(blend, "player_name")
    blend_by_nrm   = safe_set_index_dict(blend, "_nrm", prefer_high_abs="rating")
    blend_by_pid   = safe_set_index_dict(
        blend.assign(_pid=lambda d: d["player_id"].astype(str)),
        "_pid", prefer_high_abs="rating")
    blend_dedup_df = (blend.assign(_abs_r=lambda d: d["rating"].abs())
                      .sort_values("_abs_r", ascending=False)
                      .drop_duplicates(subset=["_nrm"], keep="first")
                      .drop(columns=["_abs_r"]))

    # -- Step 1b: wnba_name_map -------------------------------------------
    hr("Step 1b: load wnba_name_map bridge")
    try:
        nmap = pd.read_sql(sql_text("""
            SELECT canonical_name, nba_player_id FROM wnba_name_map
            WHERE nba_player_id IS NOT NULL
        """), engine)
    except Exception as e:
        print(f"FATAL: name_map query failed: {e!r}", file=sys.stderr)
        return 1
    nmap["_nrm"] = nmap["canonical_name"].apply(normalize_name)
    namemap_by_nrm = safe_set_index_dict(nmap.dropna(subset=["_nrm"]), "_nrm")
    print(f"  name_map entries with nba_player_id: {len(nmap)} "
          f"({len(namemap_by_nrm)} unique normalized)")

    # -- Step 1c: load v3_intl_fiba priors (delta v81 cold-start source) --
    hr("Step 1c: load v3_intl_fiba priors")
    try:
        intl_priors = pd.read_sql(sql_text(f"""
            SELECT player_name, rating, source, wnba_player_id
            FROM wnba_player_priors_v2
            WHERE model_version = '{INTL_MV}'
        """), engine)
    except Exception as e:
        print(f"FATAL: intl priors query failed: {e!r}", file=sys.stderr)
        return 1
    intl_priors["_nrm"] = intl_priors["player_name"].apply(normalize_name)
    intl_by_nrm = safe_set_index_dict(
        intl_priors.dropna(subset=["_nrm","rating"]), "_nrm", prefer_high_abs="rating")
    print(f"  v3_intl_fiba priors loaded: {len(intl_priors)} rows; {len(intl_by_nrm)} unique normalized")

    # -- Step 2: latest BDL snapshot --------------------------------------
    hr("Step 2: load latest BDL snapshot")
    try:
        bdl_at = pd.read_sql(sql_text("""
            SELECT MAX(snapshot_at) AS last FROM wnba_bdl_injury_snapshots
        """), engine).iloc[0]["last"]
    except Exception as e:
        print(f"FATAL: BDL snapshot query failed: {e!r}", file=sys.stderr)
        return 1
    if bdl_at is None:
        print("FATAL: wnba_bdl_injury_snapshots is empty", file=sys.stderr)
        return 1
    print(f"  Latest BDL snapshot: {bdl_at}")
    bdl = pd.read_sql(sql_text(f"""
        SELECT bdl_player_id, player_first_name, player_last_name, team_abbreviation,
               status, return_date, description
        FROM wnba_bdl_injury_snapshots
        WHERE snapshot_at = '{bdl_at}'
    """), engine)
    bdl = bdl[bdl["bdl_player_id"].notna()].copy()  # drop NO_INJURIES marker
    print(f"  BDL injured players (excl. marker): {len(bdl)}")
    bdl["full_name"] = bdl["player_first_name"].str.strip() + " " + bdl["player_last_name"].str.strip()
    bdl["_nrm"] = bdl["full_name"].apply(normalize_name)
    bdl_by_exact = safe_set_index_dict(bdl, "full_name")
    bdl_by_nrm   = safe_set_index_dict(bdl, "_nrm")
    bdl_by_id    = safe_set_index_dict(bdl, "bdl_player_id")

    # -- Step 3: previous snapshot + carry-forward ------------------------
    hr("Step 3: previous snapshot + carry-forward")
    prev = pd.read_sql(sql_text(f"""
        SELECT player_id, player_name, team_abbr, bdl_player_id, rating, n_stints_basis,
               factor_source
        FROM wnba_player_availability_current
        WHERE computed_at = '{latest_at}'
    """), engine)
    print(f"  Previous snapshot rows: {len(prev)}")
    prev_by_name = safe_set_index_dict(prev, "player_name", prefer_col="bdl_player_id")

    cf = pd.read_sql(sql_text("""
        SELECT DISTINCT ON (player_name)
               player_name, player_id, rating, n_stints_basis, factor_source
        FROM wnba_player_availability_current
        WHERE rating IS NOT NULL AND ABS(rating) > 1e-9
        ORDER BY player_name, computed_at DESC
    """), engine)
    cf["_nrm"] = cf["player_name"].apply(normalize_name)
    prev_rating_by_nrm = safe_set_index_dict(
        cf.dropna(subset=["_nrm"]), "_nrm", prefer_high_abs="rating")
    print(f"  carry-forward candidates (last non-zero per player, all history): {len(prev_rating_by_nrm)}")

    # -- Step 3b: build KEEP UNION (ghost filter, delta v84) ---------------
    hr("Step 3b: build KEEP UNION (active-roster signals only)")
    try:
        bdl_active_raw = pd.read_sql(sql_text("""
            WITH counts AS (
                SELECT snapshot_date, COUNT(*) AS n
                FROM wnba_bdl_active_players GROUP BY snapshot_date
            ), latest_narrow AS (
                SELECT snapshot_date FROM counts
                WHERE n BETWEEN 180 AND 250
                ORDER BY snapshot_date DESC LIMIT 1
            )
            SELECT (raw_payload->>'player_name') AS player_name
            FROM wnba_bdl_active_players
            WHERE snapshot_date = (SELECT snapshot_date FROM latest_narrow)
        """), engine)
        bdl_active_raw["_nrm"] = bdl_active_raw["player_name"].apply(normalize_name)
        keep_bdl_active = set(bdl_active_raw["_nrm"].dropna().unique().tolist())
    except Exception as e:
        print(f"WARN: BDL active query failed ({e!r:.80}); KEEP signal A is empty")
        keep_bdl_active = set()

    try:
        gl_2026 = pd.read_sql(sql_text("""
            SELECT DISTINCT player_name FROM wnba_game_logs
            WHERE game_date::date >= DATE '2026-01-01'
        """), engine)
        gl_2026["_nrm"] = gl_2026["player_name"].apply(normalize_name)
        keep_gl_2026 = set(gl_2026["_nrm"].dropna().unique().tolist())
    except Exception as e:
        print(f"WARN: 2026 game_logs query failed ({e!r:.80}); KEEP signal B is empty")
        keep_gl_2026 = set()

    keep_intl_set = set(intl_priors["_nrm"].dropna().unique().tolist())

    try:
        bdl_inj_names = pd.read_sql(sql_text(f"""
            SELECT DISTINCT (COALESCE(player_first_name,'') || ' ' ||
                             COALESCE(player_last_name,'')) AS full_name
            FROM wnba_bdl_injury_snapshots
            WHERE snapshot_at = '{bdl_at}' AND bdl_player_id IS NOT NULL
        """), engine)
        bdl_inj_names["_nrm"] = bdl_inj_names["full_name"].apply(normalize_name)
        keep_bdl_inj = set(bdl_inj_names["_nrm"].dropna().unique().tolist())
    except Exception as e:
        keep_bdl_inj = set()

    try:
        dr = pd.read_sql(sql_text("""
            SELECT DISTINCT player_name FROM wnba_daily_rosters
            WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM wnba_daily_rosters)
        """), engine)
        dr["_nrm"] = dr["player_name"].apply(normalize_name)
        keep_daily_rosters = set(dr["_nrm"].dropna().unique().tolist())
    except Exception:
        keep_daily_rosters = set()

    keep_union = (keep_bdl_active | keep_gl_2026 | keep_intl_set
                  | keep_bdl_inj | keep_daily_rosters)
    print(f"  KEEP signals: bdl_active={len(keep_bdl_active)}, gl_2026={len(keep_gl_2026)}, "
          f"intl={len(keep_intl_set)}, bdl_inj={len(keep_bdl_inj)}, daily_rosters={len(keep_daily_rosters)}")
    print(f"  KEEP UNION: {len(keep_union)} unique norm names")
    if len(keep_union) < 50:
        print(f"FATAL: KEEP UNION too small ({len(keep_union)}); refusing to write a near-empty snapshot",
              file=sys.stderr)
        return 1

    # -- Step 4: resolution cascade (intl-aware, delta v81) ---------------
    def resolve_rating(player_name):
        if player_name in blend_by_exact:
            b = blend_by_exact[player_name]
            return (float(b["rating"]), py(b["player_id"]), py(b["n_stints_basis"]),
                    "blend_exact", player_name, None)
        nrm = normalize_name(player_name)
        if nrm and nrm in blend_by_nrm:
            b = blend_by_nrm[nrm]
            return (float(b["rating"]), py(b["player_id"]), py(b["n_stints_basis"]),
                    "blend_norm", b["player_name"], None)
        if nrm and nrm in namemap_by_nrm:
            pid = namemap_by_nrm[nrm].get("nba_player_id")
            if pid is not None and not pd.isna(pid):
                pid_str = str(int(pid))
                if pid_str in blend_by_pid:
                    b = blend_by_pid[pid_str]
                    return (float(b["rating"]), py(b["player_id"]), py(b["n_stints_basis"]),
                            "name_map_pid", b["player_name"], None)
        # NEW step 4 (delta v81): v3_intl_fiba prior — must come BEFORE
        # carry-forward, otherwise carry-forward locks in the stale -0.008
        # flat sentinel for international cold-starts.
        if nrm and nrm in intl_by_nrm:
            ir = intl_by_nrm[nrm]
            if pd.notna(ir["rating"]):
                return (float(ir["rating"]), py(ir.get("wnba_player_id")), None,
                        "v3_intl_fiba", ir["player_name"], str(ir.get("source") or ""))
        if nrm and nrm in prev_rating_by_nrm:
            pr = prev_rating_by_nrm[nrm]
            pr_rating = pr.get("rating")
            if pr_rating is not None and not pd.isna(pr_rating) and abs(float(pr_rating)) > 1e-9:
                return (float(pr_rating), py(pr.get("player_id")), py(pr.get("n_stints_basis")),
                        "prev_carryforward", player_name, None)
        return (0.0, None, None, "neutralfill", player_name, None)

    def match_to_bdl(player_name, known_bdl_id=None):
        if known_bdl_id is not None and known_bdl_id in bdl_by_id:
            m = dict(bdl_by_id[known_bdl_id]); m["bdl_player_id"] = known_bdl_id
            return m
        if player_name in bdl_by_exact:
            return bdl_by_exact[player_name]
        nrm = normalize_name(player_name)
        if nrm and nrm in bdl_by_nrm:
            return bdl_by_nrm[nrm]
        return None

    # -- Step 5: canonical universe ---------------------------------------
    hr("Step 5: build canonical player universe")
    universe, universe_nrms = [], set()
    for nm in blend_dedup_df["player_name"]:
        nrm = normalize_name(nm)
        if nrm not in universe_nrms:
            universe.append(nm); universe_nrms.add(nrm)
    n_from_blend = len(universe)
    for _, brow in bdl.iterrows():
        if brow["_nrm"] not in universe_nrms:
            universe.append(brow["full_name"]); universe_nrms.add(brow["_nrm"])
    n_after_bdl = len(universe)
    for nm in prev_by_name.keys():
        nrm = normalize_name(nm)
        if nrm not in universe_nrms:
            universe.append(nm); universe_nrms.add(nrm)
    n_pre_filter = len(universe)
    # APPLY GHOST FILTER (delta v84) — keep only names in KEEP UNION.
    universe = [nm for nm in universe if normalize_name(nm) in keep_union]
    print(f"  Universe size: {n_pre_filter} (blend={n_from_blend}, "
          f"+BDL-only={n_after_bdl - n_from_blend}, +prev-only={n_pre_filter - n_after_bdl})  "
          f"-> POST GHOST FILTER: {len(universe)} ({n_pre_filter - len(universe)} ghosts dropped)")

    # -- Step 6: build rows -----------------------------------------------
    hr("Step 6: build refreshed rows")
    new_computed_at = datetime.utcnow()
    print(f"  New computed_at: {new_computed_at} (UTC)")
    source_counts = {"blend_exact": 0, "blend_norm": 0, "name_map_pid": 0,
                     "v3_intl_fiba": 0, "prev_carryforward": 0, "neutralfill": 0}
    status_counts = {"healthy": 0, "day-to-day": 0, "out": 0}
    new_rows = []
    for input_name in universe:
        rating, player_id, n_stints, source, canonical_name, intl_source = resolve_rating(input_name)
        source_counts[source] += 1

        team_abbr = None
        for try_name in (canonical_name, input_name):
            if try_name in prev_by_name:
                ta_raw = prev_by_name[try_name].get("team_abbr")
                if ta_raw:
                    team_abbr = to_prod_abbr(ta_raw); break
        if not team_abbr and canonical_name in blend_by_exact:
            ta_raw = blend_by_exact[canonical_name].get("team_abbr")
            if ta_raw:
                team_abbr = to_prod_abbr(ta_raw)

        known_id = None
        if canonical_name in prev_by_name:
            bid = prev_by_name[canonical_name].get("bdl_player_id")
            if bid is not None and not pd.isna(bid):
                known_id = int(bid)
        bdl_match = match_to_bdl(canonical_name, known_bdl_id=known_id)
        if bdl_match is None and input_name != canonical_name:
            bdl_match = match_to_bdl(input_name)

        if bdl_match is not None:
            status_norm = bdl_status_to_norm(bdl_match["status"])
            injury_status = bdl_match["status"]
            return_date = bdl_match["return_date"]
            injury_description = bdl_match["description"]
            bdl_player_id = int(bdl_match["bdl_player_id"])
            if not team_abbr:
                team_abbr = to_prod_abbr(bdl_match["team_abbreviation"])
        else:
            status_norm = "healthy"
            injury_status = "Healthy"
            return_date = None
            injury_description = None
            bdl_player_id = known_id

        status_counts[status_norm] += 1
        play_prob = PLAY_PROB_MAP[status_norm]
        adj = rating * play_prob if rating is not None else None

        # factor_source + rating_model_version lineage tags (delta v81+v84):
        # intl rows record tier + n_sr from the priors source string.
        if source == "v3_intl_fiba" and intl_source:
            tier_part = intl_source.split(";")[0].split("=")[-1] if "=" in intl_source else "intl"
            nsr_part  = intl_source.split(";")[1].split("=")[-1] if ";" in intl_source else "0"
            fac_source = f"{INTL_MV}:tier_{tier_part}:n_sr_{nsr_part}"
            rating_mv_out = INTL_MV
        else:
            fac_source = FACTOR_SOURCE_DEFAULT
            rating_mv_out = RATING_MV

        new_rows.append({
            "computed_at":                  new_computed_at,
            "player_id":                    player_id,
            "player_name":                  canonical_name,
            "team_abbr":                    team_abbr,
            "rating":                       float(rating) if rating is not None else None,
            "n_stints_basis":               float(n_stints) if n_stints is not None else None,
            "rating_model_version":         rating_mv_out,
            "bdl_player_id":                bdl_player_id,
            "injury_status":                injury_status,
            "status_norm":                  status_norm,
            "injury_snapshot_at":           py(bdl_at),
            "return_date":                  str(return_date) if return_date is not None else None,
            "injury_description":           injury_description,
            "play_prob":                    float(play_prob),
            "availability_factor":          float(play_prob),
            "adjusted_rating_contribution": float(adj) if adj is not None else None,
            "factor_source":                fac_source,
        })

    new_df = pd.DataFrame(new_rows)
    print(f"  Resolution sources: {source_counts}")
    print(f"  Status distribution: {status_counts}")

    # -- Step 7: accent-cascade gate (runs on in-memory frame; pre-insert) -
    hr("Step 7: accented-veteran gate (snapshot vs blend truth)")
    new_df["_nrm"] = new_df["player_name"].apply(normalize_name)
    n_vet_fail = 0
    for orig in ACCENT_VET_CHECK:
        vn = normalize_name(orig)
        expected, _pid, _ns, src, _cn, _is = resolve_rating(orig)
        rows = new_df[new_df["_nrm"] == vn]
        if len(rows) == 0:
            print(f"    {orig:22s} NOT IN SNAPSHOT (not rostered/injured — ok)")
            continue
        snap_rating = float(rows.iloc[0]["rating"])
        failed = (abs(snap_rating - float(expected)) > 1e-6
                  and abs(float(expected)) > 1e-9 and abs(snap_rating) < 1e-9)
        flag = "  <-- CASCADE FAIL" if failed else ("  (genuine zero)" if abs(snap_rating) < 1e-9 else "")
        if failed:
            n_vet_fail += 1
        print(f"    {rows.iloc[0]['player_name']:22s} {rows.iloc[0]['team_abbr']!s:4s} "
              f"snap={snap_rating:+.5f} blend={float(expected):+.5f} src={src}{flag}")

    n_neutralfill = source_counts["neutralfill"]
    print(f"\n  neutralfill count: {n_neutralfill}  |  carry-forward: {source_counts['prev_carryforward']}")
    if n_vet_fail > 0:
        print(f"\nFATAL: {n_vet_fail} accented veteran(s) zeroed despite a real blend rating — "
              f"port/cascade broke. Refusing to write.", file=sys.stderr)
        return 1

    # -- Step 8: persist (INSERT-ONLY) ------------------------------------
    if dry_run:
        hr("Step 8: DRY_RUN — skipping INSERT")
        print(f"  Would insert {len(new_df)} rows at computed_at = {new_computed_at}")
        print(f"  >>> MAX(computed_at) UNCHANGED = {latest_at}")
        return 0

    hr("Step 8: INSERT refreshed snapshot")
    try:
        check = pd.read_sql(sql_text(f"""
            SELECT COUNT(*) AS n FROM wnba_player_availability_current
            WHERE computed_at = '{new_computed_at}'
        """), engine).iloc[0]["n"]
        if check > 0:
            print(f"FATAL: rows already exist for {new_computed_at}", file=sys.stderr)
            return 1
        insert_df = new_df.drop(columns=["_nrm"])[cols]
        insert_df.to_sql("wnba_player_availability_current", engine,
                         if_exists="append", index=False, method="multi", chunksize=200)
    except Exception as e:
        print(f"FATAL: INSERT failed: {e!r}", file=sys.stderr)
        return 1
    print(f"  Inserted {len(new_df)} rows at computed_at = {new_computed_at}")

    after = pd.read_sql(sql_text("""
        SELECT MAX(computed_at) AS mx FROM wnba_player_availability_current
    """), engine).iloc[0]["mx"]
    print(f"  >>> MAX(computed_at) AFTER = {after}  (advanced from {latest_at})")
    print("\n  play_prob_log's snapshot_at will advance on the next shadow_aware run.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
