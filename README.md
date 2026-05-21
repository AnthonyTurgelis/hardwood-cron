# hardwood-cron

GitHub Actions cron jobs that keep The Hardwood's time-series tables fresh.

## Workflows

| Workflow | Schedule (UTC) | Script | Writes to |
|---|---|---|---|
| `bdl-injury-snapshot.yml` | every 30 min (`*/30 * * * *`) | `scripts/bdl_injury.py` | `wnba_bdl_injury_snapshots` |
| `odds-snapshot.yml`       | every 30 min (`*/30 * * * *`) | `scripts/odds_snapshot.py` | `wnba_live_game_line_snapshots`, `wnba_live_player_prop_snapshots` |
| `freshness-check.yml`     | every 30 min (`*/30 * * * *`) | `scripts/freshness_check.py` | (read-only — exits 1 if any table is stale > 30 min during May-Oct) |

All three workflows also support **manual trigger** via the Actions tab (`workflow_dispatch`) — useful for testing or running ad-hoc.

## Secrets required

Add these at **Settings → Secrets and variables → Actions**:

| Secret | Used by | Source |
|---|---|---|
| `SUPABASE_URL` | all three | the workspace `.env` — sqlalchemy psycopg2 URL |
| `BDL_API_KEY`  | bdl-injury-snapshot | Ball Don't Lie account dashboard |
| `ODDS_API_KEY` | odds-snapshot | the-odds-api.com account dashboard |

The freshness check only needs `SUPABASE_URL`. If a snapshot workflow's secret is missing, that workflow exits 1 (and GitHub emails the repo owner).

## Local testing

```powershell
$env:SUPABASE_URL = "<paste from main workspace .env>"
$env:BDL_API_KEY  = "<paste>"
$env:ODDS_API_KEY = "<paste>"

pip install -r requirements.txt

python scripts/bdl_injury.py        # should print "Captured N rows from BDL injuries at <ts>"
python scripts/odds_snapshot.py     # should print "Captured N game-line rows + N prop rows"
python scripts/freshness_check.py   # should print FRESH/STALE per table
```

## Failure & alerting

- Snapshot scripts: exit 1 only on real errors (missing secret, API outage, DB write failure). "No upcoming games" or "0 injuries" are exit 0.
- `freshness_check.py`: exit 1 only during in-season (May-Oct) if any table is stale > 30 min. (Tight threshold matched to the 30-min snapshot cadence; a single missed snapshot run plus jitter still stays under threshold.)
- Any exit-1 → workflow fails → GitHub auto-emails repo owner. That's the alert path.

## Schema reference

`wnba_live_game_line_snapshots` PK: `(event_id, snapshot_at, bookmaker, market, outcome_label)` — `ON CONFLICT DO NOTHING` makes the snapshot insert idempotent.

`wnba_live_player_prop_snapshots` PK: `(event_id, snapshot_at, bookmaker, market, player_name, side)`.

`wnba_bdl_injury_snapshots` has no UNIQUE constraint; each run appends one row per (snapshot_at, player). De-duplication happens downstream.

## Why every 30 minutes

Freshness alert threshold is 30 min so snapshot cadence has to match. 48 snapshots/day per workflow gives near-continuous coverage of opening→closing line evolution — fine-grained enough for accurate CLV measurement and intraday line-movement features.

## Cost

The Odds API: ~30 credits per game per snapshot (3 markets × 1 region × 10 credits) plus per-event property calls. Per-run cost = ~12 API calls × 30 credits ≈ 360 credits when 6 events are upcoming. 48 runs/day × 360 = ~17K credits/day = ~520K credits/month. Subscription has ~4.7M credits — about 9 months of runway at this cadence. Off-season (no games) costs near-zero.

BDL: free at our usage level.

GitHub Actions: each snapshot run ~30-60s, freshness check ~15s. 48 runs/day × 3 workflows × ~50s avg ≈ 120 min/day ≈ 3,600 min/month. **Free tier is 2,000 min/month for private repos** — this will overshoot by ~1,600 min and bill ~$13/month overage (Linux runner at $0.008/min). Public repo = unlimited free.

## History

Replaces the prior pattern where `cell_85_live_line_snapshot.py` had to be run manually from a developer machine. That pattern caused a 3-month gap in 2024-06 → 2024-09 opening-line capture (silent failure, no monitoring) and ~3-week gap at the start of 2026 season. See `HARDWOOD_MASTER_2026_05_21_v51_*.md` for the incident analysis.
