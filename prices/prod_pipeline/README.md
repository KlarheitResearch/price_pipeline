# Prod Pipeline (`backend/prices/prod_pipeline`)

## Current Production Mode

This pipeline now runs against legacy production price/cap tables (`gecko_*`) via workflow env overrides.

Important:
- Script defaults in `common.py` are still `pp_*` for local/dev fallback.
- GitHub prod workflows explicitly set `PP_TABLE_*` to legacy tables.
- Coverage/repair support tables are also mapped to legacy names in workflows:
  - `coin_daily_coverage_ranges`
  - `coin_daily_availability`
  - `coin_intraday_coverage`
  - `job_locks`
- Pipeline health tables are separate and default to:
  - `pp_pipeline_runs`
  - `pp_pipeline_latest`

## Runtime Architecture

Runtime chain:
1. `AA_load_live_selected.py`
2. `BB_refresh_live_derivatives.py`
3. `CC_build_10m_intraday.py`
4. `DD_build_hourly_and_finalize.py`
5. `EE_build_daily_and_finalize.py`
6. `EG_build_monthly_from_daily.py`
7. `HH_write_market_caps.py`

Maintenance chain:
- `90_audit_10m_gaps.py`
- `91_update_coin_data_availability.py`
- `92_repair_timeseries.py`
- `93_backfill_monthly_from_daily.py`
- `94_materialize_asset_categories.py`
- `95_recompute_market_caps_full.py`
- `96_bootstrap_new_entrants_1y.py`

`II_run_pipeline_cycle.py` runs `AA..HH` in order.

## Active Production Table Targets

These are the table mappings set by all `gecko_prod_*` workflows.

Price/Candle/Cap (legacy):
- `gecko_prices_live`
- `gecko_prices_live_ranked`
- `gecko_prices_live_rolling`
- `gecko_prices_10m_7d`
- `gecko_candles_hourly_30d`
- `gecko_candles_daily_contin`
- `gecko_candles_monthly`
- `gecko_market_cap_live`
- `gecko_market_cap_10m_7d`
- `gecko_market_cap_hourly_30d`
- `gecko_market_cap_daily_contin`

Coverage/Locks (legacy):
- `coin_daily_coverage_ranges`
- `coin_daily_availability`
- `coin_intraday_coverage`
- `job_locks`

Pipeline health:
- `pp_pipeline_runs`
- `pp_pipeline_latest`

## One-Time / Schema Notes

If health tables are missing, create only these two:
- `pp_pipeline_runs`
- `pp_pipeline_latest`

Do not run full `bootstrap_tables.py` in production if you do not want the full `pp_*` test schema recreated.

Minimal CQL:

```sql
CREATE TABLE IF NOT EXISTS pp_pipeline_runs (
    script text,
    started_at timestamp,
    run_id text,
    workflow text,
    trigger_source text,
    scope text,
    rank_start int,
    rank_end int,
    status text,
    ended_at timestamp,
    duration_sec int,
    metrics_json text,
    error text,
    host text,
    updated_at timestamp,
    PRIMARY KEY ((script), started_at, run_id)
) WITH CLUSTERING ORDER BY (started_at DESC, run_id DESC);

CREATE TABLE IF NOT EXISTS pp_pipeline_latest (
    script text PRIMARY KEY,
    run_id text,
    workflow text,
    trigger_source text,
    scope text,
    rank_start int,
    rank_end int,
    status text,
    started_at timestamp,
    ended_at timestamp,
    duration_sec int,
    metrics_json text,
    error text,
    host text,
    updated_at timestamp
);
```

Schema docs now include health tables in:
- `backend/docs/db/schemas/default_keyspace.cql`

## Workflow and Trigger Model

Prod workflows (manual + Cloudflare-dispatched):
- `backend/.github/workflows/gecko_prod_live_tier1_5m.yml`
- `backend/.github/workflows/gecko_prod_tier1_10m.yml`
- `backend/.github/workflows/gecko_prod_tier2_hourly.yml`
- `backend/.github/workflows/gecko_prod_tier3_4h.yml`
- `backend/.github/workflows/gecko_prod_repair_tier1_hourly.yml`
- `backend/.github/workflows/gecko_prod_repair_tier2_4h.yml`
- `backend/.github/workflows/gecko_prod_repair_tier3_daily.yml`
- `backend/.github/workflows/gecko_prod_bootstrap_new_entrants_1y.yml`

Current trigger strategy:
- GitHub `schedule:` blocks are commented out in prod workflow files.
- Cloudflare Workers dispatch `workflow_dispatch` events.
- Fallback guard remains in workflows:
  - `if: github.event_name != 'schedule' || vars.ENABLE_GH_FALLBACK_SCHEDULE == '1'`
- If you temporarily re-enable GitHub cron blocks, keep `ENABLE_GH_FALLBACK_SCHEDULE=0` to avoid duplicate schedule runs.

Cloudflare setup is documented here:
- `backend/prices/prod_pipeline/CLOUDFLARE_WORKERS.md`

Cloudflare dispatch matrix (current):
- `prod-core-5m` (`*/5 * * * *`)
  - always: `gecko_prod_live_tier1_5m.yml` (`run_live=true`)
  - every 10m: `gecko_prod_tier1_10m.yml` with:
    - `run_live=false`
    - `run_10m=true`
    - `run_derivatives=true`
    - `run_hourly=false`
    - `run_daily=false`
    - `run_monthly=false`
  - hourly at `:05`: `gecko_prod_tier2_hourly.yml` with:
    - `run_live=true`
    - `run_10m=true`
    - `run_mcap=false`
    - `run_derivatives=true`
    - `run_hourly=true`
    - `run_daily=true`
    - `run_monthly=false`
  - daily at `00:50 UTC`: `gecko_prod_tier2_hourly.yml` mcap-only:
    - `run_live=false`, `run_10m=false`, `run_mcap=true`,
      `run_derivatives=false`, `run_hourly=false`, `run_daily=false`, `run_monthly=false`
  - every 4h at `:15`: `gecko_prod_tier3_4h.yml`
- `prod-repair-hourly` (`37 * * * *`): repair tier1 every hour; repair tier2 every 4h
- `prod-repair-daily` (`55 2 * * *`): repair tier3 daily
- `prod-bootstrap-daily` (`23 3 * * *`): bootstrap entrants daily

Note:
- `5m` and `10m` workflows run at the same wall-clock minute on `:00/:10/:20/...` by design.
- This is expected; the `10m` dispatch above skips `run_live`, so overlap is limited and intentional.

## Data Consistency Behavior

### Live projection across open candles
`AA_load_live_selected.py` can project live values into open candles:
- `PP_LIVE_PROJECT_PARTIALS=1`
- `PP_LIVE_PROJECT_10M=1`
- `PP_LIVE_PROJECT_HOURLY=1`
- `PP_LIVE_PROJECT_DAILY=1`

This keeps "now" aligned across 10m/hour/day until slot close/finalization.

### Duplicate rank hygiene (`gecko_prices_live`)
`BB_refresh_live_derivatives.py` now enforces unique positive ranks in `TABLE_LIVE` by default:
- `PP_ENFORCE_UNIQUE_LIVE_RANKS=1` (default)
- `PP_DUPLICATE_RANK_ACTION=demote|delete` (default `demote`)
- `PP_PRUNE_UNRANKED_STALE_HOURS=72` (default)
- if multiple coins share the same positive rank, the freshest row is kept on that rank
- loser rows are demoted to `market_cap_rank = null` (or deleted if action is `delete`)
- unranked rows are excluded from `gecko_prices_live_ranked` rebuilds

### 10m build + immediate heal
`CC_build_10m_intraday.py`:
- builds recent slots from rolling live points
- attempts immediate API heal for recent missing slots (`PP_CC_HEAL_RECENT_SLOTS`, default `3`)
- supports interpolation fallback
- prevents write downgrades from strong API-backed rows to weaker carry/interpolated rows

### Hourly/daily finalization
`DD` and `EE`:
- keep current bucket partial from lower granularity
- finalize closed buckets via CoinGecko
- finalized rows are protected from provisional overwrite

### Repair path
`92_repair_timeseries.py`:
- repairs missing/rewrite-eligible recent 10m slots
- supports lock-based mutual exclusion via `job_locks`
- optional follow-up hourly/daily/monthly/mcap steps

## CoinGecko Key Rotation and Rate/Credit Handling

Keys are loaded in this order:
- `COINGECKO_API_KEYS` (CSV)
- `COINGECKO_API_KEY_AA`
- `COINGECKO_API_KEY_BB`
- `COINGECKO_API_KEY_CC`
- `COINGECKO_API_KEY_DD`
- `COINGECKO_API_KEY`
- additional `COINGECKO_API_KEY_*` vars (sorted)

Selection and recovery behavior:
- hash-based sticky routing for hinted requests, round-robin otherwise
- retries hop keys
- per-key pacing and RPM guard
- temporary 429 suspension with cooldown
- credit-exhaustion detection and key suspension for the current run/process
- optional month-end suspension remains available via `CG_CREDIT_EXHAUSTED_UNTIL_MONTH_END=1`
  (default is `0`, so key rotation between runs works as expected)
- auth-failure suspension
- when all keys are temporarily rate-limited, client can wait and retry instead of hard-failing (`CG_WAIT_ON_ALL_KEYS_SUSPENDED=1`)

## Verbose Logging

All prod scripts share verbose controls:
- `VERBOSE_PRINTS=true`
- `PP_HEARTBEAT_SEC` (default `20`)
- `PP_PROGRESS_EVERY` (default `10`)
- `PP_ASTRA_MAX_IN_FLIGHT` (default `64`, heavy async writers)

## Pipeline Health Tracking

Every active prod script writes run telemetry to Astra:
- `pp_pipeline_runs` (history)
- `pp_pipeline_latest` (latest per script)

Health status lifecycle:
- `running` on start
- `success` / `noop` / `failed` on completion

Captured fields include:
- script, run_id, workflow, trigger source
- scope and rank window
- duration, metrics JSON, error text, host

Fail-open behavior:
- if health writes fail (table missing/misconfig), script continues and disables health writes for that process.

Quick check:

```powershell
python backend/prices/prod_pipeline/health_check.py
```

Optional:
- `PP_TABLE_PIPELINE_RUNS`, `PP_TABLE_PIPELINE_LATEST`
- `PP_HEALTH_STALE_MINUTES`
- `PP_HEALTH_ERROR_PREVIEW`

## Typical Manual Commands

Tier1 runtime chain:

```powershell
$env:PP_RANK_START='1'
$env:PP_RANK_END='200'
python backend/prices/prod_pipeline/AA_load_live_selected.py
python backend/prices/prod_pipeline/BB_refresh_live_derivatives.py
python backend/prices/prod_pipeline/CC_build_10m_intraday.py
python backend/prices/prod_pipeline/DD_build_hourly_and_finalize.py
python backend/prices/prod_pipeline/EE_build_daily_and_finalize.py
python backend/prices/prod_pipeline/EG_build_monthly_from_daily.py
python backend/prices/prod_pipeline/HH_write_market_caps.py
```

Repair run example:

```powershell
$env:PP_RANK_START='201'
$env:PP_RANK_END='600'
$env:PP_REPAIR_10M_HOURS='12'
$env:PP_REPAIR_REWRITE_NON_API='1'
$env:PP_REPAIR_INTERPOLATE='1'
python backend/prices/prod_pipeline/92_repair_timeseries.py
```

Entrant bootstrap example:

```powershell
$env:PP_RANK_START='1'
$env:PP_RANK_END='1000'
$env:PP_BOOTSTRAP_DAYS='365'
$env:PP_BOOTSTRAP_MAX_COINS='20'
python backend/prices/prod_pipeline/96_bootstrap_new_entrants_1y.py
```
