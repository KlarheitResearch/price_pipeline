# Prod Pipeline (`backend/prices/prod_pipeline`)

This folder is an isolated next-generation CoinGecko/Astra pipeline that runs in parallel with legacy `backend/prices/*` logic.

It uses:
- `pp_*` tables (no writes into `gecko_*` tables)
- `PP_*` env namespace
- tiered rank windows for scale (`1-200`, `201-600`, `601-1000`)

## Architecture

The pipeline separates **runtime ingestion/build** from **maintenance/repair**.

Runtime pipeline (scheduled):
1. `AA_load_live_selected.py`
2. `BB_refresh_live_derivatives.py`
3. `CC_build_10m_intraday.py`
4. `DD_build_hourly_and_finalize.py`
5. `EE_build_daily_and_finalize.py`
6. `EG_build_monthly_from_daily.py`
7. `HH_write_market_caps.py`

Maintenance pipeline (manual/nightly as needed):
- `90_audit_10m_gaps.py`
- `91_update_coin_data_availability.py`
- `92_repair_timeseries.py`
- `93_backfill_monthly_from_daily.py`
- `94_materialize_asset_categories.py`
- `95_recompute_market_caps_full.py`
- `96_bootstrap_new_entrants_1y.py`

`II_run_pipeline_cycle.py` runs runtime steps `AA..HH` in order.

## Table Schema

Create tables with:

```powershell
python backend/prices/prod_pipeline/bootstrap_tables.py
```

or run:

- `backend/prices/prod_pipeline/00_create_test_tables.cql`

Core runtime tables:
- `pp_prices_live`
- `pp_prices_live_ranked`
- `pp_prices_live_rolling`
- `pp_prices_10m_7d`
- `pp_candles_hourly_30d`
- `pp_candles_daily_contin`
- `pp_candles_monthly`
- `pp_market_cap_live`
- `pp_market_cap_10m_7d`
- `pp_market_cap_hourly_30d`
- `pp_market_cap_daily_contin`

Maintenance tables:
- `pp_asset_categories`
- `pp_coin_daily_coverage_ranges`
- `pp_coin_daily_availability`
- `pp_coin_intraday_coverage`
- `pp_job_locks`

## File-by-File Documentation

### `common.py`
Shared foundation for all scripts:
- Astra connection (`connect_astra`)
- CoinGecko API client with retry/backoff and key rotation (`COINGECKO_API_KEY_AA/BB`)
- scope resolution (`PP_RANK_START/PP_RANK_END` or `PP_TEST_COIN_IDS`, with optional timed test override via `PP_TEST_MODE_UNTIL_UTC` / `PP_FORCE_TEST_MODE`)
- category mapping loader from `backend/prices/category_mapping.csv`
- time helpers (UTC normalization, flooring, Cassandra timestamp conversion)

This is the central glue used by all runtime and maintenance files.

### `AA_load_live_selected.py`
Purpose:
- pull fresh live market data from `/coins/markets`
- write:
  - `pp_prices_live` (latest snapshot per coin)
  - `pp_prices_live_rolling` (time series append by `last_updated`)

How it works:
- rank-window mode: fetches enough 250-row pages to cover `PP_RANK_END`
- id mode: fetches only `PP_TEST_COIN_IDS`
- enriches `category` from CSV map (fallback `Other`)

Why it exists:
- establishes the canonical coin universe and freshest point source for 10m/hour/day/month.
- in production cadence, tier1 live snapshots run every 5m to reduce slot-edge delays before 10m candle materialization.

### `BB_refresh_live_derivatives.py`
Purpose:
- rebuild derivative live tables from `pp_prices_live`:
  - `pp_prices_live_ranked` (bucketed by rank)
  - `pp_market_cap_live` (category + ALL aggregates)

How it works:
- sorts by rank (with sentinel for invalid/missing rank)
- rewrites rank bucket atomically (delete bucket then insert top `PP_TOP_N`)
- truncates/recomputes `pp_market_cap_live`

Why it exists:
- keeps ranked search/live market-cap views consistent with the latest live snapshot.

### `CC_build_10m_intraday.py`
Purpose:
- build 10m candles into `pp_prices_10m_7d` from rolling live points.

How it works:
- processes last `PP_SLOTS_BACKFILL` slots
- uses live points inside slot for OHLC when present
- if slot is empty, attempts immediate API heal for recent slots (`PP_CC_HEAL_RECENT_SLOTS`, default `3`)
- if still empty, carries previous value (`carry_prev`)
- write quality guard prevents downgrading an existing stronger row (e.g. `repair_api_points`) with weaker data

Why it exists:
- gives fast intraday continuity and low-latency candle updates before API-finalized higher frames.

### `DD_build_hourly_and_finalize.py`
Purpose:
- keep current hour partial candle fresh
- finalize closed hour candles via CoinGecko API
- write `pp_candles_hourly_30d`

How it works:
- current hour: recomputed from 10m (`candle_source=10m_partial`)
- closed hours in lookback: provisional 10m then API final (`cg_hourly_final`)
- finalized-guard: once `cg_hourly_final` exists, closed row is never overwritten by provisional logic

Why it exists:
- guarantees both freshness (current hour) and stable correctness (closed hours).

### `EE_build_daily_and_finalize.py`
Purpose:
- keep current day partial candle fresh
- finalize closed day candles via CoinGecko API
- write `pp_candles_daily_contin`

How it works:
- current day: recomputed from 10m (`10m_partial`)
- closed days: provisional then API final (`cg_daily_final`)
- finalized-guard mirrors hourly behavior

Why it exists:
- avoids stale day candles while preserving immutability of finalized history.

### `EG_build_monthly_from_daily.py`
Purpose:
- build monthly candles into `pp_candles_monthly`.

How it works:
- closed months finalized from daily (`daily_final`)
- current month is partial + adjusted with latest live close (`daily_partial_live`)

Why it exists:
- monthly views stay near-real-time while closed months remain stable.

### `HH_write_market_caps.py`
Purpose:
- recompute category aggregates from coin-level tables:
  - `pp_market_cap_10m_7d`
  - `pp_market_cap_hourly_30d`
  - `pp_market_cap_daily_contin`

How it works:
- reads top `PP_TOP_N` ranked coins from `pp_prices_live`
- aggregates across selected rolling windows (`PP_MCAP_10M_SLOTS`, `PP_MCAP_HOURS`, `PP_MCAP_DAYS`)
- ranks categories by market cap (`ALL=0`)

Why it exists:
- ensures category charts are derived from final coin candles/points, not API category shortcuts.

### `II_run_pipeline_cycle.py`
Purpose:
- run full runtime pipeline in strict order.

Why it exists:
- convenient local/staging smoke cycle and deterministic sequence execution.

### `90_audit_10m_gaps.py`
Purpose:
- audit missing 10m coverage for scoped coins over `PP_AUDIT_WINDOW_DAYS`.

Outputs:
- missing day count
- missing slot count per coin
- optional slot-level samples (`PP_AUDIT_SHOW_SLOT_DETAILS=1`)

### `91_update_coin_data_availability.py`
Purpose:
- maintain coverage metadata:
  - `pp_coin_daily_coverage_ranges`
  - `pp_coin_daily_availability`
  - `pp_coin_intraday_coverage`

How it works:
- daily coverage window summary (`PP_AVAIL_DAILY_WINDOW_DAYS`)
- intraday bitmaps for 10m/hour windows (`PP_AVAIL_10M_DAYS`, `PP_AVAIL_HOURLY_DAYS`)

Why it exists:
- gives fast DQ/monitoring without scanning raw candle tables each time.

### `92_repair_timeseries.py`
Purpose:
- repair recent 10m slots from CoinGecko market-chart data.

How it works:
- scans missing slots in recent window (`PP_REPAIR_10M_HOURS`)
- optionally re-targets existing non-API rows in-window (`PP_REPAIR_REWRITE_NON_API=1`, default on)
- fills from in-slot API points when available
- optional interpolation/carry fallback
- downgrade guard skips overwriting stronger existing rows with weaker fallback output
- writes with repair `candle_source`
- optional follow-up finalize/recompute steps
- optional distributed lock via `pp_job_locks`

Why it exists:
- handles short outages/gaps quickly and upgrades temporary non-API rows toward API-backed 10m data.

### `93_backfill_monthly_from_daily.py`
Purpose:
- full monthly backfill/rebuild from daily data.

Why it exists:
- replacement for one-time historical monthly backfill jobs.

### `94_materialize_asset_categories.py`
Purpose:
- optional materialization of category mapping to `pp_asset_categories`.

Why it exists:
- replacement for legacy category loader when a dedicated categories table is needed.

### `95_recompute_market_caps_full.py`
Purpose:
- full rebuild equivalent of legacy market-cap recalculation jobs.

How it works:
- truncates `pp_market_cap_10m_7d`, `pp_market_cap_hourly_30d`, `pp_market_cap_daily_contin`
- computes wide windows from `PP_FULL_MCAP_*` settings
- delegates recompute to `HH_write_market_caps.py`

Why it exists:
- gives a deterministic full-refresh path after outages/migrations/backfills.

### `96_bootstrap_new_entrants_1y.py`
Purpose:
- bootstrap up to 1 year of daily OHLC for newly entered top-1000 coins that do not yet have deep daily history.

How it works:
- scans scoped live universe for coins whose earliest daily row is newer than target history window
- processes up to `PP_BOOTSTRAP_MAX_COINS` candidates per run
- fetches CoinGecko market-chart range once per candidate and writes missing day rows as `cg_daily_bootstrap`
- optional follow-up monthly rebuild via `PP_BOOTSTRAP_RUN_MONTHLY_BACKFILL=1`

Why it exists:
- gives better UX for new entrants without slowing the regular runtime/repair loops.

### `LEGACY_COVERAGE.md`
Mapping from legacy scripts to new equivalents and migration recommendations.

## Workflow Integration

Tier workflows:
- `backend/.github/workflows/gecko_prod_live_tier1_5m.yml`
- `backend/.github/workflows/gecko_prod_tier1_10m.yml`
- `backend/.github/workflows/gecko_prod_tier2_hourly.yml`
- `backend/.github/workflows/gecko_prod_tier3_4h.yml`
- `backend/.github/workflows/gecko_prod_repair_tier1_hourly.yml`
- `backend/.github/workflows/gecko_prod_repair_tier2_4h.yml`
- `backend/.github/workflows/gecko_prod_repair_tier3_daily.yml`
- `backend/.github/workflows/gecko_prod_bootstrap_new_entrants_1y.yml`

Cadence:
- tier1 live snapshot (`AA`, rank `1..200`) every 5m
- tier1 build/finalize (`BB+CC+DD+EE`, rank `1..200`) every 10m (`PP_SLOTS_BACKFILL=4`)
- tier2 rank `201..600` hourly
- tier3 rank `601..1000` every 4h
- repair tier1 rank `1..200` hourly (`PP_REPAIR_10M_HOURS=6`)
- repair tier2 rank `201..600` every 4h (`PP_REPAIR_10M_HOURS=12`)
- repair tier3 rank `601..1000` daily (`PP_REPAIR_10M_HOURS=24`)
- entrant bootstrap rank `1..1000` daily (`PP_BOOTSTRAP_DAYS=365`, capped by `PP_BOOTSTRAP_MAX_COINS`)

Temporary parallel-run scope override:
- if `PP_TEST_MODE_UNTIL_UTC` is set and current UTC is before that instant, rank windows are ignored and all workflows run only `PP_TEST_COIN_IDS`.
- when cutoff is reached, the same workflows auto-return to tier rank windows (no code/workflow edits required).
- optional hard override: `PP_FORCE_TEST_MODE=1`.

All call lettered runtime entrypoints so execution order is explicit in logs and config.

Cloudflare-triggered production mode:
- keep workflow `schedule:` blocks as hot-standby only
- set repo variable `ENABLE_GH_FALLBACK_SCHEDULE=0` (default) to prevent duplicate scheduled runs
- if Cloudflare triggering is unavailable, set `ENABLE_GH_FALLBACK_SCHEDULE=1` to re-enable GitHub scheduled execution without code changes

## API Key Strategy

CoinGecko keys are loaded in this order:
- `COINGECKO_API_KEYS` (comma-separated list, if set)
- `COINGECKO_API_KEY_AA`
- `COINGECKO_API_KEY_BB`
- `COINGECKO_API_KEY_CC`
- `COINGECKO_API_KEY`
- optional exclusion list: `COINGECKO_DISABLED_KEYS` (comma-separated raw keys or env var names like `COINGECKO_API_KEY_CC`)

Request-key selection behavior:
- requests with a `hint` use stable hash routing (`crc32(hint) % key_count`)
- requests without a `hint` use round-robin
- retries for hinted requests hop to the next key (`base_index + retry_attempt`)
- per-key throttling enforces both `CG_REQUEST_INTERVAL_S` and `CG_MAX_RPM_PER_KEY`
- 429 rate-limit responses temporarily suspend that key (`CG_RATE_LIMIT_COOLDOWN_S`, default 75s)
- credit/quota exhaustion responses suspend that key longer (`CG_CREDIT_EXHAUSTED_COOLDOWN_S`, default 12h)
- 401 auth failures suspend that key (`CG_AUTH_FAILURE_COOLDOWN_S`, default 6h)

## Data Consistency Rules

1. Current partial candles (hour/day/month) are continuously refreshed.
2. Closed hour/day candles become immutable after API finalization (`cg_hourly_final`, `cg_daily_final`).
3. 10m can be quickly repaired (`92`) without full backfills.
4. Category aggregates are recomputed from coin-level tables, not guessed.

## Verbose Runtime Logging

All prod scripts now support shared verbose progress + heartbeat logging.

- `VERBOSE_PRINTS=true` enables high-frequency progress logs and heartbeats.
- `PP_HEARTBEAT_SEC` controls heartbeat cadence (default `20` seconds).
- `PP_PROGRESS_EVERY` controls verbose progress frequency (default every `10` items).
- `PP_ASTRA_MAX_IN_FLIGHT` caps concurrent async Astra writes in heavy writers like `AA` and `BB` (default `64`).
- `.env` is loaded at module import; make sure `.env` changes are saved before launching a script.

## Typical Commands

Top2 smoke cycle:

```powershell
$env:PP_TEST_COIN_IDS='bitcoin,ethereum'
python backend/prices/prod_pipeline/II_run_pipeline_cycle.py
```

Scoped tier test:

```powershell
$env:PP_RANK_START='201'
$env:PP_RANK_END='600'
$env:PP_SLOTS_BACKFILL='6'
$env:PP_CC_HEAL_RECENT_SLOTS='3'
python backend/prices/prod_pipeline/AA_load_live_selected.py
python backend/prices/prod_pipeline/BB_refresh_live_derivatives.py
python backend/prices/prod_pipeline/CC_build_10m_intraday.py
python backend/prices/prod_pipeline/DD_build_hourly_and_finalize.py
python backend/prices/prod_pipeline/EE_build_daily_and_finalize.py
python backend/prices/prod_pipeline/EG_build_monthly_from_daily.py
python backend/prices/prod_pipeline/HH_write_market_caps.py
```

Repair + follow-up:

```powershell
$env:PP_RANK_START='1'
$env:PP_RANK_END='200'
$env:PP_REPAIR_10M_HOURS='24'
$env:PP_REPAIR_RUN_HOURLY='1'
$env:PP_REPAIR_RUN_DAILY='1'
$env:PP_REPAIR_RUN_MCAP='1'
$env:PP_REPAIR_REWRITE_NON_API='1'
python backend/prices/prod_pipeline/92_repair_timeseries.py
```

Entrant bootstrap:

```powershell
$env:PP_RANK_START='1'
$env:PP_RANK_END='1000'
$env:PP_BOOTSTRAP_DAYS='365'
$env:PP_BOOTSTRAP_MAX_COINS='20'
python backend/prices/prod_pipeline/96_bootstrap_new_entrants_1y.py
```

## Shadow Run Guidance

1. Keep legacy `gecko_*` workflows running.
2. Run `gecko_prod_tier*` workflows in parallel.
3. Compare outputs and API responses against `pp_*` tables.
4. Switch frontend env table names to `pp_*` in staging.
5. Cut over production reads only after parity is stable.
