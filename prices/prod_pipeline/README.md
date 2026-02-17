# Legacy-Efficient Price Pipeline Mode

This folder is now the active runtime basis for the low-API-cost mode.
The former paid-tier prod implementation has been moved to `backend/prices/potential_future`.

## Runtime behavior

- Live ingestion basis: `AA_gck_load_prices_live.py` (top 1000 via `/coins/markets` pages).
- 10m candles: `CC_gck_append_10m_from_live.py` (from live rolling table, no CoinGecko per-coin API).
- Hourly candles: `DD_gck_create_hourly_from_10m.py` (from 10m, no CoinGecko).
- Daily candles: `EE_gck_create_daily_from_10m.py` (from 10m, no CoinGecko).
- Monthly candles: `EG_gck_update_monthly_from_daily.py` (from daily/live, no CoinGecko).
- True daily close API enrichment: `EF_gck_close_daily_topn_api.py` (default rank 1-300), once daily.
- API-key handling for all CoinGecko callers is centralized in `cg_key_pool.py` (AA/BB/CC/DD rotation + cooldown/suspension).

## Defaults changed

- `TOP_N` default is now `1000` in:
  - `CC_gck_append_10m_from_live.py`
  - `DD_gck_create_hourly_from_10m.py`
  - `EE_gck_create_daily_from_10m.py`
  - `EG_gck_update_monthly_from_daily.py`

## Scheduled/triggered workflows

Active workflow set is intentionally small:

- `gecko_legacy_core.yml`: main 10m cycle for AA + CC + DD.
- `gecko_legacy_daily_partial.yml`: dedicated EE updates from 10m/live (frequent partial updates + nightly full finalize).
- `gecko_legacy_daily_api_close.yml`: true daily API close (`EF_gck_close_daily_topn_api.py`) with rank window, inclusive day range, and optional coin-id filter.
- `gecko_legacy_maintenance.yml`: availability refresh + 10m gap audit.
- `gecko_legacy_manual_repair.yml`: manual rank/time-range intraday repair.

All workflow script steps run through:

- `prices/prod_pipeline/pipeline_health_runner.py`

So each run writes `running/success/failed` to:

- `pp_pipeline_runs`
- `pp_pipeline_latest`

Legacy script IDs written to health tables:

- `AA_gck_load_prices_live`
- `CC_gck_append_10m_from_live`
- `DD_gck_create_hourly_from_10m`
- `EE_gck_create_daily_from_10m`
- `EG_gck_update_monthly_from_daily`
- `EF_gck_close_daily_topn_api`
- `FF_gck_coin_data_availability`
- `audit_10m_gaps`
- `GM_gck_manual_repair_intraday`

### Coverage and availability tables

Data availability monitoring remains based on:

- `coin_daily_coverage_ranges`
- `coin_daily_availability`
- `coin_intraday_coverage`

Population path:

- `FF_gck_coin_data_availability.py` refreshes all three tables (daily + intraday windows).
- `GG_gck_dq_repair_timeseries.py` consumes these tables for targeted repair guidance.

Recommended schedule:

- Run `gecko_legacy_maintenance.yml` daily after the API daily close workflow.
- Keep maintenance independent from 10m runtime cadence to avoid contention.

CC runtime tuning knobs (for timeout control):

- `COIN_WORKERS` (default `8`) for bounded per-coin planning concurrency.
- `WRITE_CONCURRENCY` (default `16`) for bounded async insert pipeline depth.
- `APPEND_SKIP_EXISTING=1` to skip already-filled slots early.
- `PROGRESS_EVERY` to keep logs light in non-verbose mode.

Original prod-era workflow files are archived (not active) at:

- `backend/prices/potential_future/workflows_archive/original_github/`

## Manual intraday repair

Use `GM_gck_manual_repair_intraday.py` for on-demand 10m/hourly repairs by rank range and UTC time window.
You can optionally narrow to explicit coin ids.

Local example:

```bash
cd backend
PYTHONPATH=. python prices/prod_pipeline/GM_gck_manual_repair_intraday.py \
  --rank-start 1 \
  --rank-end 100 \
  --coin-ids bitcoin,ethereum \
  --from-utc 2026-02-14T00:00:00Z \
  --to-utc 2026-02-15T00:00:00Z \
  --granularity both \
  --dry-run
```

GitHub Actions manual dispatch:

- Workflow: `gecko-legacy-manual-repair` (`backend/.github/workflows/gecko_legacy_manual_repair.yml`).
- Inputs: rank window, optional coin ids, UTC range, granularity, overwrite mode, dry-run mode.

## Optional Cloudflare dispatch scripts

If you want cleaner scheduling (fewer skipped workflow runs), deploy these worker scripts instead of the prod dispatch set:

- `backend/prices/prod_pipeline/cloudflare/legacy-core-5m.js`
- `backend/prices/prod_pipeline/cloudflare/legacy-maintenance-daily.js`

## Required env/secrets

- `ASTRA_BUNDLE_BASE64`, `ASTRA_TOKEN`, `ASTRA_KEYSPACE`
- `COINGECKO_API_TIER`
- `COINGECKO_API_KEY_AA`, `COINGECKO_API_KEY_BB`, `COINGECKO_API_KEY_CC`, `COINGECKO_API_KEY_DD`
- Optional fallback only: `COINGECKO_API_KEY` + `COINGECKO_ALLOW_GENERIC_KEY_FALLBACK=1`
