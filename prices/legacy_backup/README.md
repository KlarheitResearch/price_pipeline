# Legacy-Efficient Price Pipeline Mode

This folder is now the active runtime basis for the low-API-cost mode.
`backend/prices/prod_pipeline` remains unchanged for future paid-tier usage.

## Runtime behavior

- Live ingestion basis: `AA_gck_load_prices_live.py` (top 1000 via `/coins/markets` pages).
- 10m candles: `CC_gck_append_10m_from_live.py` (from live rolling table, no CoinGecko per-coin API).
- Hourly candles: `DD_gck_create_hourly_from_10m.py` (from 10m, no CoinGecko).
- Daily candles: `EE_gck_create_daily_from_10m.py` (from 10m, no CoinGecko).
- Monthly candles: `EG_gck_update_monthly_from_daily.py` (from daily/live, no CoinGecko).
- True daily close API enrichment: `EF_gck_close_daily_topn_api.py` for top 100 only (default), once daily.

## Defaults changed

- `TOP_N` default is now `1000` in:
  - `CC_gck_append_10m_from_live.py`
  - `DD_gck_create_hourly_from_10m.py`
  - `EE_gck_create_daily_from_10m.py`
  - `EG_gck_update_monthly_from_daily.py`

## Scheduled/triggered workflows

Active workflow set is intentionally small:

- `gecko_legacy_core.yml`: main legacy cycle (AA + CC every 10m, optional DD/EE/EG by dispatch input).
- `gecko_legacy_daily_api_close.yml`: top-100 true daily API close (`EF_gck_close_daily_topn_api.py`).
- `gecko_legacy_maintenance.yml`: availability refresh + 10m gap audit.
- `gecko_legacy_manual_repair.yml`: manual rank/time-range intraday repair.

Original prod-era workflow files are archived (not active) at:

- `backend/prices/prod_pipeline/workflows_archive/original_github/`

## Manual intraday repair

Use `GM_gck_manual_repair_intraday.py` for on-demand 10m/hourly repairs by rank range and UTC time window.

Local example:

```bash
cd backend
PYTHONPATH=. python prices/legacy_backup/GM_gck_manual_repair_intraday.py \
  --rank-start 1 \
  --rank-end 100 \
  --from-utc 2026-02-14T00:00:00Z \
  --to-utc 2026-02-15T00:00:00Z \
  --granularity both \
  --dry-run
```

GitHub Actions manual dispatch:

- Workflow: `gecko-legacy-manual-repair` (`backend/.github/workflows/gecko_legacy_manual_repair.yml`).
- Inputs: rank window, UTC range, granularity, overwrite mode, dry-run mode.

## Optional Cloudflare dispatch scripts

If you want cleaner scheduling (fewer skipped workflow runs), deploy these worker scripts instead of the prod dispatch set:

- `backend/prices/legacy_backup/cloudflare_workers/legacy-core-5m.js`
- `backend/prices/legacy_backup/cloudflare_workers/legacy-maintenance-daily.js`

## Required env/secrets

- `ASTRA_BUNDLE_BASE64`, `ASTRA_TOKEN`, `ASTRA_KEYSPACE`
- `COINGECKO_API_TIER`
- `COINGECKO_API_KEY` (or key ring via `COINGECKO_API_KEY_AA/BB/CC/DD` or `COINGECKO_API_KEYS`)
