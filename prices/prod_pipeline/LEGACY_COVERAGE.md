# Legacy Coverage Matrix

This maps legacy `backend/prices/*` scripts to the new `backend/prices/prod_pipeline/*` pipeline.

## Runtime Pipeline

| Legacy | New | Status | Recommendation |
|---|---|---|---|
| `AA_gck_load_prices_live.py` | `AA_load_live_selected.py` + `BB_refresh_live_derivatives.py` | Covered | Use new pipeline for live/rolling/ranked/mcap-live in shadow run. |
| `CC_gck_append_10m_from_live.py` | `CC_build_10m_intraday.py` | Covered (core) | Use new pipeline. Keep legacy gapfill tooling only as fallback. |
| `DD_gck_create_hourly_from_10m.py` | `DD_build_hourly_and_finalize.py` | Covered (improved finalize) | Use new pipeline. |
| `EE_gck_create_daily_from_10m.py` | `EE_build_daily_and_finalize.py` | Covered (improved finalize) | Use new pipeline. |
| `EG_gck_update_monthly_from_daily.py` | `EG_build_monthly_from_daily.py` | Covered | Use new pipeline. |
| `HH_recalculate_mcaps.py` | `HH_write_market_caps.py` + `95_recompute_market_caps_full.py` | Covered | Use `HH` for scheduled rolling recompute, `95` for full rebuilds. |

## Data Inputs / Mapping

| Legacy | New | Status | Recommendation |
|---|---|---|---|
| `category_mapping.csv` | `common.py` category loader (`PP_CATEGORY_FILE`) | Covered | Keep using CSV; no change needed. |
| `load_categories.py` | `94_materialize_asset_categories.py` | Covered (optional) | Use when you want a dedicated categories table (`pp_asset_categories`). |

## DQ / Repair / Audit

| Legacy | New | Status | Recommendation |
|---|---|---|---|
| `audit_10m_gaps.py` | `90_audit_10m_gaps.py` | Covered | Use new script for scoped top-N/rank-window 10m gap audits. |
| `FF_gck_coin_data_availability.py` | `91_update_coin_data_availability.py` | Covered | Use new script to maintain `pp_coin_*` coverage tables. |
| `GG_gck_dq_repair_timeseries.py` | `92_repair_timeseries.py` | Covered (streamlined) | Use new script for recent-window 10m repair + optional finalize follow-ups. |
| `dq_config.py`, `dq_cassandra.py`, `dq_utils.py`, `dq_aggregates.py` | Built into `90/91/92` scripts | Covered (consolidated) | Keep legacy DQ stack only as temporary fallback if needed. |
| `ZZ_gck_backfill_monthly_from_daily.py` | `93_backfill_monthly_from_daily.py` | Covered | Use new script for one-time or periodic full monthly backfills from daily. |

## Practical Migration Guidance

1. Use new lettered runtime pipeline (`AA..HH`) for normal scheduled processing.
2. Use new maintenance scripts (`90..96`) for audit/coverage/repair/backfill/bootstrap/category materialization.
3. Keep legacy DQ/repair scripts active at lower frequency only during early shadow run.
4. Retire legacy runtime scripts only after shadow-run parity is stable.
