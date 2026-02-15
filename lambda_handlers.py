# lambda_handlers.py
import importlib
import os
from datetime import datetime


def _log(msg):
    print(f"[{datetime.utcnow().isoformat()}Z] {msg}")


def _set_prod_legacy_table_defaults() -> None:
    # Route prod_pipeline scripts to legacy gecko_* tables unless explicitly overridden.
    os.environ.setdefault("PP_FORCE_TEST_MODE", "0")
    os.environ.setdefault("PP_TEST_MODE_UNTIL_UTC", "")
    os.environ.setdefault("PP_TABLE_LIVE", "gecko_prices_live")
    os.environ.setdefault("PP_TABLE_LIVE_RANKED", "gecko_prices_live_ranked")
    os.environ.setdefault("PP_TABLE_ROLLING", "gecko_prices_live_rolling")
    os.environ.setdefault("PP_TABLE_10M", "gecko_prices_10m_7d")
    os.environ.setdefault("PP_TABLE_HOURLY", "gecko_candles_hourly_30d")
    os.environ.setdefault("PP_TABLE_DAILY", "gecko_candles_daily_contin")
    os.environ.setdefault("PP_TABLE_MONTHLY", "gecko_candles_monthly")
    os.environ.setdefault("PP_TABLE_MCAP_LIVE", "gecko_market_cap_live")
    os.environ.setdefault("PP_TABLE_MCAP_10M", "gecko_market_cap_10m_7d")
    os.environ.setdefault("PP_TABLE_MCAP_HOURLY", "gecko_market_cap_hourly_30d")
    os.environ.setdefault("PP_TABLE_MCAP_DAILY", "gecko_market_cap_daily_contin")
    os.environ.setdefault("PP_TABLE_DAILY_RANGES", "coin_daily_coverage_ranges")
    os.environ.setdefault("PP_TABLE_DAILY_AVAIL", "coin_daily_availability")
    os.environ.setdefault("PP_TABLE_INTRADAY_COV", "coin_intraday_coverage")
    os.environ.setdefault("PP_TABLE_JOB_LOCKS", "job_locks")


def _run(mod_name: str, func_name: str = "main"):
    _log(f"importing {mod_name}.{func_name}()")
    mod = importlib.import_module(mod_name)
    fn = getattr(mod, func_name)
    _log(f"running {mod_name}.{func_name}()")
    return fn()


def realtime_handler(event, context):
    """Handler: runs AA then BB then CC (live -> ranked -> 10m)."""
    _log("realtime_handler start")
    os.environ.setdefault("ASTRA_BUNDLE_PATH", "/var/task/secure-connect.zip")
    _set_prod_legacy_table_defaults()
    _run("prices.prod_pipeline.AA_load_live_selected")
    _run("prices.prod_pipeline.BB_refresh_live_derivatives")
    _run("prices.prod_pipeline.CC_build_10m_intraday")
    _log("realtime_handler done")
    return {"ok": True}


def candles_handler(event, context):
    """Handler: runs DD then EE (hourly -> daily)."""
    _log("candles_handler start")
    os.environ.setdefault("ASTRA_BUNDLE_PATH", "/var/task/secure-connect.zip")
    _set_prod_legacy_table_defaults()
    _run("prices.prod_pipeline.DD_build_hourly_and_finalize")
    _run("prices.prod_pipeline.EE_build_daily_and_finalize")
    _log("candles_handler done")
    return {"ok": True}
