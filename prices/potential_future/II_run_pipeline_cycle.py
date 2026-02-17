#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import subprocess
import sys

from prices.potential_future.common import Heartbeat, get_test_coin_ids, now_str, should_log_progress


def main() -> None:
    hb = Heartbeat("II_run_pipeline_cycle")
    base = pathlib.Path(__file__).resolve().parent
    scripts = [
        "AA_load_live_selected.py",
        "BB_refresh_live_derivatives.py",
        "CC_build_10m_intraday.py",
        "DD_build_hourly_and_finalize.py",
        "EE_build_daily_and_finalize.py",
        "EG_build_monthly_from_daily.py",
        "HH_write_market_caps.py",
    ]

    print(f"[{now_str()}] Starting prod_pipeline top2 cycle for {get_test_coin_ids()}")
    for idx, script in enumerate(scripts, 1):
        if should_log_progress(idx, len(scripts), default_every=1):
            print(f"[{now_str()}] step {idx}/{len(scripts)} -> {script}")
        hb.maybe(extra=f"step={idx}/{len(scripts)}")
        path = base / script
        subprocess.run([sys.executable, str(path)], check=True)
    print(f"[{now_str()}] Cycle complete.")


if __name__ == "__main__":
    main()
