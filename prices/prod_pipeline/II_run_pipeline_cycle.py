#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import subprocess
import sys

from common import now_str, get_test_coin_ids


def main() -> None:
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
    for script in scripts:
        path = base / script
        print(f"[{now_str()}] -> running {script}")
        subprocess.run([sys.executable, str(path)], check=True)
    print(f"[{now_str()}] Cycle complete.")


if __name__ == "__main__":
    main()
